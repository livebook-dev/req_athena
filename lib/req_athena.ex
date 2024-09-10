defmodule ReqAthena do
  @moduledoc """
  `Req` plugin for [AWS Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

  ReqAthena makes it easy to make Athena queries and save the results into S3 buckets.

  By default, `ReqAthena` will query results and use the default output format,
  which is CSV. To change that, you can use the `:format` option documented bellow.
  """
  require Logger

  alias Req.Request

  @allowed_options ~w(
    access_key_id
    secret_access_key
    token
    workgroup
    region
    database
    athena
    output_location
    cache_query
    format
    decode_body
    output_compression
  )a

  @credential_keys ~w(access_key_id secret_access_key region token)a

  defguardp is_empty(value) when value in [nil, ""]

  @doc """
  Attaches to Req request.

  ## Request Options

    * `:access_key_id` - Required. The Access Key ID from AWS credentials.

    * `:secret_access_key` - Required. The Secret Access Key from AWS credentials.

    * `:token` - Optional. The Session Token from AWS credentials.

    * `:region` - Required. The AWS region where AWS Athena is installed.

    * `:database` - Required. The AWS Athena database name.

    * `:output_location` - Conditional. The S3 URL location to output AWS Athena query results.
      Results will be saved as Parquet and loaded with Explorer only if this option is given.

    * `:workgroup` - Conditional. The AWS Athena workgroup.

    * `:cache_query` - Optional. Forces a non-cached result from AWS Athena.

    * `:format` - Optional. It changes the output format. By default this is
      `:none`, which means that we return the decoded result from the Athena API.
      The supported formats are: `:csv`, `:explorer,`, and `:json`.

      For `:csv`, the contents of the CSV file are the output instead of the API return.
      When `:json` is used, the contents of the JSON files are going to be the output.
      Notice that the body is decoded by default and to prevent that, you need to use
      the `:decode_body` option, so you get the "raw" data.
      The `:explorer` format will perform the query unloading it to Parquet files, and
      then will lazy load these parquet files into an Explorer dataframe.

      There are some limitations when using the `:json` and `:explorer` format.
      First, you need to install Explorer in order to use the `:explorer` format.
      Second, when using these format, you always need to provide a different output location.
      See the [`UNLOAD` command docs](https://docs.aws.amazon.com/athena/latest/ug/unload.html#unload-considerations-and-limitations)
      for more details.

    * `:output_compression` - Optional. Sets the Parquet compression format and level
      for the output when using the Explorer output format. This can be a string, like `"gzip"`,
      or a tuple with `{format, level}`, like: `{"ZSTD", 4}`. By default this is `nil`,
      which means that for Parquet (the format that Explorer uses) this is going to be `"gzip"`.

    * `:athena` - Required. The query to execute. It can be a plain SQL string or
      a `{query, params}` tuple, where `query` can contain `?` placeholders and `params`
      is a list of corresponding values.

      There is a limitation of Athena that requires the `:output_location` to be present
      for every query that outputs to a format other than "CSV". So we append "results"
      to the `:output_location` to make the partition files be saved there.

  Conditional fields must always be defined, and can be one of the fields or both.

  If you want to set any of these options when attaching the plugin, pass them as the second argument.

  ## Examples

  With plain query string:

      iex> opts = [
      ...>   access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      ...>   secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      ...>   region: System.fetch_env!("AWS_REGION"),
      ...>   database: "default",
      ...>   output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ...> ]
      iex> query = "SELECT id, type, tags, members, timestamp, visible FROM planet WHERE id = 470454 and type = 'relation'"
      iex> req = Req.new() |> ReqAthena.attach(opts)
      iex> Req.post!(req, athena: query, format: :json).body
      %{
        "id" => 470454,
        "members" => [
          %{"ref" => 670007839, "role" => "", "type" => "node"},
          %{"ref" => 670007840, "role" => "", "type" => "node"}
        ],
        "tags" => %{
          "name" => "Mérignac A",
          "network" => "NTF-5",
          "ref" => "17229A",
          "site" => "geodesic",
          "source" => "©IGN 2010 dans le cadre de la cartographie réglementaire",
          "type" => "site",
          "url" => "http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A"
        },
        "timestamp" => "2017-01-21 12:51:34",
        "type" => "relation",
        "visible" => true
      }

  With parameterized query:

      iex> opts = [
      ...>   access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      ...>   secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      ...>   region: System.fetch_env!("AWS_REGION"),
      ...>   database: "default",
      ...>   output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ...> ]
      iex> query = "SELECT id, type FROM planet WHERE id = ? and type = ?"
      iex> req = Req.new() |> ReqAthena.attach(opts)
      iex> Req.post!(req, athena: {query, [239_970_142, "node"]}, format: :json).body
      [%{"id" => 239970142, "type" => "node"}]

  """
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(athena_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
    |> maybe_put_aws_credentials()
  end

  defp run(%Request{private: %{athena_action: _}} = request), do: request

  defp run(request) do
    if query = request.options[:athena] do
      region = Request.fetch_option!(request, :region)

      url = "https://athena.#{region}.amazonaws.com"
      cache_query = Request.get_option(request, :cache_query, true)

      %{request | url: URI.parse(url)}
      |> put_request_body(query, cache_query)
      |> sign_request("StartQueryExecution")
      |> Request.append_response_steps(athena_result: &handle_athena_result/1)
    else
      request
    end
  end

  defp put_request_body(request, query, cache_query) when is_binary(query) do
    put_request_body(request, %ReqAthena.Query{query: query}, cache_query)
  end

  defp put_request_body(request, {query, []}, cache_query) do
    put_request_body(request, %ReqAthena.Query{query: query}, cache_query)
  end

  defp put_request_body(request, {query, params}, cache_query) do
    hash =
      if cache_query do
        query |> :erlang.md5() |> Base.encode16()
      else
        :os.system_time() |> to_string()
      end

    query = %ReqAthena.Query{query: query, params: params, statement_name: "query_" <> hash}

    put_request_body(request, query, cache_query)
  end

  defp put_request_body(request, %ReqAthena.Query{} = query, cache_query) do
    output_config =
      case {request.options[:output_location], request.options[:workgroup]} do
        {output, workgroup} when is_empty(output) and is_empty(workgroup) ->
          raise ArgumentError, "options must have :workgroup, :output_location or both defined"

        {output, workgroup} when is_empty(output) ->
          %{WorkGroup: workgroup}

        {output, workgroup} when is_empty(workgroup) ->
          %{ResultConfiguration: %{OutputLocation: output}}

        {output, workgroup} ->
          %{WorkGroup: workgroup, ResultConfiguration: %{OutputLocation: output}}
      end

    output_format = Request.get_option(request, :format, :none)

    query =
      if output_format not in [:csv, :none] and is_binary(request.options[:output_location]) do
        format_str =
          case output_format do
            :explorer -> "PARQUET"
            :json -> "JSON"
            other -> raise ArgumentError, ":format - not supported #{inspect(other)}"
          end

        unload_opts = [
          format: format_str,
          # We need to add this "subdirectory" because Athena expects the results directory
          # to be empty for the "UNLOAD" command.
          to: Path.join(request.options[:output_location], "results")
        ]

        ReqAthena.Query.with_unload(
          query,
          unload_opts
        )
      else
        if output_format in [:explorer, :json] do
          raise ArgumentError,
                ":output_location needs to be defined in order to use the #{inspect(output_format)} format"
        end

        query
      end

    body =
      Map.merge(output_config, %{
        QueryExecutionContext: %{Database: Request.fetch_option!(request, :database)},
        QueryString: ReqAthena.Query.to_query_string(query)
      })

    client_request_token = generate_client_request_token(body, cache_query)
    body = Map.put(body, :ClientRequestToken, client_request_token)

    Request.put_private(%{request | body: Jason.encode!(body)}, :athena_query, query)
  end

  defp generate_client_request_token(parameters, cache_query) do
    parameters =
      if cache_query do
        parameters
      else
        [parameters, :os.system_time()]
      end

    parameters
    |> :erlang.term_to_binary()
    |> :erlang.md5()
    |> Base.encode16()
  end

  defp handle_athena_result({request, %{status: 200} = response}) do
    action = Request.get_private(request, :athena_action)
    query = Request.get_private(request, :athena_query)

    case {action, ReqAthena.Query.to_prepare?(query)} do
      {"StartQueryExecution", _} ->
        get_query_state(request, response)

      {"GetQueryExecution", _} ->
        wait_query_execution(request, response)

      {"GetQueryResults", true} ->
        execute_prepared_query(request)

      {"GetQueryResults", _} ->
        output_format = Request.get_option(request, :format, :none)

        case output_format do
          :none ->
            response =
              if Request.get_option(request, :decode_body, true) do
                %{response | body: Jason.decode!(response.body)}
              else
                response
              end

            Request.halt(request, response)

          :explorer ->
            get_explorer_result(request, response)

          :csv ->
            get_csv_result(request, response)

          :json ->
            get_json_result(request, response)

          other ->
            raise ArgumentError,
                  ":format - `#{inspect(other)}` is not valid. Only :none, :csv, :json or :explorer are accepted."
        end
    end
  end

  defp handle_athena_result(request_response), do: request_response

  defp get_csv_result(request, response) do
    csv_location = Request.get_private(request, :athena_output_location)

    result =
      if Req.Request.get_option(request, :decode_body, true) do
        aws_credentials = aws_credentials_from_request(request)
        req_s3 = ReqAthena.S3.new(aws_credentials)
        ReqAthena.S3.get_body(req_s3, csv_location)
      else
        csv_location
      end

    Request.halt(request, %{response | body: result})
  end

  defp get_json_result(request, response) do
    output_location = Request.get_private(request, :athena_output_location)

    aws_credentials = aws_credentials_from_request(request)
    req_s3 = ReqAthena.S3.new(aws_credentials)

    locations = ReqAthena.S3.get_locations(req_s3, output_location)

    # OPTIMIZE: use tasks to retrieve locations.
    results =
      if Req.Request.get_option(request, :decode_body, true) do
        Enum.flat_map(locations, fn location ->
          contents = ReqAthena.S3.get_body(req_s3, location)

          for line <- String.split(contents, "\n"), line != "", do: Jason.decode!(line)
        end)
      else
        locations
      end

    Request.halt(request, %{response | body: results})
  end

  defp get_explorer_result(request, response) do
    output_location = Request.get_private(request, :athena_output_location)

    aws_credentials = aws_credentials_from_request(request)

    # This private field is only meant to be used in tests.
    fetcher_and_builder =
      Request.get_private(request, :athena_dataframe_builder, &fetch_and_build_dataframe/3)

    decode_body = Req.Request.get_option(request, :decode_body, true)

    result = fetcher_and_builder.(output_location, aws_credentials, decode_body)

    Request.halt(request, %{response | body: result})
  end

  defp aws_credentials_from_request(request) do
    for key <- @credential_keys,
        value = request.options[key],
        not is_nil(value),
        do: {key, value}
  end

  @doc false
  def fetch_and_build_dataframe(output_location, aws_credentials, decode_body) do
    req_s3 = ReqAthena.S3.new(aws_credentials)
    locations = ReqAthena.S3.get_locations(req_s3, output_location)

    if decode_body do
      build_lazy_frame(locations, aws_credentials)
    else
      locations
    end
  end

  if Code.ensure_loaded?(Explorer) do
    defp build_lazy_frame(parquet_locations, aws_credentials) do
      parquet_locations
      |> Enum.map(fn parquet_location ->
        Explorer.DataFrame.from_parquet!(parquet_location, lazy: true, config: aws_credentials)
      end)
      |> Explorer.DataFrame.concat_rows()
    end
  else
    defp build_lazy_frame(parquet_locations, aws_credentials) do
      raise ArgumentError,
            "format: :explorer - you need to install Explorer as a dependency in order to use this format"
    end
  end

  defp get_query_state(request, response) do
    response =
      %{request | body: response.body}
      |> sign_request("GetQueryExecution")
      |> Req.post!()

    Request.halt(request, response)
  end

  @wait_delay 1000

  defp wait_query_execution(request, response) do
    %{"QueryExecutionId" => query_execution_id} = Jason.decode!(request.body)
    body = Jason.decode!(response.body)
    query_status = body["QueryExecution"]["Status"]

    case query_status["State"] do
      "QUEUED" ->
        count = Request.get_private(request, :athena_wait_count, 1)

        if count >= 3 do
          Logger.info("ReqAthena: query is in QUEUED state, will retry in #{@wait_delay}ms")
        end

        request = Request.put_private(request, :athena_wait_count, count + 1)
        Process.sleep(@wait_delay)
        Request.halt(request, Req.post!(request))

      "RUNNING" ->
        Process.sleep(@wait_delay)
        Request.halt(request, Req.post!(request))

      "SUCCEEDED" ->
        request =
          request
          |> sign_request("GetQueryResults")
          |> Request.put_private(
            :athena_output_location,
            body["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
          )
          |> Request.put_private(:athena_query_execution_id, query_execution_id)

        Request.halt(request, Req.post!(request))

      "FAILED" ->
        if request.options[:http_errors] == :raise do
          raise RuntimeError,
                "failed query with error: " <> query_status["AthenaError"]["ErrorMessage"]
        else
          Request.halt(request, %{response | body: body})
        end

      other_state ->
        Logger.warning("ReqAthena: query returned an unknown state -> #{other_state}")

        if Request.get_option(request, :decode_body, true) do
          Request.halt(request, %{response | body: body})
        else
          Request.halt(request, response)
        end
    end
  end

  @athena_keys ~w(athena_action athena_query athena_wait_count)a

  defp execute_prepared_query(request) do
    {ours_private, theirs_private} = Map.split(request.private, @athena_keys)

    %ReqAthena.Query{prepared: false} = query = ours_private.athena_query
    prepared_query = %ReqAthena.Query{query | prepared: true}

    request = %{
      request
      | private: theirs_private,
        current_request_steps: Keyword.keys(request.request_steps)
    }

    Request.halt(request, Req.post!(request, athena: prepared_query))
  end

  # TODO: Add step `put_aws_sigv4` to Req
  # See: https://github.com/wojtekmach/req/issues/62
  defp sign_request(request, action) when is_binary(action) do
    request = Request.put_private(request, :athena_action, action)

    session_aws_header =
      if is_empty(request.options[:token]) do
        []
      else
        [{"X-Amz-Security-Token", request.options.token}]
      end

    aws_headers =
      [
        {"X-Amz-Target", "AmazonAthena.#{action}"},
        {"Host", request.url.host},
        {"Content-Type", "application/x-amz-json-1.1"}
      ] ++ session_aws_header

    headers =
      for {k, v} <- sign_request(request, aws_headers),
          do: {String.downcase(k, :ascii), v},
          into: []

    Req.Request.put_headers(request, headers)
  end

  defp sign_request(request, aws_headers) when is_list(aws_headers) do
    :aws_signature.sign_v4(
      request.options.access_key_id,
      request.options.secret_access_key,
      request.options.region,
      "athena",
      now(),
      "POST",
      to_string(request.url),
      aws_headers,
      request.body,
      []
    )
  end

  defp maybe_put_aws_credentials(request) do
    case aws_credentials() do
      :undefined ->
        request

      aws_credentials ->
        opts_credentials =
          for {k, v} <- request.options,
              v in @credential_keys and not is_empty(v),
              do: {k, v}

        aws_credentials =
          for {k, v} <- aws_credentials,
              k in @credential_keys and v != :undefined,
              do: {k, v}

        Req.Request.merge_options(request, Keyword.merge(aws_credentials, opts_credentials))
    end
  end

  if Code.ensure_loaded?(:aws_credentials) do
    defp aws_credentials, do: :aws_credentials.get_credentials()
  else
    defp aws_credentials, do: :undefined
  end

  defp now, do: NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()
end

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
  Builds a new `%Req.Request{}` for Athena requests.

  ## Request Options

    * `:access_key_id` - Required. The Access Key ID from AWS credentials.

    * `:secret_access_key` - Required. The Secret Access Key from AWS credentials.

    * `:token` - Optional. The Session Token from AWS credentials.

    * `:region` - Required. The AWS region where AWS Athena is installed.

    * `:database` - Required. The AWS Athena database name.

    * `:output_location` - Optional. The S3 URL location to output AWS Athena query results.

      When using `:json` or `:explorer` as the `:format` option (see below), this option is required.
      You may also need to specify a new output location for every new query when using these
      formats due to a limition of the `UNLOAD` command that `ReqAthena` uses underneath.
      Since Athena expects the directory used by `UNLOAD` to be empty, we append a "`results`"
      directory to the path of the `:output_location` to ensure it's empty.

      See the [`UNLOAD` command docs](https://docs.aws.amazon.com/athena/latest/ug/unload.html#unload-considerations-and-limitations)
      for more details.

    * `:workgroup` - Conditional. The AWS Athena workgroup.

    * `:cache_query` - Optional. Forces a non-cached result from AWS Athena.

    * `:format` - Optional. The output format. Can be one of:

        * `:none` (default) - return decoded API response from Athena.

        * `:csv` - return contents of the CSV file.

        * `:json` - return contents of the JSON file.

          Note: Req by default automatically decodes JSON response body (`Req.Steps.decode_body/1` step)
          and to prevent it from doing so, set `decode_body: false`.

        * `:explorer` - return contents in parquet format, lazy loaded into Explorer data frame.
          It means that the content is saved in the `:output_location` using parquet files.

          To use this option you first need to install `:explorer` as a dependency.

      When using `:json` or `:explorer` format, you may need to pass a different output location
      for every query. See `:output_location` for details.

    * `:output_compression` - Optional. Sets the Parquet compression format and level
      for the output when using the Explorer output format. This can be a string, like `"gzip"`,
      or a tuple with `{format, level}`, like: `{"ZSTD", 4}`. By default this is `nil`,
      which means that for Parquet (the format that Explorer uses) this is going to be `"gzip"`.

  Conditional fields must always be defined, and can be one of the fields or both.
  """
  @spec new(keyword()) :: Req.Request.t()
  def new(opts \\ []) do
    attach(Req.new(), opts)
  end

  defp attach(%Request{} = request, opts) do
    request
    |> Request.prepend_request_steps(athena_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(opts)
    |> maybe_put_aws_credentials()
    |> put_signature_options()
  end

  @doc """
  Performs a query against the Athena API.

  The SQL query can container `?` placeholders and `sql_query_params`
  is a list of corresponding values.

  This function accepts the same options as `new/1`.

  ## Examples

  With plain query:

      iex> opts = [
      ...>   access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      ...>   secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      ...>   region: System.fetch_env!("AWS_REGION"),
      ...>   database: "default",
      ...>   output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ...> ]
      iex> req = ReqAthena.new(opts)
      iex> query = "SELECT id, type, tags, members, timestamp, visible FROM planet WHERE id = 470454 and type = 'relation'"
      iex> ReqAthena.query!(req, query, [], format: :json).body
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
      iex> req = ReqAthena.new(opts)
      iex> query = "SELECT id, type FROM planet WHERE id = ? and type = ?"
      iex> ReqAthena.query!(req, query, [239_970_142, "node"], format: :json).body
      [%{"id" => 239970142, "type" => "node"}]

  """
  @spec query(Req.Request.t(), binary(), list(), Keyword.t()) ::
          {:ok, Req.Response.t()} | {:error, Exception.t()}
  def query(req, sql_query, sql_query_params \\ [], opts \\ [])

  def query(%Req.Request{} = req, sql_query, sql_query_params, opts)
      when is_binary(sql_query) and is_list(sql_query_params) and is_list(opts) do
    req
    |> attach(opts)
    |> put_request_body({sql_query, sql_query_params})
    |> Req.post()
  end

  @doc """
  Same as `query/4`, but raises in case of error.
  """
  @spec query!(Req.Request.t(), binary(), list(), Keyword.t()) :: Req.Response.t()
  def query!(req, sql_query, sql_query_params \\ [], opts \\ [])

  def query!(%Req.Request{} = req, sql_query, sql_query_params, opts) do
    case query(req, sql_query, sql_query_params, opts) do
      {:ok, response} -> response
      {:error, exception} -> raise exception
    end
  end

  defp run(%Request{private: %{athena_action: _}} = request), do: request

  defp run(request) do
    region = Request.fetch_option!(request, :region)

    url = "https://athena.#{region}.amazonaws.com"

    %{request | url: URI.parse(url)}
    |> prepare_action("StartQueryExecution")
    |> Request.append_response_steps(athena_result: &handle_athena_result/1)
  end

  defp put_request_body(request, {query, []}) do
    put_request_body(request, %ReqAthena.Query{query: query})
  end

  defp put_request_body(request, {query, params}) do
    cache_query = Request.get_option(request, :cache_query, true)

    hash =
      if cache_query do
        query |> :erlang.md5() |> Base.encode16()
      else
        :os.system_time() |> to_string()
      end

    query = %ReqAthena.Query{query: query, params: params, statement_name: "query_" <> hash}

    put_request_body(request, query)
  end

  defp put_request_body(request, %ReqAthena.Query{} = query) do
    cache_query = Request.get_option(request, :cache_query, true)

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
    defp build_lazy_frame(_parquet_locations, _aws_credentials) do
      raise ArgumentError,
            "format: :explorer - you need to install Explorer as a dependency in order to use this format"
    end
  end

  defp get_query_state(request, response) do
    response =
      %{request | body: response.body}
      |> prepare_action("GetQueryExecution")
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
          |> prepare_action("GetQueryResults")
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

    Request.halt(request, Req.post!(put_request_body(request, prepared_query)))
  end

  defp prepare_action(request, action) when is_binary(action) do
    request = Request.put_private(request, :athena_action, action)

    # We reuse the request in the response step, so we need to reset
    # the state such that the put_aws_sigv4 step runs again and signs
    # with the new headers.
    request = %{
      request
      | headers: %{},
        current_request_steps: Keyword.keys(request.request_steps)
    }

    Req.Request.put_headers(request, [
      {"X-Amz-Target", "AmazonAthena.#{action}"},
      {"Content-Type", "application/x-amz-json-1.1"}
    ])
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

  defp put_signature_options(request) do
    Req.merge(request,
      aws_sigv4: [
        access_key_id: request.options.access_key_id,
        secret_access_key: request.options.secret_access_key,
        region: request.options.region,
        service: :athena,
        token: request.options[:token]
      ]
    )
  end
end

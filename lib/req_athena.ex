defmodule ReqAthena do
  @moduledoc """
  `Req` plugin for [AWS Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

  ReqAthena makes it easy to make Athena queries. Query results are decoded into the `ReqAthena.Result` struct.
  The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.
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
  )a

  defguardp is_empty(value) when value in [nil, ""]

  @doc """
  Attaches to Req request.

  ## Request Options

    * `:access_key_id` - Required. The Access Key ID from AWS credentials.

    * `:secret_access_key` - Required. The Secret Access Key from AWS credentials.

    * `:token` - Optional. The Session Token from AWS credentials.

    * `:region` - Required. The AWS region where AWS Athena is installed.

    * `:database` - Required. The AWS Athena database name.

    * `:output_location` - Conditional. The S3 url location to output AWS Athena query results.

    * `:workgroup` - Conditional. The AWS Athena workgroup.

    * `:cache_query` - Optional. Forces a non-cached result from AWS Athena.

    * `:athena` - Required. The query to execute. It can be a plain sql string or
      a `{query, params}` tuple, where `query` can contain `?` placeholders and `params`
      is a list of corresponding values.

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
      iex> Req.post!(req, athena: query).body
      %ReqAthena.Result{
        columns: ["id", "type", "tags", "members", "timestamp", "visible"],
        output_location: "s3://my-bucket/c594d5df-9879-4bf7-8796-780e0b87a673.csv",
        query_execution_id: "c594d5df-9879-4bf7-8796-780e0b87a673",
        rows: [
          [470454, "relation",
           "{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}",
           "[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]",
           ~N[2017-01-21 12:51:34.000], true]
        ],
        statement_name: nil
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
      iex> Req.post!(req, athena: {query, [239_970_142, "node"]}).body
      %ReqAthena.Result{
        columns: ["id", "type"],
        output_location: "s3://my-bucket/dda41d66-1eea-4588-850a-945c9def9163.csv",
        query_execution_id: "dda41d66-1eea-4588-850a-945c9def9163",
        rows: [[239_970_142, "node"]],
        statement_name: "query_C71EF77B8B7B92D9846C6D7E70136448"
      }

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
      region = fetch_option!(request, :region)

      url = "https://athena.#{region}.amazonaws.com"
      cache_query = get_option(request, :cache_query, true)

      %{request | url: URI.parse(url)}
      |> put_request_body(query, cache_query)
      |> sign_request("StartQueryExecution")
      |> Request.append_response_steps(athena_result: &handle_athena_result/1)
    else
      request
    end
  end

  defp put_request_body(request, {query, []}, cache_query) do
    put_request_body(request, query, cache_query)
  end

  defp put_request_body(request, {query, _params}, cache_query) do
    hash =
      if cache_query do
        query |> :erlang.md5() |> Base.encode16()
      else
        :os.system_time() |> to_string()
      end

    statement_name = "query_" <> hash

    request
    |> put_request_body("PREPARE #{statement_name} FROM #{query}", cache_query)
    |> Request.put_private(:athena_parameterized?, true)
    |> Request.put_private(:athena_statement_name, statement_name)
  end

  defp put_request_body(request, query, cache_query)
       when is_binary(query) do
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

    body =
      Map.merge(output_config, %{
        QueryExecutionContext: %{Database: fetch_option!(request, :database)},
        QueryString: query
      })

    client_request_token = generate_client_request_token(body, cache_query)
    body = Map.put(body, :ClientRequestToken, client_request_token)

    %{request | body: Jason.encode!(body)}
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
    parameterized? = Request.get_private(request, :athena_parameterized?, false)

    case {action, parameterized?} do
      {"StartQueryExecution", _} ->
        get_query_state(request, response)

      {"GetQueryExecution", _} ->
        wait_query_execution(request, response)

      {"GetQueryResults", true} ->
        execute_prepared_query(request)

      {"GetQueryResults", _} ->
        decode_result(request, response)
    end
  end

  defp handle_athena_result(request_response), do: request_response

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
          Logger.info("ReqAthena: query is in QUEUED state, will retry in 1000ms")
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

      _other_state ->
        decode_result(request, response)
    end
  end

  @athena_keys ~w(athena_action athena_parameterized? athena_wait_count)a

  defp execute_prepared_query(request) do
    {_, params} = fetch_option!(request, :athena)
    statement_name = Req.Request.get_private(request, :athena_statement_name)
    athena = "EXECUTE #{statement_name} USING " <> Enum.map_join(params, ", ", &encode_value/1)
    {_, private} = Map.split(request.private, @athena_keys)

    request = %{
      request
      | private: private,
        current_request_steps: Keyword.keys(request.request_steps)
    }

    Request.halt(request, Req.post!(request, athena: athena))
  end

  defp decode_result(request, response) do
    body = Jason.decode!(response.body)
    statement_name = Request.get_private(request, :athena_statement_name)
    query_execution_id = Request.get_private(request, :athena_query_execution_id)
    output_location = Request.get_private(request, :athena_output_location)

    result =
      case body do
        %{
          "ResultSet" => %{
            "ResultSetMetadata" => %{"ColumnInfo" => columns_info},
            "Rows" => [%{"Data" => column_labels} | rows]
          }
        } ->
          %ReqAthena.Result{
            query_execution_id: query_execution_id,
            output_location: output_location,
            statement_name: statement_name,
            rows: decode_rows(rows, columns_info),
            columns: decode_column_labels(column_labels),
            metadata: columns_info
          }

        %{"ResultSet" => _} ->
          %ReqAthena.Result{
            query_execution_id: query_execution_id,
            output_location: output_location,
            statement_name: statement_name
          }

        body ->
          body
      end

    Request.halt(request, %{response | body: result})
  end

  defp decode_column_labels(column_labels) do
    Enum.map(column_labels, &Map.fetch!(&1, "VarCharValue"))
  end

  defp decode_rows(rows, columns_info) do
    column_types = Enum.map(columns_info, &Map.take(&1, ["Type"]))

    Enum.map(rows, fn %{"Data" => datums} ->
      Enum.zip_with([datums, column_types], fn [datum, column_type] ->
        value = datum["VarCharValue"] || ""
        decode_value(value, column_type)
      end)
    end)
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

  @credential_keys ~w(access_key_id secret_access_key region token)a

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

  defp encode_value(value) when is_binary(value), do: "'#{value}'"
  defp encode_value(%Date{} = value), do: to_string(value) |> encode_value()

  defp encode_value(%DateTime{} = value) do
    value
    |> DateTime.to_naive()
    |> encode_value()
  end

  defp encode_value(%NaiveDateTime{} = value) do
    value
    |> NaiveDateTime.truncate(:millisecond)
    |> to_string()
    |> encode_value()
  end

  defp encode_value(value), do: value

  defp decode_value(nil, _), do: nil

  @integer_types ~w(bigint smallint integer)

  defp decode_value(value, %{"Type" => type}) when type in @integer_types,
    do: String.to_integer(value)

  @float_types ~w(double float decimal)

  defp decode_value(value, %{"Type" => type}) when type in @float_types,
    do: String.to_float(value)

  defp decode_value("true", %{"Type" => "boolean"}), do: true
  defp decode_value("false", %{"Type" => "boolean"}), do: false
  defp decode_value(value, %{"Type" => "date"}), do: Date.from_iso8601!(value)

  defp decode_value(value, %{"Type" => "timestamp"}), do: NaiveDateTime.from_iso8601!(value)

  defp decode_value(value, %{"Type" => "timestamp with time zone"}) do
    [d, t, tz] = String.split(value, " ", trim: true)
    date = Date.from_iso8601!(d)
    time = Time.from_iso8601!(t)

    DateTime.new!(date, time, tz)
    |> DateTime.truncate(:millisecond)
  end

  defp decode_value(value, _), do: value

  # TODO: Use Req.Request.get_option/3 when Req 0.4.0 is out.
  defp get_option(request, key, default) when is_atom(key) do
    Map.get(request.options, key, default)
  end

  # TODO: Use Req.Request.fetch_option!/2 when Req 0.4.0 is out.
  def fetch_option!(request, key) when is_atom(key) do
    case Map.fetch(request.options, key) do
      {:ok, value} ->
        value

      :error ->
        raise KeyError,
          term: request.options,
          key: key,
          message: "option #{inspect(key)} is not set"
    end
  end
end

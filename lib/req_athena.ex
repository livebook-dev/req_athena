defmodule ReqAthena do
  require Logger

  alias Req.Request

  @allowed_options ~w(
    access_key_id
    secret_access_key
    region
    database
    athena
    output_location
  )a

  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(athena_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
  end

  defp run(%Request{private: %{athena_action: _}} = request), do: request

  defp run(%Request{options: %{athena: query} = options} = request) do
    url = "https://athena.#{options.region}.amazonaws.com"

    %{request | url: URI.parse(url)}
    |> put_request_body(query)
    |> sign_request("StartQueryExecution")
    |> Request.append_response_steps(athena_result: &handle_athena_result/1)
  end

  defp run(request), do: request

  defp put_request_body(request, {query, []}) do
    put_request_body(request, query)
  end

  defp put_request_body(request, {query, _params}) do
    statement_name = :erlang.md5(query) |> Base.encode16()

    request
    |> put_request_body("PREPARE #{statement_name} FROM #{query}")
    |> Request.put_private(:athena_parameterized?, true)
    |> Request.put_private(:athena_statement_name, statement_name)
  end

  defp put_request_body(%{options: options} = request, query) when is_binary(query) do
    body = %{
      QueryExecutionContext: %{Database: options.database},
      ResultConfiguration: %{OutputLocation: options.output_location},
      QueryString: query
    }

    client_request_token = generate_client_request_token(body)
    body = Map.put(body, :ClientRequestToken, client_request_token)

    %{request | body: Jason.encode!(body)}
  end

  defp generate_client_request_token(parameters) do
    :erlang.md5(:erlang.term_to_binary(parameters))
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

    {Request.halt(request), response}
  end

  @wait_delay 1000

  defp wait_query_execution(request, response) do
    body = Jason.decode!(response.body)

    case body["QueryExecution"]["Status"]["State"] do
      "QUEUED" ->
        count = Request.get_private(request, :athena_wait_count, 1)

        if count >= 3 do
          Logger.info("ReqAthena: query is in QUEUED state, will retry in 1000ms")
        end

        request = Request.put_private(request, :athena_wait_count, count + 1)
        Process.sleep(@wait_delay)
        {Request.halt(request), Req.post!(request)}

      "RUNNING" ->
        Process.sleep(@wait_delay)
        {Request.halt(request), Req.post!(request)}

      "SUCCEEDED" ->
        request = sign_request(request, "GetQueryResults")
        {Request.halt(request), Req.post!(request)}

      _other_state ->
        decode_result(request, response)
    end
  end

  @athena_keys ~w(athena_action athena_parameterized? athena_wait_count)a

  defp execute_prepared_query(request) do
    {_, params} = request.options.athena
    statement_name = Req.Request.get_private(request, :athena_statement_name)
    athena = "EXECUTE #{statement_name} USING " <> Enum.map_join(params, ", ", &encode_value/1)
    {_, private} = Map.split(request.private, @athena_keys)
    request = %{request | private: private}

    {Request.halt(request), Req.post!(request, athena: athena)}
  end

  defp decode_result(request, response) do
    body = Jason.decode!(response.body)
    statement_name = Request.get_private(request, :athena_statement_name)

    result =
      case body do
        %{
          "ResultSet" => %{
            "ColumnInfos" => fields,
            "ResultRows" => [%{"Data" => columns} | rows]
          }
        } ->
          %ReqAthena.Result{
            statement_name: statement_name,
            rows: decode_rows(rows, fields),
            columns: columns
          }

        %{"ResultSet" => _} ->
          %ReqAthena.Result{statement_name: statement_name}

        body ->
          body
      end

    {Request.halt(request), %{response | body: result}}
  end

  defp decode_rows(rows, fields) do
    Enum.map(rows, fn %{"Data" => columns} ->
      Enum.with_index(columns, fn value, index ->
        field = Enum.at(fields, index)
        decode_value(value, field)
      end)
    end)
  end

  # TODO: Add step `put_aws_sigv4` to Req
  # See: https://github.com/wojtekmach/req/issues/62
  defp sign_request(%{url: uri, options: options} = request, action) do
    request = Request.put_private(request, :athena_action, action)

    aws_headers = [
      {"X-Amz-Target", "AmazonAthena.#{action}"},
      {"Host", uri.host},
      {"Content-Type", "application/x-amz-json-1.1"}
    ]

    headers =
      :aws_signature.sign_v4(
        options.access_key_id,
        options.secret_access_key,
        options.region,
        "athena",
        now(),
        "POST",
        to_string(uri),
        aws_headers,
        request.body,
        []
      )

    for {name, value} <- headers, reduce: request do
      acc -> Req.Request.put_header(acc, String.downcase(name), value)
    end
  end

  defp now, do: NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()

  defp encode_value(value) when is_binary(value), do: "'#{value}'"
  defp encode_value(value), do: value

  defp decode_value(nil, _), do: nil

  defp decode_value(value, %{"Type" => "bigint"}), do: String.to_integer(value)
  defp decode_value(value, %{"Type" => "decimal"}), do: Decimal.new(value)

  # TODO: Implement a decoder for a map
  # with format: {key=value, ...}
  # i.e.: {created_by=JOSM}
  defp decode_value(value, %{"Type" => "map"}), do: value

  defp decode_value(value, %{"Type" => "array"}), do: Jason.decode!(value)
  defp decode_value("true", %{"Type" => "boolean"}), do: true
  defp decode_value("false", %{"Type" => "boolean"}), do: false
  defp decode_value(value, %{"Type" => "timestamp"}), do: NaiveDateTime.from_iso8601!(value)
  defp decode_value(value, _), do: value
end

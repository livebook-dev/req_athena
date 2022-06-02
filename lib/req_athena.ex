defmodule ReqAthena do
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

  defp put_request_body(%{options: options} = request, query) when is_binary(query) do
    parameters = %{
      QueryExecutionContext: %{
        Database: options.database
      },
      ResultConfiguration: %{
        OutputLocation: options.output_location
      },
      QueryString: query
    }

    client_request_token = generate_client_request_token(parameters)
    body = Map.put(parameters, :ClientRequestToken, client_request_token)

    %{request | body: Jason.encode!(body)}
  end

  defp generate_client_request_token(parameters) do
    :erlang.md5(:erlang.term_to_binary(parameters))
    |> Base.encode16()
  end

  defp handle_athena_result({request, %{status: 200} = response}) do
    case Request.get_private(request, :athena_action) do
      "StartQueryExecution" ->
        get_query_state(request, response)

      "GetQueryExecution" ->
        wait_query_execution(request, response)

      "GetQueryResults" ->
        {request, update_in(response.body, &Jason.decode!/1)}
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

  @waitable_states ~w(QUEUED RUNNING)

  defp wait_query_execution(request, response) do
    body = Jason.decode!(response.body)
    query_state = body["QueryExecution"]["Status"]["State"]

    cond do
      query_state in @waitable_states ->
        Process.sleep(1000)
        {Request.halt(request), Req.post!(request)}

      query_state == "SUCCEEDED" ->
        request = sign_request(request, "GetQueryResults")
        {Request.halt(request), Req.post!(request)}

      true ->
        {request, response}
    end
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
      acc -> Req.Request.put_header(acc, name, value)
    end
  end

  defp now, do: NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()
end

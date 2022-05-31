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

  defp handle_athena_result(
         {%{private: %{athena_action: "StartQueryExecution"}} = request,
          %{status: 200, body: body}}
       ) do
    request = sign_request(%{request | body: body}, "GetQueryExecution")
    response = Req.post!(request)
    {request, response}
  end

  @waitable_states ~w(QUEUED RUNNING)
  @retry_delay_ms 2000

  defp handle_athena_result(
         {%{private: %{athena_action: "GetQueryExecution"}} = request,
          %{status: 200, body: resp_body} = response}
       ) do
    body = Jason.decode!(resp_body)
    query_state = body["QueryExecution"]["Status"]["State"]

    cond do
      query_state in @waitable_states ->
        Process.sleep(@retry_delay_ms)
        {request, Req.post!(request)}

      query_state == "SUCCEEDED" ->
        request = sign_request(request, "GetQueryResults")
        {Request.halt(request), Req.post!(request)}

      true ->
        {request, response}
    end
  end

  defp handle_athena_result(
         {%{private: %{athena_action: "GetQueryResults"}} = request,
          %{status: 200, body: body} = response}
       ) do
    {request, %{response | body: Jason.decode!(body)}}
  end

  defp handle_athena_result(request_response), do: request_response

  # TODO: Add step `put_aws_sigv4` to Req
  # See: https://github.com/wojtekmach/req/issues/62
  defp sign_request(%{url: uri, options: options} = request, action) do
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

    Request.put_private(%{request | headers: headers}, :athena_action, action)
  end

  defp now, do: NaiveDateTime.utc_now() |> NaiveDateTime.to_erl()
end

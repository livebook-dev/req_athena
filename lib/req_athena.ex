defmodule ReqAthena do
  alias Req.Request

  @allowed_options ~w(
    access_key_id
    secret_access_key
    region
    workgroup
    catalog
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

  defp run(%Request{private: %{athena_action: "GetQueryResults"}} = request), do: request

  defp run(%Request{options: %{athena: query} = options} = request) do
    url = "https://athena.#{options.region}.amazonaws.com"

    %{request | url: URI.parse(url)}
    |> put_request_body(query)
    |> sign_request("StartQueryExecution")
    |> Request.append_response_steps(athena_result: &handle_athena_result/1)
  end

  defp run(any), do: any

  defp put_request_body(request, {query, _}), do: put_request_body(request, query)

  defp put_request_body(%{options: options} = request, query) when is_binary(query) do
    parameters = %{
      QueryExecutionContext: %{
        Catalog: options.catalog,
        Database: options.database
      },
      ResultConfiguration: %{
        OutputLocation: options.output_location
      },
      QueryString: query,
      WorkGroup: options.workgroup
    }

    client_request_token = build_client_request_token(parameters)
    body = Map.put(parameters, :ClientRequestToken, client_request_token)

    %{request | body: Jason.encode!(body)}
  end

  defp build_client_request_token(parameters) do
    :erlang.md5(:erlang.term_to_binary(parameters))
    |> Base.encode16()
  end

  defp handle_athena_result(
         {%{private: %{athena_action: "StartQueryExecution"}} = request,
          %{status: 200, body: body}}
       ) do
    request = sign_request(%{request | body: body}, "GetQueryResults")
    response = Req.post!(request)
    {request, response}
  end

  defp handle_athena_result(
         {%{private: %{athena_action: "GetQueryResults"}} = request,
          %{status: 200, body: body} = response}
       ) do
    {request, %{response | body: Jason.decode!(body)}}
  end

  defp handle_athena_result(any), do: any

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

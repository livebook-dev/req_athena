defmodule ReqAthena.S3 do
  @moduledoc false
  def new(aws_credentials, options \\ []) do
    options |> Req.new() |> ReqS3.attach(aws_sigv4: aws_credentials)
  end

  def get_locations(req_s3, output_location) do
    manifest_csv_location = output_location <> "-manifest.csv"

    req_s3
    |> get_body(manifest_csv_location)
    |> String.trim()
    |> String.split("\n")
  end

  def get_body(req_s3, location) do
    %{status: 200} = response = Req.get!(req_s3, url: location)

    response.body
  end
end

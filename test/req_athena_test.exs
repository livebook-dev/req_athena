defmodule ReqAthenaTest do
  use ExUnit.Case, async: true

  test "executes a query string" do
    fake_athena = fn
      %{private: %{athena_action: "GetQueryResults"}} = request ->
        assert Jason.decode!(request.body) == %{"QueryExecutionId" => "an uuid"}
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"
        assert Req.Request.get_header(request, "x-amz-target") == ["AmazonAthena.GetQueryResults"]
        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]
        assert Req.Request.get_header(request, "x-auth") == ["my awesome auth header"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        data = %{
          "ResultSet" => %{
            "ColumnInfos" => [
              %{
                "CaseSensitive" => false,
                "CatalogName" => "hive",
                "Label" => "id",
                "Name" => "id",
                "Nullable" => "UNKNOWN",
                "Precision" => 2_147_483_647,
                "Scale" => 0,
                "SchemaName" => "",
                "TableName" => "",
                "Type" => "integer"
              },
              %{
                "CaseSensitive" => false,
                "CatalogName" => "hive",
                "Label" => "name",
                "Name" => "name",
                "Nullable" => "UNKNOWN",
                "Precision" => 2_147_483_647,
                "Scale" => 0,
                "SchemaName" => "",
                "TableName" => "",
                "Type" => "varchar"
              }
            ],
            "ResultRows" => [
              %{"Data" => ["id", "name"]},
              %{"Data" => [1, "Ale"]},
              %{"Data" => [2, "Wojtek"]}
            ],
            "ResultSetMetadata" => %{
              "ColumnInfo" => [
                %{
                  "CaseSensitive" => false,
                  "CatalogName" => "hive",
                  "Label" => "id",
                  "Name" => "id",
                  "Nullable" => "UNKNOWN",
                  "Precision" => 2_147_483_647,
                  "Scale" => 0,
                  "SchemaName" => "",
                  "TableName" => "",
                  "Type" => "integer"
                },
                %{
                  "CaseSensitive" => false,
                  "CatalogName" => "hive",
                  "Label" => "name",
                  "Name" => "name",
                  "Nullable" => "UNKNOWN",
                  "Precision" => 2_147_483_647,
                  "Scale" => 0,
                  "SchemaName" => "",
                  "TableName" => "",
                  "Type" => "varchar"
                }
              ]
            },
            "Rows" => [
              %{"Data" => ["id", "name"]},
              %{"Data" => [1, "\"Ale\""]},
              %{"Data" => [2, "\"Wojtek\""]}
            ]
          },
          "UpdateCount" => 0
        }

        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}

      %{private: %{athena_action: "GetQueryExecution"}} = request ->
        assert Jason.decode!(request.body) == %{"QueryExecutionId" => "an uuid"}
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"

        assert Req.Request.get_header(request, "x-amz-target") == [
                 "AmazonAthena.GetQueryExecution"
               ]

        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        data = %{
          QueryExecution: %{
            Query: "select * from iris",
            QueryExecutionContext: %{
              Catalog: "AwsDataCatalog",
              Database: "my_awesome_database"
            },
            QueryExecutionId: "some uuid",
            ResultConfiguration: %{
              OutputLocation: "s3://foo"
            },
            StatementType: "DDL",
            Status: %{
              State: "SUCCEEDED"
            },
            WorkGroup: "primary"
          }
        }

        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}

      %{private: %{athena_action: "StartQueryExecution"}} = request ->
        assert Jason.decode!(request.body) == %{
                 "ClientRequestToken" => "279A8BD03538A2F33C1B13DF28FF1966",
                 "QueryExecutionContext" => %{
                   "Database" => "my_awesome_database"
                 },
                 "QueryString" => "select * from iris",
                 "ResultConfiguration" => %{"OutputLocation" => "s3://foo"}
               }

        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"

        assert Req.Request.get_header(request, "x-amz-target") == [
                 "AmazonAthena.StartQueryExecution"
               ]

        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        data = %{QueryExecutionId: "an uuid"}
        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}
    end

    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      output_location: "s3://foo"
    ]

    assert response =
             Req.new(adapter: fake_athena)
             |> Req.Request.put_header("x-auth", "my awesome auth header")
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert response.body == %ReqAthena.Result{
             columns: ["id", "name"],
             rows: [[1, "Ale"], [2, "Wojtek"]],
             statement_name: nil
           }
  end
end

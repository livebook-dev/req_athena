defmodule ReqAthenaTest do
  use ExUnit.Case, async: true

  test "executes a query string" do
    fake_athena = fn
      %{private: %{athena_action: "GetQueryResults"}} = request ->
        assert Jason.decode!(request.body) == %{"QueryExecutionId" => "an uuid"}
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"
        assert Req.Request.get_header(request, "X-Amz-Target") == ["AmazonAthena.GetQueryResults"]
        assert Req.Request.get_header(request, "Host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "Content-Type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "Authorization")
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
              %{"Data" => [1, "\"Ale\""]},
              %{"Data" => [2, "\"Wojtek\""]}
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

      %{private: %{athena_action: "StartQueryExecution"}} = request ->
        assert Jason.decode!(request.body) == %{
                 "ClientRequestToken" => "32AA1B72874D35863B5462EE3EC889AB",
                 "QueryExecutionContext" => %{
                   "Catalog" => "AwsDataCatalog",
                   "Database" => "my_awesome_database"
                 },
                 "QueryString" => "select * from iris",
                 "ResultConfiguration" => %{"OutputLocation" => "s3://foo"},
                 "WorkGroup" => "primary"
               }

        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"

        assert Req.Request.get_header(request, "X-Amz-Target") == [
                 "AmazonAthena.StartQueryExecution"
               ]

        assert Req.Request.get_header(request, "Host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "Content-Type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "Authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        data = %{QueryExecutionId: "an uuid"}
        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}
    end

    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      catalog: "AwsDataCatalog",
      workgroup: "primary",
      output_location: "s3://foo"
    ]

    assert response =
             Req.new(adapter: fake_athena)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert response.body == %{
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
                 %{"Data" => [1, "\"Ale\""]},
                 %{"Data" => [2, "\"Wojtek\""]}
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
  end
end

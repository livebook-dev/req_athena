defmodule ReqAthenaTest do
  use ExUnit.Case, async: true
  @moduletag :capture_log

  test "executes a query string returning a data frame" do
    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      output_location: "s3://foo"
    ]

    request_validations = %{
      "StartQueryExecution" => fn request ->
        decoded = Jason.decode!(request.body)

        assert %{
                 "ClientRequestToken" => client_req_token,
                 "QueryExecutionContext" => %{
                   "Database" => "my_awesome_database"
                 },
                 "QueryString" =>
                   "UNLOAD (select * from iris)\nTO 's3://foo/results'\nWITH (compression = 'SNAPPY', format = 'PARQUET')",
                 "ResultConfiguration" => %{"OutputLocation" => "s3://foo"}
               } = decoded

        assert is_binary(client_req_token)
      end
    }

    me = self()

    assert response =
             Req.new(adapter: fake_athena(request_validations))
             |> Req.Request.put_header("x-auth", "my awesome auth header")
             |> Req.Request.put_private(:athena_dataframe_builder, fn manifest_location,
                                                                      credentials ->
               assert manifest_location == "s3://foo-manifest.csv"

               assert Enum.sort(Keyword.take(opts, [:access_key_id, :secret_access_key, :region])) ==
                        Enum.sort(credentials)

               send(me, {:explorer_built, manifest_location})

               Explorer.DataFrame.new(id: [1, 2], name: ["Ale", "Wojtek"])
             end)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert df = %Explorer.DataFrame{} = response.body

    assert Explorer.DataFrame.to_columns(df, atom_keys: true) == %{
             id: [1, 2],
             name: ["Ale", "Wojtek"]
           }

    assert_received {:explorer_built, _output_location}
  end

  test "parses a response with a datum object missing" do
    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      no_explorer: true,
      output_location: "s3://foo"
    ]

    results = %{
      "GetQueryResults" => fn request ->
        data = %{
          "ResultSet" => %{
            "ColumnInfos" => [
              %{
                "CaseSensitive" => false,
                "CatalogName" => "hive",
                "Label" => "id",
                "Name" => "id",
                "Nullable" => "UNKNOWN",
                "Precision" => 10,
                "Scale" => 0,
                "SchemaName" => "",
                "TableName" => "",
                "Type" => "integer"
              },
              %{
                "CaseSensitive" => true,
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
              %{"Data" => ["1", "Ale"]},
              %{"Data" => ["2", "Wojtek"]}
            ],
            "ResultSetMetadata" => %{
              "ColumnInfo" => [
                %{
                  "CaseSensitive" => false,
                  "CatalogName" => "hive",
                  "Label" => "id",
                  "Name" => "id",
                  "Nullable" => "UNKNOWN",
                  "Precision" => 10,
                  "Scale" => 0,
                  "SchemaName" => "",
                  "TableName" => "",
                  "Type" => "integer"
                },
                %{
                  "CaseSensitive" => true,
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
              %{"Data" => [%{"VarCharValue" => "id"}, %{"VarCharValue" => "name"}]},
              %{"Data" => [%{"VarCharValue" => "1"}, %{"VarCharValue" => "Ale"}]},
              %{"Data" => [%{"VarCharValue" => "2"}, %{}]}
            ]
          },
          "UpdateCount" => 0
        }

        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}
      end
    }

    response =
      Req.new(adapter: fake_athena(%{}, results))
      |> Req.Request.put_header("x-auth", "my awesome auth header")
      |> ReqAthena.attach(opts)
      |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert response.body == %ReqAthena.Result{
             columns: ["id", "name"],
             output_location: "s3://foo",
             query_execution_id: "an uuid",
             rows: [[1, "Ale"], [2, ""]],
             statement_name: nil,
             metadata: [
               %{
                 "CaseSensitive" => false,
                 "CatalogName" => "hive",
                 "Label" => "id",
                 "Name" => "id",
                 "Nullable" => "UNKNOWN",
                 "Precision" => 10,
                 "Scale" => 0,
                 "SchemaName" => "",
                 "TableName" => "",
                 "Type" => "integer"
               },
               %{
                 "CaseSensitive" => true,
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
           }
  end

  test "executes a parameterized query" do
    validations = %{
      "StartQueryExecution" => fn request ->
        if Req.Request.get_private(request, :athena_query_execution_id, nil) do
          client_req_token =
            case :erlang.system_info(:otp_release) do
              ~c"27" -> "BB0174689AC2EA58B5C2CAB1B69DCF72"
              _ -> "74591A3EBE23508682D20337984FC399"
            end

          decoded = Jason.decode!(request.body)

          assert %{
                   "ClientRequestToken" => ^client_req_token,
                   "QueryExecutionContext" => %{
                     "Database" => "my_awesome_database"
                   },
                   "QueryString" => "EXECUTE query_8CD6B60FAFA18EBFA8719A6EAC192624 USING 1",
                   "ResultConfiguration" => %{"OutputLocation" => "s3://foo"}
                 } = decoded
        else
          client_req_token =
            case :erlang.system_info(:otp_release) do
              ~c"27" -> "F1C8E4D3CE5FFFDA8DC9568A0685BAC1"
              _ -> "3F8FCA289E16CFEC152E6F8C2596DA6B"
            end

          decoded = Jason.decode!(request.body)

          assert %{
                   "ClientRequestToken" => ^client_req_token,
                   "QueryExecutionContext" => %{
                     "Database" => "my_awesome_database"
                   },
                   "QueryString" =>
                     "PREPARE query_8CD6B60FAFA18EBFA8719A6EAC192624 FROM select * from iris where id = ?",
                   "ResultConfiguration" => %{"OutputLocation" => "s3://foo"}
                 } = decoded
        end
      end
    }

    results = %{
      "GetQueryResults" => fn request ->
        query = Req.Request.get_private(request, :athena_query)
        to_prepare? = ReqAthena.Query.to_prepare?(query)

        data =
          if to_prepare? do
            %{"ResultSet" => %{"Output" => ""}}
          else
            %{
              "ResultSet" => %{
                "ColumnInfos" => [
                  %{
                    "CaseSensitive" => false,
                    "CatalogName" => "hive",
                    "Label" => "id",
                    "Name" => "id",
                    "Nullable" => "UNKNOWN",
                    "Precision" => 10,
                    "Scale" => 0,
                    "SchemaName" => "",
                    "TableName" => "",
                    "Type" => "integer"
                  },
                  %{
                    "CaseSensitive" => true,
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
                "ResultRows" => [%{"Data" => ["id", "name"]}, %{"Data" => ["1", "Ale"]}],
                "ResultSetMetadata" => %{
                  "ColumnInfo" => [
                    %{
                      "CaseSensitive" => false,
                      "CatalogName" => "hive",
                      "Label" => "id",
                      "Name" => "id",
                      "Nullable" => "UNKNOWN",
                      "Precision" => 10,
                      "Scale" => 0,
                      "SchemaName" => "",
                      "TableName" => "",
                      "Type" => "integer"
                    },
                    %{
                      "CaseSensitive" => true,
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
                  %{"Data" => [%{"VarCharValue" => "id"}, %{"VarCharValue" => "name"}]},
                  %{"Data" => [%{"VarCharValue" => "1"}, %{"VarCharValue" => "Ale"}]}
                ]
              },
              "UpdateCount" => 0
            }
          end

        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}
      end
    }

    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      no_explorer: true,
      output_location: "s3://foo"
    ]

    assert response =
             Req.new(adapter: fake_athena(validations, results))
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: {"select * from iris where id = ?", [1]})

    assert response.status == 200

    assert response.body == %ReqAthena.Result{
             columns: ["id", "name"],
             output_location: "s3://foo",
             query_execution_id: "an uuid",
             rows: [[1, "Ale"]],
             statement_name: "query_8CD6B60FAFA18EBFA8719A6EAC192624",
             metadata: [
               %{
                 "CaseSensitive" => false,
                 "CatalogName" => "hive",
                 "Label" => "id",
                 "Name" => "id",
                 "Nullable" => "UNKNOWN",
                 "Precision" => 10,
                 "Scale" => 0,
                 "SchemaName" => "",
                 "TableName" => "",
                 "Type" => "integer"
               },
               %{
                 "CaseSensitive" => true,
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
           }
  end

  test "executes a query with session token" do
    token_validation = fn request ->
      assert Req.Request.get_header(request, "x-amz-security-token") == [
               "giant dummy session token"
             ]
    end

    validations = %{
      "GetQueryResults" => token_validation,
      "GetQueryExecution" => token_validation,
      "StartQueryExecution" => token_validation
    }

    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      token: "giant dummy session token",
      region: "us-east-1",
      database: "my_awesome_database",
      output_location: "s3://foo"
    ]

    me = self()

    response =
      Req.new(adapter: fake_athena(validations))
      |> ReqAthena.attach(opts)
      |> Req.Request.put_private(:athena_dataframe_builder, fn manifest_location, credentials ->
        assert manifest_location == "s3://foo-manifest.csv"

        assert Enum.sort(
                 Keyword.take(opts, [:access_key_id, :secret_access_key, :region, :token])
               ) ==
                 Enum.sort(credentials)

        send(me, :explorer_built)

        Explorer.DataFrame.new(id: [1, 2], name: ["Ale", "Wojtek"])
      end)
      |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert Explorer.DataFrame.to_columns(response.body, atom_keys: true) == %{
             id: [1, 2],
             name: ["Ale", "Wojtek"]
           }

    assert_received :explorer_built
  end

  test "executes a query with workgroup" do
    validations = %{
      "StartQueryExecution" => fn request ->
        client_req_token =
          case :erlang.system_info(:otp_release) do
            ~c"27" -> "7757497C80E919A5B56F6F59F2DAA0F0"
            _ -> "D6C3709EDB68939EA3B176B2961177C9"
          end

        decoded = Jason.decode!(request.body)

        assert %{
                 "ClientRequestToken" => ^client_req_token,
                 "QueryExecutionContext" => %{"Database" => "my_awesome_database"},
                 "QueryString" => "select * from iris",
                 "WorkGroup" => "default"
               } = decoded
      end
    }

    results = %{
      "GetQueryExecution" => fn request ->
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
            WorkGroup: "default"
          }
        }

        {request, %Req.Response{status: 200, body: Jason.encode!(data)}}
      end
    }

    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database",
      workgroup: "default"
    ]

    assert response =
             Req.new(adapter: fake_athena(validations, results))
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "select * from iris")

    assert response.status == 200

    assert response.body == %ReqAthena.Result{
             columns: ["id", "name"],
             output_location: "s3://foo",
             query_execution_id: "an uuid",
             rows: [[1, "Ale"], [2, "Wojtek"]],
             statement_name: nil,
             metadata: [
               %{
                 "CaseSensitive" => false,
                 "CatalogName" => "hive",
                 "Label" => "id",
                 "Name" => "id",
                 "Nullable" => "UNKNOWN",
                 "Precision" => 10,
                 "Scale" => 0,
                 "SchemaName" => "",
                 "TableName" => "",
                 "Type" => "integer"
               },
               %{
                 "CaseSensitive" => true,
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
           }
  end

  test "raises the request when neither workgroup and output location are defined" do
    opts = [
      access_key_id: "some key",
      secret_access_key: "dummy",
      region: "us-east-1",
      database: "my_awesome_database"
    ]

    req = Req.new(adapter: fake_athena()) |> ReqAthena.attach(opts)

    assert_raise ArgumentError,
                 "options must have :workgroup, :output_location or both defined",
                 fn -> Req.post!(req, athena: "select * from iris") end
  end

  defp fake_athena, do: fake_athena(%{})
  defp fake_athena(map) when is_map(map), do: fake_athena(map, %{})

  defp fake_athena(validations, results) do
    fn
      %{private: %{athena_action: "GetQueryResults"}} = request ->
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"
        assert Req.Request.get_header(request, "x-amz-target") == ["AmazonAthena.GetQueryResults"]
        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        if fun = validations["GetQueryResults"] do
          fun.(request)
        else
          assert Jason.decode!(request.body) == %{"QueryExecutionId" => "an uuid"}
        end

        original_data = %{
          "ResultSet" => %{
            "ColumnInfos" => [
              %{
                "CaseSensitive" => false,
                "CatalogName" => "hive",
                "Label" => "id",
                "Name" => "id",
                "Nullable" => "UNKNOWN",
                "Precision" => 10,
                "Scale" => 0,
                "SchemaName" => "",
                "TableName" => "",
                "Type" => "integer"
              },
              %{
                "CaseSensitive" => true,
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
              %{"Data" => ["1", "Ale"]},
              %{"Data" => ["2", "Wojtek"]}
            ],
            "ResultSetMetadata" => %{
              "ColumnInfo" => [
                %{
                  "CaseSensitive" => false,
                  "CatalogName" => "hive",
                  "Label" => "id",
                  "Name" => "id",
                  "Nullable" => "UNKNOWN",
                  "Precision" => 10,
                  "Scale" => 0,
                  "SchemaName" => "",
                  "TableName" => "",
                  "Type" => "integer"
                },
                %{
                  "CaseSensitive" => true,
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
              %{"Data" => [%{"VarCharValue" => "id"}, %{"VarCharValue" => "name"}]},
              %{"Data" => [%{"VarCharValue" => "1"}, %{"VarCharValue" => "Ale"}]},
              %{"Data" => [%{"VarCharValue" => "2"}, %{"VarCharValue" => "Wojtek"}]}
            ]
          },
          "UpdateCount" => 0
        }

        if fun = results["GetQueryResults"] do
          fun.(request)
        else
          {request, %Req.Response{status: 200, body: Jason.encode!(original_data)}}
        end

      %{private: %{athena_action: "GetQueryExecution"}} = request ->
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"

        assert Req.Request.get_header(request, "x-amz-target") == [
                 "AmazonAthena.GetQueryExecution"
               ]

        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        if fun = validations["GetQueryExecution"] do
          fun.(request)
        else
          assert Jason.decode!(request.body) == %{"QueryExecutionId" => "an uuid"}
        end

        original_data = %{
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

        if fun = results["GetQueryExecution"] do
          fun.(request)
        else
          {request, %Req.Response{status: 200, body: Jason.encode!(original_data)}}
        end

      %{private: %{athena_action: "StartQueryExecution"}} = request ->
        assert URI.to_string(request.url) == "https://athena.us-east-1.amazonaws.com"

        assert Req.Request.get_header(request, "x-amz-target") == [
                 "AmazonAthena.StartQueryExecution"
               ]

        assert Req.Request.get_header(request, "host") == ["athena.us-east-1.amazonaws.com"]
        assert Req.Request.get_header(request, "content-type") == ["application/x-amz-json-1.1"]

        [value] = Req.Request.get_header(request, "authorization")
        assert value =~ "us-east-1/athena/aws4_request"

        if fun = validations["StartQueryExecution"] do
          fun.(request)
        else
          client_req_token =
            case :erlang.system_info(:otp_release) do
              ~c"27" -> "4666A0FE3AC8B51C4785F1868BFD29C2"
              _ -> "279A8BD03538A2F33C1B13DF28FF1966"
            end

          decoded = Jason.decode!(request.body)

          assert %{
                   "ClientRequestToken" => ^client_req_token,
                   "QueryExecutionContext" => %{
                     "Database" => "my_awesome_database"
                   },
                   "QueryString" => "select * from iris",
                   "ResultConfiguration" => %{"OutputLocation" => "s3://foo"}
                 } = decoded
        end

        original_data = %{QueryExecutionId: "an uuid"}

        if fun = results["StartQueryExecution"] do
          fun.(request)
        else
          {request, %Req.Response{status: 200, body: Jason.encode!(original_data)}}
        end
    end
  end
end

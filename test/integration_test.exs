defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  test "returns the response from AWS Athena's API" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "livebook",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    assert response =
             Req.new(http_errors: :raise)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "SELECT * FROM iris LIMIT 1")

    assert response.status == 200
    result = response.body

    assert result == %{
             "ResultSet" => %{
               "ColumnInfos" => [
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "sepal_length",
                   "Name" => "sepal_length",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 17,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "float"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "sepal_width",
                   "Name" => "sepal_width",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 17,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "float"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "petal_length",
                   "Name" => "petal_length",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 17,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "float"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "petal_width",
                   "Name" => "petal_width",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 17,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "float"
                 },
                 %{
                   "CaseSensitive" => true,
                   "CatalogName" => "hive",
                   "Label" => "variety",
                   "Name" => "variety",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 2_147_483_647,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "varchar"
                 }
               ],
               "ResultRows" => [
                 %{
                   "Data" => [
                     "sepal_length",
                     "sepal_width",
                     "petal_length",
                     "petal_width",
                     "variety"
                   ]
                 },
                 %{"Data" => [nil, nil, nil, nil, nil]}
               ],
               "ResultSetMetadata" => %{
                 "ColumnInfo" => [
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "sepal_length",
                     "Name" => "sepal_length",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 17,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "float"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "sepal_width",
                     "Name" => "sepal_width",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 17,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "float"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "petal_length",
                     "Name" => "petal_length",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 17,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "float"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "petal_width",
                     "Name" => "petal_width",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 17,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "float"
                   },
                   %{
                     "CaseSensitive" => true,
                     "CatalogName" => "hive",
                     "Label" => "variety",
                     "Name" => "variety",
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
                 %{
                   "Data" => [
                     %{"VarCharValue" => "sepal_length"},
                     %{"VarCharValue" => "sepal_width"},
                     %{"VarCharValue" => "petal_length"},
                     %{"VarCharValue" => "petal_width"},
                     %{"VarCharValue" => "variety"}
                   ]
                 },
                 %{"Data" => [%{}, %{}, %{}, %{}, %{}]}
               ]
             },
             "UpdateCount" => 0
           }
  end

  test "wait until query is finished" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "livebook",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    assert response =
             Req.new(http_errors: :raise)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "SELECT * FROM iris")

    assert response.status == 200
    assert %{"ResultSet" => _} = response.body
  end
end

defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  @create_table """
  CREATE EXTERNAL TABLE IF NOT EXISTS planet (
    id BIGINT,
    type STRING,
    tags MAP<STRING,STRING>,
    lat DECIMAL(9,7),
    lon DECIMAL(10,7),
    nds ARRAY<STRUCT<REF:BIGINT>>,
    members ARRAY<STRUCT<TYPE:STRING,REF:BIGINT,ROLE:STRING>>,
    changeset BIGINT,
    timestamp TIMESTAMP,
    uid BIGINT,
    user STRING,
    version BIGINT,
    visible BOOLEAN
  )
  STORED AS ORCFILE
  LOCATION 's3://osm-pds/planet/';\
  """

  test "returns the response from AWS Athena's API" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    setup_database!(opts)

    assert response =
             Req.new(http_errors: :raise)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "SELECT * FROM planet LIMIT 1")

    assert response.status == 200
    result = response.body

    assert result == %{
             "ResultSet" => %{
               "ColumnInfos" => [
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "id",
                   "Name" => "id",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 19,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "bigint"
                 },
                 %{
                   "CaseSensitive" => true,
                   "CatalogName" => "hive",
                   "Label" => "type",
                   "Name" => "type",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 2_147_483_647,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "varchar"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "tags",
                   "Name" => "tags",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 0,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "map"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "lat",
                   "Name" => "lat",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 9,
                   "Scale" => 7,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "decimal"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "lon",
                   "Name" => "lon",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 10,
                   "Scale" => 7,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "decimal"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "nds",
                   "Name" => "nds",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 0,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "array"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "members",
                   "Name" => "members",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 0,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "array"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "changeset",
                   "Name" => "changeset",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 19,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "bigint"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "timestamp",
                   "Name" => "timestamp",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 3,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "timestamp"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "uid",
                   "Name" => "uid",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 19,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "bigint"
                 },
                 %{
                   "CaseSensitive" => true,
                   "CatalogName" => "hive",
                   "Label" => "user",
                   "Name" => "user",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 2_147_483_647,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "varchar"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "version",
                   "Name" => "version",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 19,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "bigint"
                 },
                 %{
                   "CaseSensitive" => false,
                   "CatalogName" => "hive",
                   "Label" => "visible",
                   "Name" => "visible",
                   "Nullable" => "UNKNOWN",
                   "Precision" => 0,
                   "Scale" => 0,
                   "SchemaName" => "",
                   "TableName" => "",
                   "Type" => "boolean"
                 }
               ],
               "ResultRows" => [
                 %{
                   "Data" => [
                     "id",
                     "type",
                     "tags",
                     "lat",
                     "lon",
                     "nds",
                     "members",
                     "changeset",
                     "timestamp",
                     "uid",
                     "user",
                     "version",
                     "visible"
                   ]
                 },
                 %{
                   "Data" => [
                     "239970142",
                     "node",
                     "{created_by=JOSM}",
                     "-2.1627500",
                     "139.3920000",
                     "[]",
                     "[]",
                     "665284",
                     "2008-01-19 15:43:38.000",
                     "7744",
                     "Keith Thomson",
                     "1",
                     "true"
                   ]
                 }
               ],
               "ResultSetMetadata" => %{
                 "ColumnInfo" => [
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "id",
                     "Name" => "id",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 19,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "bigint"
                   },
                   %{
                     "CaseSensitive" => true,
                     "CatalogName" => "hive",
                     "Label" => "type",
                     "Name" => "type",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 2_147_483_647,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "varchar"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "tags",
                     "Name" => "tags",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 0,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "map"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "lat",
                     "Name" => "lat",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 9,
                     "Scale" => 7,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "decimal"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "lon",
                     "Name" => "lon",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 10,
                     "Scale" => 7,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "decimal"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "nds",
                     "Name" => "nds",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 0,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "array"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "members",
                     "Name" => "members",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 0,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "array"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "changeset",
                     "Name" => "changeset",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 19,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "bigint"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "timestamp",
                     "Name" => "timestamp",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 3,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "timestamp"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "uid",
                     "Name" => "uid",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 19,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "bigint"
                   },
                   %{
                     "CaseSensitive" => true,
                     "CatalogName" => "hive",
                     "Label" => "user",
                     "Name" => "user",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 2_147_483_647,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "varchar"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "version",
                     "Name" => "version",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 19,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "bigint"
                   },
                   %{
                     "CaseSensitive" => false,
                     "CatalogName" => "hive",
                     "Label" => "visible",
                     "Name" => "visible",
                     "Nullable" => "UNKNOWN",
                     "Precision" => 0,
                     "Scale" => 0,
                     "SchemaName" => "",
                     "TableName" => "",
                     "Type" => "boolean"
                   }
                 ]
               },
               "Rows" => [
                 %{
                   "Data" => [
                     %{"VarCharValue" => "id"},
                     %{"VarCharValue" => "type"},
                     %{"VarCharValue" => "tags"},
                     %{"VarCharValue" => "lat"},
                     %{"VarCharValue" => "lon"},
                     %{"VarCharValue" => "nds"},
                     %{"VarCharValue" => "members"},
                     %{"VarCharValue" => "changeset"},
                     %{"VarCharValue" => "timestamp"},
                     %{"VarCharValue" => "uid"},
                     %{"VarCharValue" => "user"},
                     %{"VarCharValue" => "version"},
                     %{"VarCharValue" => "visible"}
                   ]
                 },
                 %{
                   "Data" => [
                     %{"VarCharValue" => "239970142"},
                     %{"VarCharValue" => "node"},
                     %{"VarCharValue" => "{created_by=JOSM}"},
                     %{"VarCharValue" => "-2.1627500"},
                     %{"VarCharValue" => "139.3920000"},
                     %{"VarCharValue" => "[]"},
                     %{"VarCharValue" => "[]"},
                     %{"VarCharValue" => "665284"},
                     %{"VarCharValue" => "2008-01-19 15:43:38.000"},
                     %{"VarCharValue" => "7744"},
                     %{"VarCharValue" => "Keith Thomson"},
                     %{"VarCharValue" => "1"},
                     %{"VarCharValue" => "true"}
                   ]
                 }
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
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    setup_database!(opts)

    assert response =
             Req.new(http_errors: :raise)
             |> ReqAthena.attach(opts)
             |> Req.post!(athena: "SELECT * FROM planet LIMIT 100")

    assert response.status == 200
    assert %{"ResultSet" => _} = response.body
  end

  defp setup_database!(opts) do
    req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)
    assert Req.post!(req, athena: @create_table).status == 200
  end
end

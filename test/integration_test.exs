defmodule IntegrationTest do
  use ExUnit.Case
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

  test "without a given format returns the response as it is from the API" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """
             )

    assert query_response.status == 200

    assert %{
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
                 %{"Data" => ["id", "type", "tags", "members", "timestamp", "visible"]},
                 %{
                   "Data" => [
                     "470454",
                     "relation",
                     "{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}",
                     "[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]",
                     "2017-01-21 12:51:34.000",
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
                     %{"VarCharValue" => "members"},
                     %{"VarCharValue" => "timestamp"},
                     %{"VarCharValue" => "visible"}
                   ]
                 },
                 %{
                   "Data" => [
                     %{"VarCharValue" => "470454"},
                     %{"VarCharValue" => "relation"},
                     %{
                       "VarCharValue" =>
                         "{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}"
                     },
                     %{
                       "VarCharValue" =>
                         "[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]"
                     },
                     %{"VarCharValue" => "2017-01-21 12:51:34.000"},
                     %{"VarCharValue" => "true"}
                   ]
                 }
               ]
             },
             "UpdateCount" => 0
           } == query_response.body
  end

  test "returns the response in an Explorer dataframe" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """,
               [],
               format: :explorer
             )

    assert query_response.status == 200

    assert %Explorer.DataFrame{} = ldf = query_response.body
    assert Explorer.DataFrame.lazy?(ldf)

    names = [
      "id",
      "type",
      "tags",
      "members",
      "timestamp",
      "visible"
    ]

    values = [
      470_454,
      "relation",
      [
        %{
          "key" => "source",
          "value" => "©IGN 2010 dans le cadre de la cartographie réglementaire"
        },
        %{"key" => "site", "value" => "geodesic"},
        %{
          "key" => "url",
          "value" =>
            "http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A"
        },
        %{"key" => "name", "value" => "Mérignac A"},
        %{"key" => "network", "value" => "NTF-5"},
        %{"key" => "ref", "value" => "17229A"},
        %{"key" => "type", "value" => "site"}
      ],
      [
        %{"ref" => 670_007_839, "role" => "", "type" => "node"},
        %{"ref" => 670_007_840, "role" => "", "type" => "node"}
      ],
      ~N[2017-01-21 12:51:34.000000],
      true
    ]

    df = Explorer.DataFrame.collect(ldf)

    assert Explorer.DataFrame.names(df) == names
    assert Explorer.DataFrame.to_rows(df) == [Map.new(Enum.zip(names, values))]
  end

  test "format as explorer without decoding body returns the list of parquet files" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """,
               [],
               format: :explorer,
               decode_body: false
             )

    assert query_response.status == 200

    assert [first_file | _] = query_response.body

    assert String.starts_with?(first_file, "s3://")
  end

  test "returns the response as CSV" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """,
               [],
               format: :csv
             )

    assert query_response.status == 200

    assert query_response.body ==
             ~s|"id","type","tags","members","timestamp","visible"
"470454","relation","{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}","[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]","2017-01-21 12:51:34.000","true"
|
  end

  test "format as CSV without decoding body returns the CSV file path" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """,
               [],
               format: :csv,
               decode_body: false
             )

    assert query_response.status == 200
    assert String.starts_with?(query_response.body, "s3://")
    assert String.ends_with?(query_response.body, ".csv")
  end

  test "returns the response as a list of JSON objects" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE (id in (470454, 470455))
                  and type = 'relation'
               """,
               [],
               format: :json
             )

    assert query_response.status == 200

    assert query_response.body ==
             [
               %{
                 "id" => 470_454,
                 "members" => [
                   %{"ref" => 670_007_839, "role" => "", "type" => "node"},
                   %{"ref" => 670_007_840, "role" => "", "type" => "node"}
                 ],
                 "tags" => %{
                   "name" => "Mérignac A",
                   "network" => "NTF-5",
                   "ref" => "17229A",
                   "site" => "geodesic",
                   "source" => "©IGN 2010 dans le cadre de la cartographie réglementaire",
                   "type" => "site",
                   "url" =>
                     "http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A"
                 },
                 "timestamp" => "2017-01-21 12:51:34",
                 "type" => "relation",
                 "visible" => true
               },
               %{
                 "id" => 470_455,
                 "members" => [%{"ref" => 670_007_841, "role" => "", "type" => "node"}],
                 "tags" => %{
                   "name" => "Meschers-sur-Gironde A",
                   "network" => "NTF-5",
                   "ref" => "17230A",
                   "site" => "geodesic",
                   "source" => "©IGN 2010 dans le cadre de la cartographie réglementaire",
                   "type" => "site",
                   "url" =>
                     "http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17230A"
                 },
                 "timestamp" => "2017-01-21 12:51:34",
                 "type" => "relation",
                 "visible" => true
               }
             ]
  end

  test "returns the response as a list paths to NDJSON files" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """,
               [],
               format: :json,
               decode_body: false
             )

    assert query_response.status == 200

    assert [first_file | _] = query_response.body

    assert String.starts_with?(first_file, "s3://")
  end

  test "returns the response from AWS Athena's API with parameterized query" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: Path.join(System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"), "test-#{now}")
    ]

    # create table
    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             ReqAthena.query!(
               req,
               "SELECT id, type FROM planet WHERE id = ? and type = ?",
               [239_970_142, "node"],
               format: :json
             )

    assert query_response.status == 200

    assert query_response.body == [%{"id" => 239_970_142, "type" => "node"}]
  end

  test "returns failed AWS Athena's response" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req = ReqAthena.new(opts)
    response = ReqAthena.query!(req, "SELECT ? + 10", ["foo"])

    assert response.status == 200
    assert response.body["QueryExecution"]["Status"]["State"] == "FAILED"

    assert response.body["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"] ==
             "line 1:11: '+' cannot be applied to varchar(3), integer"

    assert_raise RuntimeError,
                 "failed query with error: line 1:8: Column 'foo' cannot be resolved",
                 fn -> ReqAthena.query!(req, "SELECT foo", [], http_errors: :raise) end

    assert_raise RuntimeError,
                 "failed query with error: line 1:11: '+' cannot be applied to varchar(3), integer",
                 fn ->
                   ReqAthena.query!(req, "SELECT ? + 10", ["foo"], http_errors: :raise)
                 end
  end

  test "creates table inside AWS Athena's database with session credentials" do
    opts = [
      access_key_id: System.fetch_env!("AWS_TOKEN_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_TOKEN_SECRET_ACCESS_KEY"),
      token: System.fetch_env!("AWS_TOKEN_SESSION_TOKEN"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
    response = ReqAthena.query!(req, @create_table)

    assert response.status == 200
  end

  # TODO: check why it's not working only with "workgroup"
  # test "creates table inside AWS Athena's database with workgroup" do
  #   opts = [
  #     access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  #     secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
  #     region: System.fetch_env!("AWS_REGION"),
  #     database: "default",
  #     workgroup: "primary"
  #   ]

  #   req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
  #   response = ReqAthena.query!(req, @create_table)

  #   assert response.status == 200
  # end

  test "creates table inside AWS Athena's database with workgroup and output location" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      workgroup: "primary",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
    response = ReqAthena.query!(req, @create_table)
    assert %{} = response.body

    assert response.status == 200
  end

  test "returns the cached result from AWS Athena's response" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    query = """
    SELECT id, type, tags, members, timestamp, visible
      FROM planet
     WHERE id = 470454
       and type = 'relation'\
    """

    assert query_response = ReqAthena.query!(req, query)
    assert query_response.status == 200

    result = query_response.body

    assert response = ReqAthena.query!(req, query)
    assert response.status == 200

    assert result == response.body
  end

  test "force new result from AWS Athena's response" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION"),
      cache_query: false
    ]

    req =
      ReqAthena.new(opts)
      |> Req.merge(http_errors: :raise)

    query = """
    SELECT id, type, tags, members, timestamp, visible
      FROM planet
     WHERE id = 470454
       and type = 'relation'\
    """

    assert query_response = ReqAthena.query!(req, query)
    assert query_response.status == 200

    result = query_response.body

    assert response = ReqAthena.query!(req, query)
    assert response.status == 200

    assert result == response.body
  end

  describe "with aws_credentials" do
    @path Path.expand("./tmp/") <> "/"

    setup tags do
      if env = tags[:aws_credentials] do
        for {k, v} <- env do
          Application.put_env(:aws_credentials, k, v)
        end
      end

      if envs = tags[:envs] do
        for {k, v} <- envs do
          System.put_env(k, v)
          on_exit(fn -> System.delete_env(k) end)
        end
      end

      Application.stop(:aws_credentials)
      Application.start(:aws_credentials)

      :ok
    end

    @tag capture_log: true,
         aws_credentials: [
           credential_providers: [:aws_credentials_env]
         ]
    test "gets from system env and create table" do
      opts = [
        database: "default",
        output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ]

      req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
      response = ReqAthena.query!(req, @create_table)

      assert response.status == 200
    end

    @tag capture_log: true,
         envs: %{
           "AWS_CONFIG_FILE" => @path <> "config",
           "AWS_SHARED_CREDENTIALS_FILE" => @path <> "credentials"
         },
         aws_credentials: [
           credential_providers: [:aws_credentials_file],
           provider_options: %{credential_path: to_charlist(Path.expand("./"))}
         ]
    test "gets from the files from env and create table" do
      opts = [
        database: "default",
        output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ]

      req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
      response = ReqAthena.query!(req, @create_table)

      assert response.status == 200
    end

    @tag capture_log: true,
         envs: %{
           "AWS_CONFIG_FILE" => @path <> "config",
           "AWS_SHARED_CREDENTIALS_FILE" => @path <> "credentials_with_token"
         },
         aws_credentials: [
           credential_providers: [:aws_credentials_file],
           provider_options: %{credential_path: to_charlist(Path.expand("./"))}
         ]
    test "gets from the files from env with session token and create table" do
      opts = [
        database: "default",
        output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ]

      req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
      response = ReqAthena.query!(req, @create_table)

      assert response.status == 200
    end

    @tag capture_log: true,
         aws_credentials: [
           credential_providers: [:aws_credentials_file],
           provider_options: %{credential_path: to_charlist(@path)}
         ]
    test "gets from file system and create table" do
      opts = [
        database: "default",
        output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
      ]

      req = ReqAthena.new(opts) |> Req.merge(http_errors: :raise)
      response = ReqAthena.query!(req, @create_table)

      assert response.status == 200
    end
  end
end

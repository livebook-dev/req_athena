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

    # create table
    req =
      Req.new(http_errors: :raise)
      |> ReqAthena.attach(opts)

    response = Req.post!(req, athena: @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             Req.post!(req,
               athena: """
               SELECT id, type, tags, members, timestamp, visible
                 FROM planet
                WHERE id = 470454
                  and type = 'relation'
               """
             )

    assert query_response.status == 200

    assert query_response.body.columns == [
             "id",
             "type",
             "tags",
             "members",
             "timestamp",
             "visible"
           ]

    refute query_response.body.statement_name
    assert is_binary(query_response.body.query_execution_id)

    assert query_response.body.output_location ==
             "#{opts[:output_location]}/#{query_response.body.query_execution_id}.csv"

    assert query_response.body.rows == [
             [
               470_454,
               "relation",
               "{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}",
               "[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]",
               ~N[2017-01-21 12:51:34.000],
               true
             ]
           ]
  end

  test "returns the response from AWS Athena's API with parameterized query" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    # create table
    req =
      Req.new(http_errors: :raise)
      |> ReqAthena.attach(opts)

    response = Req.post!(req, athena: @create_table)

    assert response.status == 200

    # query single row from planet table
    assert query_response =
             Req.post!(req,
               athena:
                 {"SELECT id, type FROM planet WHERE id = ? and type = ?", [239_970_142, "node"]}
             )

    assert query_response.status == 200
    assert query_response.body.columns == ["id", "type"]
    assert query_response.body.statement_name == "query_C71EF77B8B7B92D9846C6D7E70136448"
    assert is_binary(query_response.body.query_execution_id)
    assert query_response.body.rows == [[239_970_142, "node"]]

    assert query_response.body.output_location ==
             "#{opts[:output_location]}/#{query_response.body.query_execution_id}.csv"
  end

  test "encodes and decodes types received from AWS Athena's response" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)

    value = "req"
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 1
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 1.1
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = -1.1
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = true
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 1.175494351e-38
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = 3.402823466e+38
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = Date.utc_today()
    query = "SELECT CAST(? AS DATE)"
    assert Req.post!(req, athena: {query, [value]}).body.rows == [[value]]

    naive_dt = NaiveDateTime.utc_now()
    value = NaiveDateTime.truncate(naive_dt, :millisecond)
    query = "SELECT CAST(? AS TIMESTAMP)"
    assert Req.post!(req, athena: {query, [naive_dt]}).body.rows == [[value]]

    datetime = DateTime.utc_now()
    value = DateTime.to_naive(datetime) |> NaiveDateTime.truncate(:millisecond)
    assert Req.post!(req, athena: {query, [datetime]}).body.rows == [[value]]

    query = "SELECT timestamp '2012-10-31 01:00:00.000 UTC' AT TIME ZONE 'America/Sao_Paulo'"
    value = DateTime.new!(~D[2012-10-30], ~T[23:00:00.000], "America/Sao_Paulo")
    assert Req.post!(req, athena: query).body.rows == [[value]]

    value = "{name=aleDsz, id=1}"
    query = "SELECT MAP(ARRAY['name', 'id'], ARRAY['aleDsz', '1'])"
    assert Req.post!(req, athena: query).body.rows == [[value]]

    value = "{ids=[10, 20]}"
    query = "SELECT CAST(ROW(ARRAY[10, 20]) AS ROW(ids ARRAY<INTEGER>))"
    assert Req.post!(req, athena: query).body.rows == [[value]]
  end

  test "returns failed AWS Athena's response" do
    opts = [
      access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
      region: System.fetch_env!("AWS_REGION"),
      database: "default",
      output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
    ]

    req = Req.new() |> ReqAthena.attach(opts)
    response = Req.post!(req, athena: {"SELECT ? + 10", ["foo"]})

    assert response.status == 200
    assert response.body["QueryExecution"]["Status"]["State"] == "FAILED"

    assert response.body["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"] ==
             "line 1:11: '+' cannot be applied to varchar(3), integer"

    assert_raise RuntimeError,
                 "failed query with error: line 1:8: Column 'foo' cannot be resolved",
                 fn -> Req.post!(req, http_errors: :raise, athena: "SELECT foo") end

    assert_raise RuntimeError,
                 "failed query with error: line 1:11: '+' cannot be applied to varchar(3), integer",
                 fn -> Req.post!(req, http_errors: :raise, athena: {"SELECT ? + 10", ["foo"]}) end
  end

  if Code.ensure_loaded?(:aws_credentials) do
    describe "with aws_credentials" do
      @path Path.expand("./config/") <> "/"

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

        on_exit(fn -> Application.stop(:aws_credentials) end)

        :ok
      end

      @tag capture_log: true,
           aws_credentials: [
             fail_if_unavailable: false,
             credential_providers: [:aws_credentials_env]
           ]
      test "get's from system env and create table" do
        opts = [
          database: "default",
          output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
        ]

        req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)
        response = Req.post!(req, athena: @create_table)

        assert response.status == 200
      end

      @tag capture_log: true,
           envs: %{
             "AWS_CONFIG_FILE" => @path <> "config",
             "AWS_SHARED_CREDENTIALS_FILE" => @path <> "credentials"
           },
           aws_credentials: [
             fail_if_unavailable: false,
             credential_providers: [:aws_credentials_file],
             provider_options: %{credential_path: to_charlist(Path.expand("./"))}
           ]
      test "get's from the files from env and create table" do
        opts = [
          database: "default",
          output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
        ]

        req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)
        response = Req.post!(req, athena: @create_table)

        assert response.status == 200
      end

      @tag capture_log: true,
           envs: %{
             "AWS_CONFIG_FILE" => @path <> "config",
             "AWS_SHARED_CREDENTIALS_FILE" => @path <> "credentials_with_token"
           },
           aws_credentials: [
             fail_if_unavailable: false,
             credential_providers: [:aws_credentials_file],
             provider_options: %{credential_path: to_charlist(Path.expand("./"))}
           ]
      test "get's from the files from env with session token and create table" do
        opts = [
          database: "default",
          output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
        ]

        req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)
        response = Req.post!(req, athena: @create_table)

        assert response.status == 200
      end

      @tag capture_log: true,
           aws_credentials: [
             fail_if_unavailable: false,
             credential_providers: [:aws_credentials_file],
             provider_options: %{credential_path: to_charlist(@path)}
           ]
      test "get's from file system and create table" do
        opts = [
          database: "default",
          output_location: System.fetch_env!("AWS_ATHENA_OUTPUT_LOCATION")
        ]

        req = Req.new(http_errors: :raise) |> ReqAthena.attach(opts)
        response = Req.post!(req, athena: @create_table)

        assert response.status == 200
      end
    end
  end
end

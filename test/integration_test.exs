defmodule IntegrationTest do
  use ExUnit.Case, async: true
  @moduletag :integration

  @create_table """
  CREATE TABLE IF NOT EXISTS planet (
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
    assert response.body == %ReqAthena.Result{columns: [], rows: [], statement_name: nil}

    # query single row from planet table
    assert query_response =
             Req.post!(req,
               athena:
                 "SELECT id, type, tags, nds, members, timestamp, visible FROM planet WHERE id = 1641521394 and type = 'node'"
             )

    assert query_response.status == 200

    assert query_response.body == %ReqAthena.Result{
             columns: [
               "id",
               "type",
               "tags",
               "nds",
               "members",
               "timestamp",
               "visible"
             ],
             rows: [
               [
                 1_641_521_394,
                 "node",
                 %{"natural" => "tree", "source" => "bing"},
                 [],
                 [],
                 ~N[2012-02-21 07:53:08],
                 true
               ]
             ],
             statement_name: nil
           }
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
    assert response.body == %ReqAthena.Result{columns: [], rows: [], statement_name: nil}

    # query single row from planet table
    assert query_response =
             Req.post!(req,
               athena:
                 {"SELECT id, type FROM planet WHERE id = ? and type = ?", [239_970_142, "node"]}
             )

    assert query_response.status == 200

    assert query_response.body == %ReqAthena.Result{
             columns: ["id", "type"],
             rows: [[239_970_142, "node"]],
             statement_name: "query_C71EF77B8B7B92D9846C6D7E70136448"
           }
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

    value = Decimal.new("1.1")
    query = "SELECT CAST(CAST(? AS DOUBLE) AS DECIMAL(38,1))"
    assert Req.post!(req, athena: {query, [value]}).body.rows == [[value]]

    value = Decimal.new("1.10")
    query = "SELECT CAST(CAST(? AS DOUBLE) AS DECIMAL(38,2))"
    assert Req.post!(req, athena: {query, [value]}).body.rows == [[value]]

    value = Decimal.new("-1.1")
    query = "SELECT CAST(CAST(? AS DOUBLE) AS DECIMAL(38,1))"
    assert Req.post!(req, athena: {query, [value]}).body.rows == [[value]]

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

    value = String.to_float("1.175494351E-38")
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = String.to_float("3.402823466E+38")
    assert Req.post!(req, athena: {"SELECT ?", [value]}).body.rows == [[value]]

    value = Date.utc_today()
    query = "SELECT CAST(? AS DATE)"
    assert Req.post!(req, athena: {query, [value]}).body.rows == [[value]]

    naive_dt = NaiveDateTime.utc_now()
    value = NaiveDateTime.truncate(naive_dt, :second)
    query = "SELECT CAST(? AS TIMESTAMP)"
    assert Req.post!(req, athena: {query, [naive_dt]}).body.rows == [[value]]

    datetime = DateTime.utc_now()
    value = DateTime.to_naive(datetime) |> NaiveDateTime.truncate(:second)
    assert Req.post!(req, athena: {query, [datetime]}).body.rows == [[value]]

    query = "SELECT timestamp '2012-10-31 01:00:00 UTC' AT TIME ZONE 'America/Sao_Paulo'"
    value = DateTime.new!(~D[2012-10-30], ~T[23:00:00], "America/Sao_Paulo")
    assert Req.post!(req, athena: query).body.rows == [[value]]

    value = %{"id" => "1", "name" => "aleDsz"}
    query = "SELECT MAP(ARRAY['name', 'id'], ARRAY['aleDsz', '1'])"
    assert Req.post!(req, athena: query).body.rows == [[value]]

    value = %{"ids" => [10, 20]}
    query = "SELECT CAST(ROW(ARRAY[10, 20]) AS ROW(ids ARRAY<INTEGER>))"
    assert Req.post!(req, athena: query).body.rows == [[value]]
  end
end

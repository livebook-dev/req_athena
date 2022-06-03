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
               athena: "SELECT id, type FROM planet WHERE id = 1641521394 and type = 'node'"
             )

    assert query_response.status == 200

    assert query_response.body == %ReqAthena.Result{
             columns: ["id", "type"],
             rows: [[1_641_521_394, "node"]],
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
             statement_name: "C71EF77B8B7B92D9846C6D7E70136448"
           }
  end
end

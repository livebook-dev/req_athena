defmodule ReqAthena.QueryTest do
  use ExUnit.Case, async: true

  alias ReqAthena.Query

  describe "to_query_string/1" do
    test "simple query without params" do
      query = %Query{query: "SELECT name, id FROM users"}

      assert Query.to_query_string(query) == "SELECT name, id FROM users"
    end

    test "query with params unprepared" do
      query = %Query{
        query: "SELECT name, id FROM users WHERE id > ?",
        params: [420],
        statement_name: "test_statement"
      }

      assert Query.to_query_string(query) ==
               "PREPARE test_statement FROM SELECT name, id FROM users WHERE id > ?"
    end

    test "query with params and prepared" do
      query = %Query{
        query: "SELECT name, id FROM users WHERE id > ?",
        params: [420],
        prepared: true,
        statement_name: "test_statement"
      }

      assert Query.to_query_string(query) ==
               "EXECUTE test_statement USING 420"
    end
  end

  describe "with_unload/2" do
    test "unload attributes" do
      query = %Query{query: "SELECT name, id FROM users"}
      query = Query.with_unload(query, to: "s3://my-bucket/my-dir")

      assert Query.to_query_string(query) ==
               "UNLOAD (SELECT name, id FROM users)\nTO 's3://my-bucket/my-dir'\nWITH (compression = 'SNAPPY', format = 'PARQUET')"
    end

    test "unload attributes and a prepare statement does use unload command" do
      query = %Query{
        query: "SELECT name, id FROM users WHERE id > ?",
        params: [420],
        statement_name: "test_statement"
      }

      query = Query.with_unload(query, to: "s3://my-bucket/my-dir")

      assert Query.to_query_string(query) ==
               "PREPARE test_statement FROM UNLOAD (SELECT name, id FROM users WHERE id > ?)\nTO 's3://my-bucket/my-dir'\nWITH (compression = 'SNAPPY', format = 'PARQUET')"
    end

    test "unload attributes and an execute command does not use the unload command" do
      query = %Query{
        query: "SELECT name, id FROM users WHERE id > ?",
        params: [420],
        prepared: true,
        statement_name: "test_statement"
      }

      query = Query.with_unload(query, to: "s3://my-bucket/my-dir")

      assert Query.to_query_string(query) ==
               "EXECUTE test_statement USING 420"
    end
  end
end

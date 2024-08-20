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
end

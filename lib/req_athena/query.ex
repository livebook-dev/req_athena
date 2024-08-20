defmodule ReqAthena.Query do
  @moduledoc false
  # This module represents a query and its attributes.

  defstruct query: nil, params: nil, statement_name: nil, prepared: false

  @doc """
  Returns if this query is using params or not.
  """
  def parameterized?(%__MODULE__{} = query), do: List.wrap(query.params) != []

  @doc """
  Returns if this query is using params and if it was not prepared.

  This is useful to determine if the query is going to perform an "EXECUTE" or
  a "PREPARE" command.
  """
  def to_prepare?(%__MODULE__{} = query), do: parameterized?(query) and query.prepared == false

  @doc """
  Builds the final query to send to the Athena service.
  """
  def to_query_string(%__MODULE__{} = query) do
    cond do
      query.prepared ->
        "EXECUTE #{query.statement_name} USING " <>
          Enum.map_join(query.params, ", ", &encode_value/1)

      parameterized?(query) ->
        if is_nil(query.statement_name),
          do: raise(":statement_name is required for a parameterized query")

        "PREPARE #{query.statement_name} FROM #{query.query}"

      true ->
        query.query
    end
  end

  defp encode_value(value) when is_binary(value), do: "'#{value}'"
  defp encode_value(%Date{} = value), do: to_string(value) |> encode_value()

  defp encode_value(%DateTime{} = value) do
    value
    |> DateTime.to_naive()
    |> encode_value()
  end

  defp encode_value(%NaiveDateTime{} = value) do
    value
    |> NaiveDateTime.truncate(:millisecond)
    |> to_string()
    |> encode_value()
  end

  defp encode_value(value), do: value
end

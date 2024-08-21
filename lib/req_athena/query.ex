defmodule ReqAthena.Query do
  @moduledoc false
  # This module represents a query and its attributes.

  defstruct query: nil, params: nil, statement_name: nil, prepared: false, unload: nil

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

        "PREPARE #{query.statement_name} FROM #{maybe_around_unload(query)}"

      true ->
        maybe_around_unload(query)
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

  defp maybe_around_unload(%{query: query_string, unload: [_ | _] = opts})
       when is_binary(query_string) do
    # UNLOAD works only with SELECT
    if query_string =~ ~r/^[\s]*select/i do
      {to, props} = Keyword.pop!(opts, :to)

      props =
        Enum.intersperse(
          for(
            {key, value} <- props,
            not is_nil(value),
            do: [Atom.to_string(key), " = ", encode_value(value)]
          ),
          ", "
        )

      IO.iodata_to_binary([
        "UNLOAD (",
        query_string,
        ")",
        "\n",
        "TO ",
        encode_value(to),
        "\n",
        "WITH (",
        props,
        ")"
      ])
    else
      query_string
    end
  end

  defp maybe_around_unload(%{query: query_string}), do: query_string

  @doc """
  Add attributes required by the "UNLOAD" command.

  See: https://docs.aws.amazon.com/athena/latest/ug/unload.html
  """
  def with_unload(%__MODULE__{} = query, opts) do
    opts =
      Keyword.validate!(opts,
        to: nil,
        format: "PARQUET",
        compression: "SNAPPY",
        compression_level: nil,
        field_delimiter: nil,
        partitioned_by: nil
      )

    if opts[:to] in ["", nil] do
      raise "`:to` is required by UNLOAD"
    end

    %{query | unload: opts}
  end
end

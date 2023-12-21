defmodule ReqAthena.Result do
  @moduledoc """
  Result struct returned from any successful query.

  Its fields are:

    * `columns` - The column names;
    * `rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `statement_name` - The statement name from executed query;
    * `query_execution_id` - The id from executed query;
    * `output_location` - The S3 url location where the result was output.
    * `metadata` - The columnInfo from https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html
  """

  @type t :: %__MODULE__{
          columns: [String.t()],
          rows: [[term()]],
          statement_name: binary(),
          query_execution_id: binary(),
          output_location: binary(),
          metadata: %{column_infos: [term()]}
        }

  defstruct [
    :statement_name,
    :query_execution_id,
    :output_location,
    rows: [],
    columns: [],
    metadata: []
  ]
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: ReqAthena.Result do
    def init(result) do
      {:rows, %{{:athena, :column_infos} => result.metadata, columns: result.columns}, result.rows}
    end
  end
end

# ReqAthena

[Req](https://github.com/wojtekmach/req) plugin for [AWS Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

ReqAthena makes it easy to make Athena queries. Query results are decoded into the `ReqAthena.Result` struct.
The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.

## Usage

```elixir
Mix.install([
  {:req, github: "wojtekmach/req"},
  {:req_athena, github: "livebook-dev/req_athena"}
])

opts = [
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
  region: System.fetch_env!("AWS_REGION"),
  database: "default",
  output_location: "s3://my-bucket"
]

req = Req.new() |> ReqAthena.attach(opts)

# Create table from Registry of Open Data on AWS
# See: https://registry.opendata.aws/osm/
query = \"""
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
LOCATION 's3://osm-pds/planet/';
\"""

Req.post!(req, athena: query).body
#=>
# %ReqAthena.Result{
#   columns: [],
#   output_location: "s3://my-bucket/a034610b-daaf-4c8d-aa61-d1a706231062.txt",
#   query_execution_id: "a034610b-daaf-4c8d-aa61-d1a706231062",
#   rows: [],
#   statement_name: nil
# }

# With plain string query
query = "SELECT id, type FROM planet WHERE id = 470454 and type = 'relation'"

Req.post!(req, athena: query).body
#=>
# %ReqAthena.Result{
#   columns: ["id", "type"],
#   output_location: "s3://my-bucket/7788bdd3-7d09-4851-be4c-e128ef27f215.csv",
#   query_execution_id: "7788bdd3-7d09-4851-be4c-e128ef27f215",
#   rows: [[470_454, "node"]],
#   statement_name: nil
# }

# With parameterized query
query = "SELECT id, type FROM planet WHERE id = ? and type = ?"

Req.post!(req, athena: {query, [239_970_142, "node"]}).body
#=>
# %ReqAthena.Result{
#   columns: ["id", "type"],
#   output_location: "s3://my-bucket/dda41d66-1eea-4588-850a-945c9def9163.csv",
#   query_execution_id: "dda41d66-1eea-4588-850a-945c9def9163",
#   rows: [[239970142, "node"]],
#   statement_name: "query_C71EF77B8B7B92D9846C6D7E70136448"
# }
```

## License

Copyright (C) 2022 Dashbit

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

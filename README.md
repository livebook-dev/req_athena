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
STORED AS ORCFILE
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
query = "SELECT id, type, tags, members, timestamp, visible FROM planet WHERE id = 470454 and type = 'relation'"

Req.post!(req, athena: query).body
#=>
# %ReqAthena.Result{
#   columns: ["id", "type", "tags", "members", "timestamp", "visible"],
#   output_location: "s3://my-bucket/c594d5df-9879-4bf7-8796-780e0b87a673.csv",
#   query_execution_id: "c594d5df-9879-4bf7-8796-780e0b87a673",
#   rows: [
#     [470454, "relation",
#      "{ref=17229A, site=geodesic, name=Mérignac A, source=©IGN 2010 dans le cadre de la cartographie réglementaire, type=site, url=http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A, network=NTF-5}",
#      "[{type=node, ref=670007839, role=}, {type=node, ref=670007840, role=}]",
#      ~N[2017-01-21 12:51:34.000], true]
#   ],
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

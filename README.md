# ReqAthena

[![Docs](https://img.shields.io/badge/hex.pm-docs-8e7ce6.svg)](https://hexdocs.pm/req_athena)
[![Hex pm](http://img.shields.io/hexpm/v/req_athena.svg?style=flat&color=blue)](https://hex.pm/packages/req_athena)

[Req](https://github.com/wojtekmach/req) plugin for [AWS Athena](https://docs.aws.amazon.com/athena/latest/APIReference/Welcome.html).

ReqAthena makes it easy to make Athena queries and save the results into S3 buckets.

By default, `ReqAthena` will query results and use the default output format,
which is CSV. To change that, you can use the `:format` option documented bellow.

## Usage

```elixir
Mix.install([
  {:req, "~> 0.5.8"},
  {:req_athena, "~> 0.3.0"}
])

opts = [
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY"),
  region: System.fetch_env!("AWS_REGION"),
  database: "default",
  # This may need to be a new directory for every query using the `:json` or `:explorer` formats.
  # See the docs for details: https://hexdocs.pm/req_athena/ReqAthena.html#new/1
  output_location: "s3://my-bucket/my-location"
]

req = ReqAthena.new(opts)

# Create table from Registry of Open Data on AWS
# See: https://registry.opendata.aws/osm/
query = """
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
LOCATION 's3://osm-pds/planet/';
"""

ReqAthena.query!(req, query).body
# =>
# %{
#   "Output" => "",
#   "ResultSet" => %{
#     "ColumnInfos" => [],
#     "ResultRows" => [],
#     "ResultSetMetadata" => %{"ColumnInfo" => []},
#     "Rows" => []
#   }
# }

# With plain string query
query = "SELECT id, type, tags, members, timestamp, visible FROM planet WHERE id = 470454 and type = 'relation'"

ReqAthena.query!(req, query, [], format: :json).body
# =>
# [
#  %{
#    "id" => 470454,
#    "members" => [
#      %{"ref" => 670007839, "role" => "", "type" => "node"},
#      %{"ref" => 670007840, "role" => "", "type" => "node"}
#    ],
#    "tags" => %{
#      "name" => "Mérignac A",
#      "network" => "NTF-5",
#      "ref" => "17229A",
#      "site" => "geodesic",
#      "source" => "©IGN 2010 dans le cadre de la cartographie réglementaire",
#      "type" => "site",
#      "url" => "http://geodesie.ign.fr/fiches/index.php?module=e&action=fichepdf&source=carte&sit_no=17229A"
#    },
#    "timestamp" => "2017-01-21 12:51:34",
#    "type" => "relation",
#    "visible" => true
#  }
# ]

# With parameterized query
query = "SELECT id, type FROM planet WHERE id = ? and type = ?"

ReqAthena.query!(req, query, [239_970_142, "node"], format: :json).body
#=> [%{"id" => 239970142, "type" => "node"}]
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

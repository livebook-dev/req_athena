defmodule ReqAthena.MixProject do
  use Mix.Project

  @version "0.1.4"
  @description "Req plugin for AWS Athena"

  def project do
    [
      app: :req_athena,
      version: @version,
      description: @description,
      name: "ReqAthena",
      elixir: "~> 1.14",
      preferred_cli_env: [
        "test.all": :test,
        docs: :docs,
        "hex.publish": :docs
      ],
      start_permanent: Mix.env() == :prod,
      docs: docs(),
      deps: deps(),
      aliases: aliases(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/livebook-dev/req_athena",
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end

  defp deps do
    [
      {:req, "~> 0.3.5"},
      {:aws_signature, "~> 0.3.0"},
      # {:aws_credentials, github: "aws-beam/aws_credentials", runtime: false, optional: true},
      {:table, "~> 0.1.1", optional: true},
      {:tzdata, "~> 1.1.1", only: :test},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/livebook-dev/req_athena"
      }
    ]
  end
end

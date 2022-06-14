defmodule ReqAthena.MixProject do
  use Mix.Project

  def project do
    [
      app: :req_athena,
      version: "0.1.0",
      elixir: "~> 1.12",
      preferred_cli_env: [
        "test.all": :test,
        docs: :docs,
        "hex.publish": :docs
      ],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:req, github: "wojtekmach/req"},
      {:aws_signature, "~> 0.3.0"},
      {:table, "~> 0.1.1", optional: true},
      {:tzdata, "~> 1.1.1", only: :test}
    ]
  end

  def aliases do
    ["test.all": ["test --include integration"]]
  end
end

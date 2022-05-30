defmodule ReqAthena.MixProject do
  use Mix.Project

  def project do
    [
      app: :req_athena,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:aws_signature, "~> 0.3.0"}
    ]
  end
end

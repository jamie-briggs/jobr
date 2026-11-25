defmodule Jobr.MixProject do
  use Mix.Project

  def project do
    [
      app: :jobr,
      version: "0.1.0",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Jobr.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mqttx, "~> 0.13.0"},
      # Optional transport adapter: lets tests run an in-process MqttX broker
      # instead of requiring a locally installed Mosquitto/EMQX.
      {:thousand_island, "~> 1.4", only: :test}
    ]
  end

  # Test-only support modules (e.g. the in-process MQTT test broker).
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end

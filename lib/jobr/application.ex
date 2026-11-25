defmodule Jobr.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [Jobr.Broker] ++
        if enabled?(), do: [Jobr.Mqtt], else: []

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Jobr.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # The MQTT bridge is opt-in so tests can start it explicitly against their
  # own in-process broker instead of dialling out to a real one.
  defp enabled? do
    :jobr
    |> Application.get_env(Jobr.Mqtt, [])
    |> Keyword.get(:enabled, false)
  end
end

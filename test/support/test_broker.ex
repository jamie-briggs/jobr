defmodule Jobr.TestBroker do
  @moduledoc """
  A minimal in-process MQTT broker for tests.

  Wraps `MqttX.Server` so the test suite exercises the real wire protocol over
  a real socket, without requiring Mosquitto or EMQX to be installed. Every
  inbound PUBLISH is echoed back to all subscribers via
  `MqttX.Server.deliver/3`, which makes it usable as a round-trip broker.

  Started once for the whole suite by `test/test_helper.exs`.
  """

  use MqttX.Server

  require Logger

  @port 18_883

  @doc "The TCP port the test broker listens on."
  def port, do: @port

  @doc """
  Starts the broker on `#{@port}`.

  Returns `{:ok, pid}` or `{:error, {:already_started, pid}}` if it is already
  running, so it is safe to call from `test_helper.exs` and from individual
  tests.
  """
  def start_link(port \\ @port) do
    MqttX.Server.start_link(
      __MODULE__,
      [],
      transport: MqttX.Transport.ThousandIsland,
      port: port,
      name: __MODULE__
    )
  end

  @impl true
  def init(_opts), do: %{}

  @impl true
  def handle_connect(client_id, _credentials, _info, state) do
    Logger.debug("[TestBroker] connect: #{client_id}")
    {:ok, state}
  end

  @impl true
  def handle_publish(topic, payload, opts, state) do
    # Echo to every subscriber, including the publisher when it subscribed.
    MqttX.Server.deliver(__MODULE__, topic, payload, opts)
    {:ok, state}
  end

  @impl true
  def handle_subscribe(topics, state) do
    {:ok, Enum.map(topics, & &1.qos), state}
  end

  @impl true
  def handle_disconnect(_reason, _state), do: :ok
end

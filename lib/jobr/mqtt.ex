defmodule Jobr.Mqtt do
  @moduledoc """
  A supervised MQTT bridge built on [`mqttx`](https://hex.pm/packages/mqttx).

  This module connects to an MQTT broker with `use MqttX` and forwards every
  inbound message onto the local `Jobr.Broker`, so the rest of the application
  subscribes to `Jobr.Broker` topics and never has to know about MQTT.

  ## Protocol versions

  Both MQTT 5.0 and MQTT 3.1.1 are supported by the same module; pick one with
  the `:protocol_version` option:

    * `5` — MQTT 5.0 (the default). Required for CONNECT properties such as
      `session_expiry_interval`, and for reason codes on PUBACK.
    * `4` — MQTT 3.1.1. Broadest compatibility with older brokers and devices.

  The bridge itself behaves identically on both; only the wire protocol and the
  properties available in CONNACK differ.

  ## Configuration

  Options are read from `config :jobr, Jobr.Mqtt` and may be overridden by
  passing them to `start_link/1`:

      config :jobr, Jobr.Mqtt,
        host: "broker.example.com",
        port: 1883,
        client_id: "jobr-node",
        protocol_version: 5,
        topics: ["jobr/#"],
        qos: 1

  ## Supervision

  Add it to a supervision tree as a normal child:

      children = [Jobr.Broker, Jobr.Mqtt]

  The client reconnects with backoff on its own, and granted subscriptions are
  replayed automatically after a reconnect that lost the session, so
  `handle_connected/2` does not resubscribe.
  """

  use MqttX

  require Logger

  @defaults [
    host: "localhost",
    port: 1883,
    protocol_version: 5,
    keepalive: 30,
    topics: ["jobr/#"],
    qos: 1
  ]

  @typedoc "Options accepted by `start_link/1`; see the module docs."
  @type option ::
          {:host, String.t()}
          | {:port, :inet.port_number()}
          | {:client_id, String.t()}
          | {:protocol_version, 4 | 5}
          | {:keepalive, non_neg_integer()}
          | {:topics, [String.t()]}
          | {:qos, 0 | 1 | 2}

  @doc """
  Starts the MQTT bridge.

  Configuration is read from `config :jobr, Jobr.Mqtt` and merged with
  `opts`, which take precedence.

  ## Options

  See `t:option/0`. `:client_id` defaults to a name derived from the node,
  but note that sessions, retained messages, and shared subscriptions are all
  keyed by client id — set a stable one in production.
  """
  @spec start_bridge(keyword()) :: GenServer.on_start()
  def start_bridge(opts \\ []) do
    config =
      @defaults
      |> Keyword.merge(Application.get_env(:jobr, __MODULE__, []))
      |> Keyword.merge(opts)

    MqttX.SimpleClient.start_link(__MODULE__, config)
  end

  @doc """
  Child specification for the bridge.

  `use MqttX` injects a `child_spec/1` that calls the generated
  `start_link/1`; this override routes supervision through `start_bridge/1`
  so the configured defaults are applied.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_bridge, [opts]},
      type: :worker
    }
  end

  @impl true
  def init(opts) do
    topics = Keyword.fetch!(opts, :topics)
    qos = Keyword.fetch!(opts, :qos)

    Logger.info(fn ->
      "[Jobr.Mqtt] connecting to #{opts[:host]}:#{opts[:port]} " <>
        "as #{opts[:client_id]} (MQTT v#{opts[:protocol_version]})"
    end)

    {:ok, %{topics: topics, qos: qos}}
  end

  @impl true
  def handle_connected(info, state) do
    Logger.info(fn -> "[Jobr.Mqtt] connected (session_present: #{info.session_present})" end)

    # Granted subscriptions are replayed by mqttx after a reconnect that lost
    # the session, so subscribing here once is enough for a long-lived client.
    case subscribe(state.topics, qos: state.qos) do
      {:ok, granted} ->
        Logger.info(fn ->
          "[Jobr.Mqtt] subscribed to #{inspect(state.topics)} -> #{inspect(granted)}"
        end)

        {:ok, state}

      {:error, reason} ->
        Logger.error(fn -> "[Jobr.Mqtt] subscribe failed: #{inspect(reason)}" end)
        {:ok, state}
    end
  end

  @impl true
  def handle_disconnected(reason, state) do
    Logger.warning(fn -> "[Jobr.Mqtt] disconnected: #{inspect(reason)} (reconnecting)" end)
    {:ok, state}
  end

  @impl true
  def handle_message(topic, payload, _packet, state) do
    # `topic` arrives as a list of segments, not the original string.
    topic = Enum.join(topic, "/")
    Logger.debug(fn -> "[Jobr.Mqtt] <- #{topic}: #{inspect(payload)}" end)

    # `Jobr.Broker` topics are atoms, but MQTT topics are strings, so every
    # inbound message is delivered to the fixed `:mqtt` topic with the real
    # MQTT topic carried in the payload. Subscribers match on the topic they
    # care about from there.
    :ok = Jobr.Broker.send(:mqtt, {topic, payload})

    {:ok, state}
  end

  @impl true
  def handle_publish_error(topic, packet_id, reason_code, state) do
    Logger.error(fn ->
      "[Jobr.Mqtt] broker rejected publish to #{inspect(topic)} " <>
        "(id #{inspect(packet_id)}): #{inspect(reason_code)}"
    end)

    {:ok, state}
  end

  @doc """
  Publishes `payload` to `topic` on the broker.

  Returns `:ok` once the packet is written to the socket, or
  `{:error, :not_connected}` while the connection is down — mqttx does not
  queue publishes, so callers that need durability must buffer themselves.
  """
  @spec publish_to_broker(String.t(), binary(), keyword()) :: :ok | {:error, term()}
  def publish_to_broker(topic, payload, opts \\ []) do
    publish(topic, payload, opts)
  end
end

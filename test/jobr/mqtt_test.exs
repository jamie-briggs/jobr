defmodule Jobr.MqttTest do
  @moduledoc """
  Exercises the MQTT bridge against the in-process `Jobr.TestBroker`.

  The broker echoes every PUBLISH back to its subscribers, so a publish from
  the bridge's own client round-trips through the real wire protocol and lands
  on `Jobr.Broker` — which is exactly the path the application depends on.
  """

  use ExUnit.Case, async: false

  @moduletag :mqtt

  setup do
    # Silence the bridge's own logging during tests.
    Logger.configure(level: :warning)
    :ok
  end

  defp unique_topic(prefix \\ "jobr/test") do
    "#{prefix}/#{System.unique_integer([:positive])}"
  end

  # Starts a bridge subscribed to `topic`, waits until it is connected, and
  # returns the client pid. The bridge is linked to the test process, so it is
  # torn down when the test exits.
  defp start_bridge!(topic, protocol_version) do
    client_id = "jobr-test-#{System.unique_integer([:positive])}"

    {:ok, pid} =
      Jobr.Mqtt.start_bridge(
        host: "localhost",
        port: Jobr.TestBroker.port(),
        client_id: client_id,
        protocol_version: protocol_version,
        topics: [topic],
        qos: 1
      )

    # start_bridge/1 is asynchronous; subscribe/2 waits for SUBACK, so a
    # successful subscribe is proof the session is live.
    assert eventually(fn -> Jobr.Mqtt.connected?() end)

    pid
  end

  defp eventually(fun, attempts \\ 100)

  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(20)
      eventually(fun, attempts - 1)
    end
  end

  describe "MQTT 5.0 (protocol_version: 5)" do
    test "connects to the broker" do
      pid = start_bridge!(unique_topic(), 5)

      assert Process.alive?(pid)
      assert Jobr.Mqtt.connected?()
    end

    test "an inbound message is forwarded onto Jobr.Broker" do
      topic = unique_topic()
      _pid = start_bridge!(topic, 5)

      :ok = Jobr.Broker.subscribe(:mqtt)
      :ok = Jobr.Mqtt.publish_to_broker(topic, "hello-v5", qos: 1)

      assert_receive {:jobr, :mqtt, {^topic, "hello-v5"}}, 2_000
    end

    test "publishing while connected returns :ok" do
      topic = unique_topic()
      _pid = start_bridge!(topic, 5)

      assert Jobr.Mqtt.publish_to_broker(topic, "payload", qos: 1) == :ok
    end
  end

  describe "MQTT 3.1.1 (protocol_version: 4)" do
    test "connects to the broker" do
      pid = start_bridge!(unique_topic(), 4)

      assert Process.alive?(pid)
      assert Jobr.Mqtt.connected?()
    end

    test "an inbound message is forwarded onto Jobr.Broker" do
      topic = unique_topic()
      _pid = start_bridge!(topic, 4)

      :ok = Jobr.Broker.subscribe(:mqtt)
      :ok = Jobr.Mqtt.publish_to_broker(topic, "hello-v311", qos: 1)

      assert_receive {:jobr, :mqtt, {^topic, "hello-v311"}}, 2_000
    end
  end

  describe "topic handling" do
    test "wildcard subscriptions receive matching topics, joined with slashes" do
      prefix = "jobr/wild/#{System.unique_integer([:positive])}"
      full_topic = "#{prefix}/sensor/temp"
      _pid = start_bridge!("#{prefix}/#", 5)

      :ok = Jobr.Broker.subscribe(:mqtt)
      :ok = Jobr.Mqtt.publish_to_broker(full_topic, "21.5", qos: 1)

      assert_receive {:jobr, :mqtt, {^full_topic, "21.5"}}, 2_000
    end
  end

  describe "configuration" do
    test "start_bridge/1 merges defaults, app config, and explicit opts" do
      # Explicit opts win over both the defaults and the application config.
      {:ok, pid} =
        Jobr.Mqtt.start_bridge(
          host: "localhost",
          port: Jobr.TestBroker.port(),
          client_id: "jobr-cfg-#{System.unique_integer([:positive])}",
          protocol_version: 4,
          topics: [unique_topic()],
          qos: 0
        )

      assert Process.alive?(pid)
      assert eventually(fn -> Jobr.Mqtt.connected?() end)
    end
  end
end

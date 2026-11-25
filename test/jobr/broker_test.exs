defmodule Jobr.BrokerTest do
  use ExUnit.Case, async: false

  alias Jobr.Broker

  # The broker's Registry is started by the application supervision tree.
  # Each test uses a unique topic to stay isolated from other tests.

  defp unique_topic, do: :"topic_#{System.unique_integer([:positive])}"

  # A value that is neither an atom nor a binary, built at runtime so the type
  # checker does not flag the deliberate misuse of the guards under test.
  defp invalid_topic, do: System.unique_integer([:positive])

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  describe "subscribe/1" do
    test "registers the calling process for a topic" do
      topic = unique_topic()

      assert Broker.subscribe(topic) == :ok
      assert self() in Broker.subscribers(topic)
    end

    test "is idempotent in effect but allows duplicate subscriptions" do
      topic = unique_topic()

      assert Broker.subscribe(topic) == :ok
      assert Broker.subscribe(topic) == :ok

      # `subscribers/1` reports distinct pids, so the duplicate is not visible
      # there; the duplicate shows up as two deliveries of one publish.
      assert Broker.subscribers(topic) == [self()]

      :ok = Broker.send(topic, :twice)
      assert_receive {:jobr, ^topic, :twice}
      assert_receive {:jobr, ^topic, :twice}
      refute_receive {:jobr, ^topic, :twice}, 50
    end

    test "raises for a non-atom, non-binary topic" do
      fun = fn -> Broker.subscribe(invalid_topic()) end
      assert_raise FunctionClauseError, fun
    end
  end

  describe "unsubscribe/1" do
    test "removes the calling process's subscription" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      assert Broker.unsubscribe(topic) == :ok

      refute self() in Broker.subscribers(topic)
    end

    test "is idempotent when not subscribed" do
      topic = unique_topic()

      assert Broker.unsubscribe(topic) == :ok
      assert Broker.unsubscribe(topic) == :ok
    end

    test "removes all duplicate subscriptions at once" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      :ok = Broker.subscribe(topic)
      :ok = Broker.unsubscribe(topic)

      refute self() in Broker.subscribers(topic)
    end

    test "stops delivery of subsequent messages" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      :ok = Broker.unsubscribe(topic)
      :ok = Broker.send(topic, :ignored)

      refute_receive {:jobr, ^topic, :ignored}
    end

    test "raises for a non-atom, non-binary topic" do
      fun = fn -> Broker.unsubscribe(invalid_topic()) end
      assert_raise FunctionClauseError, fun
    end
  end

  describe "send/2" do
    test "delivers {:jobr, topic, msg} to the subscriber" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      assert Broker.send(topic, :hello) == :ok

      assert_receive {:jobr, ^topic, :hello}
    end

    test "delivers to every subscriber of a topic" do
      topic = unique_topic()
      parent = self()

      pids =
        for _ <- 1..3 do
          spawn(fn ->
            :ok = Broker.subscribe(topic)
            send(parent, :ready)

            receive do
              {:jobr, ^topic, msg} -> send(parent, {:got, self(), msg})
            end
          end)
        end

      for _ <- pids, do: assert_receive(:ready)

      assert Broker.send(topic, :broadcast) == :ok

      received =
        for _ <- pids do
          assert_receive {:got, pid, :broadcast}
          pid
        end

      assert Enum.sort(received) == Enum.sort(pids)
    end

    test "does not deliver to processes subscribed to other topics" do
      topic = unique_topic()
      other = unique_topic()
      parent = self()

      spawn(fn ->
        :ok = Broker.subscribe(other)
        send(parent, :ready)

        receive do
          {:jobr, _, _} = msg -> send(parent, {:unexpected, msg})
        after
          200 -> send(parent, :no_message)
        end
      end)

      assert_receive :ready
      :ok = Broker.subscribe(topic)

      assert Broker.send(topic, :only_mine) == :ok
      assert_receive {:jobr, ^topic, :only_mine}
      assert_receive :no_message, 500
    end

    test "returns :ok when nobody is subscribed" do
      assert Broker.send(unique_topic(), :nobody) == :ok
    end

    test "delivers each message as a distinct mailbox entry" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      :ok = Broker.send(topic, 1)
      :ok = Broker.send(topic, 2)

      assert_receive {:jobr, ^topic, 1}
      assert_receive {:jobr, ^topic, 2}
    end

    test "supports arbitrary term payloads" do
      topic = unique_topic()
      payload = %{id: 1, nested: [a: {:b, "c"}]}

      :ok = Broker.subscribe(topic)
      :ok = Broker.send(topic, payload)

      assert_receive {:jobr, ^topic, ^payload}
    end

    test "raises for a non-atom, non-binary topic" do
      fun = fn -> Broker.send(invalid_topic(), :msg) end
      assert_raise FunctionClauseError, fun
    end
  end

  describe "binary (MQTT) topics" do
    test "subscribe/send/unsubscribe work with a binary topic" do
      topic = "sensors/room#{System.unique_integer([:positive])}/temp"

      :ok = Broker.subscribe(topic)
      assert self() in Broker.subscribers(topic)

      :ok = Broker.send(topic, "21.5")
      assert_receive {:jobr, ^topic, "21.5"}

      :ok = Broker.unsubscribe(topic)
      refute self() in Broker.subscribers(topic)
    end

    test "binary topics are matched exactly, not by wildcard" do
      prefix = "sensors/room#{System.unique_integer([:positive])}"
      subscribed = "#{prefix}/temp"
      other = "#{prefix}/humidity"

      :ok = Broker.subscribe(subscribed)
      :ok = Broker.send(other, "50")

      refute_receive {:jobr, ^other, _}
      refute_receive {:jobr, ^subscribed, _}
    end

    test "an atom topic and a binary topic with the same text are distinct" do
      n = System.unique_integer([:positive])
      atom_topic = :"shared_#{n}"
      binary_topic = "shared_#{n}"

      :ok = Broker.subscribe(binary_topic)
      :ok = Broker.send(atom_topic, :for_atom)

      refute_receive {:jobr, ^atom_topic, :for_atom}
      refute_receive {:jobr, ^binary_topic, _}
    end

    test "binary topics do not create atoms" do
      topic = "sensors/never_an_atom_#{System.unique_integer([:positive])}"

      :ok = Broker.subscribe(topic)
      :ok = Broker.send(topic, :payload)
      assert_receive {:jobr, ^topic, :payload}

      # The topic must still be a binary, not an atom created by the broker.
      assert is_binary(topic)
      refute String.to_existing_atom(topic)
    rescue
      ArgumentError -> :ok
    end
  end

  describe "subscribers/1" do
    test "returns an empty list for an unknown topic" do
      assert Broker.subscribers(unique_topic()) == []
    end

    test "returns all pids subscribed to a topic" do
      topic = unique_topic()
      parent = self()

      pid =
        spawn(fn ->
          :ok = Broker.subscribe(topic)
          send(parent, :ready)
          Process.sleep(:infinity)
        end)

      assert_receive :ready

      assert pid in Broker.subscribers(topic)
    end
  end

  describe "subscriber lifecycle" do
    test "removes subscriptions when a subscriber process dies" do
      topic = unique_topic()
      parent = self()

      pid =
        spawn(fn ->
          :ok = Broker.subscribe(topic)
          send(parent, :ready)
          receive do: (:stop -> :ok)
        end)

      assert_receive :ready
      assert pid in Broker.subscribers(topic)

      ref = Process.monitor(pid)
      send(pid, :stop)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      # Monitor cleanup is asynchronous; poll briefly for the registry to settle.
      assert eventually(fn -> pid not in Broker.subscribers(topic) end)
    end
  end

  describe "integration with the application supervision tree" do
    test "the broker registry is running" do
      assert Process.whereis(Jobr.Broker.Registry) != nil
    end

    test "a full subscribe/send/unsubscribe cycle works end to end" do
      topic = unique_topic()

      :ok = Broker.subscribe(topic)
      :ok = Broker.send(topic, :first)
      assert_receive {:jobr, ^topic, :first}

      :ok = Broker.unsubscribe(topic)
      :ok = Broker.send(topic, :second)
      refute_receive {:jobr, ^topic, :second}

      flush()
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end

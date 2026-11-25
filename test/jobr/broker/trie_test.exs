defmodule Jobr.Broker.TrieTest do
  use ExUnit.Case, async: false

  alias Jobr.Broker.Trie

  # The trie is started by the application supervision tree, so each test uses
  # unique topic prefixes to stay isolated. Subscriptions belonging to the test
  # process are removed in `on_exit/1`; spawned subscribers are cleaned up when
  # their process dies.

  setup do
    test_pid = self()

    on_exit(fn ->
      for {{filter, pid}, _ref} <- :ets.tab2list(Jobr.Broker.Trie.Subscriptions),
          pid == test_pid do
        Jobr.Broker.Trie.unsubscribe(filter, pid)
      end
    end)

    :ok
  end

  defp unique(prefix \\ "t") do
    "#{prefix}/#{System.unique_integer([:positive])}"
  end

  # Spawns a subscriber linked to the test process, so it is torn down (and its
  # subscriptions removed by the trie's monitor) when the test exits.
  defp spawn_subscriber(filter, parent \\ self()) do
    pid =
      spawn_link(fn ->
        :ok = Trie.subscribe(filter)
        send(parent, {:subscribed, self()})

        receive do
          {:jobr, _topic, msg} -> send(parent, {:got, self(), msg})
          :stop -> :ok
        end
      end)

    assert_receive {:subscribed, ^pid}
    pid
  end

  describe "exact filters" do
    test "match only their own topic" do
      base = unique()
      pid = spawn_subscriber("#{base}/a")

      assert Trie.match("#{base}/a") == [pid]
      assert Trie.match("#{base}/b") == []
    end
  end

  describe "+ wildcard" do
    test "matches exactly one level" do
      base = unique()
      pid = spawn_subscriber("#{base}/+/temp")

      assert Trie.match("#{base}/room1/temp") == [pid]
      assert Trie.match("#{base}/room2/temp") == [pid]
      assert Trie.match("#{base}/room1/temp/extra") == []
      assert Trie.match("#{base}/temp") == []
    end

    test "matches several levels independently" do
      base = unique()
      pid = spawn_subscriber("#{base}/+/+")

      assert Trie.match("#{base}/a/b") == [pid]
      assert Trie.match("#{base}/a") == []
      assert Trie.match("#{base}/a/b/c") == []
    end
  end

  describe "# wildcard" do
    test "matches zero or more levels" do
      base = unique()
      pid = spawn_subscriber("#{base}/#")

      assert Trie.match(base) == [pid]
      assert Trie.match("#{base}/a") == [pid]
      assert Trie.match("#{base}/a/b/c") == [pid]
    end

    test "does not match a sibling prefix" do
      base = unique()
      _pid = spawn_subscriber("#{base}/a/#")

      assert Trie.match("#{base}/b") == []
    end

    test "a bare # matches everything" do
      pid = spawn_subscriber("#")

      assert pid in Trie.match("anything/at/all")
      assert pid in Trie.match("single")
    end
  end

  describe "multiple matching filters" do
    test "a pid with several matching filters appears once" do
      base = unique()
      pid = spawn_subscriber("#{base}/+")
      :ok = Trie.subscribe("#{base}/#", pid)

      matches = Trie.match("#{base}/a")
      assert matches == [pid]
    end

    test "distinct pids are all returned" do
      base = unique()
      p1 = spawn_subscriber("#{base}/+")
      p2 = spawn_subscriber("#{base}/#")

      matches = Trie.match("#{base}/a")
      assert Enum.sort(matches) == Enum.sort([p1, p2])
    end
  end

  describe "unsubscribe/2" do
    test "removes the subscription" do
      base = unique()
      filter = "#{base}/+"
      pid = spawn_subscriber(filter)

      assert Trie.match("#{base}/a") == [pid]
      assert Trie.unsubscribe(filter, pid) == :ok
      assert Trie.match("#{base}/a") == []
    end

    test "is idempotent" do
      filter = "#{unique()}/+"

      assert Trie.unsubscribe(filter, self()) == :ok
      assert Trie.unsubscribe(filter, self()) == :ok
    end

    test "removes all duplicate subscriptions to one filter" do
      base = unique()
      filter = "#{base}/+"

      :ok = Trie.subscribe(filter)
      :ok = Trie.subscribe(filter)
      assert length(Trie.subscribers(filter)) == 2

      :ok = Trie.unsubscribe(filter)
      assert Trie.subscribers(filter) == []
      assert Trie.match("#{base}/a") == []
    end

    test "leaves other subscribers of the same filter intact" do
      base = unique()
      filter = "#{base}/+"
      pid = spawn_subscriber(filter)

      :ok = Trie.subscribe(filter)
      :ok = Trie.unsubscribe(filter)

      # The spawned subscriber is still there; only the test process left.
      assert Trie.match("#{base}/a") == [pid]
    end
  end

  describe "subscriber lifecycle" do
    test "subscriptions are removed when the subscriber dies" do
      base = unique()
      pid = spawn_subscriber("#{base}/+")

      assert Trie.match("#{base}/a") == [pid]

      ref = Process.monitor(pid)
      send(pid, :stop)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}

      assert eventually(fn -> Trie.match("#{base}/a") == [] end)
    end
  end

  describe "validation" do
    test "raises for an invalid filter" do
      assert_raise ArgumentError, fn -> Trie.subscribe("a/#/b") end
      assert_raise ArgumentError, fn -> Trie.subscribe("") end
    end
  end

  describe "size/0" do
    test "counts distinct filters, not subscribers" do
      base = unique()
      before = Trie.size()

      :ok = Trie.subscribe("#{base}/+")
      :ok = Trie.subscribe("#{base}/+")

      assert Trie.size() == before + 1
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

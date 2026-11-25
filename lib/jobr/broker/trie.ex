defmodule Jobr.Broker.Trie do
  @moduledoc """
  An ETS-backed subscription trie for binary MQTT topics with wildcards.

  `Registry` only does exact-key lookups, so wildcard filters (`+`, `#`) need a
  structure that can be walked level by level. This module owns that structure
  and the subscriber lifecycle around it.

  ## Storage

  Two ETS tables, both owned by the `GenServer` started by `start_link/1`:

    * `subscriptions` — `{{filter, pid}, ref}`. The authoritative index, read
      on the publish path. `:duplicate_bag` so a pid may subscribe to the same
      filter more than once, mirroring `Registry`'s `keys: :duplicate`.
    * `by_pid` — `{pid, filter}`. Used to clean up on unsubscribe and to
      rebuild a subscriber's filters after a process restart.

  A third table holds the trie itself: `{{level, parent_node}, child_node}`,
  mapping a path of levels to an integer node id, plus a `{node, filter}`
  index so a matched node yields its filters without a reverse walk.

  Reads (`match/1`) never touch the `GenServer` — they hit ETS directly, so
  publishing does not serialise on a single process.

  ## Subscriber cleanup

  The `GenServer` monitors every subscribing pid. When a subscriber dies, all
  of its filters are removed in one pass, which is what keeps the trie from
  leaking entries for dead processes.

  ## Matching

  Matching is a recursive walk over the topic's levels that branches at `+`
  and terminates at `#`, so a topic of `n` levels costs O(n) in the common
  case and only fans out where wildcards actually exist.
  """

  use GenServer

  alias Jobr.Broker.Topic

  @subscriptions __MODULE__.Subscriptions
  @by_pid __MODULE__.ByPid
  @nodes __MODULE__.Nodes

  @root 0

  @type filter :: binary()
  @type subscriber :: pid()

  # ---------------------------------------------------------------- lifecycle

  @doc """
  Returns a child specification for the trie.

  Add this to your supervision tree alongside the broker's `Registry`.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Starts the trie's owner process and creates its ETS tables.

  The tables are created here (not in `init/1` of a caller) so that they are
  owned by a long-lived process and survive the subscribing processes.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    :ets.new(@subscriptions, [
      :named_table,
      # `:bag`, not `:duplicate_bag`: bag keys on the first element, so
      # `:ets.lookup(@subscriptions, filter)` finds every `{{filter, pid}, ref}`
      # row. `:duplicate_bag` keys on the whole object and would never match.
      # The `ref` keeps rows distinct, so one pid may subscribe repeatedly.
      :bag,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@by_pid, [
      :named_table,
      :bag,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@nodes, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    {:ok, %{monitors: %{}}}
  end

  # ---------------------------------------------------------------------- api

  @doc """
  Subscribes `pid` to `filter`, which may contain MQTT wildcards.

  Returns `:ok`. Subscribing twice to the same filter is allowed and results in
  duplicate delivery for that filter, matching `Registry`'s behaviour; use
  `unsubscribe/2` to remove all of a pid's subscriptions to it.

  The trie monitors `pid`, so its subscriptions are removed automatically when
  it dies.
  """
  @spec subscribe(filter(), pid()) :: :ok
  def subscribe(filter, pid \\ self()) when is_binary(filter) and is_pid(pid) do
    # Validate in the caller: raising inside handle_call/3 would take the
    # GenServer down and drop every subscription it owns.
    unless Topic.valid_filter?(filter) do
      raise ArgumentError, "invalid MQTT topic filter: #{inspect(filter)}"
    end

    GenServer.call(__MODULE__, {:subscribe, filter, pid})
  end

  @doc """
  Removes all of `pid`'s subscriptions to `filter`.

  Returns `:ok` whether or not the pid was subscribed, making the call
  idempotent.
  """
  @spec unsubscribe(filter(), pid()) :: :ok
  def unsubscribe(filter, pid \\ self()) when is_binary(filter) and is_pid(pid) do
    GenServer.call(__MODULE__, {:unsubscribe, filter, pid})
  end

  @doc """
  Returns the pids subscribed to any filter matching `topic`.

  Each pid appears **at most once**, even when several of its filters match —
  a subscriber to both `"a/+"` and `"a/#"` receives one copy of a message
  published to `"a/b"`.
  """
  @spec match(binary()) :: [pid()]
  def match(topic) when is_binary(topic) do
    topic
    |> Topic.split()
    |> collect(@root, [])
    |> Enum.flat_map(fn {node, terminal?} -> filters_at(node, terminal?) end)
    |> Enum.uniq()
  end

  @doc """
  Returns the pids subscribed to exactly `filter`, with duplicates preserved.

  Intended for introspection, debugging, and testing.
  """
  @spec subscribers(filter()) :: [pid()]
  def subscribers(filter) when is_binary(filter) do
    @subscriptions
    |> :ets.select([{{{filter, :"$1"}, :_}, [], [:"$1"]}])
  end

  @doc """
  Returns the number of distinct filters currently in the trie.
  """
  @spec size() :: non_neg_integer()
  def size do
    @subscriptions
    |> :ets.select([{{{:"$1", :_}, :_}, [], [:"$1"]}])
    |> Enum.uniq()
    |> length()
  end

  # ------------------------------------------------------------------ callbacks

  @impl true
  def handle_call({:subscribe, filter, pid}, _from, state) do
    insert(filter, pid)
    {:reply, :ok, monitor_subscriber(state, pid)}
  end

  def handle_call({:unsubscribe, filter, pid}, _from, state) do
    delete(filter, pid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    remove_pid(pid)
    {:noreply, %{state | monitors: Map.delete(state.monitors, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------- internals

  defp monitor_subscriber(state, pid) do
    if Map.has_key?(state.monitors, pid) do
      state
    else
      %{state | monitors: Map.put(state.monitors, pid, Process.monitor(pid))}
    end
  end

  defp insert(filter, pid) do
    ref = make_ref()
    :ets.insert(@subscriptions, {{filter, pid}, ref})
    :ets.insert(@by_pid, {pid, filter})
    index(filter)
    :ok
  end

  defp delete(filter, pid) do
    # `:ets.delete_object/2` matches structurally — `:_` would be a literal
    # atom there — so use `match_delete/2` for a pattern-based removal.
    :ets.match_delete(@subscriptions, {{filter, pid}, :_})
    :ets.delete_object(@by_pid, {pid, filter})

    # Only unindex when no subscriber is left on this filter.
    unless subscribed?(filter), do: unindex(filter)

    :ok
  end

  # True when at least one subscriber row exists for `filter`.
  defp subscribed?(filter) do
    :ets.select_count(@subscriptions, [{{{filter, :_}, :_}, [], [true]}]) > 0
  end

  defp remove_pid(pid) do
    filters =
      @by_pid
      |> :ets.lookup(pid)
      |> Enum.map(fn {_pid, filter} -> filter end)
      |> Enum.uniq()

    :ets.delete(@by_pid, pid)

    # Remove every subscription row for this pid in one sweep, then unindex
    # whichever filters became empty.
    @subscriptions
    |> :ets.select_delete([{{{:_, pid}, :_}, [], [true]}])

    Enum.each(filters, fn filter ->
      unless subscribed?(filter), do: unindex(filter)
    end)
  end

  # -------------------------------------------------------------- trie index

  # Walk (creating as needed) the path of literal levels for `filter` and
  # record the filter on the terminal node.
  #
  # A trailing `#` is stored as a marker on its *parent* node rather than as a
  # child edge, because `#` matches zero or more remaining levels — so the node
  # it hangs off is already a match for the topic that ends there.
  defp index(filter) do
    levels = Topic.split(filter)

    {path, hash?} =
      case List.last(levels) do
        "#" -> {Enum.drop(levels, -1), true}
        _ -> {levels, false}
      end

    node = Enum.reduce(path, @root, fn level, parent -> ensure_node(level, parent) end)

    :ets.insert(@nodes, {{:filter, node, filter}, true})
    if hash?, do: :ets.insert(@nodes, {{:hash, node, filter}, true})
    :ok
  end

  defp ensure_node(level, parent) do
    key = {level, parent}

    case :ets.lookup(@nodes, key) do
      [{^key, child}] ->
        child

      [] ->
        child = :ets.info(@nodes, :size) + 1
        :ets.insert(@nodes, {key, child})
        child
    end
  end

  defp unindex(filter) do
    levels = Topic.split(filter)

    {path, hash?} =
      case List.last(levels) do
        "#" -> {Enum.drop(levels, -1), true}
        _ -> {levels, false}
      end

    case Enum.reduce_while(path, @root, fn level, parent ->
           case :ets.lookup(@nodes, {level, parent}) do
             [{_key, child}] -> {:cont, child}
             [] -> {:halt, nil}
           end
         end) do
      nil ->
        :ok

      node ->
        :ets.delete_object(@nodes, {{:filter, node, filter}, true})
        if hash?, do: :ets.delete_object(@nodes, {{:hash, node, filter}, true})
        :ok
    end
  end

  # Collect `{node, terminal?}` for every node visited walking `levels`.
  #
  # All visited nodes are returned, not just the final one, because `#` is
  # stored as a marker on its parent: `a/#` must be picked up at the node for
  # `a` even when the topic continues past it. Only the final node is
  # `terminal?`, so a plain filter such as `a/+/+` is not reported for a longer
  # topic that merely passes through its node.
  defp collect([], node, acc), do: [{node, true} | acc]

  defp collect([level | rest], node, acc) do
    # A literal level may also be reachable through a `+` edge. When the topic
    # level *is* "+" the two edges coincide, so only walk it once.
    edges = if level == "+", do: ["+"], else: [level, "+"]

    Enum.reduce(edges, [{node, false} | acc], fn edge, acc ->
      case child(node, edge) do
        nil -> acc
        child -> collect(rest, child, acc)
      end
    end)
  end

  defp child(node, edge) do
    case :ets.lookup(@nodes, {edge, node}) do
      [{_key, child}] -> child
      [] -> nil
    end
  end

  # `:filter` markers only count on the terminal node; `:hash` markers count on
  # every visited node, since `a/#` matches `a` and everything under it.
  defp filters_at(node, terminal?) do
    matchers =
      if terminal? do
        [
          {{{:filter, node, :"$1"}, :_}, [], [:"$1"]},
          {{{:hash, node, :"$1"}, :_}, [], [:"$1"]}
        ]
      else
        [{{{:hash, node, :"$1"}, :_}, [], [:"$1"]}]
      end

    @nodes
    |> :ets.select(matchers)
    |> Enum.flat_map(fn filter -> subscribers(filter) end)
  end
end

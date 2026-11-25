defmodule Jobr.Broker do
  @moduledoc """
  A topic-based publish/subscribe broker with MQTT wildcard support.

  Processes may subscribe to a `topic` and will then receive every message
  published to that topic. Messages are delivered with the standard Elixir
  `send/2` function as a tagged tuple:

      {:jobr, topic, msg}

  A topic is either an **atom** (`:news`) for internal application events or a
  **binary** (`"sensors/room1/temp"`) for topics bridged in from MQTT. Binary
  topics are never converted to atoms, so untrusted topic strings cannot
  exhaust the atom table.

  ## Wildcards

  Binary subscriptions may use MQTT wildcards (MQTT 5.0 §4.7):

    * `+` matches exactly one level — `"sensors/+/temp"` matches
      `"sensors/room1/temp"` but not `"sensors/room1/attic/temp"`.
    * `#` matches zero or more levels and must be last — `"sensors/#"` matches
      `"sensors"`, `"sensors/room1"`, and `"sensors/room1/temp"`.
    * `$`-prefixed topics are not matched by a filter starting with a wildcard
      (§4.7.2), so `"#"` does not receive `"$SYS/broker/uptime"`.

  A subscriber whose several filters all match one topic receives the message
  **once**, not once per filter.

  Atoms and binaries are separate namespaces: a wildcard filter only ever
  matches binary topics.

  ## Design

  Two indexes back the broker:

    * A `Registry` (`keys: :duplicate`) holds atom topics and exact binary
      topics. Lookup is a single exact-key hit, which is the common case.
    * `Jobr.Broker.Trie` holds binary filters containing wildcards, in ETS,
      and is walked level by level on publish.

  Splitting the two keeps the exact-match path as cheap as it was before
  wildcards existed, while wildcard filters pay only for the walk they need.
  Both indexes monitor their subscribers, so a subscription is removed
  automatically when the subscribing process exits.

  ## Examples

      iex> Jobr.Broker.subscribe(:news)
      :ok
      iex> Jobr.Broker.send(:news, "hello")
      :ok
      # The calling process now has a message in its mailbox:
      iex> receive do {:jobr, :news, msg} -> msg end
      "hello"

  Binary topics work the same way, and may use wildcards:

      iex> Jobr.Broker.subscribe("sensors/+/temp")
      :ok
      iex> Jobr.Broker.send("sensors/room1/temp", "21.5")
      :ok
      iex> receive do {:jobr, "sensors/room1/temp", msg} -> msg end
      "21.5"

  """

  alias Jobr.Broker.Trie

  @registry Jobr.Broker.Registry

  @typedoc """
  A topic identifying a channel of messages.

  Atoms are used for internal application events; binaries are used for MQTT
  topics such as `"sensors/room1/temp"`. Binaries may contain MQTT wildcards
  when subscribing.
  """
  @type topic :: atom() | binary()

  @typedoc "The tagged tuple delivered to subscribers."
  @type message :: {:jobr, topic(), term()}

  @doc """
  Returns a child specification for the broker.

  Starts both the `Registry` and the wildcard trie. Add `Jobr.Broker` to your
  supervision tree to start the broker; it is intended to be started once per
  node.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      type: :supervisor,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc """
  Starts the broker's `Registry` and wildcard trie under a supervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    children = [
      Registry.child_spec(keys: :duplicate, name: @registry),
      Trie
    ]

    Supervisor.start_link(children, Keyword.put_new(opts, :strategy, :one_for_one))
  end

  @doc """
  Subscribes the calling process to `topic`.

  Binary topics may contain MQTT wildcards (`+`, `#`). Returns `:ok`.
  Subscribing to a topic the process already subscribes to is allowed and
  results in duplicate delivery for that topic; call `unsubscribe/1` to remove
  all of the caller's subscriptions to it.

  Raises `ArgumentError` if a binary topic is not a valid MQTT filter (for
  example `"a/#/b"`, `"a//b"`, or the empty string).

  ## Examples

      iex> Jobr.Broker.subscribe(:news)
      :ok
      iex> Jobr.Broker.subscribe("sensors/+/temp")
      :ok

  """
  @spec subscribe(topic()) :: :ok
  def subscribe(topic) when is_atom(topic) or is_binary(topic) do
    if wildcard_topic?(topic) do
      Trie.subscribe(topic)
    else
      {:ok, _} = Registry.register(@registry, topic, nil)
      :ok
    end
  end

  @doc """
  Removes the calling process's subscription to `topic`.

  Returns `:ok` whether or not the caller was subscribed, making the call
  idempotent. If the caller subscribed multiple times to the same topic, all
  of those subscriptions are removed.

  ## Examples

      iex> Jobr.Broker.subscribe(:news)
      :ok
      iex> Jobr.Broker.unsubscribe(:news)
      :ok
      iex> Jobr.Broker.unsubscribe(:news)
      :ok

  """
  @spec unsubscribe(topic()) :: :ok
  def unsubscribe(topic) when is_atom(topic) or is_binary(topic) do
    if wildcard_topic?(topic) do
      Trie.unsubscribe(topic)
    else
      Registry.unregister(@registry, topic)
    end
  end

  @doc """
  Publishes `msg` to every process subscribed to `topic`.

  Each subscriber receives the tuple `{:jobr, topic, msg}` via `send/2`.
  Returns `:ok` even when no process is subscribed.

  A subscriber with several filters matching `topic` receives one copy.

  ## Examples

      iex> Jobr.Broker.send(:news, "hello")
      :ok

  """
  @spec send(topic(), term()) :: :ok
  def send(topic, msg) when is_atom(topic) or is_binary(topic) do
    message = {:jobr, topic, msg}

    exact = exact_subscribers(topic)
    wildcard = if is_binary(topic), do: Trie.match(topic), else: []

    # A pid that matches both exactly and through a wildcard gets one copy,
    # but duplicate subscriptions within one index still deliver twice — that
    # is the documented `Registry` behaviour preserved for exact topics.
    exact
    |> Enum.reject(&(&1 in wildcard))
    |> Kernel.++(wildcard)
    |> Enum.each(&Kernel.send(&1, message))

    :ok
  end

  @doc """
  Returns the list of pids currently subscribed to `topic`.

  For a binary `topic` this includes subscribers whose wildcard filters match
  it, deduplicated. Intended for introspection, debugging, and testing. The
  order is not guaranteed.

  ## Examples

      iex> Jobr.Broker.subscribers(:news)
      []

  """
  @spec subscribers(topic()) :: [pid()]
  def subscribers(topic) when is_atom(topic) or is_binary(topic) do
    exact = exact_subscribers(topic)
    wildcard = if is_binary(topic), do: Trie.match(topic), else: []

    Enum.uniq(exact ++ wildcard)
  end

  # ---------------------------------------------------------------- internals

  defp exact_subscribers(topic) do
    @registry
    |> Registry.lookup(topic)
    |> Enum.map(fn {pid, _value} -> pid end)
  end

  # A binary containing a wildcard level is routed to the trie; everything else
  # (atoms, and exact binary topics) uses the Registry's exact-key lookup.
  defp wildcard_topic?(topic) when is_binary(topic), do: Jobr.Broker.Topic.wildcard?(topic)
  defp wildcard_topic?(_atom), do: false
end

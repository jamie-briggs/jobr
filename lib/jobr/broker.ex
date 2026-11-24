defmodule Jobr.Broker do
  @moduledoc """
  A topic-based publish/subscribe broker.

  Processes may subscribe to a `topic` (an atom) and will then receive every
  message published to that topic. Messages are delivered with the standard
  Elixir `send/2` function as a tagged tuple:

      {:jmsg, topic, msg}

  ## Design

  Subscriptions are tracked in a `Registry` started with `keys: :duplicate`,
  which lets many processes subscribe to the same topic concurrently without
  any custom `GenServer` state or a single serialization point.

  The registry is configured to monitor subscribers, so a subscription is
  automatically removed when the subscribing process exits. This prevents
  leaked subscriptions from dead processes.

  ## Examples

      iex> Jobr.Broker.subscribe(:news)
      :ok
      iex> Jobr.Broker.send(:news, "hello")
      :ok
      # The calling process now has a message in its mailbox:
      iex> receive do {:jmsg, :news, msg} -> msg end
      "hello"

  """

  @registry Jobr.Broker.Registry

  @typedoc "A topic is an atom identifying a channel of messages."
  @type topic :: atom()

  @typedoc "The tagged tuple delivered to subscribers."
  @type message :: {:jmsg, topic(), term()}

  @doc """
  Returns a child specification for the broker's `Registry`.

  Add this to your supervision tree to start the broker. The registry is
  registered under `#{inspect(@registry)}` and is intended to be started once
  per node.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(_opts \\ []) do
    Registry.child_spec(keys: :duplicate, name: @registry)
  end

  @doc """
  Subscribes the calling process to `topic`.

  Returns `:ok`. Subscribing to a topic the process already subscribes to is
  allowed and results in duplicate delivery for that topic; call
  `unsubscribe/1` to remove all of the caller's subscriptions to it.

  ## Examples

      iex> Jobr.Broker.subscribe(:news)
      :ok

  """
  @spec subscribe(topic()) :: :ok
  def subscribe(topic) when is_atom(topic) do
    {:ok, _} = Registry.register(@registry, topic, nil)
    :ok
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
  def unsubscribe(topic) when is_atom(topic) do
    Registry.unregister(@registry, topic)
  end

  @doc """
  Publishes `msg` to every process subscribed to `topic`.

  Each subscriber receives the tuple `{:jmsg, topic, msg}` via `send/2`.
  Returns `:ok` even when no process is subscribed.

  ## Examples

      iex> Jobr.Broker.send(:news, "hello")
      :ok

  """
  @spec send(topic(), term()) :: :ok
  def send(topic, msg) when is_atom(topic) do
    message = {:jmsg, topic, msg}

    @registry
    |> Registry.dispatch(topic, fn entries ->
      for {pid, _value} <- entries, do: Kernel.send(pid, message)
    end)

    :ok
  end

  @doc """
  Returns the list of pids currently subscribed to `topic`.

  Intended for introspection, debugging, and testing. The order is not
  guaranteed, and a pid may appear more than once if it subscribed repeatedly.

  ## Examples

      iex> Jobr.Broker.subscribers(:news)
      []

  """
  @spec subscribers(topic()) :: [pid()]
  def subscribers(topic) when is_atom(topic) do
    @registry
    |> Registry.lookup(topic)
    |> Enum.map(fn {pid, _value} -> pid end)
  end
end

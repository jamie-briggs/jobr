defmodule Jobr.Broker.Topic do
  @moduledoc """
  MQTT topic filter parsing and matching.

  Implements the subscription-filter rules from MQTT 5.0 §4.7 so that the
  broker can match binary topics against wildcard filters such as
  `"sensors/+/temp"` and `"sensors/#"`.

  ## Rules implemented

    * `+` matches exactly one topic level.
    * `#` matches zero or more levels and must be the last level of a filter.
    * `$`-prefixed topics (e.g. `"$SYS/broker/uptime"`) are **not** matched by
      a filter whose first level is a wildcard (MQTT §4.7.2). They must be
      subscribed to explicitly, either literally or with a `$SYS/...` prefix.
    * The empty string is not a valid topic or filter.

  Filters are compiled once at subscription time into a list of levels, which
  makes matching a straight recursive walk with no re-parsing per publish.

  ## Examples

      iex> Jobr.Broker.Topic.valid_filter?("sensors/+/temp")
      true
      iex> Jobr.Broker.Topic.valid_filter?("sensors/#/temp")
      false
      iex> Jobr.Broker.Topic.matches?("sensors/+/temp", "sensors/room1/temp")
      true
      iex> Jobr.Broker.Topic.matches?("sensors/#", "sensors/room1/temp")
      true
      iex> Jobr.Broker.Topic.matches?("#", "$SYS/broker/uptime")
      false

  """

  @typedoc "A topic or filter split into its levels, with wildcards preserved."
  @type levels :: [binary()]

  @doc """
  Splits a topic or filter into levels.

  ## Examples

      iex> Jobr.Broker.Topic.split("a/b/c")
      ["a", "b", "c"]

  """
  @spec split(binary()) :: levels()
  def split(topic) when is_binary(topic), do: String.split(topic, "/")

  @doc """
  Returns `true` if `filter` is a syntactically valid MQTT subscription filter.

  Rejects empty filters, empty levels, `#` anywhere but last, and wildcards
  embedded in a level (e.g. `"a+/b"`).

  ## Examples

      iex> Jobr.Broker.Topic.valid_filter?("a/b")
      true
      iex> Jobr.Broker.Topic.valid_filter?("a/#")
      true
      iex> Jobr.Broker.Topic.valid_filter?("a/#/b")
      false
      iex> Jobr.Broker.Topic.valid_filter?("a//b")
      false
      iex> Jobr.Broker.Topic.valid_filter?("")
      false

  """
  @spec valid_filter?(binary()) :: boolean()
  def valid_filter?(filter) when is_binary(filter) and filter != "" do
    levels = split(filter)

    Enum.all?(levels, &valid_filter_level?/1) and
      match?([], Enum.drop_while(levels, &(&1 != "#")) |> Enum.drop(1))
  end

  def valid_filter?(_filter), do: false

  defp valid_filter_level?("#"), do: true
  defp valid_filter_level?("+"), do: true
  defp valid_filter_level?(""), do: false
  defp valid_filter_level?(level), do: not String.contains?(level, ["+", "#"])

  @doc """
  Returns `true` if `filter` matches the concrete `topic`.

  Both arguments are topic strings; `filter` may contain wildcards, `topic`
  may not. The `$`-prefix rule of MQTT §4.7.2 is applied.

  ## Examples

      iex> Jobr.Broker.Topic.matches?("a/b", "a/b")
      true
      iex> Jobr.Broker.Topic.matches?("a/+", "a/b")
      true
      iex> Jobr.Broker.Topic.matches?("a/+", "a/b/c")
      false
      iex> Jobr.Broker.Topic.matches?("a/#", "a")
      true

  """
  @spec matches?(binary(), binary()) :: boolean()
  def matches?(filter, topic) when is_binary(filter) and is_binary(topic) do
    filter_levels = split(filter)
    topic_levels = split(topic)

    not (dollar_topic?(topic_levels) and wildcard_first?(filter_levels)) and
      match_levels(filter_levels, topic_levels)
  end

  @doc """
  Returns `true` if the filter contains any wildcard level.

  Used by the broker to decide whether a filter needs the trie or can take the
  exact-match `Registry` fast path.
  """
  @spec wildcard?(binary()) :: boolean()
  def wildcard?(filter) when is_binary(filter) do
    filter |> split() |> Enum.any?(&(&1 in ["+", "#"]))
  end

  # A filter starting with a wildcard must not see $-prefixed topics.
  defp wildcard_first?(["+" | _]), do: true
  defp wildcard_first?(["#" | _]), do: true
  defp wildcard_first?(_), do: false

  defp dollar_topic?(["$" <> _ | _]), do: true
  defp dollar_topic?(_), do: false

  defp match_levels(["#" | _], _topic_levels), do: true
  defp match_levels([], []), do: true
  defp match_levels([], _topic_levels), do: false
  defp match_levels(_filter_levels, []), do: false

  defp match_levels(["+" | filter_rest], [_topic_level | topic_rest]),
    do: match_levels(filter_rest, topic_rest)

  defp match_levels([level | filter_rest], [level | topic_rest]),
    do: match_levels(filter_rest, topic_rest)

  defp match_levels(_filter_levels, _topic_levels), do: false
end

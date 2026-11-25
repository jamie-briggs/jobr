defmodule Jobr.Broker.TopicTest do
  use ExUnit.Case, async: true

  alias Jobr.Broker.Topic

  describe "split/1" do
    test "splits a topic into levels" do
      assert Topic.split("a/b/c") == ["a", "b", "c"]
    end

    test "keeps wildcards as their own levels" do
      assert Topic.split("a/+/#") == ["a", "+", "#"]
    end

    test "a single level has no separators" do
      assert Topic.split("a") == ["a"]
    end
  end

  describe "valid_filter?/1" do
    test "accepts literal filters" do
      assert Topic.valid_filter?("a/b/c")
      assert Topic.valid_filter?("sensors/room1/temp")
    end

    test "accepts + at any level" do
      assert Topic.valid_filter?("+")
      assert Topic.valid_filter?("a/+/c")
      assert Topic.valid_filter?("+/+/+")
    end

    test "accepts # only as the last level" do
      assert Topic.valid_filter?("#")
      assert Topic.valid_filter?("a/#")
      refute Topic.valid_filter?("a/#/b")
      refute Topic.valid_filter?("#/a")
    end

    test "rejects empty levels" do
      refute Topic.valid_filter?("a//b")
      refute Topic.valid_filter?("/a")
      refute Topic.valid_filter?("a/")
    end

    test "rejects wildcards embedded in a level" do
      refute Topic.valid_filter?("a+/b")
      refute Topic.valid_filter?("a/b#")
      refute Topic.valid_filter?("a/b+")
    end

    test "rejects the empty filter" do
      refute Topic.valid_filter?("")
    end

    test "rejects non-binaries" do
      refute Topic.valid_filter?(:news)
      refute Topic.valid_filter?(nil)
    end
  end

  describe "matches?/2" do
    test "literal filters match only themselves" do
      assert Topic.matches?("a/b", "a/b")
      refute Topic.matches?("a/b", "a/c")
      refute Topic.matches?("a/b", "a/b/c")
    end

    test "+ matches exactly one level" do
      assert Topic.matches?("a/+/c", "a/b/c")
      refute Topic.matches?("a/+/c", "a/b/d")
      refute Topic.matches?("a/+", "a/b/c")
      refute Topic.matches?("a/+", "a")
    end

    test "+ matches an empty level" do
      # "a//c" is not a valid filter, but a published topic may contain an
      # empty level, and `+` matches it.
      assert Topic.matches?("a/+/c", "a//c")
    end

    test "# matches zero or more levels" do
      assert Topic.matches?("a/#", "a")
      assert Topic.matches?("a/#", "a/b")
      assert Topic.matches?("a/#", "a/b/c")
      refute Topic.matches?("a/#", "b")
    end

    test "# alone matches everything except $-prefixed topics" do
      assert Topic.matches?("#", "a")
      assert Topic.matches?("#", "a/b/c")
      refute Topic.matches?("#", "$SYS/broker/uptime")
    end

    test "$-prefixed topics are not matched by a leading wildcard" do
      refute Topic.matches?("+/broker/uptime", "$SYS/broker/uptime")
      assert Topic.matches?("$SYS/#", "$SYS/broker/uptime")
      assert Topic.matches?("$SYS/broker/+", "$SYS/broker/uptime")
    end

    test "a $ topic is matched by a literal filter" do
      assert Topic.matches?("$SYS/broker/uptime", "$SYS/broker/uptime")
    end

    test "a non-leading wildcard still matches inside a $ topic" do
      assert Topic.matches?("$SYS/+/uptime", "$SYS/broker/uptime")
    end
  end

  describe "wildcard?/1" do
    test "detects wildcard levels" do
      assert Topic.wildcard?("a/#")
      assert Topic.wildcard?("a/+")
      assert Topic.wildcard?("#")
    end

    test "returns false for literal filters" do
      refute Topic.wildcard?("a/b")
      refute Topic.wildcard?("a/b/c")
    end
  end
end

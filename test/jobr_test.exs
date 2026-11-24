defmodule JobrTest do
  use ExUnit.Case
  doctest Jobr

  test "greets the world" do
    assert Jobr.hello() == :world
  end
end

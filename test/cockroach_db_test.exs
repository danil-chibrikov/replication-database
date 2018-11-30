defmodule CockroachDbTest do
  use ExUnit.Case
  doctest CockroachDb

  test "greets the world" do
    assert CockroachDb.hello() == :world
  end
end

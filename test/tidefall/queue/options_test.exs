defmodule Tidefall.Queue.OptionsTest do
  use ExUnit.Case, async: true

  alias Tidefall.Queue.Options

  describe "validate_runtime_options!/1 — :sort_key" do
    test "ok: accepts an arity-1 function" do
      fun = fn item -> item end

      assert Options.validate_runtime_options!(sort_key: fun)[:sort_key] == fun
    end

    test "ok: accepts an arity-0 function" do
      fun = fn -> 1 end

      assert Options.validate_runtime_options!(sort_key: fun)[:sort_key] == fun
    end

    test "ok: omitting :sort_key validates and leaves it unset" do
      refute Keyword.has_key?(Options.validate_runtime_options!([]), :sort_key)
    end

    test "ok: the shared :partition_key option still validates alongside :sort_key" do
      opts = Options.validate_runtime_options!(sort_key: fn -> 1 end, partition_key: :p)

      assert opts[:partition_key] == :p
    end

    test "error: rejects a non-function term" do
      for bad <- [:high, 123, "x", %{}] do
        assert_raise NimbleOptions.ValidationError, fn ->
          Options.validate_runtime_options!(sort_key: bad)
        end
      end
    end

    test "error: rejects a function of the wrong arity" do
      assert_raise NimbleOptions.ValidationError, fn ->
        Options.validate_runtime_options!(sort_key: fn _a, _b -> :x end)
      end
    end
  end

  describe "runtime_options_docs/0" do
    test "ok: renders the :sort_key option" do
      assert Options.runtime_options_docs() =~ "sort_key"
    end
  end
end

defmodule Tidefall.Generators do
  @moduledoc false

  import StreamData

  # Atoms that are operators in match-spec guards and bodies
  @op_atoms ~w(const =:= + element andalso hd)a

  @doc """
  Any term: numbers (bignums, `0.0`, `-0.0`), binaries, pids and
  atoms, nested in lists, improper lists, tuples, op-like tuples
  such as `{:const, x}`, and maps.

  With `:key_safe`, it never generates maps or the atoms a match
  head reads as patterns (`:_`, `:"$1"`, `:"$_"`, ...).
  """
  @spec any_term(:any | :key_safe) :: StreamData.t(term())
  def any_term(mode \\ :any) do
    mode
    |> leaf()
    |> tree(&one_of(containers(&1, mode)))
    |> scale(&min(&1, 8))
  end

  @doc """
  A version for `put_newer`: small integers, atoms and short
  binaries, so equal versions and mixed types are common.
  """
  @spec version() :: StreamData.t(integer() | atom() | binary())
  def version do
    one_of([
      integer(-5..5),
      member_of(atoms(:any)),
      member_of(["", "a", "b", "z"])
    ])
  end

  defp leaf(mode) do
    one_of([
      integer(),
      member_of([2 ** 70, -(2 ** 90)]),
      float(),
      member_of([0.0, -0.0]),
      binary(max_length: 8),
      constant(self()),
      # nil is the usual sentinel, so it gets its own branch
      constant(nil),
      member_of(atoms(mode))
    ])
  end

  defp containers(child, :any) do
    [map_of(child, child, max_length: 2) | containers(child, :key_safe)]
  end

  defp containers(child, :key_safe) do
    [
      list_of(child, max_length: 3),
      map(list_of(child, max_length: 3), &List.to_tuple/1),
      tuple({member_of(@op_atoms), child}),
      map({child, child}, fn {head, tail} -> [head | tail] end)
    ]
  end

  # A match head reads these as patterns, so keys without a hasher skip them
  defp atoms(:any), do: ~w(_ $0 $1 $2 $_ $$)a ++ atoms(:key_safe)
  defp atoms(:key_safe), do: ~w(a b nil true false)a ++ @op_atoms
end

defmodule Tidefall.CoalescedMap.Entry do
  @moduledoc """
  A processed entry from `Tidefall.CoalescedMap`.

  The processor receives a list of these structs on every
  processing tick:

      fn batch ->
        Enum.each(batch, fn %Tidefall.CoalescedMap.Entry{key: k, value: v} ->
          process(k, v)
        end)
      end

  See `t:t/0` for the full field-by-field semantics.
  """

  defstruct [:key, :value, :version, :updates]

  @typedoc """
  A processed entry struct.

  ## Fields

    * `:key` — The user-facing key. If the entry was written with
      the `:key_hasher` option, `:key` holds the **original**
      (pre-hash) key; the hash is an internal storage detail and
      is never surfaced here.
    * `:value` — The stored value.
    * `:version` — The version stamp. Set explicitly via
      `Tidefall.CoalescedMap.put_newer/4` /
      `Tidefall.CoalescedMap.put_all_newer/3`; `0` for entries
      written via plain `Tidefall.CoalescedMap.put/4` /
      `Tidefall.CoalescedMap.put_all/3`.
    * `:updates` — How many times this entry was conditionally
      replaced by a newer version. `0` for new inserts and for
      entries written via plain `Tidefall.CoalescedMap.put/4` /
      `Tidefall.CoalescedMap.put_all/3`.

  """
  @type t() :: %__MODULE__{
          key: any(),
          value: any(),
          version: Tidefall.CoalescedMap.version(),
          updates: non_neg_integer()
        }
end

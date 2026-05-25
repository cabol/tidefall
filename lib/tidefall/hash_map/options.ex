defmodule Tidefall.HashMap.Options do
  @moduledoc false

  alias Tidefall.Buffer

  # HashMap-specific runtime options, layered on top of the shared
  # runtime options exposed by `Tidefall.Buffer.Options` (currently just
  # `:partition_key`).
  hm_runtime_opts = [
    key_hasher: [
      type: {:or, [{:in, [true]}, {:fun, 1}]},
      required: false,
      doc: """
      Optional key hashing for complex keys that don't survive ETS
      match-spec equality (e.g. keys containing maps).

      Can be one of:

        * Omitted (default) — no hashing; the key is stored and
          looked up as-is.
        * `true` — hash the key with `:erlang.phash2/1`. Fast, but
          28-bit and so collision-prone; use only when collisions
          are acceptable.
        * A function of arity 1 — applied to the key to produce the
          storage/lookup key. Use a cryptographic hash (e.g.
          `&:crypto.hash(:sha256, :erlang.term_to_binary(&1))`) if
          you need collision resistance.

      > #### Use consistently {: .warning}
      >
      > If you write a key with a given `:key_hasher`, you **must**
      > pass the same `:key_hasher` to every subsequent `get/4`,
      > `delete/3`, `put_newer/4`, and `put_all_newer/3` call for
      > that key. Otherwise the lookup will compute a different
      > storage key and miss the entry. Mixing hashed and non-hashed
      > writes for the same logical key produces two distinct entries
      > under the hood — that's a caller error, not a library bug.
      """
    ]
  ]

  # Additional options accepted by `put_newer/4` on top of the shared
  # runtime options.
  put_newer_opts =
    hm_runtime_opts ++
      [
        version: [
          type: {:or, [:integer, :atom, :string]},
          required: false,
          doc: """
          Version stamp used to resolve conflicts. Accepted types:
          integer, atom, or binary. Defaults (at call time) to
          `:erlang.unique_integer([:monotonic, :positive])` — a
          positive, strictly-monotonic VM-local integer; it's safe
          to mix with plain `put/4` entries (whose version is `0`).

          Versions are compared with Erlang's `>` operator, which
          uses [the total term order](https://www.erlang.org/doc/system/expressions.html#term-comparisons).
          Typical choices: integers (sequence numbers, timestamps),
          binaries (e.g. ULIDs, lexicographic IDs), or atoms
          (status-like ordering).

          > #### Mixing types {: .warning}
          >
          > Erlang's total order is well-defined across types
          > (`number < atom < ... < bitstring`), but mixing types
          > on the same key produces surprising results — for
          > example, `100 < :any_atom` and `:zzz < "anystring"`.
          > Pick one version type per key and stick with it.
          > Correctness here is the caller's responsibility.
          """
        ]
      ]

  # Runtime schema: shared opts + HashMap opts.
  @runtime_opts_schema NimbleOptions.new!(Buffer.Options.runtime_opts() ++ hm_runtime_opts)

  # put_newer schema: runtime schema + :version.
  @put_newer_opts_schema NimbleOptions.new!(Buffer.Options.runtime_opts() ++ put_newer_opts)

  ## API

  @spec runtime_options_docs() :: binary()
  def runtime_options_docs do
    NimbleOptions.docs(@runtime_opts_schema)
  end

  @spec put_newer_options_docs() :: binary()
  def put_newer_options_docs do
    NimbleOptions.docs(@put_newer_opts_schema)
  end

  @spec validate_runtime_options!(keyword()) :: keyword()
  def validate_runtime_options!(opts) do
    NimbleOptions.validate!(opts, @runtime_opts_schema)
  end

  @spec validate_put_newer_options!(keyword()) :: keyword()
  def validate_put_newer_options!(opts) do
    NimbleOptions.validate!(opts, @put_newer_opts_schema)
  end
end

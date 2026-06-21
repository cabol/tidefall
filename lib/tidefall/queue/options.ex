defmodule Tidefall.Queue.Options do
  @moduledoc false

  alias Tidefall.Buffer

  # Queue-specific runtime options, layered on top of the shared runtime
  # options exposed by `Tidefall.Buffer.Options` (currently `:partition_key`).
  # `:sort_key` is resolved per call (like `:partition_key`), so the client
  # builds the ETS key without any per-buffer lookup.
  queue_runtime_opts = [
    sort_key: [
      type: {:or, [{:fun, 1}, {:fun, 0}]},
      required: false,
      doc: """
      Controls the term used to order buffered items within a partition.
      The ETS key is `{sort_key_term, ref}`; `ref` is always retained for
      uniqueness, so distinct items are never overwritten regardless of the
      `:sort_key` value.

      Can be one of:

        * Omitted (default) — items order by insertion time
          (`System.monotonic_time/0`), i.e. the order they were pushed.
        * A function of arity 1 — applied to each item to derive its sort
          term (e.g. `& &1.priority`, or an event timestamp carried in the
          payload).
        * A function of arity 0 — evaluated per item to generate the sort
          term at push time (e.g. a custom clock or sequence).

      > #### Ordering scope and ties {: .info}
      >
      > Ordering is **per partition** — items are routed across partitions
      > first, and each partition's batch is ordered independently. For a
      > single global order use `partitions: 1` or a `:partition_key` that
      > co-locates the items you need ordered together. Order among items
      > with the **same** sort term is unspecified (broken by `ref`),
      > consistent with how same-timestamp items already drain today. Sort
      > terms are compared with Erlang's total term order, so prefer
      > integers, atoms, or binaries; complex terms (maps, tuples) sort in
      > non-obvious ways.
      """
    ]
  ]

  # Runtime schema: shared opts + Queue opts.
  @runtime_opts_schema NimbleOptions.new!(Buffer.Options.runtime_opts() ++ queue_runtime_opts)

  ## API

  @spec runtime_options_docs() :: binary()
  def runtime_options_docs do
    NimbleOptions.docs(@runtime_opts_schema)
  end

  @spec validate_runtime_options!(keyword()) :: keyword()
  def validate_runtime_options!(opts) do
    NimbleOptions.validate!(opts, @runtime_opts_schema)
  end
end

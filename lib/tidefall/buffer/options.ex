defmodule Tidefall.Buffer.Options do
  @moduledoc false

  # update options
  update_opts = [
    processing_interval: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(5),
      doc: """
      How often (in milliseconds) each partition checks its buffer and initiates
      processing. Messages are processed at this interval if any are buffered.
      Lower values mean faster processing but more frequent task spawning.
      """
    ],
    processing_timeout: [
      type: :timeout,
      required: false,
      default: :timer.minutes(1),
      doc: """
      Maximum time (in milliseconds) for a processing task to complete before
      being forcefully terminated. Used during graceful shutdown to wait for
      in-flight processing to complete. See `Task.Supervisor.async_nolink/3`
      for more information.
      """
    ],
    processing_batch_size: [
      type: {:or, [:pos_integer, {:in, [:table]}]},
      required: false,
      default: 10,
      doc: """
      Controls how buffered data is passed to the processor callback.

      Can be either:

        * A positive integer (default `10`): Messages are read from the ETS
          table in batches of up to this size using `ets:select` with
          continuations. The processor is called once per batch. This
          optimizes memory usage for large tables.

        * `:table`: The ETS table identifier (a tid) is passed directly to
          the processor instead of reading and batching the data. The
          processor has full control over how it reads and processes the
          table.

          In this mode the **processor takes ownership of the table**. The
          buffer creates a fresh table for incoming writes on the next
          swap and does NOT delete the table you received. When the
          processing task exits (after the processor returns), ETS will
          auto-delete the table unless you have already transferred it
          elsewhere via `:ets.give_away/3`. If you want to **keep the
          table** beyond the task's lifetime, call `:ets.give_away/3` to
          hand it off to another process before returning.
      """
    ]
  ]

  # Start options
  start_opts =
    [
      name: [
        type: :atom,
        required: true,
        doc: """
        The buffer name (used to identify the buffer).
        """
      ],
      processor: [
        type: {:or, [:mfa, fun: 1]},
        required: true,
        doc: """
        A callback that processes batches of messages. Called with a list
        of accumulated messages and should handle the processing logic (e.g., send
        to external service, persist to database, etc.).

        Can be either:

          * A function of arity 1: `fn batch -> ... end` or `&MyModule.process/1`.
          * An MFA tuple `{Module, Function, Args}`: The batch is prepended to
            the arguments, e.g., `{MyModule, :process, [extra_arg]}` will call
            `MyModule.process(batch, extra_arg)`.
        """
      ],
      partitions: [
        type: :non_neg_integer,
        required: false,
        doc: """
        Number of partitions to create. Each partition has its own buffer and
        processing cycle. Defaults to `System.schedulers_online()` to match the
        number of available schedulers. More partitions reduce lock contention
        but increase per-partition overhead.
        """
      ]
    ] ++ update_opts

  auto_opts = [
    module: [
      type: :atom,
      required: true,
      doc: """
      The buffer implementation module (e.g., `Tidefall.Queue`).

      This option is automatically set when starting a buffer through a
      specific implementation like `Tidefall.Queue.start_link/1`.
      It must be provided explicitly when calling
      `Tidefall.Buffer.start_link/1` directly.
      """
    ]
  ]

  # Shared runtime options. Exposed via `runtime_opts/0` so impl-specific
  # option modules (e.g. `Tidefall.HashMap.Options`) can compose these
  # with their own options into a single NimbleOptions schema.
  @runtime_opts [
    partition_key: [
      type: :any,
      required: false,
      default: nil,
      doc: """
      Determines what value is used as the routing key for partitioning
      messages.

      Can be one of four values:

        * `nil` (default): The message itself is used as the routing key.
          Messages with the same content are routed to the same partition.

        * A function of arity 1: Applied to each message to return the routing
          key. Allows grouping related messages together (e.g., by user ID or
          account ID) to keep them in the same partition.

        * An MFA tuple `{Module, Function, Args}`: The function is applied with
          the message prepended to the arguments. Useful for delegating routing
          logic to a module function while keeping configuration declarative.

        * Any static term: Used as the routing key for all messages, giving
          explicit control over which partition receives them (e.g., `:logs`,
          `:events`, or an identifier).

      Fundamentally, this option determines how messages are distributed across
      partitions. Use it to keep related messages together (for ordering or
      state locality) or spread unrelated messages across partitions
      (for parallelism).
      """
    ]
  ]

  # Start options schema
  @start_opts_schema NimbleOptions.new!(start_opts ++ auto_opts)

  # Start options schema only for docs
  @start_opts_docs_schema NimbleOptions.new!(start_opts)

  # Runtime options schema
  @runtime_opts_schema NimbleOptions.new!(@runtime_opts)

  # update options schema
  @update_opts_schema NimbleOptions.new!(update_opts)

  ## API

  @spec start_options_docs() :: binary()
  def start_options_docs do
    NimbleOptions.docs(@start_opts_docs_schema)
  end

  @spec runtime_options_docs() :: binary()
  def runtime_options_docs do
    NimbleOptions.docs(@runtime_opts_schema)
  end

  @spec update_options_docs() :: binary()
  def update_options_docs do
    NimbleOptions.docs(@update_opts_schema)
  end

  @spec validate_start_options!(keyword()) :: keyword()
  def validate_start_options!(opts) do
    opts
    |> Keyword.take(Keyword.keys(@start_opts_schema.schema))
    |> NimbleOptions.validate!(@start_opts_schema)
  end

  @spec validate_runtime_options!(keyword()) :: keyword()
  def validate_runtime_options!(opts) do
    NimbleOptions.validate!(opts, @runtime_opts_schema)
  end

  @spec validate_update_options!(keyword()) :: keyword()
  def validate_update_options!(opts) do
    NimbleOptions.validate!(opts, @update_opts_schema)
  end

  @doc """
  The raw NimbleOptions keyword list of shared runtime options.

  Used by impl-specific option modules to compose these options
  with their own.
  """
  @spec runtime_opts() :: keyword()
  def runtime_opts, do: @runtime_opts
end

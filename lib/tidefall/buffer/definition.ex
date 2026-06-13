defmodule Tidefall.Buffer.Definition do
  @moduledoc false

  # Shared code generation for buffer-type definition modules (the
  # `use Tidefall.Queue` / `use Tidefall.HashMap` facade). Each buffer
  # type ships its own `__using__/1` that calls `define/3` with its
  # buffer-type module and the list of public operations to delegate.
  # This module owns: config precedence (compile opts < app env <
  # explicit opts), the `start_link/1` + `child_spec/1` wrappers, and
  # the distinct-arity delegation scheme described below.
  #
  # ## Distinct-arity delegation (no defaulted leading name)
  #
  # For each buffer-type op `f(buffer, a, b, opts \\ [])` we generate:
  #
  #   * nameless variants that mirror the buffer type's own optional
  #     params and pre-bind `__MODULE__` as the buffer — e.g. `f(a, b)`
  #     and `f(a, b, opts)`.
  #   * ONE nameful variant at the FULL arity with every param explicit
  #     (opts required, NOT defaulted) — e.g. `f(name, a, b, opts)`.
  #
  # We never emit `f(name \\ __MODULE__, ..., opts \\ [])`: a leading
  # defaulted name plus a trailing defaulted opts silently misroutes
  # (e.g. `put(:k, "v", partition_key: 1)` would bind `:k` as the name).
  # Distinct arities make every call unambiguous at compile time.

  @typedoc false
  @type op_spec() :: {atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @doc """
  Generates the definition-module body for `buffer_type` with the given
  per-op `specs`.

  `opts` are the compile-time `use` options; `:otp_app` is required and a
  missing one raises `ArgumentError` at compile time. Each spec is
  `{name, leading_params, min_optional, max_optional}` where
  `leading_params` is the count of required non-buffer/non-opts
  params, and the optional-param window controls how many nameless
  arities are emitted. The full nameful arity is always
  `leading_params + max_optional + 1` (the `+1` is the leading name).
  Each spec is checked against `buffer_type` at compile time — a spec
  whose required arities are not exported raises `ArgumentError`, so a
  miscounted tuple fails the build instead of emitting wrong delegations.
  """
  @spec define(module(), [op_spec()], keyword()) :: Macro.t()
  def define(buffer_type, specs, opts) do
    validate_otp_app!(buffer_type, opts)
    validate_specs!(buffer_type, specs)

    ops = Enum.map(specs, &op_clauses(buffer_type, &1))

    quote do
      @doc false
      def __buffer_type__, do: unquote(buffer_type)

      # Inject the compile-time `use` opts into a function body (not a
      # module attribute) so function captures in the opts — e.g.
      # `processor: &Foo.run/1` — compile naturally instead of being
      # broken by `Macro.escape/1`.
      @doc false
      def __compile_opts__, do: unquote(opts)

      @doc false
      def start_link(start_opts \\ []) do
        __MODULE__
        |> unquote(__MODULE__).resolve_opts(start_opts)
        |> unquote(buffer_type).start_link()
      end

      @doc false
      def child_spec(child_opts) do
        resolved = unquote(__MODULE__).resolve_opts(__MODULE__, child_opts)

        %{
          id: resolved[:name] || __MODULE__,
          start: {__MODULE__, :start_link, [child_opts]},
          type: :supervisor
        }
      end

      defoverridable child_spec: 1, start_link: 1

      unquote(ops)
    end
  end

  @doc """
  Merges config layers (lowest → highest): compile-time `use` opts,
  then the application env for `otp_app`/`module`, then the explicit
  `start_link`/child-spec opts. Finally `Keyword.put_new(:name, module)`
  so an explicit name always wins over the default.

  `:otp_app` is required in the `use` opts; this raises `KeyError`
  when it is missing.
  """
  @spec resolve_opts(module(), keyword()) :: keyword()
  def resolve_opts(module, explicit_opts) do
    compile_opts = module.__compile_opts__()
    {otp_app, compile_opts} = Keyword.pop!(compile_opts, :otp_app)
    env_opts = Application.get_env(otp_app, module, [])

    compile_opts
    |> Keyword.merge(env_opts)
    |> Keyword.merge(explicit_opts)
    |> Keyword.put_new(:name, module)
  end

  ## Private functions

  # `:otp_app` is mandatory; fail at compile time with an actionable
  # message rather than letting `resolve_opts/2` raise a bare KeyError at
  # the first start.
  defp validate_otp_app!(buffer_type, opts) do
    case Keyword.get(opts, :otp_app) do
      app when is_atom(app) and not is_nil(app) ->
        :ok

      _ ->
        raise ArgumentError,
              "#{inspect(buffer_type)} requires the :otp_app option to be an OTP " <>
                "application name (atom) — use it as " <>
                "`use #{inspect(buffer_type)}, otp_app: :your_app`"
    end
  end

  # The op specs are hand-authored; assert every arity they will generate
  # actually exists on the backend so a miscounted tuple fails the build
  # instead of silently delegating to a wrong/absent arity.
  defp validate_specs!(buffer_type, specs) do
    Code.ensure_loaded?(buffer_type) ||
      raise ArgumentError, "#{inspect(buffer_type)} could not be loaded for op-spec validation"

    for {name, leading, min_opt, max_opt} <- specs,
        arity <- (1 + leading + min_opt)..(1 + leading + max_opt) do
      function_exported?(buffer_type, name, arity) ||
        raise ArgumentError,
              "#{inspect(buffer_type)} does not export #{name}/#{arity}, " <>
                "required by op spec {#{inspect(name)}, #{leading}, #{min_opt}, #{max_opt}}"
    end
  end

  # Builds the def clauses for a single op: the nameless variants
  # (pre-binding `__MODULE__`) across the optional-param window, plus
  # the single full-arity nameful variant.
  defp op_clauses(buffer_type, {name, leading, min_opt, max_opt}) do
    # Distinct variable names across leading + optional params so a
    # single clause never binds two args to the same name.
    all_vars = Macro.generate_arguments(leading + max_opt, __MODULE__)

    nameless =
      for n_opt <- min_opt..max_opt do
        args = Enum.take(all_vars, leading + n_opt)

        quote do
          def unquote(name)(unquote_splicing(args)) do
            unquote(buffer_type).unquote(name)(__MODULE__, unquote_splicing(args))
          end
        end
      end

    name_var = Macro.var(:name, __MODULE__)
    full_args = [name_var | all_vars]

    nameful =
      quote do
        def unquote(name)(unquote_splicing(full_args)) do
          unquote(buffer_type).unquote(name)(unquote_splicing(full_args))
        end
      end

    nameless ++ [nameful]
  end
end

defmodule Tidefall.MixProject do
  use Mix.Project

  @version "1.0.0-rc.1"
  @source_url "https://github.com/cabol/tidefall"

  def project do
    [
      app: :tidefall,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),

      # Testing
      test_coverage: [tool: ExCoveralls, export: "test-coverage"],

      # Usage rules
      usage_rules: usage_rules(),

      # Dialyzer
      dialyzer: dialyzer(),

      # Hex
      package: package(),
      description: "ETS-based buffer for high-throughput writes and batch processing",

      # Docs
      name: "Tidefall",
      docs: docs()
    ]
  end

  def application do
    [
      mod: {Tidefall.Application, []},
      extra_applications: [:logger]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "test.ci": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:nimble_options, "~> 1.0"},
      {:telemetry, "~> 1.0"},

      # Test & Code Analysis
      {:excoveralls, "~> 0.18", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},

      # Benchmarks
      {:benchee, "~> 1.5", only: [:dev, :test]},

      # Agent usage rules
      {:usage_rules, "~> 1.2", only: :dev, runtime: false},

      # Docs
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "test.ci": [
        "deps.unlock --check-unused",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "credo --strict",
        "coveralls.html",
        "dialyzer --format short"
      ],
      "ur.sync": ["usage_rules.sync"]
    ]
  end

  defp package do
    [
      name: :tidefall,
      links: %{"GitHub" => @source_url},
      files: ~w(lib usage-rules .formatter.exs mix.exs README* CHANGELOG*),
      licenses: ~w(MIT)
    ]
  end

  defp docs do
    [
      main: "Tidefall",
      source_ref: "v#{@version}",
      source_url: @source_url,
      canonical: "https://hexdocs.pm/tidefall",
      groups_for_modules: [
        # Tidefall
        # Tidefall.Buffer
        # Tidefall.Buffer.Partition

        "Buffer implementations": [
          Tidefall.HashMap,
          Tidefall.Queue
        ]
      ]
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/" <> plt_file_name()},
      flags: [
        :error_handling,
        :extra_return,
        :no_opaque,
        :no_return
      ]
    ]
  end

  defp plt_file_name do
    "dialyzer-#{Mix.env()}-Elixir-#{System.version()}-OTP-#{System.otp_release()}.plt"
  end

  defp usage_rules do
    [
      # The file to write usage rules into (required for usage_rules syncing)
      file: "AGENTS.md",

      # Imported base guidance referenced from AGENTS.md via @deps/... links,
      # so contributors read the canonical rules without inlined drift.
      # Local rules in `usage-rules/` are not duplicated here.
      usage_rules: [
        {:usage_rules, [sub_rules: ["elixir", "otp"], link: :at]}
      ]
    ]
  end
end

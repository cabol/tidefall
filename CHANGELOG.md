# Changelog

All notable changes to this project will be documented in this file.

This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.0.0-rc.0](https://github.com/cabol/tidefall/tree/v1.0.0-rc.0) (2026-06-13)

### Enhancements

- `Tidefall.Queue` — insertion-ordered ETS buffer (`:ordered_set`) that
  accumulates items and drains them to a processor in periodic batches.
- `Tidefall.HashMap` — coalescing key-value buffer (`:set`, last-write-wins)
  with version-aware conditional writes (`put_newer/4`, `put_all_newer/3`)
  and an optional `:key_hasher` for complex keys.
- Module-based buffers via `use Tidefall.Queue` / `use Tidefall.HashMap` —
  the module name becomes the default instance, with start options layered
  across compile-time `use` opts, the application environment, and explicit
  `start_link`/child-spec opts. `:otp_app` is required and validated at
  compile time.
- Partitioned writes (`:erlang.phash2/2` routing, configurable
  `:partition_key`) with double-buffering for zero-downtime processing.
- Telemetry events under `[:tidefall, :partition, ...]` covering partition
  lifecycle and batch processing.

# Changelog

All notable changes to this project will be documented in this file.

This project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
> [Full Changelog](https://github.com/cabol/tidefall/compare/v1.0.0-rc.0...HEAD)

### Enhancements

- [Tidefall.Queue] Added the `:sort_key` runtime option on `push/3` to control
  how buffered items are ordered within a partition. Defaults to insertion
  order; accepts a function (arity 1 applied to each item, or arity 0 evaluated
  per item) that returns the sort term. Ordering is per partition and no item
  is ever dropped.
- [Tidefall.Buffer] Added the `:drain_threshold` and `:drain_check_interval`
  options (available on both `Tidefall.Queue` and `Tidefall.HashMap`) to drain
  a partition early once it reaches `:drain_threshold` items, instead of
  waiting for the next `:processing_interval` tick — draining on whichever
  fires first. Per-partition, lossless (an early-flush trigger, not a cap), and
  off by default.

### Bug Fixes

- [Tidefall.Buffer] Fixed `update_options/2` so a partial update changes only
  the options you pass; previously, omitting an option silently reset it to its
  default (e.g. updating `:processing_interval` also reset
  `:processing_batch_size` to `10`).

## [v1.0.0-rc.0](https://github.com/cabol/tidefall/tree/v1.0.0-rc.0) (2026-06-13)

### Enhancements

- [Tidefall.Queue] Insertion-ordered ETS buffer (`:ordered_set`) that
  accumulates items and drains them to a processor in periodic batches.
- [Tidefall.HashMap] Coalescing key-value buffer (`:set`, last-write-wins)
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

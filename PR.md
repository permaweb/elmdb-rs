# Draft PR: Fix LMDB Environment Error Propagation

## Summary
Improve `env_open` error propagation so callers can reliably distinguish host-level space/lock pressure from generic environment failures.

## Problem
`env_open` currently collapses some LMDB open failures into `environment_error`, which makes recovery logic in dependent systems less precise.

Two concrete gaps:
- `no_lock` option is not parsed/applied in the NIF.
- LMDB OS error `Other(28)` is not surfaced as `no_space`.

## Changes
File changed:
- `native/elmdb_nif/src/lib.rs`

Minimal functional edits:
1. Parse `no_lock` in environment options (`parse_env_options` / `EnvOptions`).
2. Apply `EnvironmentFlags::NO_LOCK` when `no_lock` is set.
3. Map `lmdb::Error::Other(28)` to `atoms::no_space()` in `env_open`.

No API shape changes, no behavior changes outside these error/flag paths.

## Why This Is Minimal
- No new modules, no refactors, no call-site changes.
- Only the smallest set of lines needed to expose existing LMDB signal and wire an already-used option.
- Preserves all existing return formats and atoms.

## Validation
- Built NIF and Erlang wrapper via `rebar3 eunit` (compile phase successful).
- On this host, eunit setup fails due a pre-existing LMDB/system condition returning `{error,no_space}` in test setup paths; this is consistent with the error-propagation path addressed by this change.

## Downstream Impact
Consumers can now:
- Retry/open with clearer branching when encountering `no_space`.
- Explicitly request `NO_LOCK` behavior by passing `no_lock` in `env_open` options.

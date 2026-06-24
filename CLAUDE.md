# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`elmdb` is a C NIF binding for LMDB, scoped to exactly the operations HyperBEAM's
`hb_store_lmdb` uses. Keep it that way: lean, auditable, no speculative surface.

## Build / test

```
make            # rebar3 compile (pre-hook builds the C NIF)
make test       # rebar3 eunit
rebar3 eunit --module=elmdb_test
```

The NIF is built by `native/elmdb_nif/build_c_nif.sh` (invoked by the rebar
`pre_hooks`), not by rebar itself. It compiles `elmdb_nif.c` together with the
vendored LMDB sources and links one shared object into `priv/`.

## Architecture

- `src/elmdb.erl` — thin Erlang layer: NIF stubs + the `match/2` wrapper.
- `native/elmdb_nif/c_src/elmdb_nif.c` — the NIF. Global registries map a path to
  a singleton env/db resource. Writes are buffered per-db and flushed before
  reads / at `batch_size` / on close.
- `native/elmdb_nif/c_src/lmdb/` — vendored LMDB. **Do not edit it.**

## Invariants to preserve

- **API surface = live HB paths only:** `env_open`, `db_open`, `put`, `get`,
  `list`, `match`, `env_close_by_name`. Don't add functions unless `hb_store_lmdb`
  needs them.
- **Read concurrency:** reads hold the env's shared rwlock (`env_lock`) for the
  txn; env open/close take it exclusively. Never serialize reads behind a mutex.
- **Lock order:** `REGISTRY_MUTEX` > `db->mutex` > `env->mutex` > `env_lock`.
- **Zero-copy scans:** `list`/`match` keep pointers into the mmap (valid for the
  txn) and copy once into a packed binary before the txn ends.
- **Vendored LMDB stays pristine** — no patches to `mdb.c`/`midl.c`.

## Conventions

- Always binaries on the wire for keys/values.
- Hyphenated error atoms; structured returns (`ok` / `{ok, _}` / `not_found` /
  `{error, Type, Msg}`).
- Match the surrounding C style; keep diffs surgical.

# elmdb

A small, fast Erlang NIF binding for [LMDB](https://www.symas.com/lmdb)
(Lightning Memory-Mapped Database), written in C. It exposes exactly the
operations HyperBEAM's `hb_store_lmdb` backend needs — no more — so the surface
is easy to audit and the hot paths stay lean.

## Layout

```
src/elmdb.erl                       Erlang API (NIF stubs + the match wrapper)
native/elmdb_nif/c_src/elmdb_nif.c  the NIF
native/elmdb_nif/c_src/lmdb/        vendored LMDB, unmodified
native/elmdb_nif/build_c_nif.sh     compiles the NIF (run by the rebar pre-hook)
test/elmdb_test.erl                 EUnit suite (correctness + concurrency)
```

LMDB is vendored verbatim — there are no local edits to `mdb.c`/`midl.c`.

## Build & test

```
make            # compile the NIF + Erlang
make test       # run the EUnit suite
```

Requires Erlang/OTP 24+ and a C11 compiler. The NIF is compiled on the host by
`build_c_nif.sh` with `-O3 -flto` and native tuning (`-mcpu=native`/`-march=native`).

## API

```erlang
{ok, Env} = elmdb:env_open(Path, Opts).   %% singleton per Path
{ok, DB}  = elmdb:db_open(Env, [create]).

ok               = elmdb:put(DB, Key, Value).       %% buffered
{ok, Value}      = elmdb:get(DB, Key).              %% or not_found
{ok, [Child]}    = elmdb:list(DB, <<"prefix/">>).   %% distinct immediate children
{ok, [Id]}       = elmdb:match(DB, [{Suffix, Value}]).  %% entities matching all patterns
ok               = elmdb:env_close_by_name(Path).   %% soft close; reopens lazily
```

`env_open` options: `{map_size, Bytes}`, `{batch_size, N}`, `{max_readers, N}`,
`no_mem_init`, `no_sync`, `no_lock`, `no_readahead`, `write_map`.

### Semantics

- **Writes are buffered.** `put` appends to an in-memory batch that is committed
  in one transaction when it reaches `batch_size`, before any read, or when the
  environment closes. No explicit flush call is needed — a `get`/`list`/`match`
  always observes this process's earlier `put`s.
- **`list`** returns the distinct first path segments under a prefix (the
  immediate children), sorted. `<<"a/b">>` and `<<"a/c/d">>` under `<<"a/">>`
  list as `[<<"b">>, <<"c">>]`.
- **`match`** treats keys as `id/suffix`; an id is returned only if, for every
  `{Suffix, Value}` pattern, it has a key ending in `/Suffix` whose value equals
  `Value` exactly.

## Concurrency & performance

- **Reads run in parallel.** A read takes only a shared read-lock that guards the
  environment's lifetime (so a concurrent `env_close` can't free the map out from
  under it); the LMDB read transaction itself is lock-free. Reads scale across
  cores instead of serializing.
- **Reads are zero-copy through the scan.** `list`/`match` collect pointers into
  the memory map during one cursor pass and copy into a single result binary at
  the end — no per-row allocation, no second scan.
- `env_close` takes the lock exclusively, draining in-flight reads before closing.

## License

Apache-2.0 (see `LICENSE`). Vendored LMDB is under the OpenLDAP Public License
(see `native/elmdb_nif/c_src/lmdb/LICENSE`).

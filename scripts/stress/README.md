# elmdb-rs stress benchmark

Manual-only stress harness for elmdb-rs. Designed to be re-run unchanged
against historical versions — it uses only the public Erlang API
(`elmdb:env_open/2`, `db_open/2`, `put/3`, `get/2`, `list/2`, `flush/1`,
`overlay_count/1`, `env_close/1`).

Not wired into `make test`, `rebar3 eunit`, or any other automatic target.
Only triggers on explicit invocation:

```
./scripts/stress/stress.sh
```

## What it does

- Spawns `WRITERS` Erlang processes that hammer `elmdb:put` as fast as the
  BEAM will let them. Writes intentionally exceed disk throughput so the
  backpressure path is exercised continuously.
- Spawns `READERS` processes that mix four read patterns
  (`point_random`, `zipfian_point`, `range_random`, `zipfian_range`)
  against keys the writers have already produced.
- Samples BEAM memory, `/proc/self/status` + `/proc/self/smaps_rollup`,
  cgroup v2 `memory.current`/`memory.peak`/`memory.stat`, and
  `elmdb:overlay_count/1` every 100ms.
- Aborts loudly on any of three catastrophic gates:
  - `mem_cap_exceeded` — process RSS exceeds `MEM_CAP_MIB`,
  - `disk_full` — free space on the DB filesystem drops below 1 GiB,
  - `time_limit` — wall clock exceeds `DURATION_S_MAX` (default 30 min).
- Stops cleanly when `MAX_BYTES` of `key+value` bytes have been accepted
  (default 15 GiB), calls `elmdb:flush/1`, captures total wall-clock,
  and writes a JSON report to `RESULTS_DIR`.

## Environment variables

| Var | Default | Notes |
|---|---|---|
| `DB_DIR` | `/home/user/mnt/stress` | wiped before AND after each run |
| `RESULTS_DIR` | `/home/user/mnt/stress-results` | persists across runs |
| `MAX_BYTES` | `15 * 1024^3` (≈ 16.1 GB) | stop cleanly at this many bytes accepted |
| `MEM_CAP_MIB` | `2048` | **Anonymous** RSS ceiling; crossing it = catastrophic abort. File-backed RSS (LMDB mmap working set) is excluded because it's reclaimable for free under any real memory pressure. |
| `WRITERS` | `8` | concurrent writer processes |
| `READERS` | `4` | concurrent reader processes |
| `SIZE` | `avg` | preset; see below |
| `READ_MIX` | `point_random:40,zipfian_point:30,range_random:20,zipfian_range:10` | weights are integers, percents recommended |
| `RANGE_SIZE` | `64` | informational; range queries return whatever the underlying bucket holds |
| `MAP_SIZE` | `50 * 1024^3` | LMDB `map_size` |
| `BATCH_SIZE` | unset → elmdb default | forwarded as `{batch_size, _}` if set |
| `FLUSH_BYTES` | unset → elmdb default | forwarded as `{flush_bytes, _}` if set |
| `FLUSH_IDLE_SECONDS` | unset → elmdb default | forwarded as `{flush_idle_timeout_seconds, _}` if set |
| `NO_SYNC` | unset | set `1` to pass `no_sync` |
| `WRITE_MAP` | unset | set `1` to pass `write_map` |
| `RUN_LABEL` | `git rev-parse --short HEAD` | tag added to result filename |
| `DURATION_S_MAX` | `1800` | hard wall-clock backstop (30 min) |
| `SYSTEMD_HEADROOM_MIB` | `512` | when `systemd-run` is available, the kernel `MemoryMax` is set to `MEM_CAP_MIB + this`. Gives the in-Erlang watchdog a head start so it can write a structured JSON; the kernel backstop still fires if Erlang is wedged. If the kernel kill wins the race, a stub `*.killed.json` is left in `RESULTS_DIR`. |

### `SIZE` preset grammar

- `xsmall` — fixed K=5, V=5
- `avg` — fixed K=50, V=512
- `large` — fixed K=400, V=1048576 (1 MiB)
- `custom:K,V` — fixed K, V (e.g. `custom:128,4096`)
- `random:Kmin-Kmax,Vmin-Vmax` — each record's size is deterministic
  from `(writer_id, counter)` so readers can reproduce the exact keys

## Sample invocations

```bash
# Defaults: 15 GiB, avg sizing, 8 writers, 4 readers
./scripts/stress/stress.sh

# Stress small values (RSS-pressure scenario from aidocs/013)
SIZE=xsmall ./scripts/stress/stress.sh

# Big values, fewer writers
SIZE=large WRITERS=4 ./scripts/stress/stress.sh

# Random sized payloads (real-world-ish HB messages)
SIZE='random:20-200,32-65536' ./scripts/stress/stress.sh

# Quick smoke run — finishes in seconds
MAX_BYTES=$((512 * 1024 * 1024)) WRITERS=2 READERS=1 ./scripts/stress/stress.sh

# Compare versions: tag results so you can diff
RUN_LABEL=before  ./scripts/stress/stress.sh
git checkout other-branch && rebar3 compile
RUN_LABEL=after   ./scripts/stress/stress.sh
diff <(jq -S . $RESULTS_DIR/run_before_*.json | tail -1) \
     <(jq -S . $RESULTS_DIR/run_after_*.json  | tail -1)
```

## What the JSON report contains

Top-level keys:

- `run_label`, `outcome` (`ok` | `catastrophic`), `abort_detail`.
- `config` — every env-var-derived setting, for reproducibility.
- `size_spec`, `read_mix` — parsed forms.
- `timing.overlay_seconds`, `timing.total_seconds`, `timing.flush_us`.
- `throughput.overlay_bytes_per_s` — rate of writes accepted into the
  overlay; matches what the user called *overlay speed*.
- `throughput.total_bytes_per_s` — end-to-end from first put through the
  final `flush/1` return; matches *total suite speed*.
- `writes.hist_summary` — write-latency `{min, p50, p95, p99, p999, max,
  avg}` (µs), plus `slow_puts` (count of puts >1 ms, a coarse
  backpressure indicator).
- `reads.point_hist`, `reads.range_hist` — same shape for reads.
- `reads.point_hit`, `point_miss`, `range_ops`, `range_keys`, `errors`.
- `memory_summary` — `{p50, p95, p99, max, avg}` for each layer:
  `beam_total`, `beam_processes`, `beam_binary`, `beam_ets`,
  `proc_rss`, `proc_pss`, `proc_anon`, `proc_file` (≈ LMDB mmap),
  `proc_vm_rss`, `proc_vm_hwm`, `cgroup_current`, `cgroup_peak`,
  `cgroup_anon`, `cgroup_file`.
- `overlay_counts` — `{p50, p95, p99, max}` of overlay entry counts.

Independently, `/usr/bin/time -v` output is written to
`run_<label>_<ts>.time.txt` so you have a peak-RSS reading that survives
even a kernel OOM-kill.

## Why the layered memory view matters

The benchmark separates memory by who pays for it:

- BEAM heap: `erlang:memory(total)` / processes / binary / ets
- Erlang process RSS overall: `/proc/.../status` VmRSS
- Anonymous RSS (overlay + heap + stacks): `smaps_rollup` Anonymous
- File-backed RSS (LMDB mmap working set): `Rss - Anonymous`
- Cgroup totals (caps & peak the kernel enforces)

That decomposition is what lets you say "memory grew but it's all mmap,
which the kernel can reclaim under pressure" versus "anon RSS grew,
which means the overlay or BEAM is the culprit." See
`aidocs/013_rss_value_size_matrix.md` for why this distinction matters
for elmdb-rs specifically.

## Rebuilds on every run

`stress.sh` runs `rebar3 compile` every time before launching, even
when `_build/` already exists. `rebar3 compile` is incremental — it's a
no-op when nothing changed — but this guarantees you can switch
branches and run the bench without worrying about a stale `.so` from
the previous branch. Without this, comparing branches silently risks
comparing `main` to itself because the Rust NIF wasn't rebuilt.

## Cleanup

The script removes `$DB_DIR` both before and after the run, even on
catastrophic abort, so /home/user disk doesn't fill across runs. The
`$RESULTS_DIR` is never touched automatically; rotate it yourself.

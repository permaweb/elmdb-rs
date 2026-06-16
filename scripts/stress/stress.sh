#!/usr/bin/env bash
# elmdb-rs stress benchmark entry point.
# Manual-only. NEVER wire this into a default target.
#
# All knobs are env vars. See scripts/stress/README.md for the full list.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

DB_DIR="${DB_DIR:-/home/user/mnt/stress}"
RESULTS_DIR="${RESULTS_DIR:-/home/user/mnt/stress-results}"
MEM_CAP_MIB="${MEM_CAP_MIB:-2048}"
RUN_LABEL="${RUN_LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"

mkdir -p "$RESULTS_DIR"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
GITSTATE="$RESULTS_DIR/run_${RUN_LABEL}_${STAMP}.gitstate"
TIMEOUT_FILE="$RESULTS_DIR/run_${RUN_LABEL}_${STAMP}.time.txt"

{
  echo "=== git rev ==="
  git rev-parse HEAD 2>/dev/null || true
  echo
  echo "=== git status ==="
  git status --short 2>/dev/null || true
  echo
  echo "=== git diff ==="
  git diff 2>/dev/null || true
} > "$GITSTATE"

# Always rebuild — rebar3 compile is incremental (no-op when nothing
# changed) and we cannot trust that an existing _build/ matches the
# currently-checked-out source. Stale NIFs across branch switches were a
# real footgun: with main's .so loaded against backpressure's source,
# benchmarks silently compared a branch to itself.
echo "[stress] rebar3 compile (incremental)" >&2
rebar3 compile

STRESS_EBIN="${STRESS_EBIN:-/tmp/elmdb_stress_ebin}"
mkdir -p "$STRESS_EBIN"
erlc -o "$STRESS_EBIN" "$REPO_ROOT/scripts/stress/histogram.erl" \
                       "$REPO_ROOT/scripts/stress/proc_sampler.erl" \
                       "$REPO_ROOT/scripts/stress/elmdb_stress.erl"

if [ -e "$DB_DIR" ]; then
  rm -rf "$DB_DIR"
fi
mkdir -p "$DB_DIR"

# -noinput closes stdin so the BEAM never tries to read from the terminal.
# +B disables the break handler — without it, Ctrl-C opens the BREAK menu
# and a second Ctrl-C dumps state and crashes the linked worker tree,
# leaving a wall of garbage on the terminal. With +B, SIGINT just kills
# the OS process, which is what every other CLI does.
CMD=(erl -noinput +B
     -pa _build/default/lib/elmdb/ebin
     -pa "$STRESS_EBIN"
     -s elmdb_stress run
     -s init stop)

# Catch peak RSS independently of the in-Erlang sampler. /usr/bin/time -v
# reports it at exit even if the BEAM is killed.
TIME_BIN="/usr/bin/time"
if [ -x "$TIME_BIN" ]; then
  WRAPPER=("$TIME_BIN" -v -o "$TIMEOUT_FILE")
else
  WRAPPER=()
fi

# Kernel-level memory backstop via systemd-run --user, if available and the
# user manager is reachable. Otherwise rely on the in-Erlang mem watchdog.
USE_SYSTEMD=0
if command -v systemd-run >/dev/null 2>&1 \
   && systemctl --user is-active --quiet default.target 2>/dev/null; then
  USE_SYSTEMD=1
fi

# Give the in-Erlang watchdog a head start: kernel cap is the in-process cap
# plus a small headroom (default 512 MiB) so Erlang almost always wins the
# race and writes a structured JSON. Kernel backstop still fires if Erlang
# is wedged. Tune via SYSTEMD_HEADROOM_MIB.
SYSTEMD_HEADROOM_MIB="${SYSTEMD_HEADROOM_MIB:-512}"
KERNEL_CAP_MIB=$((MEM_CAP_MIB + SYSTEMD_HEADROOM_MIB))

# Run BEAM in the background so we can track its PID and forward
# SIGINT/SIGTERM cleanly. Without this, Ctrl-C reaches BEAM directly and
# we lose the chance to clean up DB_DIR.
BEAM_PID=
on_interrupt() {
  echo >&2
  echo "[stress] interrupted, killing benchmark and cleaning up..." >&2
  if [ -n "$BEAM_PID" ] && kill -0 "$BEAM_PID" 2>/dev/null; then
    kill -TERM "$BEAM_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      kill -0 "$BEAM_PID" 2>/dev/null || break
      sleep 0.2
    done
    kill -KILL "$BEAM_PID" 2>/dev/null || true
  fi
  if [ -e "$DB_DIR" ]; then
    rm -rf "$DB_DIR"
  fi
  exit 130
}
trap on_interrupt INT TERM

set +e
if [ "$USE_SYSTEMD" = "1" ]; then
  systemd-run --user --scope --quiet \
    -p MemoryMax="${KERNEL_CAP_MIB}M" \
    -p MemorySwapMax=0 \
    -- "${WRAPPER[@]}" "${CMD[@]}" &
else
  "${WRAPPER[@]}" "${CMD[@]}" &
fi
BEAM_PID=$!
wait "$BEAM_PID"
STATUS=$?
set -e
trap - INT TERM

# If the kernel killed us before Erlang could write a JSON, leave a stub so
# the run still produces a result file callers can grep over.
KILL_STUB="$RESULTS_DIR/run_${RUN_LABEL}_${STAMP}.killed.json"
if [ "$STATUS" -ge 128 ] || [ "$STATUS" -eq 137 ] || [ "$STATUS" -eq 9 ]; then
  cat > "$KILL_STUB" <<EOF
{"run_label":"${RUN_LABEL}","outcome":"catastrophic","abort_detail":"kernel_oom_kill","exit_status":${STATUS},"mem_cap_mib":${MEM_CAP_MIB},"systemd_cap_mib":${KERNEL_CAP_MIB},"time_v_output":"${TIMEOUT_FILE}"}
EOF
  echo "[stress] killed by signal; stub written: $KILL_STUB" >&2
fi

# Best-effort cleanup of the work directory regardless of outcome.
if [ -e "$DB_DIR" ]; then
  rm -rf "$DB_DIR"
fi

if [ -f "$TIMEOUT_FILE" ]; then
  echo "[stress] /usr/bin/time -v output: $TIMEOUT_FILE"
fi

exit $STATUS

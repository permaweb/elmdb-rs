#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
PRIV="$ROOT/priv"
C_SRC="$ROOT/native/elmdb_nif/c_src"
LMDB_SRC="$C_SRC/lmdb"

mkdir -p "$PRIV"

ERTS_INCLUDE=$(erl -noshell -eval \
    'io:format("~s/erts-~s/include", [code:root_dir(), erlang:system_info(version)]), halt().')

CC=${CC:-cc}

# Probe whether the compiler/linker accepts a flag; echo it back if so. Keeps
# the aggressive flags below portable across the arm64 Macs and x86 appliances
# this NIF is built on (the build runs on the deploy host via the rebar hook).
probe() {
    # -Werror so a flag the compiler merely tolerates (e.g. clang ignoring
    # -fno-semantic-interposition) is rejected here and only kept where it is
    # actually consumed.
    if printf 'int main(void){return 0;}\n' | \
        "$CC" $1 -Werror -x c - -o /dev/null 2>/dev/null; then
        printf '%s' "$1"
    fi
}

# -O3/-DNDEBUG: drop LMDB + libc assertions from the hot path.
# -flto: whole-program optimization so mdb_get/mdb_cursor_get inline into the
#        NIF read/write loops (the files are compiled together below).
# -fno-semantic-interposition: let intra-library calls to exported symbols be
#        inlined despite -fPIC, instead of going through the PLT.
# native tuning: this NIF is compiled on the machine it runs on.
BASE_CFLAGS=${CFLAGS:-"-O3 -std=c11 -fPIC -Wall -Wextra -Wno-unused-parameter"}
OPT_CFLAGS="-DNDEBUG"
OPT_CFLAGS="$OPT_CFLAGS $(probe -flto)"
OPT_CFLAGS="$OPT_CFLAGS $(probe -fno-semantic-interposition)"
NATIVE=$(probe -mcpu=native)
[ -z "$NATIVE" ] && NATIVE=$(probe -march=native)
OPT_CFLAGS="$OPT_CFLAGS $NATIVE"

LOCK_FLAGS=""

case "$(uname -s)" in
    Darwin)
        OUT="$PRIV/libelmdb_nif.dylib"
        SHARED_FLAGS="-dynamiclib -undefined dynamic_lookup"
        LOCK_FLAGS="-DMDB_USE_POSIX_MUTEX -DMDB_USE_ROBUST=0"
        ;;
    *)
        OUT="$PRIV/libelmdb_nif.so"
        SHARED_FLAGS="-shared"
        ;;
esac

"$CC" $BASE_CFLAGS $OPT_CFLAGS $LOCK_FLAGS \
    -I"$ERTS_INCLUDE" \
    -I"$LMDB_SRC" \
    -o "$OUT" \
    "$C_SRC/elmdb_nif.c" \
    "$LMDB_SRC/mdb.c" \
    "$LMDB_SRC/midl.c" \
    $SHARED_FLAGS \
    -pthread

(
    cd "$PRIV"
    case "$(basename "$OUT")" in
        *.dylib)
            ln -sf "$(basename "$OUT")" elmdb_nif.so
            ln -sf "$(basename "$OUT")" libelmdb_nif.so
            ;;
        *.so)
            ln -sf "$(basename "$OUT")" elmdb_nif.so
            ln -sf "$(basename "$OUT")" libelmdb_nif.dylib
            ;;
    esac
)

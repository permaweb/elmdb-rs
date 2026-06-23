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
BASE_CFLAGS=${CFLAGS:-"-O3 -std=c11 -fPIC -Wall -Wextra -Wno-unused-parameter"}
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

"$CC" $BASE_CFLAGS $LOCK_FLAGS \
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

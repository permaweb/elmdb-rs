#include "lmdb/chacha8.h"
#include "lmdb/lmdb.h"

#include <erl_nif.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define DEFAULT_MAP_SIZE ((size_t)1024 * 1024 * 1024)
#define DEFAULT_BATCH_SIZE 1000
#define DEFAULT_MODE 0664
#define MAX_KEY_SIZE 511
#define MAX_MATCH_RESULTS 100000

typedef struct {
    unsigned char *key;
    size_t key_len;
    unsigned char *value;
    size_t value_len;
    uint64_t seq;
} WriteOp;

typedef struct {
    char *path;
    MDB_env *env;
    size_t map_size;
    unsigned int max_readers;
    size_t batch_size;
    unsigned int flags;
    int encrypted;
    unsigned char encryption_key[CHACHA8_KEY_SIZE];
    unsigned long generation;
    unsigned int ref_count;
    int close_requested;
    ErlNifMutex *mutex;
    /* Guards the lifetime of `env`: any thread running an LMDB txn holds this as
     * a shared read-lock so readers run concurrently; only open/close take it
     * exclusively, which drains in-flight txns before swapping `env`. */
    ErlNifRWLock *env_lock;
} EnvResource;

typedef struct {
    EnvResource *env;
    MDB_dbi dbi;
    unsigned long dbi_generation;
    int dbi_open;
    int create_if_missing;
    int closed;
    WriteOp *ops;
    size_t op_count;
    size_t op_cap;
    size_t max_ops;
    uint64_t next_seq;
    /* Lock-free "buffer is non-empty" hint so reads can skip db->mutex entirely
     * when nothing is pending. Published under db->mutex, peeked without it. */
    atomic_int has_pending;
    ErlNifMutex *mutex;
} DbResource;

typedef struct EnvNode {
    EnvResource *res;
    struct EnvNode *next;
} EnvNode;

typedef struct DbNode {
    DbResource *res;
    struct DbNode *next;
} DbNode;

typedef struct {
    unsigned char *bytes;
    size_t len;
} Bytes;

typedef struct {
    size_t key_offset;
    size_t key_len;
    size_t value_offset;
    size_t value_len;
} RowRef;

typedef struct {
    const unsigned char *src;
    size_t len;
    size_t offset;
} ChildRef;

typedef struct {
    ErlNifBinary key;
    ErlNifBinary value;
} Pattern;

static int ensure_db_open_locked(DbResource *db, int *rc_out);
static int flush_locked(DbResource *db, int *rc_out);

static ErlNifResourceType *ENV_RESOURCE;
static ErlNifResourceType *DB_RESOURCE;
static ErlNifMutex *REGISTRY_MUTEX;
static EnvNode *ENVIRONMENTS;
static DbNode *DATABASES;

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_CREATE;
static ERL_NIF_TERM ATOM_MAP_SIZE;
static ERL_NIF_TERM ATOM_MAX_READERS;
static ERL_NIF_TERM ATOM_BATCH_SIZE;
static ERL_NIF_TERM ATOM_NO_MEM_INIT;
static ERL_NIF_TERM ATOM_NO_SYNC;
static ERL_NIF_TERM ATOM_NO_LOCK;
static ERL_NIF_TERM ATOM_NO_SUBDIR;
static ERL_NIF_TERM ATOM_WRITE_MAP;
static ERL_NIF_TERM ATOM_NO_READAHEAD;
static ERL_NIF_TERM ATOM_ENCRYPT;
static ERL_NIF_TERM ATOM_INVALID;
static ERL_NIF_TERM ATOM_INVALID_PATH;
static ERL_NIF_TERM ATOM_PERMISSION_DENIED;
static ERL_NIF_TERM ATOM_ENVIRONMENT_ERROR;
static ERL_NIF_TERM ATOM_DATABASE_ERROR;
static ERL_NIF_TERM ATOM_TRANSACTION_ERROR;
static ERL_NIF_TERM ATOM_VALIDATION_ERROR;
static ERL_NIF_TERM ATOM_KEY_EXIST;
static ERL_NIF_TERM ATOM_MAP_FULL;
static ERL_NIF_TERM ATOM_TXN_FULL;
static ERL_NIF_TERM ATOM_PAGE_NOT_FOUND;
static ERL_NIF_TERM ATOM_PANIC;
static ERL_NIF_TERM ATOM_DBS_FULL;
static ERL_NIF_TERM ATOM_READERS_FULL;
static ERL_NIF_TERM ATOM_TLS_FULL;
static ERL_NIF_TERM ATOM_CURSOR_FULL;
static ERL_NIF_TERM ATOM_PAGE_FULL;
static ERL_NIF_TERM ATOM_DIRECTORY_NOT_FOUND;
static ERL_NIF_TERM ATOM_NO_SPACE;
static ERL_NIF_TERM ATOM_IO_ERROR;
static ERL_NIF_TERM ATOM_CORRUPTED;
static ERL_NIF_TERM ATOM_VERSION_MISMATCH;
static ERL_NIF_TERM ATOM_CRYPTO_FAIL;
static ERL_NIF_TERM ATOM_ENV_ENCRYPTION;
static ERL_NIF_TERM ATOM_MAP_RESIZED;
static ERL_NIF_TERM ATOM_INCOMPATIBLE;
static ERL_NIF_TERM ATOM_BAD_RSLOT;
static ERL_NIF_TERM ATOM_BAD_TXN;
static ERL_NIF_TERM ATOM_BAD_VAL_SIZE;
static ERL_NIF_TERM ATOM_BAD_DBI;

static ERL_NIF_TERM make_atom(ErlNifEnv *env, const char *name) {
    return enif_make_atom(env, name);
}

static ERL_NIF_TERM make_string(ErlNifEnv *env, const char *message) {
    return enif_make_string(env, message, ERL_NIF_LATIN1);
}

static ERL_NIF_TERM error2(ErlNifEnv *env, ERL_NIF_TERM reason) {
    return enif_make_tuple2(env, ATOM_ERROR, reason);
}

static ERL_NIF_TERM error3(ErlNifEnv *env, ERL_NIF_TERM type, const char *message) {
    return enif_make_tuple3(env, ATOM_ERROR, type, make_string(env, message));
}

static ERL_NIF_TERM error3_rc(ErlNifEnv *env, ERL_NIF_TERM type, const char *context, int rc) {
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "%s: %s", context, mdb_strerror(rc));
    return error3(env, type, buffer);
}

static int term_to_path(ErlNifEnv *env, ERL_NIF_TERM term, char **out) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, term, &bin)) {
        char *path = enif_alloc(bin.size + 1);
        if (path == NULL) {
            return 0;
        }
        memcpy(path, bin.data, bin.size);
        path[bin.size] = '\0';
        *out = path;
        return 1;
    }

    unsigned int len = 0;
    if (enif_get_list_length(env, term, &len)) {
        char *path = enif_alloc((size_t)len + 1);
        if (path == NULL) {
            return 0;
        }
        int copied = enif_get_string(env, term, path, len + 1, ERL_NIF_LATIN1);
        if (copied <= 0) {
            enif_free(path);
            return 0;
        }
        *out = path;
        return 1;
    }

    return 0;
}

static int is_atom(ErlNifEnv *env, ERL_NIF_TERM term, ERL_NIF_TERM atom) {
    (void)env;
    return enif_is_identical(term, atom);
}

static int get_ulong(ErlNifEnv *env, ERL_NIF_TERM term, unsigned long *out) {
    unsigned long value = 0;
    if (enif_get_ulong(env, term, &value)) {
        *out = value;
        return 1;
    }
    return 0;
}

static void free_write_op(WriteOp *op) {
    if (op->key != NULL) {
        enif_free(op->key);
    }
    if (op->value != NULL) {
        enif_free(op->value);
    }
    memset(op, 0, sizeof(*op));
}

static int copy_binary(ErlNifBinary *bin, unsigned char **out) {
    unsigned char *copy = enif_alloc(bin->size);
    if (copy == NULL && bin->size != 0) {
        return 0;
    }
    if (bin->size != 0) {
        memcpy(copy, bin->data, bin->size);
    }
    *out = copy;
    return 1;
}

static ERL_NIF_TERM binary_from_bytes(ErlNifEnv *env, const void *data, size_t size) {
    ERL_NIF_TERM term;
    unsigned char *out = enif_make_new_binary(env, size, &term);
    if (out == NULL && size != 0) {
        return enif_make_badarg(env);
    }
    if (size != 0) {
        memcpy(out, data, size);
    }
    return term;
}

static int key_has_prefix(MDB_val *key, const unsigned char *prefix, size_t prefix_len) {
    return prefix_len == 0 ||
        (key->mv_size >= prefix_len &&
         memcmp(key->mv_data, prefix, prefix_len) == 0);
}

static int write_op_compare(const void *left, const void *right) {
    const WriteOp *a = (const WriteOp *)left;
    const WriteOp *b = (const WriteOp *)right;
    size_t min = a->key_len < b->key_len ? a->key_len : b->key_len;
    int cmp = min == 0 ? 0 : memcmp(a->key, b->key, min);
    if (cmp != 0) {
        return cmp;
    }
    if (a->key_len < b->key_len) {
        return -1;
    }
    if (a->key_len > b->key_len) {
        return 1;
    }
    return a->seq < b->seq ? -1 : (a->seq > b->seq ? 1 : 0);
}

static ERL_NIF_TERM lmdb_error_atom(int rc) {
    switch (rc) {
    case MDB_KEYEXIST: return ATOM_KEY_EXIST;
    case MDB_NOTFOUND: return ATOM_NOT_FOUND;
    case MDB_PAGE_NOTFOUND: return ATOM_PAGE_NOT_FOUND;
    case MDB_CORRUPTED: return ATOM_CORRUPTED;
    case MDB_PANIC: return ATOM_PANIC;
    case MDB_VERSION_MISMATCH: return ATOM_VERSION_MISMATCH;
    case MDB_CRYPTO_FAIL: return ATOM_CRYPTO_FAIL;
    case MDB_ENV_ENCRYPTION: return ATOM_ENV_ENCRYPTION;
    case MDB_INVALID: return ATOM_INVALID;
    case MDB_MAP_FULL: return ATOM_MAP_FULL;
    case MDB_DBS_FULL: return ATOM_DBS_FULL;
    case MDB_READERS_FULL: return ATOM_READERS_FULL;
    case MDB_TLS_FULL: return ATOM_TLS_FULL;
    case MDB_TXN_FULL: return ATOM_TXN_FULL;
    case MDB_CURSOR_FULL: return ATOM_CURSOR_FULL;
    case MDB_PAGE_FULL: return ATOM_PAGE_FULL;
    case MDB_MAP_RESIZED: return ATOM_MAP_RESIZED;
    case MDB_INCOMPATIBLE: return ATOM_INCOMPATIBLE;
    case MDB_BAD_RSLOT: return ATOM_BAD_RSLOT;
    case MDB_BAD_TXN: return ATOM_BAD_TXN;
    case MDB_BAD_VALSIZE: return ATOM_BAD_VAL_SIZE;
    case MDB_BAD_DBI: return ATOM_BAD_DBI;
    case ENOSPC: return ATOM_NO_SPACE;
    case EACCES: return ATOM_PERMISSION_DENIED;
    default: return ATOM_IO_ERROR;
    }
}

static int path_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0;
}

static int path_is_dir_or_raw(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 &&
        (S_ISDIR(st.st_mode) || S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode));
}

static int parent_path_is_dir(const char *path) {
    const char *slash = strrchr(path, '/');
    size_t len;
    char *parent;
    int ok;

    if (slash == NULL) {
        return 1;
    }
    len = (size_t)(slash - path);
    if (len == 0) {
        len = 1;
    }
    parent = enif_alloc(len + 1);
    if (parent == NULL) {
        return 0;
    }
    memcpy(parent, path, len);
    parent[len] = '\0';
    ok = path_is_dir_or_raw(parent);
    enif_free(parent);
    return ok;
}

static int path_is_valid_lmdb_target(const char *path, unsigned int flags) {
    if (path_is_dir_or_raw(path)) {
        return 1;
    }
    return (flags & MDB_NOSUBDIR) && parent_path_is_dir(path);
}

static int elmdb_chacha8_encrypt(
    const MDB_val *src,
    MDB_val *dst,
    const MDB_val *key,
    int encdec
) {
    (void)encdec;
    if (key[0].mv_size != CHACHA8_KEY_SIZE || key[1].mv_size < CHACHA8_IV_SIZE) {
        return EINVAL;
    }
    chacha8(
        src->mv_data,
        src->mv_size,
        key[0].mv_data,
        key[1].mv_data,
        dst->mv_data
    );
    return 0;
}

static int parse_env_options(ErlNifEnv *env, ERL_NIF_TERM list, EnvResource *target) {
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    const ERL_NIF_TERM *tuple;
    int arity;

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (enif_get_tuple(env, head, &arity, &tuple) && arity == 2) {
            unsigned long value = 0;
            if (is_atom(env, tuple[0], ATOM_MAP_SIZE) && get_ulong(env, tuple[1], &value)) {
                target->map_size = (size_t)value;
            } else if (is_atom(env, tuple[0], ATOM_MAX_READERS) &&
                       get_ulong(env, tuple[1], &value)) {
                target->max_readers = (unsigned int)value;
            } else if (is_atom(env, tuple[0], ATOM_BATCH_SIZE) &&
                       get_ulong(env, tuple[1], &value) && value > 0) {
                target->batch_size = (size_t)value;
            } else if (is_atom(env, tuple[0], ATOM_ENCRYPT)) {
                ErlNifBinary key;
                if (!enif_inspect_binary(env, tuple[1], &key) ||
                    key.size != CHACHA8_KEY_SIZE) {
                    return 0;
                }
                memcpy(target->encryption_key, key.data, CHACHA8_KEY_SIZE);
                target->encrypted = 1;
            }
        } else if (is_atom(env, head, ATOM_NO_MEM_INIT)) {
            target->flags |= MDB_NOMEMINIT;
        } else if (is_atom(env, head, ATOM_NO_SYNC)) {
            target->flags |= MDB_NOSYNC;
        } else if (is_atom(env, head, ATOM_NO_LOCK)) {
            target->flags |= MDB_NOLOCK;
        } else if (is_atom(env, head, ATOM_NO_SUBDIR)) {
            target->flags |= MDB_NOSUBDIR;
        } else if (is_atom(env, head, ATOM_WRITE_MAP)) {
            target->flags |= MDB_WRITEMAP;
        } else if (is_atom(env, head, ATOM_NO_READAHEAD)) {
            target->flags |= MDB_NORDAHEAD;
        }
    }

    return enif_is_empty_list(env, tail);
}

static int env_encryption_matches(EnvResource *left, EnvResource *right) {
    if (left->encrypted != right->encrypted) {
        return 0;
    }
    return !left->encrypted ||
        memcmp(left->encryption_key, right->encryption_key, CHACHA8_KEY_SIZE) == 0;
}

static void apply_env_options(EnvResource *target, EnvResource *source) {
    target->map_size = source->map_size;
    target->max_readers = source->max_readers;
    target->batch_size = source->batch_size;
    target->flags = source->flags;
    target->encrypted = source->encrypted;
    memcpy(target->encryption_key, source->encryption_key, CHACHA8_KEY_SIZE);
}

static int open_env_locked(EnvResource *res, int *rc_out) {
    if (res->env != NULL) {
        return 1;
    }

    MDB_env *mdb_env = NULL;
    int rc = mdb_env_create(&mdb_env);
    if (rc != MDB_SUCCESS) {
        *rc_out = rc;
        return 0;
    }

    rc = mdb_env_set_mapsize(mdb_env, res->map_size == 0 ? DEFAULT_MAP_SIZE : res->map_size);
    if (rc == MDB_SUCCESS && res->max_readers > 0) {
        rc = mdb_env_set_maxreaders(mdb_env, res->max_readers);
    }
    if (rc == MDB_SUCCESS && res->encrypted) {
        MDB_val key = { CHACHA8_KEY_SIZE, res->encryption_key };
        rc = mdb_env_set_encrypt(mdb_env, elmdb_chacha8_encrypt, &key, 0);
    }
    if (rc == MDB_SUCCESS) {
        rc = mdb_env_open(mdb_env, res->path, res->flags, DEFAULT_MODE);
    }
    if (rc != MDB_SUCCESS) {
        mdb_env_close(mdb_env);
        *rc_out = rc;
        return 0;
    }

    /* Publish the new env under the exclusive lock so a concurrent reader sees
     * either the old state or the fully-opened one, never a torn pointer. */
    enif_rwlock_rwlock(res->env_lock);
    res->env = mdb_env;
    res->close_requested = 0;
    res->generation++;
    enif_rwlock_rwunlock(res->env_lock);
    return 1;
}

static void close_env_locked(EnvResource *res) {
    res->close_requested = 1;
    /* The exclusive lock blocks until every in-flight read/write txn has
     * released its shared lock, so it is safe to close the env underneath them. */
    enif_rwlock_rwlock(res->env_lock);
    if (res->env != NULL) {
        mdb_env_close(res->env);
        res->env = NULL;
        res->generation++;
    }
    enif_rwlock_rwunlock(res->env_lock);
}

static void release_env_use(EnvResource *res) {
    enif_rwlock_runlock(res->env_lock);
}

/* Acquire shared access to a live env + open dbi for a read txn. On success the
 * caller holds the env's shared read-lock and must pair it with
 * release_env_use(). The common case (env open, no buffered writes) takes only
 * one shared rlock and a lockless op_count read — readers never serialize. */
static int acquire_read_handles(DbResource *db, MDB_env **mdb_env, MDB_dbi *dbi, int *rc_out) {
    EnvResource *env = db->env;

    /* Flush buffered writes only when some exist, so read-heavy traffic never
     * touches db->mutex. The flag is published under db->mutex by the same
     * process's earlier write, so its own subsequent read observes and flushes
     * it; a stale read just re-checks under the lock and no-ops. */
    if (atomic_load_explicit(&db->has_pending, memory_order_acquire)) {
        enif_mutex_lock(db->mutex);
        int ok = flush_locked(db, rc_out);
        enif_mutex_unlock(db->mutex);
        if (!ok) {
            return 0;
        }
    }

    enif_rwlock_rlock(env->env_lock);
    if (env->env != NULL && db->dbi_open && db->dbi_generation == env->generation) {
        *mdb_env = env->env;
        *dbi = db->dbi;
        return 1;
    }
    enif_rwlock_runlock(env->env_lock);

    /* Slow path: env or dbi needs (re)opening. */
    enif_mutex_lock(db->mutex);
    int ok = ensure_db_open_locked(db, rc_out);
    enif_mutex_unlock(db->mutex);
    if (!ok) {
        return 0;
    }

    enif_rwlock_rlock(env->env_lock);
    if (env->env == NULL || !db->dbi_open) {
        enif_rwlock_runlock(env->env_lock);
        *rc_out = MDB_BAD_TXN;
        return 0;
    }
    *mdb_env = env->env;
    *dbi = db->dbi;
    return 1;
}

static int ensure_db_open_locked(DbResource *db, int *rc_out) {
    int rc = MDB_SUCCESS;

    enif_mutex_lock(db->env->mutex);
    if (!open_env_locked(db->env, &rc)) {
        enif_mutex_unlock(db->env->mutex);
        *rc_out = rc;
        return 0;
    }

    if (db->dbi_open && db->dbi_generation == db->env->generation) {
        db->closed = 0;
        enif_mutex_unlock(db->env->mutex);
        return 1;
    }

    MDB_txn *txn = NULL;
    rc = mdb_txn_begin(db->env->env, NULL, 0, &txn);
    if (rc == MDB_SUCCESS) {
        unsigned int flags = db->create_if_missing ? MDB_CREATE : 0;
        rc = mdb_dbi_open(txn, NULL, flags, &db->dbi);
        if (rc == MDB_SUCCESS) {
            rc = mdb_txn_commit(txn);
            txn = NULL;
        }
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    if (rc != MDB_SUCCESS) {
        enif_mutex_unlock(db->env->mutex);
        *rc_out = rc;
        return 0;
    }

    /* Publish the dbi handle and its validity under the exclusive lock. `db->dbi`
     * was written above (still under env->mutex); the wlock release pairs with the
     * reader's rlock acquire so a reader that observes dbi_open == 1 is guaranteed
     * to also observe the matching dbi handle, without locking on the read path. */
    enif_rwlock_rwlock(db->env->env_lock);
    db->dbi_open = 1;
    db->dbi_generation = db->env->generation;
    db->closed = 0;
    enif_rwlock_rwunlock(db->env->env_lock);
    enif_mutex_unlock(db->env->mutex);
    return 1;
}

static int reserve_ops(DbResource *db, size_t need) {
    if (need <= db->op_cap) {
        return 1;
    }
    size_t next = db->op_cap == 0 ? 16 : db->op_cap;
    while (next < need) {
        if (next > SIZE_MAX / 2) {
            return 0;
        }
        next *= 2;
    }
    if (next > SIZE_MAX / sizeof(WriteOp)) {
        return 0;
    }
    WriteOp *ops = enif_realloc(db->ops, next * sizeof(WriteOp));
    if (ops == NULL) {
        return 0;
    }
    db->ops = ops;
    db->op_cap = next;
    return 1;
}

static int append_op_locked(DbResource *db, ErlNifBinary *key, ErlNifBinary *value) {
    if (key->size == 0 || key->size > MAX_KEY_SIZE) {
        return 0;
    }
    if (!reserve_ops(db, db->op_count + 1)) {
        return 0;
    }

    WriteOp *op = &db->ops[db->op_count];
    memset(op, 0, sizeof(*op));
    op->key_len = key->size;
    op->value_len = value->size;
    op->seq = db->next_seq++;
    if (!copy_binary(key, &op->key) || !copy_binary(value, &op->value)) {
        free_write_op(op);
        return 0;
    }
    db->op_count++;
    atomic_store_explicit(&db->has_pending, 1, memory_order_release);
    return 1;
}

static int write_immediate_locked(DbResource *db, WriteOp *ops, size_t op_count, int *rc_out) {
    if (op_count == 0) {
        return 1;
    }
    qsort(ops, op_count, sizeof(WriteOp), write_op_compare);

    int rc = MDB_SUCCESS;
    if (!ensure_db_open_locked(db, &rc)) {
        *rc_out = rc;
        return 0;
    }

    /* Hold the shared lock for the txn so the env cannot be closed underneath
     * it; LMDB serializes concurrent write txns on its own internal mutex. */
    enif_rwlock_rlock(db->env->env_lock);
    if (db->env->env == NULL) {
        enif_rwlock_runlock(db->env->env_lock);
        *rc_out = MDB_BAD_TXN;
        return 0;
    }

    MDB_txn *txn = NULL;
    MDB_cursor *cursor = NULL;
    rc = mdb_txn_begin(db->env->env, NULL, 0, &txn);
    if (rc == MDB_SUCCESS) {
        rc = mdb_cursor_open(txn, db->dbi, &cursor);
    }
    for (size_t i = 0; rc == MDB_SUCCESS && i < op_count; i++) {
        MDB_val key = { ops[i].key_len, ops[i].key };
        MDB_val value = { ops[i].value_len, ops[i].value };
        rc = mdb_cursor_put(cursor, &key, &value, 0);
    }
    if (cursor != NULL) {
        mdb_cursor_close(cursor);
    }
    if (rc == MDB_SUCCESS) {
        rc = mdb_txn_commit(txn);
        txn = NULL;
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    enif_rwlock_runlock(db->env->env_lock);

    if (rc != MDB_SUCCESS) {
        *rc_out = rc;
        return 0;
    }
    return 1;
}

static int flush_locked(DbResource *db, int *rc_out) {
    if (db->op_count == 0) {
        return 1;
    }

    WriteOp *ops = db->ops;
    size_t op_count = db->op_count;
    db->ops = NULL;
    db->op_count = 0;
    db->op_cap = 0;
    atomic_store_explicit(&db->has_pending, 0, memory_order_release);

    int ok = write_immediate_locked(db, ops, op_count, rc_out);
    for (size_t i = 0; i < op_count; i++) {
        free_write_op(&ops[i]);
    }
    enif_free(ops);
    return ok;
}

static EnvResource *find_env_by_path(const char *path) {
    for (EnvNode *node = ENVIRONMENTS; node != NULL; node = node->next) {
        if (strcmp(node->res->path, path) == 0) {
            return node->res;
        }
    }
    return NULL;
}

static DbResource *find_db_by_path(const char *path) {
    for (DbNode *node = DATABASES; node != NULL; node = node->next) {
        if (strcmp(node->res->env->path, path) == 0) {
            return node->res;
        }
    }
    return NULL;
}

static ERL_NIF_TERM env_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    char *path = NULL;
    if (argc != 2 || !term_to_path(env, argv[0], &path) || !enif_is_list(env, argv[1])) {
        return enif_make_badarg(env);
    }
    EnvResource requested;
    memset(&requested, 0, sizeof(requested));
    requested.map_size = DEFAULT_MAP_SIZE;
    requested.batch_size = DEFAULT_BATCH_SIZE;
    if (!parse_env_options(env, argv[1], &requested)) {
        enif_free(path);
        return enif_make_badarg(env);
    }

    enif_mutex_lock(REGISTRY_MUTEX);
    EnvResource *existing = find_env_by_path(path);
    if (existing != NULL) {
        enif_keep_resource(existing);
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_free(path);

        enif_mutex_lock(existing->mutex);
        EnvResource merged = *existing;
        if (!parse_env_options(env, argv[1], &merged)) {
            enif_mutex_unlock(existing->mutex);
            enif_release_resource(existing);
            return enif_make_badarg(env);
        }
        if (existing->env != NULL && !env_encryption_matches(existing, &merged)) {
            enif_mutex_unlock(existing->mutex);
            enif_release_resource(existing);
            return error2(env, ATOM_ENV_ENCRYPTION);
        }
        apply_env_options(existing, &merged);
        int rc = MDB_SUCCESS;
        int ok = open_env_locked(existing, &rc);
        enif_mutex_unlock(existing->mutex);
        if (!ok) {
            enif_release_resource(existing);
            return error2(env, ATOM_ENVIRONMENT_ERROR);
        }
        ERL_NIF_TERM term = enif_make_resource(env, existing);
        enif_release_resource(existing);
        return enif_make_tuple2(env, ATOM_OK, term);
    }

    if (!path_exists(path) &&
        !((requested.flags & MDB_NOSUBDIR) && parent_path_is_dir(path))) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_free(path);
        return error2(env, ATOM_DIRECTORY_NOT_FOUND);
    }
    if (!path_is_valid_lmdb_target(path, requested.flags)) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_free(path);
        return error2(env, ATOM_INVALID_PATH);
    }

    EnvResource *res = enif_alloc_resource(ENV_RESOURCE, sizeof(EnvResource));
    if (res == NULL) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_free(path);
        return error2(env, ATOM_ENVIRONMENT_ERROR);
    }
    memset(res, 0, sizeof(*res));
    res->path = path;
    apply_env_options(res, &requested);
    res->mutex = enif_mutex_create("elmdb_env");
    res->env_lock = enif_rwlock_create("elmdb_env_lock");
    if (res->mutex == NULL || res->env_lock == NULL) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_release_resource(res);
        return error2(env, ATOM_ENVIRONMENT_ERROR);
    }
    int rc = MDB_SUCCESS;
    enif_mutex_lock(res->mutex);
    int ok = open_env_locked(res, &rc);
    enif_mutex_unlock(res->mutex);
    if (!ok) {
        ERL_NIF_TERM reason = lmdb_error_atom(rc);
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_release_resource(res);
        return error2(env, reason);
    }

    EnvNode *node = enif_alloc(sizeof(EnvNode));
    if (node == NULL) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_release_resource(res);
        return error2(env, ATOM_ENVIRONMENT_ERROR);
    }
    enif_keep_resource(res);
    node->res = res;
    node->next = ENVIRONMENTS;
    ENVIRONMENTS = node;
    enif_mutex_unlock(REGISTRY_MUTEX);

    ERL_NIF_TERM term = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple2(env, ATOM_OK, term);
}

static ERL_NIF_TERM close_env_resource_nif(ErlNifEnv *env, EnvResource *res) {
    DbResource *db = NULL;
    enif_mutex_lock(REGISTRY_MUTEX);
    db = find_db_by_path(res->path);
    if (db != NULL) {
        enif_keep_resource(db);
    }
    enif_mutex_unlock(REGISTRY_MUTEX);

    if (db != NULL) {
        int rc = MDB_SUCCESS;
        enif_mutex_lock(db->mutex);
        flush_locked(db, &rc);
        db->closed = 1;
        db->dbi_open = 0;
        enif_mutex_unlock(db->mutex);
        enif_release_resource(db);
    }

    enif_mutex_lock(res->mutex);
    close_env_locked(res);
    enif_mutex_unlock(res->mutex);
    return ATOM_OK;
}

static ERL_NIF_TERM env_close_by_name_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    char *path = NULL;
    if (argc != 1 || !term_to_path(env, argv[0], &path)) {
        return enif_make_badarg(env);
    }
    enif_mutex_lock(REGISTRY_MUTEX);
    EnvResource *res = find_env_by_path(path);
    if (res != NULL) {
        enif_keep_resource(res);
    }
    enif_mutex_unlock(REGISTRY_MUTEX);
    enif_free(path);
    if (res == NULL) {
        return error2(env, ATOM_NOT_FOUND);
    }
    ERL_NIF_TERM result = close_env_resource_nif(env, res);
    enif_release_resource(res);
    return result;
}

static ERL_NIF_TERM db_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    EnvResource *env_res;
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], ENV_RESOURCE, (void **)&env_res) ||
        !enif_is_list(env, argv[1])) {
        return enif_make_badarg(env);
    }

    int create = 0;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = argv[1];
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (is_atom(env, head, ATOM_CREATE)) {
            create = 1;
        }
    }

    enif_mutex_lock(REGISTRY_MUTEX);
    DbResource *existing = find_db_by_path(env_res->path);
    if (existing != NULL) {
        enif_keep_resource(existing);
        enif_mutex_unlock(REGISTRY_MUTEX);
        enif_mutex_lock(existing->mutex);
        if (create) {
            existing->create_if_missing = 1;
        }
        existing->max_ops = env_res->batch_size == 0 ? DEFAULT_BATCH_SIZE : env_res->batch_size;
        int rc = MDB_SUCCESS;
        int ok = ensure_db_open_locked(existing, &rc);
        enif_mutex_unlock(existing->mutex);
        if (!ok) {
            enif_release_resource(existing);
            return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
        }
        ERL_NIF_TERM term = enif_make_resource(env, existing);
        enif_release_resource(existing);
        return enif_make_tuple2(env, ATOM_OK, term);
    }

    DbResource *db = enif_alloc_resource(DB_RESOURCE, sizeof(DbResource));
    if (db == NULL) {
        enif_mutex_unlock(REGISTRY_MUTEX);
        return error2(env, ATOM_DATABASE_ERROR);
    }
    memset(db, 0, sizeof(*db));
    db->env = env_res;
    enif_keep_resource(env_res);
    db->create_if_missing = create;
    db->max_ops = env_res->batch_size == 0 ? DEFAULT_BATCH_SIZE : env_res->batch_size;
    db->mutex = enif_mutex_create("elmdb_db");

    enif_mutex_lock(env_res->mutex);
    env_res->ref_count++;
    enif_mutex_unlock(env_res->mutex);

    int rc = MDB_SUCCESS;
    enif_mutex_lock(db->mutex);
    int ok = ensure_db_open_locked(db, &rc);
    enif_mutex_unlock(db->mutex);
    if (!ok) {
        enif_release_resource(db);
        enif_mutex_unlock(REGISTRY_MUTEX);
        return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
    }

    DbNode *node = enif_alloc(sizeof(DbNode));
    if (node == NULL) {
        enif_release_resource(db);
        enif_mutex_unlock(REGISTRY_MUTEX);
        return error2(env, ATOM_DATABASE_ERROR);
    }
    enif_keep_resource(db);
    node->res = db;
    node->next = DATABASES;
    DATABASES = node;
    enif_mutex_unlock(REGISTRY_MUTEX);

    ERL_NIF_TERM term = enif_make_resource(env, db);
    enif_release_resource(db);
    return enif_make_tuple2(env, ATOM_OK, term);
}

static ERL_NIF_TERM put_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    ErlNifBinary key;
    ErlNifBinary value;
    if (argc != 3 ||
        !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db) ||
        !enif_inspect_binary(env, argv[1], &key) ||
        !enif_inspect_binary(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }
    if (key.size == 0 || key.size > MAX_KEY_SIZE) {
        return error3(env, ATOM_VALIDATION_ERROR, "Invalid key size");
    }

    int rc = MDB_SUCCESS;
    enif_mutex_lock(db->mutex);
    int ok = append_op_locked(db, &key, &value);
    if (ok && db->op_count >= db->max_ops) {
        ok = flush_locked(db, &rc);
    }
    enif_mutex_unlock(db->mutex);
    if (!ok) {
        return rc == MDB_SUCCESS
            ? error3(env, ATOM_VALIDATION_ERROR, "Failed to buffer write")
            : error3_rc(env, ATOM_TRANSACTION_ERROR, "Failed to put value", rc);
    }
    return ATOM_OK;
}

static int decode_batch(ErlNifEnv *env, ERL_NIF_TERM list, DbResource *db, WriteOp **ops_out, size_t *count_out) {
    unsigned int len = 0;
    if (!enif_get_list_length(env, list, &len)) {
        return 0;
    }
    if (len == 0) {
        *ops_out = NULL;
        *count_out = 0;
        return 1;
    }
    size_t bytes = (size_t)len * sizeof(WriteOp);
    if (bytes / sizeof(WriteOp) != (size_t)len) {
        return 0;
    }

    WriteOp *ops = enif_alloc(bytes);
    if (ops == NULL) {
        return 0;
    }
    memset(ops, 0, bytes);

    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    size_t index = 0;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM *tuple;
        int arity;
        ErlNifBinary key;
        ErlNifBinary value;
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2 ||
            !enif_inspect_binary(env, tuple[0], &key) ||
            !enif_inspect_binary(env, tuple[1], &value) ||
            key.size == 0 || key.size > MAX_KEY_SIZE) {
            for (size_t i = 0; i < index; i++) {
                free_write_op(&ops[i]);
            }
            enif_free(ops);
            return 0;
        }
        ops[index].key_len = key.size;
        ops[index].value_len = value.size;
        ops[index].seq = db->next_seq++;
        if (!copy_binary(&key, &ops[index].key) ||
            !copy_binary(&value, &ops[index].value)) {
            for (size_t i = 0; i <= index; i++) {
                free_write_op(&ops[i]);
            }
            enif_free(ops);
            return 0;
        }
        index++;
    }

    if (!enif_is_empty_list(env, tail)) {
        for (size_t i = 0; i < index; i++) {
            free_write_op(&ops[i]);
        }
        enif_free(ops);
        return 0;
    }

    *ops_out = ops;
    *count_out = index;
    return 1;
}

static int decode_batch_refs(ErlNifEnv *env, ERL_NIF_TERM list, WriteOp **ops_out, size_t *count_out) {
    unsigned int len = 0;
    if (!enif_get_list_length(env, list, &len)) {
        return 0;
    }
    if (len == 0) {
        *ops_out = NULL;
        *count_out = 0;
        return 1;
    }
    size_t bytes = (size_t)len * sizeof(WriteOp);
    if (bytes / sizeof(WriteOp) != (size_t)len) {
        return 0;
    }

    WriteOp *ops = enif_alloc(bytes);
    if (ops == NULL) {
        return 0;
    }
    memset(ops, 0, bytes);

    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    size_t index = 0;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM *tuple;
        int arity;
        ErlNifBinary key;
        ErlNifBinary value;
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2 ||
            !enif_inspect_binary(env, tuple[0], &key) ||
            !enif_inspect_binary(env, tuple[1], &value) ||
            key.size == 0 || key.size > MAX_KEY_SIZE) {
            enif_free(ops);
            return 0;
        }
        ops[index].key = key.data;
        ops[index].key_len = key.size;
        ops[index].value = value.data;
        ops[index].value_len = value.size;
        ops[index].seq = index;
        index++;
    }

    if (!enif_is_empty_list(env, tail)) {
        enif_free(ops);
        return 0;
    }

    *ops_out = ops;
    *count_out = index;
    return 1;
}

static ERL_NIF_TERM put_batch_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    if (argc != 2 || !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db)) {
        return enif_make_badarg(env);
    }

    int rc = MDB_SUCCESS;
    enif_mutex_lock(db->mutex);
    WriteOp *ops = NULL;
    size_t op_count = 0;
    int ok = decode_batch(env, argv[1], db, &ops, &op_count);
    if (ok && op_count > 0) {
        if (reserve_ops(db, db->op_count + op_count)) {
            memcpy(&db->ops[db->op_count], ops, sizeof(WriteOp) * op_count);
            db->op_count += op_count;
            atomic_store_explicit(&db->has_pending, 1, memory_order_release);
            ops = NULL;
            op_count = 0;
            if (db->op_count >= db->max_ops) {
                ok = flush_locked(db, &rc);
            }
        } else {
            ok = 0;
        }
    }
    enif_mutex_unlock(db->mutex);

    if (ops != NULL) {
        for (size_t i = 0; i < op_count; i++) {
            free_write_op(&ops[i]);
        }
        enif_free(ops);
    }
    if (!ok) {
        return rc == MDB_SUCCESS
            ? error3(env, ATOM_VALIDATION_ERROR, "Invalid batch")
            : error3_rc(env, ATOM_TRANSACTION_ERROR, "Failed to write batch", rc);
    }
    return ATOM_OK;
}

static ERL_NIF_TERM put_batch_direct_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    if (argc != 2 || !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db)) {
        return enif_make_badarg(env);
    }

    int rc = MDB_SUCCESS;
    WriteOp *ops = NULL;
    size_t op_count = 0;
    int ok = decode_batch_refs(env, argv[1], &ops, &op_count);
    enif_mutex_lock(db->mutex);
    ok = ok && flush_locked(db, &rc);
    if (ok) {
        ok = write_immediate_locked(db, ops, op_count, &rc);
    }
    enif_mutex_unlock(db->mutex);

    if (ops != NULL) {
        enif_free(ops);
    }
    if (!ok) {
        return rc == MDB_SUCCESS
            ? error3(env, ATOM_VALIDATION_ERROR, "Invalid batch")
            : error3_rc(env, ATOM_TRANSACTION_ERROR, "Failed to write batch", rc);
    }
    return ATOM_OK;
}

static ERL_NIF_TERM flush_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    if (argc != 1 || !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db)) {
        return enif_make_badarg(env);
    }
    int rc = MDB_SUCCESS;
    enif_mutex_lock(db->mutex);
    int ok = flush_locked(db, &rc);
    enif_mutex_unlock(db->mutex);
    return ok
        ? ATOM_OK
        : error3_rc(env, ATOM_TRANSACTION_ERROR, "Failed to flush writes", rc);
}

static ERL_NIF_TERM get_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    ErlNifBinary key_bin;
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db) ||
        !enif_inspect_binary(env, argv[1], &key_bin)) {
        return enif_make_badarg(env);
    }

    int rc = MDB_SUCCESS;
    ERL_NIF_TERM result;
    MDB_env *mdb_env;
    MDB_dbi dbi;
    if (!acquire_read_handles(db, &mdb_env, &dbi, &rc)) {
        return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
    }

    MDB_txn *txn = NULL;
    rc = mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn);
    if (rc == MDB_SUCCESS) {
        MDB_val key = { key_bin.size, key_bin.data };
        MDB_val value;
        rc = mdb_get(txn, dbi, &key, &value);
        if (rc == MDB_SUCCESS) {
            result = enif_make_tuple2(
                env,
                ATOM_OK,
                binary_from_bytes(env, value.mv_data, value.mv_size)
            );
        } else if (rc == MDB_NOTFOUND) {
            result = ATOM_NOT_FOUND;
        } else {
            result = error3_rc(env, ATOM_DATABASE_ERROR, "Failed to get value", rc);
        }
    } else {
        result = error3_rc(env, ATOM_TRANSACTION_ERROR, "Failed to begin read transaction", rc);
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    release_env_use(db->env);
    return result;
}

static ERL_NIF_TERM list_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    ErlNifBinary prefix;
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db) ||
        !enif_inspect_binary(env, argv[1], &prefix)) {
        return enif_make_badarg(env);
    }

    int rc = MDB_SUCCESS;
    ChildRef *children = NULL;
    size_t child_count = 0;
    size_t child_cap = 0;
    size_t total = 0;
    ERL_NIF_TERM result = ATOM_NOT_FOUND;

    MDB_env *mdb_env;
    MDB_dbi dbi;
    if (!acquire_read_handles(db, &mdb_env, &dbi, &rc)) {
        return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
    }

    MDB_txn *txn = NULL;
    MDB_cursor *cursor = NULL;
    rc = mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn);
    if (rc == MDB_SUCCESS) {
        rc = mdb_cursor_open(txn, dbi, &cursor);
    }

    MDB_val key = { prefix.size, prefix.data };
    MDB_val value;
    if (rc == MDB_SUCCESS) {
        rc = prefix.size == 0
            ? mdb_cursor_get(cursor, &key, &value, MDB_FIRST)
            : mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE);
    }

    /* Collect distinct immediate-child names. Keys are sorted so duplicates are
     * adjacent (compare against the previous one). The names point into the
     * mmap and are packed into one binary below, before the txn ends. */
    while (rc == MDB_SUCCESS && key_has_prefix(&key, prefix.data, prefix.size)) {
        const unsigned char *remaining = (const unsigned char *)key.mv_data + prefix.size;
        size_t remaining_len = key.mv_size - prefix.size;
        if (remaining_len != 0) {
            size_t len = 0;
            while (len < remaining_len && remaining[len] != '/') {
                len++;
            }
            if (len != 0) {
                int is_new =
                    child_count == 0 ||
                    children[child_count - 1].len != len ||
                    memcmp(children[child_count - 1].src, remaining, len) != 0;
                if (is_new) {
                    if (child_count == child_cap) {
                        size_t next = child_cap == 0 ? 16 : child_cap * 2;
                        if (next < child_cap || next > SIZE_MAX / sizeof(ChildRef)) {
                            rc = ENOMEM;
                            break;
                        }
                        ChildRef *grown = enif_realloc(children, sizeof(ChildRef) * next);
                        if (grown == NULL) {
                            rc = ENOMEM;
                            break;
                        }
                        children = grown;
                        child_cap = next;
                    }
                    if (SIZE_MAX - total < len) {
                        rc = ENOMEM;
                        break;
                    }
                    children[child_count].src = remaining;
                    children[child_count].len = len;
                    children[child_count].offset = total;
                    total += len;
                    child_count++;
                }
            }
        }
        rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
    }

    if (rc == MDB_NOTFOUND) {
        rc = MDB_SUCCESS;
    }
    if (rc == MDB_SUCCESS && child_count > 0) {
        ErlNifBinary packed;
        if (!enif_alloc_binary(total, &packed)) {
            rc = ENOMEM;
        } else {
            for (size_t i = 0; i < child_count; i++) {
                memcpy(packed.data + children[i].offset, children[i].src, children[i].len);
            }
            ERL_NIF_TERM parent = enif_make_binary(env, &packed);
            ERL_NIF_TERM list = enif_make_list(env, 0);
            for (size_t i = child_count; i > 0; i--) {
                ERL_NIF_TERM child = enif_make_sub_binary(
                    env, parent, children[i - 1].offset, children[i - 1].len);
                list = enif_make_list_cell(env, child, list);
            }
            result = enif_make_tuple2(env, ATOM_OK, list);
        }
    }
    if (rc != MDB_SUCCESS) {
        result = error3_rc(env, ATOM_DATABASE_ERROR, "Failed to list prefix", rc);
    }

    if (cursor != NULL) {
        mdb_cursor_close(cursor);
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    release_env_use(db->env);

    if (children != NULL) {
        enif_free(children);
    }
    return result;
}

static ERL_NIF_TERM read_prefix_rows_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    ErlNifBinary prefix;
    if (argc != 2 ||
        !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db) ||
        !enif_inspect_binary(env, argv[1], &prefix)) {
        return enif_make_badarg(env);
    }

    int rc = MDB_SUCCESS;
    ERL_NIF_TERM result = ATOM_NOT_FOUND;
    MDB_env *mdb_env;
    MDB_dbi dbi;
    if (!acquire_read_handles(db, &mdb_env, &dbi, &rc)) {
        return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
    }

    MDB_txn *txn = NULL;
    MDB_cursor *cursor = NULL;
    rc = mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn);
    if (rc == MDB_SUCCESS) {
        rc = mdb_cursor_open(txn, dbi, &cursor);
    }

    MDB_val key = { prefix.size, prefix.data };
    MDB_val value = { 0, NULL };
    if (rc == MDB_SUCCESS) {
        rc = prefix.size == 0
            ? mdb_cursor_get(cursor, &key, &value, MDB_FIRST)
            : mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE);
    }

    size_t count = 0;
    size_t total = 0;
    while (rc == MDB_SUCCESS && key_has_prefix(&key, prefix.data, prefix.size)) {
        if (SIZE_MAX - total < key.mv_size ||
            SIZE_MAX - total - key.mv_size < value.mv_size) {
            rc = ENOMEM;
            break;
        }
        total += key.mv_size + value.mv_size;
        count++;
        rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
    }
    if (rc == MDB_NOTFOUND) {
        rc = MDB_SUCCESS;
    }

    if (rc == MDB_SUCCESS && count > 0) {
        ErlNifBinary packed;
        if (count > SIZE_MAX / sizeof(RowRef)) {
            rc = ENOMEM;
        } else {
            RowRef *rows = enif_alloc(sizeof(RowRef) * count);
            if (rows == NULL || !enif_alloc_binary(total, &packed)) {
                if (rows != NULL) {
                    enif_free(rows);
                }
                rc = ENOMEM;
            } else {
                key.mv_size = prefix.size;
                key.mv_data = prefix.data;
                value.mv_size = 0;
                value.mv_data = NULL;
                rc = prefix.size == 0
                    ? mdb_cursor_get(cursor, &key, &value, MDB_FIRST)
                    : mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE);
                size_t offset = 0;
                size_t row = 0;
                while (rc == MDB_SUCCESS && row < count &&
                       key_has_prefix(&key, prefix.data, prefix.size)) {
                    rows[row].key_offset = offset;
                    rows[row].key_len = key.mv_size;
                    memcpy(packed.data + offset, key.mv_data, key.mv_size);
                    offset += key.mv_size;
                    rows[row].value_offset = offset;
                    rows[row].value_len = value.mv_size;
                    memcpy(packed.data + offset, value.mv_data, value.mv_size);
                    offset += value.mv_size;
                    row++;
                    rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
                }
                if (rc == MDB_NOTFOUND) {
                    rc = MDB_SUCCESS;
                }
                if (rc == MDB_SUCCESS) {
                    ERL_NIF_TERM parent = enif_make_binary(env, &packed);
                    ERL_NIF_TERM list = enif_make_list(env, 0);
                    for (size_t i = count; i > 0; i--) {
                        ERL_NIF_TERM k = enif_make_sub_binary(
                            env, parent, rows[i - 1].key_offset, rows[i - 1].key_len);
                        ERL_NIF_TERM v = enif_make_sub_binary(
                            env, parent, rows[i - 1].value_offset, rows[i - 1].value_len);
                        list = enif_make_list_cell(env, enif_make_tuple2(env, k, v), list);
                    }
                    result = enif_make_tuple2(env, ATOM_OK, list);
                } else {
                    enif_release_binary(&packed);
                }
                enif_free(rows);
            }
        }
    }
    if (rc != MDB_SUCCESS) {
        result = error3_rc(env, ATOM_DATABASE_ERROR, "Failed to read prefix", rc);
    }

    if (cursor != NULL) {
        mdb_cursor_close(cursor);
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    release_env_use(db->env);
    return result;
}

static int decode_patterns(ErlNifEnv *env, ERL_NIF_TERM list, Pattern **patterns_out, size_t *count_out) {
    unsigned int len = 0;
    if (!enif_get_list_length(env, list, &len)) {
        return 0;
    }
    if (len == 0) {
        *patterns_out = NULL;
        *count_out = 0;
        return 1;
    }
    Pattern *patterns = enif_alloc(sizeof(Pattern) * len);
    if (patterns == NULL) {
        return 0;
    }
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail = list;
    size_t index = 0;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM *tuple;
        int arity;
        if (!enif_get_tuple(env, head, &arity, &tuple) || arity != 2 ||
            !enif_inspect_binary(env, tuple[0], &patterns[index].key) ||
            !enif_inspect_binary(env, tuple[1], &patterns[index].value)) {
            enif_free(patterns);
            return 0;
        }
        index++;
    }
    *patterns_out = patterns;
    *count_out = index;
    return enif_is_empty_list(env, tail);
}

static int all_seen(unsigned char *seen, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (!seen[i]) {
            return 0;
        }
    }
    return 1;
}

/* Record a match as a pointer into the (txn-stable) mmap key plus its length.
 * The id bytes are copied into result binaries later, while the txn is still
 * open, so no per-entity allocation or copy happens during the scan. */
static int append_result(Bytes **results, size_t *count, size_t *cap, const unsigned char *id, size_t id_len) {
    if (*count >= MAX_MATCH_RESULTS) {
        return 1;
    }
    if (*count == *cap) {
        size_t next = *cap == 0 ? 16 : *cap * 2;
        if (next < *cap || next > SIZE_MAX / sizeof(Bytes)) {
            return 0;
        }
        Bytes *grown = enif_realloc(*results, sizeof(Bytes) * next);
        if (grown == NULL) {
            return 0;
        }
        *results = grown;
        *cap = next;
    }
    (*results)[*count].bytes = (unsigned char *)id;
    (*results)[*count].len = id_len;
    (*count)++;
    return 1;
}

static ERL_NIF_TERM match_pattern_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    DbResource *db;
    if (argc != 2 || !enif_get_resource(env, argv[0], DB_RESOURCE, (void **)&db)) {
        return enif_make_badarg(env);
    }

    Pattern *patterns = NULL;
    size_t pattern_count = 0;
    if (!decode_patterns(env, argv[1], &patterns, &pattern_count)) {
        return enif_make_badarg(env);
    }
    if (pattern_count == 0) {
        return ATOM_NOT_FOUND;
    }
    unsigned char *seen = enif_alloc(pattern_count);
    if (seen == NULL) {
        enif_free(patterns);
        return error3(env, ATOM_DATABASE_ERROR, "Failed to allocate match state");
    }
    memset(seen, 0, pattern_count);

    Bytes *results = NULL;
    size_t result_count = 0;
    size_t result_cap = 0;
    const unsigned char *current_id = NULL;
    size_t current_id_len = 0;
    int rc = MDB_SUCCESS;
    ERL_NIF_TERM result = ATOM_NOT_FOUND;

    MDB_env *mdb_env;
    MDB_dbi dbi;
    if (!acquire_read_handles(db, &mdb_env, &dbi, &rc)) {
        enif_free(patterns);
        enif_free(seen);
        return error3_rc(env, ATOM_DATABASE_ERROR, "Failed to open database", rc);
    }

    MDB_txn *txn = NULL;
    MDB_cursor *cursor = NULL;
    rc = mdb_txn_begin(mdb_env, NULL, MDB_RDONLY, &txn);
    if (rc == MDB_SUCCESS) {
        rc = mdb_cursor_open(txn, dbi, &cursor);
    }
    MDB_val key;
    MDB_val value;
    if (rc == MDB_SUCCESS) {
        rc = mdb_cursor_get(cursor, &key, &value, MDB_FIRST);
    }

    while (rc == MDB_SUCCESS) {
        unsigned char *key_bytes = key.mv_data;
        size_t key_len = key.mv_size;
        size_t slash = key_len;
        while (slash > 0 && key_bytes[slash - 1] != '/') {
            slash--;
        }
        size_t id_len = slash == 0 ? key_len : slash - 1;
        unsigned char *suffix = slash == 0 ? (unsigned char *)"" : key_bytes + slash;
        size_t suffix_len = slash == 0 ? 0 : key_len - slash;

        if (current_id == NULL ||
            current_id_len != id_len ||
            memcmp(current_id, key_bytes, id_len) != 0) {
            if (current_id != NULL && all_seen(seen, pattern_count)) {
                if (!append_result(&results, &result_count, &result_cap, current_id, current_id_len)) {
                    rc = ENOMEM;
                    break;
                }
                if (result_count >= MAX_MATCH_RESULTS) {
                    break;
                }
            }
            /* Point at the current key's id bytes; they stay valid for the txn,
             * including the prior id we may have just recorded above. */
            current_id = key_bytes;
            current_id_len = id_len;
            memset(seen, 0, pattern_count);
        }

        for (size_t i = 0; i < pattern_count; i++) {
            if (patterns[i].key.size == suffix_len &&
                patterns[i].value.size == value.mv_size &&
                memcmp(patterns[i].key.data, suffix, suffix_len) == 0 &&
                memcmp(patterns[i].value.data, value.mv_data, value.mv_size) == 0) {
                seen[i] = 1;
            }
        }
        rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
    }

    if (rc == MDB_NOTFOUND) {
        rc = MDB_SUCCESS;
    }
    if (rc == MDB_SUCCESS && current_id != NULL && all_seen(seen, pattern_count)) {
        append_result(&results, &result_count, &result_cap, current_id, current_id_len);
    }
    if (rc == MDB_SUCCESS && result_count > 0) {
        ERL_NIF_TERM list = enif_make_list(env, 0);
        for (size_t i = result_count; i > 0; i--) {
            ERL_NIF_TERM id = binary_from_bytes(env, results[i - 1].bytes, results[i - 1].len);
            list = enif_make_list_cell(env, id, list);
        }
        result = enif_make_tuple2(env, ATOM_OK, list);
    } else if (rc != MDB_SUCCESS) {
        result = error3_rc(env, ATOM_DATABASE_ERROR, "Failed to match patterns", rc);
    }

    if (cursor != NULL) {
        mdb_cursor_close(cursor);
    }
    if (txn != NULL) {
        mdb_txn_abort(txn);
    }
    release_env_use(db->env);

    if (results != NULL) {
        enif_free(results);
    }
    enif_free(patterns);
    enif_free(seen);
    return result;
}

static void env_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    EnvResource *res = obj;
    if (res->env != NULL) {
        mdb_env_close(res->env);
        res->env = NULL;
    }
    if (res->path != NULL) {
        enif_free(res->path);
    }
    if (res->mutex != NULL) {
        enif_mutex_destroy(res->mutex);
    }
    if (res->env_lock != NULL) {
        enif_rwlock_destroy(res->env_lock);
    }
}

static void db_dtor(ErlNifEnv *env, void *obj) {
    (void)env;
    DbResource *db = obj;
    int rc = MDB_SUCCESS;
    if (db->mutex != NULL) {
        enif_mutex_lock(db->mutex);
        flush_locked(db, &rc);
        for (size_t i = 0; i < db->op_count; i++) {
            free_write_op(&db->ops[i]);
        }
        if (db->ops != NULL) {
            enif_free(db->ops);
        }
        enif_mutex_unlock(db->mutex);
        enif_mutex_destroy(db->mutex);
    }
    if (db->env != NULL) {
        enif_release_resource(db->env);
    }
}

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info) {
    (void)priv;
    (void)info;
    ErlNifResourceFlags tried;
    ENV_RESOURCE = enif_open_resource_type(
        env, NULL, "elmdb_env_resource", env_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, &tried);
    DB_RESOURCE = enif_open_resource_type(
        env, NULL, "elmdb_db_resource", db_dtor,
        ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, &tried);
    if (ENV_RESOURCE == NULL || DB_RESOURCE == NULL) {
        return 1;
    }
    REGISTRY_MUTEX = enif_mutex_create("elmdb_registry");
    if (REGISTRY_MUTEX == NULL) {
        return 1;
    }

    ATOM_OK = make_atom(env, "ok");
    ATOM_ERROR = make_atom(env, "error");
    ATOM_NOT_FOUND = make_atom(env, "not_found");
    ATOM_CREATE = make_atom(env, "create");
    ATOM_MAP_SIZE = make_atom(env, "map_size");
    ATOM_MAX_READERS = make_atom(env, "max_readers");
    ATOM_BATCH_SIZE = make_atom(env, "batch_size");
    ATOM_NO_MEM_INIT = make_atom(env, "no_mem_init");
    ATOM_NO_SYNC = make_atom(env, "no_sync");
    ATOM_NO_LOCK = make_atom(env, "no_lock");
    ATOM_NO_SUBDIR = make_atom(env, "no_subdir");
    ATOM_WRITE_MAP = make_atom(env, "write_map");
    ATOM_NO_READAHEAD = make_atom(env, "no_readahead");
    ATOM_ENCRYPT = make_atom(env, "encrypt");
    ATOM_INVALID = make_atom(env, "invalid");
    ATOM_INVALID_PATH = make_atom(env, "invalid_path");
    ATOM_PERMISSION_DENIED = make_atom(env, "permission_denied");
    ATOM_ENVIRONMENT_ERROR = make_atom(env, "environment_error");
    ATOM_DATABASE_ERROR = make_atom(env, "database_error");
    ATOM_TRANSACTION_ERROR = make_atom(env, "transaction_error");
    ATOM_VALIDATION_ERROR = make_atom(env, "validation_error");
    ATOM_KEY_EXIST = make_atom(env, "key_exist");
    ATOM_MAP_FULL = make_atom(env, "map_full");
    ATOM_TXN_FULL = make_atom(env, "txn_full");
    ATOM_PAGE_NOT_FOUND = make_atom(env, "page_not_found");
    ATOM_PANIC = make_atom(env, "panic");
    ATOM_DBS_FULL = make_atom(env, "dbs_full");
    ATOM_READERS_FULL = make_atom(env, "readers_full");
    ATOM_TLS_FULL = make_atom(env, "tls_full");
    ATOM_CURSOR_FULL = make_atom(env, "cursor_full");
    ATOM_PAGE_FULL = make_atom(env, "page_full");
    ATOM_DIRECTORY_NOT_FOUND = make_atom(env, "directory_not_found");
    ATOM_NO_SPACE = make_atom(env, "no_space");
    ATOM_IO_ERROR = make_atom(env, "io_error");
    ATOM_CORRUPTED = make_atom(env, "corrupted");
    ATOM_VERSION_MISMATCH = make_atom(env, "version_mismatch");
    ATOM_CRYPTO_FAIL = make_atom(env, "crypto_fail");
    ATOM_ENV_ENCRYPTION = make_atom(env, "env_encryption");
    ATOM_MAP_RESIZED = make_atom(env, "map_resized");
    ATOM_INCOMPATIBLE = make_atom(env, "incompatible");
    ATOM_BAD_RSLOT = make_atom(env, "bad_rslot");
    ATOM_BAD_TXN = make_atom(env, "bad_txn");
    ATOM_BAD_VAL_SIZE = make_atom(env, "bad_val_size");
    ATOM_BAD_DBI = make_atom(env, "bad_dbi");
    return 0;
}

static void unload(ErlNifEnv *env, void *priv) {
    (void)env;
    (void)priv;
    if (REGISTRY_MUTEX != NULL) {
        enif_mutex_destroy(REGISTRY_MUTEX);
        REGISTRY_MUTEX = NULL;
    }
}

static ErlNifFunc nif_funcs[] = {
    {"env_open", 2, env_open_nif, 0},
    {"env_close_by_name", 1, env_close_by_name_nif, 0},
    {"db_open", 2, db_open_nif, 0},
    {"put", 3, put_nif, 0},
    {"put_batch", 2, put_batch_nif, 0},
    {"put_batch_direct", 2, put_batch_direct_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"get", 2, get_nif, 0},
    {"flush", 1, flush_nif, 0},
    {"list", 2, list_nif, 0},
    {"read_prefix_rows", 2, read_prefix_rows_nif, 0},
    {"match_pattern", 2, match_pattern_nif, 0}
};

ERL_NIF_INIT(elmdb, nif_funcs, load, NULL, NULL, unload)

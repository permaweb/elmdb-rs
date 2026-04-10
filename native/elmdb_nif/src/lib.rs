//! elmdb_nif - High-performance LMDB bindings for Erlang via Rust NIF
//!
//! This module implements a Native Implemented Function (NIF) that provides
//! Erlang/Elixir applications with access to LMDB (Lightning Memory-Mapped Database).
//!
//! # Architecture
//!
//! The implementation uses a two-layer architecture:
//! - **Resource Management**: Environments and databases are managed as Erlang resources
//! - **Write Optimization**: Dual-map overlay with background flush worker
//!
//! # Safety
//!
//! - All LMDB operations are wrapped in safe Rust abstractions
//! - Resources are automatically cleaned up when no longer referenced
//! - Thread-safe through Arc/ArcSwap/Mutex wrappers
//! - Prevents use-after-close errors through validation checks
//!
//! # Performance
//!
//! Key optimizations include:
//! - Lock-free put via scc::HashMap behind ArcSwap
//! - Background flush worker decouples LMDB I/O from Erlang schedulers
//! - Zero-copy reads through memory mapping
//! - Efficient cursor iteration for list operations
//! - Early termination for prefix searches

use arc_swap::{ArcSwap, ArcSwapOption};
use cachelog_rs::{CacheLogConfig, CacheLogMap, FlushBatch};
use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags,
};
use quanta::Clock;
use rustler::types::binary::Binary;
use rustler::types::binary::OwnedBinary;
use rustler::{Encoder, Env, Error, NifResult, ResourceArc, Term};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant as StdInstant};

// LMDB cursor operation constants (instead of importing lmdb-sys only for constants from lmdb_sys::ffi).
// To be improved in the future.
const MDB_FIRST: u32 = 0;
const MDB_NEXT: u32 = 8;
const MDB_SET_RANGE: u32 = 17;
// Default LMDB max key size. This is controlled by LMDB's compile-time MDB_MAXKEYSIZE.
// If the Rust lmdb crate exposes mdb_env_get_maxkeysize safely in the future, prefer that.
const LMDB_DEFAULT_MAX_KEY_SIZE: usize = 511;
const AUTO_FLUSH_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const METRICS_CHECK_MASK: u64 = 1023;
const GET_LATENCY_BUCKETS: usize = 16;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        not_found,
        nif_not_loaded,
        // LMDB-specific atoms
        map_size,
        max_readers,
        no_mem_init,
        no_sync,
        no_lock,
        read_only,
        write_map,
        no_readahead,
        batch_size,
        lru_size,
        create,
        iterator,
        start,
        undefined,
        // Error atoms
        invalid_path,
        permission_denied,
        already_open,
        environment_error,
        database_error,
        transaction_error,
        key_exist,
        map_full,
        txn_full,
        page_not_found,
        panic,
        invalid,
        dbs_full,
        readers_full,
        tls_full,
        cursor_full,
        page_full,
        // Specific environment errors
        directory_not_found,
        no_space,
        io_error,
        corrupted,
        version_mismatch,
        map_resized,
        incompatible,
        bad_rslot,
        bad_txn,
        bad_val_size,
        bad_dbi,
        validation_error,
    }
}

/// Iterator cursor token passed between Erlang and Rust.
///
/// The token is intentionally stateless on the Rust side:
/// - `{iterator, start}` means "start before the first key"
/// - `{iterator, LastKey}` means "start after LastKey"
enum IteratorCursor {
    Start,
    AfterKey(Vec<u8>),
}

struct DbState {
    cached_db: Option<(Database, u64)>,
    create_if_missing: bool,
}

type CacheOverlay = CacheLogMap<Vec<u8>, Vec<u8>, ahash::RandomState>;
type FlushSyncState = Arc<(Mutex<Option<Result<(), String>>>, Condvar)>;
type PersistFn<'a> = dyn FnMut(&[u8], &[u8]) -> Result<(), String> + 'a;
type PutFn = fn(&LmdbDatabase, Vec<u8>, Vec<u8>) -> Result<(), String>;
type PutBatchFn = fn(&LmdbDatabase, Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String>;

trait FlushBatchHandle: Send {
    fn persist(&self, persist: &mut PersistFn<'_>) -> Result<(), String>;
    fn mark_committed(self: Box<Self>) -> usize;
}

struct CacheLogFlushBatch {
    overlay: Arc<CacheOverlay>,
    batch: FlushBatch<Vec<u8>, Vec<u8>>,
}

impl FlushBatchHandle for CacheLogFlushBatch {
    fn persist(&self, persist: &mut PersistFn<'_>) -> Result<(), String> {
        for entry in self.batch.iter() {
            persist(&entry.key, &entry.value)?;
        }
        Ok(())
    }

    fn mark_committed(self: Box<Self>) -> usize {
        self.overlay.mark_flushed(&self.batch)
    }
}

enum WorkerCommand {
    FlushSync(FlushSyncState),
    FlushHint,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StrategyKind {
    DirectReadOnly,
    DirtyReadWrite,
}

struct StrategyOps {
    lookup: fn(&LmdbDatabase, &[u8]) -> Option<Vec<u8>>,
    put: PutFn,
    put_batch: PutBatchFn,
    visible_len: fn(&LmdbDatabase) -> usize,
    dirty_len: fn(&LmdbDatabase) -> usize,
    prepare_flush_batch: fn(&LmdbDatabase, usize) -> Option<Box<dyn FlushBatchHandle>>,
}

fn no_lookup(_db: &LmdbDatabase, _key: &[u8]) -> Option<Vec<u8>> {
    None
}

fn overlay_lookup(db: &LmdbDatabase, key: &[u8]) -> Option<Vec<u8>> {
    db.overlay
        .load_full()
        .and_then(|overlay| overlay.read(key, |_, value, _, _| value.clone()))
}

fn read_only_put(_db: &LmdbDatabase, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), String> {
    Err("Database opened read_only".to_string())
}

fn dirty_put(db: &LmdbDatabase, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
    let overlay = db
        .overlay
        .load_full()
        .ok_or_else(|| "Dirty strategy overlay is not initialized".to_string())?;
    let _ = overlay.insert_dirty(key, value);
    Ok(())
}

fn read_only_put_batch(_db: &LmdbDatabase, _pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
    Err("Database opened read_only".to_string())
}

fn dirty_put_batch(db: &LmdbDatabase, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
    let overlay = db
        .overlay
        .load_full()
        .ok_or_else(|| "Dirty strategy overlay is not initialized".to_string())?;
    for (key, value) in pairs {
        let _ = overlay.insert_dirty(key, value);
    }
    Ok(())
}

fn no_visible_len(_db: &LmdbDatabase) -> usize {
    0
}

fn overlay_visible_len(db: &LmdbDatabase) -> usize {
    db.overlay
        .load_full()
        .map_or(0, |overlay| overlay.visible_len())
}

fn no_dirty_len(_db: &LmdbDatabase) -> usize {
    0
}

fn overlay_dirty_len(db: &LmdbDatabase) -> usize {
    db.overlay
        .load_full()
        .map_or(0, |overlay| overlay.dirty_log_len())
}

fn no_flush_batch(_db: &LmdbDatabase, _limit: usize) -> Option<Box<dyn FlushBatchHandle>> {
    None
}

fn overlay_flush_batch(db: &LmdbDatabase, limit: usize) -> Option<Box<dyn FlushBatchHandle>> {
    let overlay = db.overlay.load_full()?;
    let batch = overlay.flush_batch(limit.max(1));
    if batch.is_empty() {
        None
    } else {
        Some(Box::new(CacheLogFlushBatch { overlay, batch }))
    }
}

static DIRECT_RO_OPS: StrategyOps = StrategyOps {
    lookup: no_lookup,
    put: read_only_put,
    put_batch: read_only_put_batch,
    visible_len: no_visible_len,
    dirty_len: no_dirty_len,
    prepare_flush_batch: no_flush_batch,
};

static DIRTY_RW_OPS: StrategyOps = StrategyOps {
    lookup: overlay_lookup,
    put: dirty_put,
    put_batch: dirty_put_batch,
    visible_len: overlay_visible_len,
    dirty_len: overlay_dirty_len,
    prepare_flush_batch: overlay_flush_batch,
};

/// LMDB Environment resource
///
/// Represents an LMDB environment that can contain multiple databases.
/// Environments are reference-counted and shared across database instances.
#[derive(Debug)]
pub struct LmdbEnv {
    /// Path to the database directory
    path: String,
    /// Environment options used when reopening
    options: Arc<std::sync::RwLock<EnvOptions>>,
    /// Mutable runtime environment state
    state: Arc<std::sync::RwLock<EnvState>>,
    /// Reference count for active databases using this environment
    ref_count: Arc<Mutex<usize>>,
    /// Atomic generation counter for lock-free fast path
    generation: AtomicU64,
}

/// LMDB Database resource
///
/// Represents a database within an LMDB environment.
/// Uses a statically selected read/write strategy plus a background flush worker
/// for dirty strategies.
pub struct LmdbDatabase {
    env: ResourceArc<LmdbEnv>,
    strategy_kind: StrategyKind,
    ops: &'static StrategyOps,
    needs_worker: bool,
    worker_running: AtomicBool,
    overlay: ArcSwapOption<CacheOverlay>,
    overlay_generation: AtomicU64,
    /// Dirty backlog threshold that triggers an automatic flush check.
    flush_trigger_dirty: AtomicUsize,
    /// Maximum number of dirty items drained per automatic flush transaction.
    flush_batch_limit: AtomicUsize,
    /// Coalescing gate for FlushHint command (true means one hint is already queued/in-flight).
    flush_hint: AtomicBool,
    /// Cache: true when db is closed (atomic fast-path, no lock needed)
    is_closed: AtomicBool,
    /// Permanently disables a stale handle after the registry replaces it.
    retired: AtomicBool,
    /// Atomic fast-path: true when fatal_error is set
    has_fatal_error: AtomicBool,
    /// Metadata state (cached_db, closed, create_if_missing)
    state: Mutex<DbState>,
    /// Fatal flush error set by worker thread
    fatal_error: Mutex<Option<String>>,
    /// Lock-free read fast path: cached (Arc<Environment>, Database, generation)
    hot_handles: ArcSwap<Option<(Arc<Environment>, Database, u64)>>,
    /// Channel to send commands to the worker thread
    worker_tx: Mutex<Option<Sender<WorkerCommand>>>,
    /// Handle to the worker thread for joining
    worker_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct EnvState {
    env: Option<Arc<Environment>>,
    close_requested: bool,
    generation: u64,
}

// Global registry of open environments
//
// Ensures that each directory path has at most one environment open,
// preventing LMDB conflicts and improving resource sharing.
lazy_static::lazy_static! {
    static ref ENVIRONMENTS: Arc<Mutex<HashMap<String, ResourceArc<LmdbEnv>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref DATABASES: Arc<Mutex<HashMap<String, ResourceArc<LmdbDatabase>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref GET_CACHE_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static ref GET_LMDB_HIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static ref GET_MISS_COUNT: AtomicU64 = AtomicU64::new(0);
    static ref GET_LATENCY_HISTOGRAM: [AtomicU64; GET_LATENCY_BUCKETS] =
        std::array::from_fn(|_| AtomicU64::new(0));
    static ref GET_METRIC_CLOCK: Clock = Clock::new();
}

enum GetMetricKind {
    CacheHit,
    LmdbHit,
    Miss,
}

struct LocalGetMetrics {
    cache_hits: u64,
    lmdb_hits: u64,
    misses: u64,
    latency_histogram: [u64; GET_LATENCY_BUCKETS],
    ops_since_check: u64,
    last_flush: StdInstant,
}

impl LocalGetMetrics {
    fn new() -> Self {
        Self {
            cache_hits: 0,
            lmdb_hits: 0,
            misses: 0,
            latency_histogram: [0; GET_LATENCY_BUCKETS],
            ops_since_check: 0,
            last_flush: StdInstant::now(),
        }
    }
}

thread_local! {
    static LOCAL_GET_METRICS: RefCell<LocalGetMetrics> = RefCell::new(LocalGetMetrics::new());
}

/// Initialize the NIF module
///
/// Registers resource types with the Erlang runtime.
/// This function is called automatically when the NIF is loaded.
#[allow(non_local_definitions)]
fn init(env: Env, _info: Term) -> bool {
    lazy_static::initialize(&GET_METRIC_CLOCK);
    rustler::resource!(LmdbEnv, env) && rustler::resource!(LmdbDatabase, env)
}

fn cachelog_config(batch_size: usize) -> CacheLogConfig {
    let dirty_log_capacity = batch_size.max(1).saturating_mul(2);
    let clean_capacity = 1;
    let visible_capacity = dirty_log_capacity.saturating_add(clean_capacity).max(1024);
    CacheLogConfig::new(visible_capacity, dirty_log_capacity, clean_capacity)
}

fn flush_batch_limit() -> usize {
    512
}

fn build_overlay(kind: StrategyKind, batch_size: usize) -> Option<Arc<CacheOverlay>> {
    match kind {
        StrategyKind::DirectReadOnly => None,
        StrategyKind::DirtyReadWrite => Some(Arc::new(CacheOverlay::with_hasher(
            cachelog_config(batch_size),
            ahash::RandomState::default(),
        ))),
    }
}

fn strategy_ops(kind: StrategyKind) -> &'static StrategyOps {
    match kind {
        StrategyKind::DirectReadOnly => &DIRECT_RO_OPS,
        StrategyKind::DirtyReadWrite => &DIRTY_RW_OPS,
    }
}

fn select_strategy_kind(options: &EnvOptions) -> StrategyKind {
    match options.read_only {
        true => StrategyKind::DirectReadOnly,
        false => StrategyKind::DirtyReadWrite,
    }
}

fn build_environment(path: &str, options: &EnvOptions) -> Result<Environment, lmdb::Error> {
    let mut env_builder = Environment::new();

    if let Some(map_size) = options.map_size {
        env_builder.set_map_size(map_size as usize);
    } else {
        env_builder.set_map_size(1024 * 1024 * 1024);
    }

    if let Some(max_readers) = options.max_readers {
        env_builder.set_max_readers(max_readers);
    }

    let mut flags = EnvironmentFlags::empty();
    if options.no_mem_init {
        flags |= EnvironmentFlags::NO_MEM_INIT;
    }
    if options.no_sync {
        flags |= EnvironmentFlags::NO_SYNC;
    }
    if options.no_lock {
        flags |= EnvironmentFlags::NO_LOCK;
    }
    if options.read_only {
        flags |= EnvironmentFlags::READ_ONLY;
    }
    if options.write_map {
        flags |= EnvironmentFlags::WRITE_MAP;
    }
    if options.no_readahead {
        flags |= EnvironmentFlags::NO_READAHEAD;
    }
    env_builder.set_flags(flags);

    env_builder.open(Path::new(path))
}

impl LmdbEnv {
    fn set_options(&self, options: EnvOptions) -> Result<(), String> {
        let mut stored = self
            .options
            .write()
            .map_err(|_| "Failed to write environment options".to_string())?;
        *stored = options;
        Ok(())
    }

    fn write_buffer_size(&self) -> Result<usize, String> {
        let options = self
            .options
            .read()
            .map_err(|_| "Failed to read environment options".to_string())?;
        Ok(options.batch_size.unwrap_or(1000))
    }

    fn strategy_kind(&self) -> Result<StrategyKind, String> {
        let options = self
            .options
            .read()
            .map_err(|_| "Failed to read environment options".to_string())?;
        Ok(select_strategy_kind(&options))
    }

    fn ensure_open(&self) -> Result<(Arc<Environment>, u64), String> {
        {
            let state = self
                .state
                .read()
                .map_err(|_| "Failed to read environment state".to_string())?;

            if let Some(existing_env) = state.env.as_ref() {
                if !state.close_requested {
                    return Ok((existing_env.clone(), state.generation));
                }
                if Arc::strong_count(existing_env) > 1 {
                    return Ok((existing_env.clone(), state.generation));
                }
            }
        }

        let mut state = self
            .state
            .write()
            .map_err(|_| "Failed to write environment state".to_string())?;

        if let Some(existing_env) = state.env.as_ref() {
            if !state.close_requested {
                return Ok((existing_env.clone(), state.generation));
            }
            if Arc::strong_count(existing_env) > 1 {
                return Ok((existing_env.clone(), state.generation));
            }
            state.env = None;
            state.close_requested = false;
            state.generation += 1;
            self.generation.fetch_add(1, Ordering::Release);
        }

        let options = self
            .options
            .read()
            .map_err(|_| "Failed to read environment options".to_string())?
            .clone();

        let reopened = build_environment(&self.path, &options)
            .map_err(|e| format!("Failed to open environment: {:?}", e))?;
        let reopened = Arc::new(reopened);
        state.env = Some(reopened.clone());
        state.close_requested = false;
        state.generation += 1;
        Ok((reopened, state.generation))
    }

    fn request_close(&self) -> Result<(), String> {
        let mut state = self
            .state
            .write()
            .map_err(|_| "Failed to write environment state".to_string())?;

        state.close_requested = true;
        if let Some(existing_env) = state.env.as_ref() {
            if Arc::strong_count(existing_env) == 1 {
                state.env = None;
                state.close_requested = false;
                state.generation += 1;
                self.generation.fetch_add(1, Ordering::Release);
            }
        }

        Ok(())
    }

    fn is_closed(&self) -> Result<bool, String> {
        let state = self
            .state
            .read()
            .map_err(|_| "Failed to read environment state".to_string())?;
        Ok(state.env.is_none() || state.close_requested)
    }
}

fn do_flush(db: &LmdbDatabase) -> Result<(), String> {
    let limit = db.flush_batch_limit.load(Ordering::Relaxed);
    loop {
        let Some(batch) = (db.ops.prepare_flush_batch)(db, limit) else {
            return Ok(());
        };

        let (live_env, live_db) = db.fast_get_handles()?;

        let mut txn = match live_env.begin_rw_txn() {
            Ok(txn) => txn,
            Err(_) => return Err("Failed to begin write transaction".to_string()),
        };

        batch.persist(&mut |key, value| {
            txn.put(live_db, &key, &value, WriteFlags::empty())
                .map_err(|e| format!("Failed to put value: {:?}", e))
        })?;

        txn.commit()
            .map_err(|_| "Failed to commit batch transaction".to_string())?;

        if batch.mark_committed() == 0 {
            return Ok(());
        }
    }
}

fn drain_remaining_sync_waiters(rx: &Receiver<WorkerCommand>, fatal_error: Option<String>) {
    while let Ok(cmd) = rx.try_recv() {
        if let WorkerCommand::FlushSync(signal) = cmd {
            let err_msg = fatal_error
                .clone()
                .unwrap_or_else(|| "Worker shut down".to_string());
            let (lock, cvar) = &*signal;
            if let Ok(mut guard) = lock.lock() {
                *guard = Some(Err(err_msg));
            }
            cvar.notify_all();
        }
    }
}

fn worker_loop(rx: Receiver<WorkerCommand>, db: ResourceArc<LmdbDatabase>) {
    let mut exit_error: Option<String> = None;
    loop {
        match rx.recv_timeout(AUTO_FLUSH_CHECK_INTERVAL) {
            Ok(WorkerCommand::FlushSync(signal)) => {
                let result = do_flush(&db);
                let is_err = result.is_err();
                if is_err {
                    let e = result.as_ref().unwrap_err().clone();
                    if let Ok(mut guard) = db.fatal_error.lock() {
                        *guard = Some(e.clone());
                    }
                    db.has_fatal_error.store(true, Ordering::Release);
                    exit_error = Some(e);
                }
                {
                    let (lock, cvar) = &*signal;
                    if let Ok(mut guard) = lock.lock() {
                        *guard = Some(result);
                    }
                    cvar.notify_all();
                }
                if is_err {
                    break;
                }
            }
            Ok(WorkerCommand::FlushHint) => {
                db.flush_hint.store(false, Ordering::Release);
                if (db.ops.dirty_len)(&db) != 0 {
                    let result = do_flush(&db);
                    if let Err(e) = result {
                        if let Ok(mut guard) = db.fatal_error.lock() {
                            *guard = Some(e.clone());
                        }
                        db.has_fatal_error.store(true, Ordering::Release);
                        exit_error = Some(e);
                        break;
                    }
                }
            }
            Ok(WorkerCommand::Shutdown) => {
                let result = do_flush(&db);
                if let Err(e) = result {
                    if let Ok(mut guard) = db.fatal_error.lock() {
                        *guard = Some(e.clone());
                    }
                    db.has_fatal_error.store(true, Ordering::Release);
                    exit_error = Some(e);
                }
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                let threshold = db.flush_trigger_dirty.load(Ordering::Relaxed).max(1);
                if (db.ops.dirty_len)(&db) >= threshold {
                    let result = do_flush(&db);
                    if let Err(e) = result {
                        if let Ok(mut guard) = db.fatal_error.lock() {
                            *guard = Some(e.clone());
                        }
                        db.has_fatal_error.store(true, Ordering::Release);
                        exit_error = Some(e);
                        break;
                    }
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    db.worker_running.store(false, Ordering::Relaxed);
    drain_remaining_sync_waiters(&rx, exit_error);
}

fn send_flush_hint(db_handle: &LmdbDatabase) {
    if !db_handle.needs_worker {
        return;
    }
    if db_handle.flush_hint.swap(true, Ordering::AcqRel) {
        return;
    }
    let tx_guard = match db_handle.worker_tx.lock() {
        Ok(guard) => guard,
        Err(_) => {
            db_handle.flush_hint.store(false, Ordering::Release);
            return;
        }
    };
    if let Some(sender) = tx_guard.as_ref() {
        if sender.send(WorkerCommand::FlushHint).is_err() {
            db_handle.flush_hint.store(false, Ordering::Release);
        }
    } else {
        db_handle.flush_hint.store(false, Ordering::Release);
    }
}

fn flush_sync(db_handle: &LmdbDatabase) -> Result<(), String> {
    let signal = Arc::new((Mutex::new(None::<Result<(), String>>), Condvar::new()));
    {
        let tx = db_handle
            .worker_tx
            .lock()
            .map_err(|_| "Failed to lock worker channel".to_string())?;
        if let Some(ref sender) = *tx {
            sender
                .send(WorkerCommand::FlushSync(signal.clone()))
                .map_err(|_| "Worker thread is gone".to_string())?;
        } else {
            return Err("Worker thread is not running".to_string());
        }
    }
    let (lock, cvar) = &*signal;
    let mut result = lock
        .lock()
        .map_err(|_| "Failed to lock signal".to_string())?;
    while result.is_none() {
        result = cvar
            .wait(result)
            .map_err(|_| "Condvar wait failed".to_string())?;
    }
    result.take().unwrap()
}

fn spawn_worker(resource: &ResourceArc<LmdbDatabase>) -> (Sender<WorkerCommand>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let worker_resource = resource.clone();
    let handle = thread::spawn(move || {
        worker_loop(rx, worker_resource);
    });
    (tx, handle)
}

fn ensure_worker(db_handle: &ResourceArc<LmdbDatabase>) {
    if !db_handle.needs_worker {
        return;
    }
    if db_handle.worker_running.load(Ordering::Relaxed) {
        return;
    }
    let mut wtx = match db_handle.worker_tx.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    if wtx.is_some() {
        db_handle.worker_running.store(true, Ordering::Relaxed);
        return;
    }

    let (tx, handle) = spawn_worker(db_handle);
    *wtx = Some(tx);
    db_handle.worker_running.store(true, Ordering::Relaxed);
    drop(wtx);

    if let Ok(mut wh) = db_handle.worker_handle.lock() {
        *wh = Some(handle);
    } else {
        db_handle.worker_running.store(false, Ordering::Relaxed);
        if let Ok(mut tx_guard) = db_handle.worker_tx.lock() {
            let _ = tx_guard.take();
        }
        let _ = handle.join();
    }
}

fn current_fatal_error(db_handle: &LmdbDatabase) -> Option<String> {
    if !db_handle.has_fatal_error.load(Ordering::Acquire) {
        return None;
    }
    if let Ok(guard) = db_handle.fatal_error.lock() {
        if let Some(err) = guard.as_ref() {
            return Some(err.clone());
        }
    }
    Some("Database is in fatal error state".to_string())
}

fn soft_close_db(db_handle: &ResourceArc<LmdbDatabase>) -> Result<(), String> {
    let was_open = !db_handle.is_closed.swap(true, Ordering::AcqRel);
    db_handle.worker_running.store(false, Ordering::Relaxed);

    {
        let mut tx = db_handle
            .worker_tx
            .lock()
            .map_err(|_| "Failed to lock worker channel".to_string())?;
        if let Some(sender) = tx.take() {
            let _ = sender.send(WorkerCommand::Shutdown);
        }
    }
    {
        let mut wh = db_handle
            .worker_handle
            .lock()
            .map_err(|_| "Failed to lock worker handle".to_string())?;
        if let Some(handle) = wh.take() {
            let _ = handle.join();
        }
    }

    let fatal_err = db_handle.fatal_error.lock().ok().and_then(|g| g.clone());

    {
        let mut state = db_handle
            .state
            .lock()
            .map_err(|_| "Failed to lock database state".to_string())?;
        state.cached_db = None;
    }

    db_handle.hot_handles.store(Arc::new(None));

    if was_open {
        let mut ref_count = db_handle
            .env
            .ref_count
            .lock()
            .map_err(|_| "Failed to update environment reference count".to_string())?;
        if *ref_count > 0 {
            *ref_count -= 1;
        }
    }

    if let Some(err) = fatal_err {
        return Err(err);
    }

    Ok(())
}

///===================================================================
/// Environment Management
///===================================================================

#[rustler::nif]
fn env_open<'a>(env: Env<'a>, path: Term<'a>, options: Vec<Term<'a>>) -> NifResult<Term<'a>> {
    let path_string = if let Ok(binary) = path.decode::<Binary>() {
        std::str::from_utf8(&binary)
            .map_err(|_| Error::BadArg)?
            .to_string()
    } else if let Ok(string) = path.decode::<String>() {
        string
    } else if let Ok(chars) = path.decode::<Vec<u8>>() {
        std::str::from_utf8(&chars)
            .map_err(|_| Error::BadArg)?
            .to_string()
    } else {
        return Err(Error::BadArg);
    };
    let path_str = &path_string;
    let has_options = !options.is_empty();
    let parsed_options = parse_env_options(options)?;

    if let Some(existing_env) = {
        let environments = ENVIRONMENTS.lock().map_err(|_| Error::BadArg)?;
        environments.get(path_str).cloned()
    } {
        if !Path::new(path_str).exists() {
            if let Some(db_handle) = {
                let databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
                databases.get(path_str).cloned()
            } {
                db_handle.hot_handles.store(Arc::new(None));
            }
            if let Err(error_msg) = existing_env.request_close() {
                return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
            }
        }
        if has_options {
            if let Err(error_msg) = existing_env.set_options(parsed_options) {
                return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
            }
        }
        if let Err(error_msg) = existing_env.ensure_open() {
            return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
        }
        return Ok((atoms::ok(), existing_env).encode(env));
    }

    let lmdb_env_result = build_environment(path_str, &parsed_options);

    let lmdb_environment = match lmdb_env_result {
        Ok(env) => env,
        Err(e) => {
            let path = Path::new(path_str);

            let error_atom = if !path.exists() {
                atoms::directory_not_found()
            } else if path.is_file() {
                atoms::invalid_path()
            } else {
                match std::fs::File::create(path.join(".lmdb_test")) {
                    Ok(_) => {
                        let _ = std::fs::remove_file(path.join(".lmdb_test"));
                        lmdb_error_to_atom(e)
                    }
                    Err(io_err) => match io_err.kind() {
                        std::io::ErrorKind::PermissionDenied => atoms::permission_denied(),
                        _ => atoms::environment_error(),
                    },
                }
            };

            return Ok((atoms::error(), error_atom).encode(env));
        }
    };

    let lmdb_env = LmdbEnv {
        path: path_str.to_string(),
        options: Arc::new(std::sync::RwLock::new(parsed_options.clone())),
        state: Arc::new(std::sync::RwLock::new(EnvState {
            env: Some(Arc::new(lmdb_environment)),
            close_requested: false,
            generation: 1,
        })),
        ref_count: Arc::new(Mutex::new(0)),
        generation: AtomicU64::new(1),
    };
    let resource = ResourceArc::new(lmdb_env);

    {
        let mut environments = ENVIRONMENTS.lock().map_err(|_| Error::BadArg)?;
        environments.insert(path_str.to_string(), resource.clone());
    }

    Ok((atoms::ok(), resource).encode(env))
}

#[rustler::nif]
fn env_sync<'a>(env: Env<'a>, env_handle: ResourceArc<LmdbEnv>) -> NifResult<Term<'a>> {
    let (live_env, _) = match env_handle.ensure_open() {
        Ok(data) => data,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
        }
    };

    match live_env.sync(true) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err_msg) => Ok((
            atoms::error(),
            atoms::environment_error(),
            format!("Environment sync failed: {}", err_msg),
        )
            .encode(env)),
    }
}

#[rustler::nif]
fn env_close<'a>(env: Env<'a>, env_handle: ResourceArc<LmdbEnv>) -> NifResult<Term<'a>> {
    if let Some(db_handle) = {
        let databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
        databases.get(&env_handle.path).cloned()
    } {
        let _ = soft_close_db(&db_handle);
    }

    if let Err(error_msg) = env_handle.request_close() {
        return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
    }

    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn env_close_by_name<'a>(env: Env<'a>, path: Term<'a>) -> NifResult<Term<'a>> {
    let path_string = if let Ok(binary) = path.decode::<Binary>() {
        std::str::from_utf8(&binary)
            .map_err(|_| Error::BadArg)?
            .to_string()
    } else if let Ok(string) = path.decode::<String>() {
        string
    } else if let Ok(chars) = path.decode::<Vec<u8>>() {
        std::str::from_utf8(&chars)
            .map_err(|_| Error::BadArg)?
            .to_string()
    } else {
        return Err(Error::BadArg);
    };
    let path_str = &path_string;

    let env_handle = {
        let environments = ENVIRONMENTS.lock().map_err(|_| Error::BadArg)?;
        environments.get(path_str).cloned()
    };

    if let Some(db_handle) = {
        let databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
        databases.get(path_str).cloned()
    } {
        let _ = soft_close_db(&db_handle);
    }

    if let Some(env_handle) = env_handle {
        if let Err(error_msg) = env_handle.request_close() {
            return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
        }
        Ok(atoms::ok().encode(env))
    } else {
        Ok((atoms::error(), atoms::not_found()).encode(env))
    }
}

///===================================================================
/// Database Operations
///===================================================================

#[rustler::nif]
fn db_close<'a>(env: Env<'a>, db_handle: ResourceArc<LmdbDatabase>) -> NifResult<Term<'a>> {
    match soft_close_db(&db_handle) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error_msg) => Ok((atoms::error(), atoms::database_error(), error_msg).encode(env)),
    }
}

#[rustler::nif]
fn db_open<'a>(
    env: Env<'a>,
    env_handle: ResourceArc<LmdbEnv>,
    options: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    let parsed_options = parse_db_options(options)?;
    if let Err(error_msg) = env_handle.ensure_open() {
        return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
    }
    let batch_size = match env_handle.write_buffer_size() {
        Ok(size) => size,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
        }
    };
    let strategy_kind = match env_handle.strategy_kind() {
        Ok(kind) => kind,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::environment_error(), error_msg).encode(env));
        }
    };

    let db_key = env_handle.path.clone();
    {
        let mut databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
        if let Some(existing_db) = databases.get(&db_key).cloned() {
            if existing_db.strategy_kind != strategy_kind {
                if !existing_db.is_closed.load(Ordering::Acquire) {
                    return Ok((
                        atoms::error(),
                        atoms::database_error(),
                        "Database already open with different strategy".to_string(),
                    )
                        .encode(env));
                }

                existing_db.retired.store(true, Ordering::Release);
                existing_db.hot_handles.store(Arc::new(None));
                databases.remove(&db_key);
                // fall through with lock dropped to create a fresh handle
            } else {
                // Reopen path: clone the handle then drop the map lock before doing
                // any work that may acquire inner locks (worker, fatal_error, etc.).
                let reopen_db = existing_db.clone();
                drop(databases);

                if parsed_options.create {
                    if let Err(error_msg) = reopen_db.set_create_if_missing(true) {
                        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
                    }
                }
                reopen_db
                    .flush_trigger_dirty
                    .store(batch_size, Ordering::Release);
                reopen_db
                    .flush_batch_limit
                    .store(flush_batch_limit(), Ordering::Release);
                if reopen_db.has_fatal_error.load(Ordering::Acquire) {
                    if reopen_db.needs_worker {
                        if let Ok(mut wh) = reopen_db.worker_handle.lock() {
                            if let Some(handle) = wh.take() {
                                let _ = handle.join();
                            }
                        }
                        let (new_tx, new_handle) = spawn_worker(&reopen_db);
                        if let Ok(mut tx) = reopen_db.worker_tx.lock() {
                            let _ = tx.replace(new_tx);
                            reopen_db.worker_running.store(true, Ordering::Relaxed);
                        }
                        if let Ok(mut wh) = reopen_db.worker_handle.lock() {
                            *wh = Some(new_handle);
                        }
                    }
                    if let Ok(mut fe) = reopen_db.fatal_error.lock() {
                        *fe = None;
                    }
                    reopen_db.has_fatal_error.store(false, Ordering::Release);
                }
                if let Err(error_msg) = reopen_db.reopen_if_closed() {
                    return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
                }
                ensure_worker(&reopen_db);
                if let Err(error_msg) = reopen_db.validate_database() {
                    return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
                }
                return Ok((atoms::ok(), reopen_db).encode(env));
            }
        }
    }

    // Increment reference count for this environment
    {
        let mut ref_count = env_handle.ref_count.lock().map_err(|_| Error::BadArg)?;
        *ref_count += 1;
    }

    let lmdb_db = LmdbDatabase {
        env: env_handle.clone(),
        strategy_kind,
        ops: strategy_ops(strategy_kind),
        needs_worker: matches!(strategy_kind, StrategyKind::DirtyReadWrite),
        worker_running: AtomicBool::new(false),
        overlay: ArcSwapOption::new(build_overlay(strategy_kind, batch_size)),
        overlay_generation: AtomicU64::new(env_handle.generation.load(Ordering::Acquire)),
        flush_trigger_dirty: AtomicUsize::new(batch_size),
        flush_batch_limit: AtomicUsize::new(flush_batch_limit()),
        flush_hint: AtomicBool::new(false),
        state: Mutex::new(DbState {
            cached_db: None,
            create_if_missing: parsed_options.create,
        }),
        fatal_error: Mutex::new(None),
        has_fatal_error: AtomicBool::new(false),
        is_closed: AtomicBool::new(false),
        retired: AtomicBool::new(false),
        hot_handles: ArcSwap::from_pointee(None),
        worker_tx: Mutex::new(None),
        worker_handle: Mutex::new(None),
    };
    let resource = ResourceArc::new(lmdb_db);

    if resource.needs_worker {
        let (tx, handle) = spawn_worker(&resource);
        if let Ok(mut wtx) = resource.worker_tx.lock() {
            *wtx = Some(tx);
            resource.worker_running.store(true, Ordering::Relaxed);
        }
        if let Ok(mut wh) = resource.worker_handle.lock() {
            *wh = Some(handle);
        }
    }

    {
        let mut databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
        databases.insert(db_key, resource.clone());
    }

    if let Err(error_msg) = resource.validate_database() {
        {
            let mut databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
            databases.remove(&env_handle.path);
        }
        {
            let mut ref_count = env_handle.ref_count.lock().map_err(|_| Error::BadArg)?;
            if *ref_count > 0 {
                *ref_count -= 1;
            }
        }
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    Ok((atoms::ok(), resource).encode(env))
}

impl LmdbDatabase {
    fn set_create_if_missing(&self, create: bool) -> Result<(), String> {
        if !create {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Failed to lock database state".to_string())?;
        state.create_if_missing = true;
        Ok(())
    }

    fn reopen_if_closed(&self) -> Result<(), String> {
        if self.retired.load(Ordering::Acquire) {
            return Err("Database handle has been retired; reopen with db_open/2".to_string());
        }
        if self
            .is_closed
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let mut ref_count = self
                .env
                .ref_count
                .lock()
                .map_err(|_| "Failed to update environment reference count")?;
            *ref_count += 1;

            if let Ok(mut fe) = self.fatal_error.lock() {
                *fe = None;
            }
            self.has_fatal_error.store(false, Ordering::Release);
        }
        Ok(())
    }

    fn sync_overlay_generation(&self) -> Result<(), String> {
        let current_gen = self.env.generation.load(Ordering::Acquire);
        let overlay_gen = self.overlay_generation.load(Ordering::Acquire);
        if current_gen == overlay_gen {
            return Ok(());
        }

        let fresh = match self.strategy_kind {
            StrategyKind::DirectReadOnly => None,
            _ => {
                let batch_size = self.flush_trigger_dirty.load(Ordering::Acquire);
                build_overlay(self.strategy_kind, batch_size)
            }
        };
        self.overlay.store(fresh);
        self.overlay_generation
            .store(current_gen, Ordering::Release);
        Ok(())
    }

    fn ensure_open_handles(&self) -> Result<(Arc<Environment>, Database), String> {
        let (live_env, env_generation) = self.env.ensure_open()?;
        self.reopen_if_closed()?;
        self.sync_overlay_generation()?;
        let read_only = matches!(self.strategy_kind, StrategyKind::DirectReadOnly);

        let mut state = self
            .state
            .lock()
            .map_err(|_| "Failed to lock database state".to_string())?;

        if let Some((cached_db, cached_generation)) = state.cached_db {
            if cached_generation == env_generation {
                return Ok((live_env, cached_db));
            }
        }

        let db = if read_only {
            live_env.open_db(None)
        } else if state.create_if_missing {
            live_env.create_db(None, DatabaseFlags::empty())
        } else {
            match live_env.create_db(None, DatabaseFlags::empty()) {
                Ok(db) => Ok(db),
                Err(_) => live_env.open_db(None),
            }
        }
        .map_err(|e| format!("Failed to open database: {:?}", e))?;

        state.cached_db = Some((db, env_generation));

        let gen = self.env.generation.load(Ordering::Acquire);
        self.hot_handles
            .store(Arc::new(Some((live_env.clone(), db, gen))));

        Ok((live_env, db))
    }

    fn fast_get_handles(&self) -> Result<(Arc<Environment>, Database), String> {
        if !self.is_closed.load(Ordering::Relaxed) {
            let current_gen = self.env.generation.load(Ordering::Acquire);
            let guard = self.hot_handles.load();
            if let Some((ref env, db, cached_gen)) = **guard {
                if cached_gen == current_gen {
                    return Ok((env.clone(), db));
                }
            }
        }
        self.ensure_open_handles()
    }

    fn validate_database(&self) -> Result<(), String> {
        self.reopen_if_closed()?;
        let _ = self.env.ensure_open()?;
        Ok(())
    }
}

impl Drop for LmdbDatabase {
    fn drop(&mut self) {
        {
            if let Ok(mut tx) = self.worker_tx.lock() {
                if let Some(sender) = tx.take() {
                    self.worker_running.store(false, Ordering::Relaxed);
                    let _ = sender.send(WorkerCommand::Shutdown);
                }
            }
        }
        {
            if let Ok(mut wh) = self.worker_handle.lock() {
                if let Some(handle) = wh.take() {
                    let _ = handle.join();
                }
            }
        }

        let already_closed = self.is_closed.load(Ordering::Relaxed);

        if !already_closed {
            if let Ok(mut ref_count) = self.env.ref_count.lock() {
                if *ref_count > 0 {
                    *ref_count -= 1;
                }
            }
        }
    }
}

///===================================================================
/// Key-Value Operations
///===================================================================

#[rustler::nif]
fn put<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key: Binary,
    value: Binary,
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if !db_handle.needs_worker {
        return Ok((
            atoms::error(),
            atoms::database_error(),
            "Database opened read_only".to_string(),
        )
            .encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }
    ensure_worker(&db_handle);

    let key_vec = key.as_slice().to_vec();
    let value_vec = value.as_slice().to_vec();

    if key_vec.is_empty() {
        let (live_env, live_db) = match db_handle.ensure_open_handles() {
            Ok(handles) => handles,
            Err(error_msg) => {
                return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
            }
        };
        let mut txn = match live_env.begin_rw_txn() {
            Ok(txn) => txn,
            Err(_) => {
                return Ok((
                    atoms::error(),
                    atoms::transaction_error(),
                    "Failed to begin write transaction".to_string(),
                )
                    .encode(env));
            }
        };
        match txn.put(live_db, &key_vec, &value_vec, WriteFlags::empty()) {
            Ok(()) => match txn.commit() {
                Ok(()) => {
                    return Ok(atoms::ok().encode(env));
                }
                Err(_) => {
                    return Ok((
                        atoms::error(),
                        atoms::transaction_error(),
                        "Failed to commit transaction".to_string(),
                    )
                        .encode(env))
                }
            },
            Err(lmdb_err) => {
                let error_msg = match lmdb_err {
                    lmdb::Error::BadValSize => "Empty key not supported".to_string(),
                    _ => format!("Failed to put value: {:?}", lmdb_err),
                };
                return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
            }
        }
    }

    if key_vec.len() > LMDB_DEFAULT_MAX_KEY_SIZE {
        return Ok((
            atoms::error(),
            atoms::validation_error(),
            format!(
                "Key size {} exceeds limit {}",
                key_vec.len(),
                LMDB_DEFAULT_MAX_KEY_SIZE
            ),
        )
            .encode(env));
    }

    if let Err(error_msg) = (db_handle.ops.put)(&db_handle, key_vec, value_vec) {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn get<'a>(env: Env<'a>, db_handle: &'a LmdbDatabase, key: Binary) -> NifResult<Term<'a>> {
    let metric_start = begin_get_metric();
    if db_handle.is_closed.load(Ordering::Relaxed) {
        if let Err(error_msg) = db_handle.validate_database() {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    if let Some(error_msg) = current_fatal_error(db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }

    let key_bytes = key.as_slice();

    if let Some(value) = (db_handle.ops.lookup)(db_handle, key_bytes) {
        record_get_metric(GetMetricKind::CacheHit, metric_start);
        let mut binary = OwnedBinary::new(value.len()).ok_or(Error::BadArg)?;
        binary.as_mut_slice().copy_from_slice(&value);
        return Ok((atoms::ok(), binary.release(env)).encode(env));
    }

    let (live_env, live_db) = match db_handle.fast_get_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::transaction_error(),
                "Failed to begin read transaction".to_string(),
            )
                .encode(env));
        }
    };

    match txn.get(live_db, &key_bytes) {
        Ok(value_bytes) => {
            record_get_metric(GetMetricKind::LmdbHit, metric_start);
            let mut binary = OwnedBinary::new(value_bytes.len()).ok_or(Error::BadArg)?;
            binary.as_mut_slice().copy_from_slice(value_bytes);
            Ok((atoms::ok(), binary.release(env)).encode(env))
        }
        Err(lmdb::Error::NotFound) => {
            record_get_metric(GetMetricKind::Miss, metric_start);
            Ok(atoms::not_found().encode(env))
        }
        Err(_) => Ok((
            atoms::error(),
            atoms::database_error(),
            "Failed to get value".to_string(),
        )
            .encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn flush<'a>(env: Env<'a>, db_handle: ResourceArc<LmdbDatabase>) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if !db_handle.needs_worker {
        return Ok(atoms::ok().encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }
    ensure_worker(&db_handle);
    match flush_sync(&db_handle) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error_msg) => Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env)),
    }
}

#[rustler::nif]
fn overlay_count<'a>(env: Env<'a>, db_handle: &'a LmdbDatabase) -> NifResult<Term<'a>> {
    Ok((db_handle.ops.visible_len)(db_handle).encode(env))
}

#[rustler::nif]
fn get_metrics<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    // Caller-thread TLS flushes naturally via record_get_metric sampling.
    // Cross-thread buffers lag by up to METRICS_FLUSH_INTERVAL — acceptable
    // since metrics collection runs on a periodic schedule anyway.
    let cache_hits = GET_CACHE_HIT_COUNT.load(Ordering::Relaxed);
    let lmdb_hits = GET_LMDB_HIT_COUNT.load(Ordering::Relaxed);
    let misses = GET_MISS_COUNT.load(Ordering::Relaxed);
    let histogram: Vec<u64> = GET_LATENCY_HISTOGRAM
        .iter()
        .map(|bucket| bucket.load(Ordering::Relaxed))
        .collect();
    Ok((atoms::ok(), cache_hits, lmdb_hits, misses, histogram).encode(env))
}

#[rustler::nif]
fn put_batch<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key_value_pairs: Vec<(Binary, Binary)>,
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    if !db_handle.needs_worker {
        return Ok((
            atoms::error(),
            atoms::database_error(),
            "Database opened read_only".to_string(),
        )
            .encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }
    ensure_worker(&db_handle);

    if key_value_pairs.is_empty() {
        return Ok(atoms::ok().encode(env));
    }

    for (key, _value) in key_value_pairs.iter() {
        let klen = key.as_slice().len();
        if klen == 0 {
            return Ok((
                atoms::error(),
                atoms::validation_error(),
                "Empty key in batch".to_string(),
            )
                .encode(env));
        }
        if klen > LMDB_DEFAULT_MAX_KEY_SIZE {
            return Ok((
                atoms::error(),
                atoms::validation_error(),
                format!("Key size {klen} exceeds limit {LMDB_DEFAULT_MAX_KEY_SIZE}"),
            )
                .encode(env));
        }
    }

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = key_value_pairs
        .iter()
        .map(|(key, value)| (key.as_slice().to_vec(), value.as_slice().to_vec()))
        .collect();
    if let Err(error_msg) = (db_handle.ops.put_batch)(&db_handle, pairs) {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    Ok(atoms::ok().encode(env))
}

///===================================================================
/// Iterator Operations
///===================================================================

#[rustler::nif]
fn iterator<'a>(env: Env<'a>, db_handle: &'a LmdbDatabase) -> NifResult<Term<'a>> {
    let _ = db_handle;
    Ok(encode_iterator_start(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn iterator_next<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    cursor_term: Term<'a>,
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }

    let cursor_token = match decode_iterator_cursor(cursor_term) {
        Ok(token) => token,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::invalid(), error_msg).encode(env));
        }
    };

    if (db_handle.ops.dirty_len)(&db_handle) != 0 {
        ensure_worker(&db_handle);
        if let Err(error_msg) = flush_sync(&db_handle) {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }

    let (live_env, live_db) = match db_handle.fast_get_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::transaction_error(),
                "Failed to begin read transaction".to_string(),
            )
                .encode(env));
        }
    };

    let cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::database_error(),
                "Failed to open cursor".to_string(),
            )
                .encode(env));
        }
    };

    let next_entry = match cursor_token {
        IteratorCursor::Start => match cursor.get(None, None, MDB_FIRST) {
            Ok((Some(key), value)) => Some((key.to_vec(), value.to_vec())),
            Ok((None, _)) => None,
            Err(lmdb::Error::NotFound) => None,
            Err(_) => {
                return Ok((
                    atoms::error(),
                    atoms::database_error(),
                    "Failed to read first cursor entry".to_string(),
                )
                    .encode(env));
            }
        },
        IteratorCursor::AfterKey(last_key) => {
            let positioned_entry = match cursor.get(Some(last_key.as_slice()), None, MDB_SET_RANGE)
            {
                Ok((Some(key), value)) => Some((key.to_vec(), value.to_vec())),
                Ok((None, _)) => None,
                Err(lmdb::Error::NotFound) => None,
                Err(_) => {
                    return Ok((
                        atoms::error(),
                        atoms::database_error(),
                        "Failed to position iterator cursor".to_string(),
                    )
                        .encode(env));
                }
            };

            match positioned_entry {
                Some((key, _value)) if key == last_key => match cursor.get(None, None, MDB_NEXT) {
                    Ok((Some(next_key), next_value)) => {
                        Some((next_key.to_vec(), next_value.to_vec()))
                    }
                    Ok((None, _)) => None,
                    Err(lmdb::Error::NotFound) => None,
                    Err(_) => {
                        return Ok((
                            atoms::error(),
                            atoms::database_error(),
                            "Failed to advance iterator cursor".to_string(),
                        )
                            .encode(env));
                    }
                },
                Some((key, value)) => Some((key, value)),
                None => None,
            }
        }
    };

    match next_entry {
        Some((key, value)) => {
            let key_term = encode_binary(env, &key)?;
            let value_term = encode_binary(env, &value)?;
            let next_cursor = encode_iterator_after_key(env, &key)?;
            Ok((atoms::ok(), key_term, value_term, next_cursor).encode(env))
        }
        None => Ok(atoms::undefined().encode(env)),
    }
}

///===================================================================
/// List Operations
///===================================================================

#[rustler::nif(schedule = "DirtyIo")]
fn list<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key_prefix: Binary,
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }

    let prefix_bytes = key_prefix.as_slice();
    let prefix_len = prefix_bytes.len();
    let mut children = Vec::with_capacity(64);
    let mut children_sorted = false;

    if (db_handle.ops.dirty_len)(&db_handle) != 0 {
        // Disabled for perf isolation while benchmarking list() with pending dirty entries.
        // send_flush_hint(&db_handle);
        if let Some(overlay) = db_handle.overlay.load_full() {
            overlay.for_each_prefix_key(
                prefix_bytes,
                |full_key| {
                    collect_list_child_component(
                        &mut children,
                        full_key.as_slice(),
                        prefix_len,
                        &mut children_sorted,
                    );
                },
                usize::MAX,
            );
        }
    }

    let (live_env, live_db) = match db_handle.fast_get_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::transaction_error(),
                "Failed to begin read transaction".to_string(),
            )
                .encode(env));
        }
    };

    let mut cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::database_error(),
                "Failed to open cursor".to_string(),
            )
                .encode(env));
        }
    };

    if cursor.get(Some(prefix_bytes), None, MDB_SET_RANGE).is_ok() {
        let cursor_iter = cursor.iter_from(prefix_bytes);
        for (key, _value) in cursor_iter {
            if !key.starts_with(prefix_bytes) {
                break;
            }
            collect_list_child_component(&mut children, key, prefix_len, &mut children_sorted);
        }
    }

    if children.is_empty() {
        return Ok(atoms::not_found().encode(env));
    }

    if !children_sorted {
        children.sort_unstable();
    }

    let mut result_binaries = Vec::with_capacity(children.len());

    for child in children {
        let mut binary = OwnedBinary::new(child.len()).ok_or(Error::BadArg)?;
        binary.as_mut_slice().copy_from_slice(&child);
        result_binaries.push(binary.release(env));
    }

    Ok((atoms::ok(), result_binaries).encode(env))
}

fn collect_list_child_component(
    children: &mut Vec<Vec<u8>>,
    full_key: &[u8],
    prefix_len: usize,
    children_sorted: &mut bool,
) {
    let Some(remaining) = full_key.get(prefix_len..) else {
        return;
    };

    if remaining.is_empty() {
        return;
    }

    let next_component = if let Some(sep_pos) = remaining.iter().position(|&b| b == b'/') {
        &remaining[..sep_pos]
    } else {
        remaining
    };

    if next_component.is_empty() {
        return;
    }

    if !*children_sorted && children.len() >= 16 {
        children.sort_unstable();
        *children_sorted = true;
    }

    let component_exists = if !*children_sorted {
        children
            .iter()
            .any(|existing: &Vec<u8>| existing.as_slice() == next_component)
    } else {
        children
            .binary_search_by(|existing| existing.as_slice().cmp(next_component))
            .is_ok()
    };

    if !component_exists {
        let component_vec = next_component.to_vec();
        if !*children_sorted {
            children.push(component_vec);
        } else if let Err(pos) =
            children.binary_search_by(|existing| existing.as_slice().cmp(component_vec.as_slice()))
        {
            children.insert(pos, component_vec);
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn match_pattern<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    patterns: Vec<(Binary, Binary)>,
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Err(error_msg) = db_handle.sync_overlay_generation() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    if let Some(error_msg) = current_fatal_error(&db_handle) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }

    if patterns.is_empty() {
        return Ok(atoms::not_found().encode(env));
    }

    let patterns_vec: Vec<(&[u8], &[u8])> = patterns
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();

    if (db_handle.ops.dirty_len)(&db_handle) != 0 {
        ensure_worker(&db_handle);
        if let Err(error_msg) = flush_sync(&db_handle) {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }

    let (live_env, live_db) = match db_handle.fast_get_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::transaction_error(),
                "Failed to begin read transaction".to_string(),
            )
                .encode(env));
        }
    };

    let mut cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((
                atoms::error(),
                atoms::database_error(),
                "Failed to open cursor".to_string(),
            )
                .encode(env));
        }
    };

    const MAX_RESULTS: usize = 100000;
    let anchor_idx = choose_anchor_pattern(&patterns_vec);
    let (anchor_key, anchor_value) = patterns_vec[anchor_idx];
    let mut candidate_ids: Vec<Vec<u8>> = Vec::new();

    for (key_bytes, value_bytes) in cursor.iter_start() {
        if value_bytes != anchor_value {
            continue;
        }
        let (id, suffix) = split_id_suffix(key_bytes);
        if suffix == anchor_key {
            let duplicate_tail = match candidate_ids.last() {
                Some(prev) => prev.as_slice() == id,
                None => false,
            };
            if !duplicate_tail {
                candidate_ids.push(id.to_vec());
            }
        }
    }

    if candidate_ids.is_empty() {
        return Ok(atoms::not_found().encode(env));
    }

    let mut matching_ids: Vec<Vec<u8>> = Vec::new();
    'candidate: for id in candidate_ids {
        for (pattern_key, pattern_value) in patterns_vec.iter() {
            let full_key = compose_match_key(&id, pattern_key);
            match txn.get(live_db, &full_key) {
                Ok(value) if value == *pattern_value => {}
                _ => continue 'candidate,
            }
        }
        matching_ids.push(id);
        if matching_ids.len() >= MAX_RESULTS {
            break;
        }
    }

    if matching_ids.is_empty() {
        Ok(atoms::not_found().encode(env))
    } else {
        let mut result_binaries = Vec::with_capacity(matching_ids.len());
        for id in matching_ids {
            let mut binary = OwnedBinary::new(id.len()).ok_or(Error::BadArg)?;
            binary.as_mut_slice().copy_from_slice(&id);
            result_binaries.push(binary.release(env));
        }

        Ok((atoms::ok(), result_binaries).encode(env))
    }
}

fn choose_anchor_pattern(patterns: &[(&[u8], &[u8])]) -> usize {
    patterns
        .iter()
        .enumerate()
        .max_by_key(|(_, (key, value))| key.len().saturating_add(value.len()))
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

fn split_id_suffix(full_key: &[u8]) -> (&[u8], &[u8]) {
    if let Some(pos) = full_key.iter().rposition(|&b| b == b'/') {
        (&full_key[..pos], &full_key[pos + 1..])
    } else {
        (full_key, &[])
    }
}

fn compose_match_key(id: &[u8], suffix: &[u8]) -> Vec<u8> {
    if suffix.is_empty() {
        return id.to_vec();
    }
    let mut key = Vec::with_capacity(id.len().saturating_add(1).saturating_add(suffix.len()));
    key.extend_from_slice(id);
    key.push(b'/');
    key.extend_from_slice(suffix);
    key
}

///===================================================================
/// Helper Functions
///===================================================================
fn encode_binary<'a>(env: Env<'a>, bytes: &[u8]) -> NifResult<Term<'a>> {
    let mut binary = OwnedBinary::new(bytes.len()).ok_or(Error::BadArg)?;
    binary.as_mut_slice().copy_from_slice(bytes);
    Ok(binary.release(env).encode(env))
}

fn begin_get_metric() -> Option<u64> {
    LOCAL_GET_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        metrics.ops_since_check += 1;
        if metrics.ops_since_check & METRICS_CHECK_MASK == 0 {
            Some(GET_METRIC_CLOCK.raw())
        } else {
            None
        }
    })
}

fn record_get_metric(kind: GetMetricKind, started_at: Option<u64>) {
    LOCAL_GET_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        match kind {
            GetMetricKind::CacheHit => metrics.cache_hits += 1,
            GetMetricKind::LmdbHit => metrics.lmdb_hits += 1,
            GetMetricKind::Miss => metrics.misses += 1,
        }

        if let Some(start_raw) = started_at {
            let end_raw = GET_METRIC_CLOCK.raw();
            let elapsed_ns = GET_METRIC_CLOCK.delta_as_nanos(start_raw, end_raw);
            let bucket = latency_bucket(elapsed_ns);
            metrics.latency_histogram[bucket] += 1;
        }

        if metrics.ops_since_check & METRICS_CHECK_MASK != 0
            || metrics.last_flush.elapsed() < METRICS_FLUSH_INTERVAL
        {
            return;
        }

        let cache_hits = std::mem::take(&mut metrics.cache_hits);
        let lmdb_hits = std::mem::take(&mut metrics.lmdb_hits);
        let misses = std::mem::take(&mut metrics.misses);
        let latency_histogram = std::mem::take(&mut metrics.latency_histogram);
        metrics.last_flush = StdInstant::now();

        if cache_hits != 0 {
            GET_CACHE_HIT_COUNT.fetch_add(cache_hits, Ordering::Relaxed);
        }
        if lmdb_hits != 0 {
            GET_LMDB_HIT_COUNT.fetch_add(lmdb_hits, Ordering::Relaxed);
        }
        if misses != 0 {
            GET_MISS_COUNT.fetch_add(misses, Ordering::Relaxed);
        }
        for (idx, count) in latency_histogram.into_iter().enumerate() {
            if count != 0 {
                GET_LATENCY_HISTOGRAM[idx].fetch_add(count, Ordering::Relaxed);
            }
        }
    });
}

fn latency_bucket(elapsed_ns: u64) -> usize {
    if elapsed_ns == 0 {
        0
    } else {
        let bucket = (u64::BITS - 1 - elapsed_ns.leading_zeros()) as usize;
        bucket.min(GET_LATENCY_BUCKETS - 1)
    }
}

fn encode_iterator_start<'a>(env: Env<'a>) -> Term<'a> {
    (atoms::iterator(), atoms::start()).encode(env)
}

fn encode_iterator_after_key<'a>(env: Env<'a>, key: &[u8]) -> NifResult<Term<'a>> {
    Ok((atoms::iterator(), encode_binary(env, key)?).encode(env))
}

fn decode_iterator_cursor(cursor_term: Term) -> Result<IteratorCursor, String> {
    let (tag, payload): (rustler::Atom, Term) = cursor_term
        .decode()
        .map_err(|_| "Invalid iterator cursor format".to_string())?;

    if tag != atoms::iterator() {
        return Err("Invalid iterator cursor tag".to_string());
    }

    if let Ok(atom_payload) = payload.decode::<rustler::Atom>() {
        if atom_payload == atoms::start() {
            return Ok(IteratorCursor::Start);
        }
    }

    if let Ok(binary_payload) = payload.decode::<Binary>() {
        return Ok(IteratorCursor::AfterKey(binary_payload.as_slice().to_vec()));
    }

    Err("Invalid iterator cursor payload".to_string())
}

fn lmdb_error_to_atom(error: lmdb::Error) -> rustler::Atom {
    match error {
        lmdb::Error::KeyExist => atoms::key_exist(),
        lmdb::Error::NotFound => atoms::not_found(),
        lmdb::Error::PageNotFound => atoms::page_not_found(),
        lmdb::Error::Corrupted => atoms::corrupted(),
        lmdb::Error::Panic => atoms::panic(),
        lmdb::Error::VersionMismatch => atoms::version_mismatch(),
        lmdb::Error::Invalid => atoms::invalid(),
        lmdb::Error::MapFull => atoms::map_full(),
        lmdb::Error::DbsFull => atoms::dbs_full(),
        lmdb::Error::ReadersFull => atoms::readers_full(),
        lmdb::Error::TlsFull => atoms::tls_full(),
        lmdb::Error::TxnFull => atoms::txn_full(),
        lmdb::Error::CursorFull => atoms::cursor_full(),
        lmdb::Error::PageFull => atoms::page_full(),
        lmdb::Error::MapResized => atoms::map_resized(),
        lmdb::Error::Incompatible => atoms::incompatible(),
        lmdb::Error::BadRslot => atoms::bad_rslot(),
        lmdb::Error::BadTxn => atoms::bad_txn(),
        lmdb::Error::BadValSize => atoms::bad_val_size(),
        lmdb::Error::BadDbi => atoms::bad_dbi(),
        lmdb::Error::Other(28) => atoms::no_space(),
        lmdb::Error::Other(_) => atoms::io_error(),
    }
}

fn parse_env_options(options: Vec<Term>) -> NifResult<EnvOptions> {
    let mut env_opts = EnvOptions::default();

    for option in options {
        if let Ok((atom, value)) = option.decode::<(rustler::Atom, Term)>() {
            if atom == atoms::map_size() {
                if let Ok(size) = value.decode::<u64>() {
                    env_opts.map_size = Some(size);
                }
            } else if atom == atoms::max_readers() {
                if let Ok(readers) = value.decode::<u32>() {
                    env_opts.max_readers = Some(readers);
                }
            } else if atom == atoms::batch_size() {
                if let Ok(size) = value.decode::<u64>() {
                    if size > 0 && size <= usize::MAX as u64 {
                        env_opts.batch_size = Some(size as usize);
                    }
                }
            } else if atom == atoms::lru_size() {
                // Accepted for backward compatibility, currently ignored.
                let _ = value.decode::<u64>();
            }
        } else if let Ok(atom) = option.decode::<rustler::Atom>() {
            if atom == atoms::no_mem_init() {
                env_opts.no_mem_init = true;
            } else if atom == atoms::no_sync() {
                env_opts.no_sync = true;
            } else if atom == atoms::no_lock() {
                env_opts.no_lock = true;
            } else if atom == atoms::read_only() {
                env_opts.read_only = true;
            } else if atom == atoms::write_map() {
                env_opts.write_map = true;
            } else if atom == atoms::no_readahead() {
                env_opts.no_readahead = true;
            }
        }
    }

    Ok(env_opts)
}

fn parse_db_options(options: Vec<Term>) -> NifResult<DbOptions> {
    let mut db_opts = DbOptions::default();

    for option in options {
        if let Ok(atom) = option.decode::<rustler::Atom>() {
            if atom == atoms::create() {
                db_opts.create = true;
            }
        }
    }

    Ok(db_opts)
}

#[derive(Debug, Default, Clone)]
struct EnvOptions {
    map_size: Option<u64>,
    max_readers: Option<u32>,
    batch_size: Option<usize>,
    no_mem_init: bool,
    no_sync: bool,
    no_lock: bool,
    read_only: bool,
    write_map: bool,
    no_readahead: bool,
}

#[derive(Default)]
struct DbOptions {
    create: bool,
}

///===================================================================
/// Debug/Status Operations
///===================================================================

#[rustler::nif]
fn env_status<'a>(env: Env<'a>, env_handle: ResourceArc<LmdbEnv>) -> NifResult<Term<'a>> {
    let closed = env_handle.is_closed().map_err(|_| Error::BadArg)?;

    let ref_count = {
        let ref_count = env_handle.ref_count.lock().map_err(|_| Error::BadArg)?;
        *ref_count
    };

    Ok((atoms::ok(), closed, ref_count, env_handle.path.clone()).encode(env))
}

rustler::init!("elmdb", load = init);

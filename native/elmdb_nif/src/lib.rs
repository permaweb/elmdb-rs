//! elmdb_nif - High-performance LMDB bindings for Erlang via Rust NIF
//!
//! This module implements a Native Implemented Function (NIF) that provides
//! Erlang/Elixir applications with access to LMDB (Lightning Memory-Mapped Database).
//!
//! # Architecture
//!
//! The implementation uses a two-layer architecture:
//! - **Resource Management**: Environments and databases are managed as Erlang resources
//! - **Write Optimization**: Batches small writes into single transactions for performance
//!
//! # Safety
//!
//! - All LMDB operations are wrapped in safe Rust abstractions
//! - Resources are automatically cleaned up when no longer referenced
//! - Thread-safe through Arc<Mutex<>> wrappers
//! - Prevents use-after-close errors through validation checks
//!
//! # Performance
//!
//! Key optimizations include:
//! - Write batching to reduce transaction overhead
//! - Zero-copy reads through memory mapping
//! - Efficient cursor iteration for list operations
//! - Early termination for prefix searches

use rustler::{Env, Term, NifResult, Error, Encoder, ResourceArc};
use rustler::types::binary::Binary;
use rustler::types::binary::OwnedBinary;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::path::Path;
use lmdb::{Environment, EnvironmentFlags, Database, DatabaseFlags, Transaction, WriteFlags, Cursor};

// LMDB cursor operation constants (instead of importing lmdb-sys only for constants from lmdb_sys::ffi).
// To be improved in the future.
const MDB_FIRST: u32 = 0;
const MDB_NEXT: u32 = 8;
const MDB_SET_RANGE: u32 = 17;

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
        write_map,
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
        too_many_open_files,
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

/// Write operation to be batched
#[derive(Debug, Clone)]
struct WriteOperation {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Buffer for accumulating write operations before committing
/// 
/// This buffer improves write performance by batching multiple small
/// writes into a single LMDB transaction, reducing overhead significantly.
#[derive(Debug)]
struct WriteBuffer {
    operations: VecDeque<WriteOperation>,
    max_size: usize,
}

impl WriteBuffer {
    /// Create a new write buffer with specified capacity
    fn new(max_size: usize) -> Self {
        Self {
            operations: VecDeque::new(),
            max_size,
        }
    }

    /// Check if buffer has pending operations
    fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Drain all operations from the buffer
    fn drain(&mut self) -> Vec<WriteOperation> {
        self.operations.drain(..).collect()
    }

    /// Check if buffer has reached capacity and should be flushed
    fn should_flush(&self) -> bool {
        self.operations.len() >= self.max_size
    }

    /// Add operation without checking capacity
    fn add_without_check(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push_back(WriteOperation { key, value });
    }
}

/// LMDB Environment resource
/// 
/// Represents an LMDB environment that can contain multiple databases.
/// Environments are reference-counted and shared across database instances.
#[derive(Debug)]
pub struct LmdbEnv {
    /// Path to the database directory
    path: String,
    /// Environment options used when reopening
    options: Arc<Mutex<EnvOptions>>,
    /// Mutable runtime environment state
    state: Arc<Mutex<EnvState>>,
    /// Reference count for active databases using this environment
    ref_count: Arc<Mutex<usize>>,
}

/// LMDB Database resource
/// 
/// Represents a database within an LMDB environment.
/// Each database has its own write buffer for batching operations.
pub struct LmdbDatabase {
    /// Reference to the parent environment
    env: ResourceArc<LmdbEnv>,
    /// Buffer for batching write operations
    write_buffer: Arc<Mutex<WriteBuffer>>,
    /// Cached db handle for current env generation
    cached_db: Arc<Mutex<Option<(Database, u64)>>>,
    /// Whether db_open was requested with create
    create_if_missing: Arc<Mutex<bool>>,
    /// Flag indicating if database has been closed
    closed: Arc<Mutex<bool>>,
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
}

/// Initialize the NIF module
/// 
/// Registers resource types with the Erlang runtime.
/// This function is called automatically when the NIF is loaded.
fn init(env: Env, _info: Term) -> bool {
    rustler::resource!(LmdbEnv, env) && rustler::resource!(LmdbDatabase, env)
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
    if options.write_map {
        flags |= EnvironmentFlags::WRITE_MAP;
    }
    env_builder.set_flags(flags);

    env_builder.open(Path::new(path))
}

impl LmdbEnv {
    fn set_options(&self, options: EnvOptions) -> Result<(), String> {
        let mut stored = self
            .options
            .lock()
            .map_err(|_| "Failed to lock environment options".to_string())?;
        *stored = options;
        Ok(())
    }

    fn write_buffer_size(&self) -> Result<usize, String> {
        let options = self
            .options
            .lock()
            .map_err(|_| "Failed to lock environment options".to_string())?;
        Ok(options.batch_size.unwrap_or(1000))
    }

    fn ensure_open(&self) -> Result<(Arc<Environment>, u64), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Failed to lock environment state".to_string())?;

        if let Some(existing_env) = state.env.as_ref() {
            if state.close_requested {
                if Arc::strong_count(existing_env) == 1 {
                    state.env = None;
                    state.close_requested = false;
                    state.generation += 1;
                } else {
                    return Ok((existing_env.clone(), state.generation));
                }
            } else {
                return Ok((existing_env.clone(), state.generation));
            }
        }

        let options = self
            .options
            .lock()
            .map_err(|_| "Failed to lock environment options".to_string())?
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
            .lock()
            .map_err(|_| "Failed to lock environment state".to_string())?;

        state.close_requested = true;
        if let Some(existing_env) = state.env.as_ref() {
            if Arc::strong_count(existing_env) == 1 {
                state.env = None;
                state.close_requested = false;
                state.generation += 1;
            }
        }

        Ok(())
    }

    fn is_closed(&self) -> Result<bool, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "Failed to lock environment state".to_string())?;
        Ok(state.env.is_none() || state.close_requested)
    }
}

fn soft_close_db(db_handle: &ResourceArc<LmdbDatabase>) -> Result<(), String> {
    if let Err(error_msg) = db_handle.force_flush_buffer() {
        // Close remains best-effort; keep old behavior of not failing close on flush errors.
        eprintln!("Warning: Failed to flush buffer during db_close: {}", error_msg);
    }

    let was_open = {
        let mut closed = db_handle
            .closed
            .lock()
            .map_err(|_| "Failed to lock database state".to_string())?;
        let was_open = !*closed;
        *closed = true;
        was_open
    };

    {
        let mut cached = db_handle
            .cached_db
            .lock()
            .map_err(|_| "Failed to lock cached db handle".to_string())?;
        *cached = None;
    }

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

    Ok(())
}

///===================================================================
/// Environment Management
///===================================================================

#[rustler::nif]
fn env_open<'a>(env: Env<'a>, path: Term<'a>, options: Vec<Term<'a>>) -> NifResult<Term<'a>> {
    let path_string = if let Ok(binary) = path.decode::<Binary>() {
        std::str::from_utf8(&binary).map_err(|_| Error::BadArg)?.to_string()
    } else if let Ok(string) = path.decode::<String>() {
        string
    } else if let Ok(chars) = path.decode::<Vec<u8>>() {
        // Handle Erlang strings (lists of integers)
        std::str::from_utf8(&chars).map_err(|_| Error::BadArg)?.to_string()
    } else {
        return Err(Error::BadArg);
    };
    let path_str = &path_string;
    let has_options = !options.is_empty();
    let parsed_options = parse_env_options(options)?;

    // Singleton behavior: always return the existing env resource for this path.
    if let Some(existing_env) = {
        let environments = ENVIRONMENTS.lock().unwrap();
        environments.get(path_str).cloned()
    } {
        if !Path::new(path_str).exists() {
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
            // Check if the directory itself exists
            let path = Path::new(path_str);
            
            let error_atom = if !path.exists() {
                atoms::directory_not_found()
            } else if path.is_file() {
                // Path exists but is a file, not a directory
                atoms::invalid_path()
            } else {
                // Directory exists, check permissions
                match std::fs::File::create(path.join(".lmdb_test")) {
                    Ok(_) => {
                        // Clean up test file
                        let _ = std::fs::remove_file(path.join(".lmdb_test"));
                        // Permission is OK, surface the specific LMDB error atom.
                        lmdb_error_to_atom(e)
                    },
                    Err(io_err) => {
                        match io_err.kind() {
                            std::io::ErrorKind::PermissionDenied => atoms::permission_denied(),
                            _ => atoms::environment_error()
                        }
                    }
                }
            };
            
            return Ok((atoms::error(), error_atom).encode(env));
        }
    };
    
    // Create our wrapper struct
    let lmdb_env = LmdbEnv { 
        path: path_str.to_string(),
        options: Arc::new(Mutex::new(parsed_options.clone())),
        state: Arc::new(Mutex::new(EnvState {
            env: Some(Arc::new(lmdb_environment)),
            close_requested: false,
            generation: 1,
        })),
        ref_count: Arc::new(Mutex::new(0)),
    };
    let resource = ResourceArc::new(lmdb_env);
    
    // Store in global environments map
    {
        let mut environments = ENVIRONMENTS.lock().unwrap();
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
        Err(err_msg) => Ok((atoms::error(), atoms::environment_error(), 
                      format!("Environment sync failed: {}", err_msg)).encode(env))
    }
}

#[rustler::nif]
fn env_close<'a>(env: Env<'a>, env_handle: ResourceArc<LmdbEnv>) -> NifResult<Term<'a>> {
    if let Some(db_handle) = {
        let databases = DATABASES.lock().unwrap();
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
        std::str::from_utf8(&binary).map_err(|_| Error::BadArg)?.to_string()
    } else if let Ok(string) = path.decode::<String>() {
        string
    } else if let Ok(chars) = path.decode::<Vec<u8>>() {
        // Handle Erlang strings (lists of integers)
        std::str::from_utf8(&chars).map_err(|_| Error::BadArg)?.to_string()
    } else {
        return Err(Error::BadArg);
    };
    let path_str = &path_string;
    
    let env_handle = {
        let environments = ENVIRONMENTS.lock().unwrap();
        environments.get(path_str).cloned()
    };

    if let Some(db_handle) = {
        let databases = DATABASES.lock().unwrap();
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
fn db_close<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>
) -> NifResult<Term<'a>> {
    match soft_close_db(&db_handle) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error_msg) => Ok((atoms::error(), atoms::database_error(), error_msg).encode(env)),
    }
}

#[rustler::nif]
fn db_open<'a>(
    env: Env<'a>, 
    env_handle: ResourceArc<LmdbEnv>, 
    options: Vec<Term<'a>>
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

    let db_key = env_handle.path.clone();
    if let Some(existing_db) = {
        let databases = DATABASES.lock().map_err(|_| Error::BadArg)?;
        databases.get(&db_key).cloned()
    } {
        if parsed_options.create {
            if let Err(error_msg) = existing_db.set_create_if_missing(true) {
                return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
            }
        }
        if let Err(error_msg) = existing_db.set_batch_size(batch_size) {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
        if let Err(error_msg) = existing_db.reopen_if_closed() {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
        if let Err(error_msg) = existing_db.validate_database() {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
        return Ok((atoms::ok(), existing_db).encode(env));
    }
    
    // Increment reference count for this environment
    {
        let mut ref_count = env_handle.ref_count.lock().map_err(|_| Error::BadArg)?;
        *ref_count += 1;
    }
    
    // Create database resource with write buffer (default buffer size: 1000 operations)
    let lmdb_db = LmdbDatabase { 
        env: env_handle.clone(),
        write_buffer: Arc::new(Mutex::new(WriteBuffer::new(batch_size))),
        cached_db: Arc::new(Mutex::new(None)),
        create_if_missing: Arc::new(Mutex::new(parsed_options.create)),
        closed: Arc::new(Mutex::new(false)),
    };
    let resource = ResourceArc::new(lmdb_db);

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

///===================================================================
/// Write Buffer Management - Transaction Batching Optimization
///
/// This implementation provides significant performance improvements for many small writes
/// by batching them into single transactions, similar to the C NIF approach:
///
/// Key optimizations:
/// 1. Accumulates writes in a buffer (default: 1000 operations)
/// 2. Creates a single transaction per batch instead of per write
/// 3. Only commits when:
///    - Buffer reaches capacity
///    - Read operation is performed (get/list)
///    - Explicit flush is called
///    - Database is being closed
/// 4. Dramatically reduces transaction overhead and lock contention
/// 5. Maintains consistency by flushing before reads
///
/// Performance benefits:
/// - Fewer transaction creates/commits (major LMDB overhead)
/// - Reduced write lock contention
/// - Better throughput when mixing reads and writes
/// - Maintains ACID properties
///===================================================================

impl LmdbDatabase {
    // Transaction batching optimization - keeps writes in buffer until:
    // 1. Buffer reaches capacity (1000 operations by default)
    // 2. A read operation is performed (get/list)
    // 3. Explicit flush is called
    // 4. Database is being closed
    // This dramatically improves write performance by reducing transaction overhead
    
    fn set_create_if_missing(&self, create: bool) -> Result<(), String> {
        if !create {
            return Ok(());
        }

        let mut create_if_missing = self
            .create_if_missing
            .lock()
            .map_err(|_| "Failed to lock database options".to_string())?;
        *create_if_missing = true;
        Ok(())
    }

    fn set_batch_size(&self, batch_size: usize) -> Result<(), String> {
        if batch_size == 0 {
            return Err("Batch size must be greater than 0".to_string());
        }

        let mut write_buffer = self
            .write_buffer
            .lock()
            .map_err(|_| "Failed to lock write buffer".to_string())?;
        write_buffer.max_size = batch_size;
        Ok(())
    }

    fn reopen_if_closed(&self) -> Result<(), String> {
        let reopened = {
            let mut closed = self
                .closed
                .lock()
                .map_err(|_| "Failed to lock database state")?;
            if *closed {
                *closed = false;
                true
            } else {
                false
            }
        };

        if reopened {
            let mut ref_count = self
                .env
                .ref_count
                .lock()
                .map_err(|_| "Failed to update environment reference count")?;
            *ref_count += 1;
        }
        Ok(())
    }

    fn ensure_open_handles(&self) -> Result<(Arc<Environment>, Database), String> {
        let (live_env, env_generation) = self.env.ensure_open()?;
        self.reopen_if_closed()?;

        if let Some((cached_db, cached_generation)) = {
            let cached = self
                .cached_db
                .lock()
                .map_err(|_| "Failed to lock cached db handle".to_string())?;
            *cached
        } {
            if cached_generation == env_generation {
                return Ok((live_env, cached_db));
            }
        }

        let create_if_missing = {
            let create = self
                .create_if_missing
                .lock()
                .map_err(|_| "Failed to lock database options".to_string())?;
            *create
        };

        let db = if create_if_missing {
            live_env.create_db(None, DatabaseFlags::empty())
        } else {
            match live_env.create_db(None, DatabaseFlags::empty()) {
                Ok(db) => Ok(db),
                Err(_) => live_env.open_db(None),
            }
        }
        .map_err(|e| format!("Failed to open database: {:?}", e))?;

        {
            let mut cached = self
                .cached_db
                .lock()
                .map_err(|_| "Failed to lock cached db handle".to_string())?;
            *cached = Some((db, env_generation));
        }

        Ok((live_env, db))
    }


    // New method for immediate batched write without buffering
    fn write_immediate_batch(&self, operations: Vec<WriteOperation>) -> Result<(), String> {
        if operations.is_empty() {
            return Ok(());
        }

        let (live_env, live_db) = self.ensure_open_handles()?;

        // Create a write transaction for the batch
        let mut txn = live_env.begin_rw_txn()
            .map_err(|_| "Failed to begin write transaction")?;
        
        // Execute all operations in the batch
        for op in operations {
            txn.put(live_db, &op.key, &op.value, WriteFlags::empty())
                .map_err(|e| format!("Failed to put value: key_len={}, value_len={}, error={:?}", 
                                     op.key.len(), op.value.len(), e))?;
        }
        
        // Commit the transaction
        txn.commit()
            .map_err(|_| "Failed to commit batch transaction")?;
        
        Ok(())
    }

    // Optimized method that tries to batch multiple puts in a single transaction
    fn put_with_batching(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        // First, add to buffer
        let (should_batch_immediately, current_ops) = {
            let mut buffer = self.write_buffer.lock().map_err(|_| "Failed to lock write buffer")?;
            buffer.add_without_check(key, value);
            
            // If buffer is full, drain it for immediate processing
            if buffer.should_flush() {
                let ops = buffer.drain();
                (true, ops)
            } else {
                // Buffer has space, keep accumulating
                (false, Vec::new())
            }
        };

        if should_batch_immediately {
            self.write_immediate_batch(current_ops)?;
        }

        Ok(())
    }

    // Force flush any pending writes - used before reads and on close
    fn force_flush_buffer(&self) -> Result<(), String> {
        let pending_ops = {
            let mut buffer = self.write_buffer.lock().map_err(|_| "Failed to lock write buffer")?;
            if buffer.is_empty() {
                return Ok(());
            }
            buffer.drain()
        };

        self.write_immediate_batch(pending_ops)
    }

    // Check if buffer has pending writes without flushing
    fn has_pending_writes(&self) -> bool {
        if let Ok(buffer) = self.write_buffer.lock() {
            !buffer.is_empty()
        } else {
            false
        }
    }

    fn validate_database(&self) -> Result<(), String> {
        self.reopen_if_closed()?;
        let _ = self.env.ensure_open()?;
        Ok(())
    }
}

impl Drop for LmdbDatabase {
    fn drop(&mut self) {
        // Check if database was already explicitly closed
        let already_closed = {
            if let Ok(closed) = self.closed.lock() {
                *closed
            } else {
                false
            }
        };
        
        // Only decrement reference count if not already closed
        // (explicit close already decremented it)
        if !already_closed {
            // Attempt to flush any remaining buffered writes when the database is dropped
            // We ignore errors here since we can't handle them in Drop
            let _ = self.force_flush_buffer();
            
            // Decrement reference count for the environment
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
    value: Binary
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    let key_vec = key.as_slice().to_vec();
    let value_vec = value.as_slice().to_vec();
    
    // Handle empty keys directly (don't buffer them as they cause issues in LMDB batch operations)
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
                return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin write transaction".to_string()).encode(env));
            }
        };
        
        match txn.put(live_db, &key_vec, &value_vec, WriteFlags::empty()) {
            Ok(()) => {
                match txn.commit() {
                    Ok(()) => return Ok(atoms::ok().encode(env)),
                    Err(_) => return Ok((atoms::error(), atoms::transaction_error(), "Failed to commit transaction".to_string()).encode(env))
                }
            },
            Err(lmdb_err) => {
                let error_msg = match lmdb_err {
                    lmdb::Error::BadValSize => "Empty key not supported".to_string(),
                    _ => format!("Failed to put value: {:?}", lmdb_err)
                };
                return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
            }
        }
    }
    
    // Add to write buffer for non-empty keys using new batching logic
    if let Err(error_msg) = db_handle.put_with_batching(key_vec, value_vec) {
        return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
    }
    
    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn put_batch<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key_value_pairs: Vec<(Binary, Binary)>
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    
    if key_value_pairs.is_empty() {
        return Ok(atoms::ok().encode(env));
    }

    // Preserve ordering when mixed with buffered put/3 calls.
    if db_handle.has_pending_writes() {
        if let Err(error_msg) = db_handle.force_flush_buffer() {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }
    
    let (live_env, live_db) = match db_handle.ensure_open_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    // Create a write transaction for the entire batch
    let mut txn = match live_env.begin_rw_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin write transaction".to_string()).encode(env));
        }
    };
    
    let mut success_count = 0;
    let mut errors = Vec::new();
    
    // Process all key-value pairs in a single transaction
    for (i, (key, value)) in key_value_pairs.iter().enumerate() {
        let key_bytes = key.as_slice();
        let value_bytes = value.as_slice();
        
        match txn.put(live_db, &key_bytes, &value_bytes, WriteFlags::empty()) {
            Ok(()) => {
                success_count += 1;
            },
            Err(lmdb_err) => {
                let error_detail = match lmdb_err {
                    lmdb::Error::KeyExist => "Key already exists".to_string(),
                    lmdb::Error::MapFull => "Database is full".to_string(),
                    lmdb::Error::TxnFull => "Transaction is full".to_string(),
                    _ => "Failed to put value".to_string()
                };
                errors.push((i, error_detail));
            }
        }
    }
    
    // Commit the transaction
    match txn.commit() {
        Ok(()) => {
            if errors.is_empty() {
                Ok(atoms::ok().encode(env))
            } else {
                Ok((atoms::ok(), success_count, errors).encode(env))
            }
        },
        Err(_) => {
            Ok((atoms::error(), atoms::transaction_error(), "Failed to commit batch transaction".to_string()).encode(env))
        }
    }
}

#[rustler::nif]
fn get<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key: Binary
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    
    let key_bytes = key.as_slice();
    
    // Only flush write buffer if there are pending writes
    if db_handle.has_pending_writes() {
        if let Err(error_msg) = db_handle.force_flush_buffer() {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }
    
    let (live_env, live_db) = match db_handle.ensure_open_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    // Create a read-only transaction
    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin read transaction".to_string()).encode(env));
        }
    };
    
    // Get the value for the key
    let result = txn.get(live_db, &key_bytes);
    
    match result {
        Ok(value_bytes) => {
            // Convert the value to a binary that Erlang can use
            let mut binary = rustler::types::binary::OwnedBinary::new(value_bytes.len()).unwrap();
            binary.as_mut_slice().copy_from_slice(value_bytes);
            Ok((atoms::ok(), binary.release(env)).encode(env))
        },
        Err(lmdb::Error::NotFound) => {
            Ok(atoms::not_found().encode(env))
        },
        Err(_) => {
            Ok((atoms::error(), atoms::database_error(), "Failed to get value".to_string()).encode(env))
        }
    }
}

///===================================================================
/// Iterator Operations
///===================================================================

#[rustler::nif]
fn iterator<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>
) -> NifResult<Term<'a>> {
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    Ok(encode_iterator_start(env))
}

#[rustler::nif]
fn iterator_next<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    cursor_term: Term<'a>
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }

    // Decode the stateless cursor token.
    let cursor_token = match decode_iterator_cursor(cursor_term) {
        Ok(token) => token,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::invalid(), error_msg).encode(env));
        }
    };

    // Only flush write buffer if there are pending writes.
    // Iterator reads should observe all writes done before this call.
    if db_handle.has_pending_writes() {
        if let Err(error_msg) = db_handle.force_flush_buffer() {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }

    let (live_env, live_db) = match db_handle.ensure_open_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };

    // Create a read-only transaction
    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin read transaction".to_string()).encode(env));
        }
    };

    // Open a cursor for the database
    let cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((atoms::error(), atoms::database_error(), "Failed to open cursor".to_string()).encode(env));
        }
    };

    let next_entry = match cursor_token {
        IteratorCursor::Start => {
            match cursor.get(None, None, MDB_FIRST) {
                Ok((Some(key), value)) => Some((key.to_vec(), value.to_vec())),
                Ok((None, _)) => None,
                Err(lmdb::Error::NotFound) => None,
                Err(_) => {
                    return Ok((atoms::error(), atoms::database_error(), "Failed to read first cursor entry".to_string()).encode(env));
                }
            }
        }
        IteratorCursor::AfterKey(last_key) => {
            let positioned_entry = match cursor.get(Some(last_key.as_slice()), None, MDB_SET_RANGE) {
                Ok((Some(key), value)) => Some((key.to_vec(), value.to_vec())),
                Ok((None, _)) => None,
                Err(lmdb::Error::NotFound) => None,
                Err(_) => {
                    return Ok((atoms::error(), atoms::database_error(), "Failed to position iterator cursor".to_string()).encode(env));
                }
            };

            match positioned_entry {
                // If LMDB positioned on the same key, advance once so semantics stay "after cursor".
                Some((key, _value)) if key == last_key => {
                    match cursor.get(None, None, MDB_NEXT) {
                        Ok((Some(next_key), next_value)) => Some((next_key.to_vec(), next_value.to_vec())),
                        Ok((None, _)) => None,
                        Err(lmdb::Error::NotFound) => None,
                        Err(_) => {
                            return Ok((atoms::error(), atoms::database_error(), "Failed to advance iterator cursor".to_string()).encode(env));
                        }
                    }
                }
                // Key was deleted or moved; return the first lexicographically greater key if present.
                Some((key, value)) => Some((key, value)),
                None => None
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
        None => Ok(atoms::undefined().encode(env))
    }
}

///===================================================================
/// List Operations
///===================================================================

#[rustler::nif]
fn list<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    key_prefix: Binary
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    
    let prefix_bytes = key_prefix.as_slice();
    
    // Only flush write buffer if there are pending writes
    if db_handle.has_pending_writes() {
        if let Err(error_msg) = db_handle.force_flush_buffer() {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }

    let (live_env, live_db) = match db_handle.ensure_open_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };
    
    // Create a read-only transaction
    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin read transaction".to_string()).encode(env));
        }
    };

    // Open a cursor for the database
    let mut cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((atoms::error(), atoms::database_error(), "Failed to open cursor".to_string()).encode(env));
        }
    };
    
    // OPTIMIZATION: Use Vec instead of HashSet for better performance with small collections
    // Pre-allocate with reasonable capacity to avoid reallocations
    let mut children = Vec::with_capacity(64);
    let prefix_len = prefix_bytes.len();
    
    // OPTIMIZATION: Start cursor at prefix position instead of scanning from beginning
    // This dramatically reduces iterations for sparse data
    
    // Safe iteration approach that handles empty databases and missing prefixes
    // First, try to position cursor at prefix using MDB_SET_RANGE to check if key exists
    // First, try to position cursor at prefix using MDB_SET_RANGE to check if key exists
    let cursor_positioned = cursor.get(Some(prefix_bytes), None, MDB_SET_RANGE).is_ok();
    
    if !cursor_positioned {
        // No keys >= prefix exist, return not_found immediately
        return Ok(atoms::not_found().encode(env));
    }
    
    // Keys exist that are >= prefix, now safely use iter_from
    let cursor_iter = cursor.iter_from(prefix_bytes);
    
    // Iterate through keys starting from the prefix
    for (key, _value) in cursor_iter {
        
        // OPTIMIZATION: Early termination - if key doesn't start with prefix and we've already
        // found matches, we can break since keys are sorted
        if !key.starts_with(prefix_bytes) {
            // Since keys are sorted, if this key doesn't match our prefix,
            // no subsequent keys will match either
            break;
        }
        
        // Extract the next path component after the prefix
        let remaining = &key[prefix_len..];
        
        // Skip if there's no remaining path (exact match with prefix)
        if remaining.is_empty() {
            continue;
        }
        
        // OPTIMIZATION: Find separator using unsafe slice operation for better performance
        let next_component = if let Some(sep_pos) = remaining.iter().position(|&b| b == b'/') {
            &remaining[..sep_pos]
        } else {
            remaining
        };
        
        // Only process non-empty components
        if next_component.is_empty() {
            continue;
        }
        
        // OPTIMIZATION: Use binary search for duplicate detection once we have enough items
        // For small collections, linear search is still faster
        let component_exists = if children.len() < 16 {
            children.iter().any(|existing: &Vec<u8>| existing.as_slice() == next_component)
        } else {
            // For larger collections, use binary search on sorted data
            children.binary_search(&next_component.to_vec()).is_ok()
        };
        
        if !component_exists {
            let component_vec = next_component.to_vec();
            if children.len() < 16 {
                children.push(component_vec);
            } else {
                // Insert maintaining sorted order for binary search
                match children.binary_search(&component_vec) {
                    Err(pos) => children.insert(pos, component_vec),
                    Ok(_) => {} // Already exists
                }
            }
        }
    }
    
    if children.is_empty() {
        return Ok(atoms::not_found().encode(env));
    }
    
    // OPTIMIZATION: Pre-allocate result vector and minimize allocations
    let mut result_binaries = Vec::with_capacity(children.len());
    
    // OPTIMIZATION: Sort results only if we didn't maintain sorted order during insertion
    if children.len() >= 16 {
        // Already sorted during insertion via binary search
    } else {
        // Sort small collections
        children.sort_unstable();
    }
    
    for child in children {
        // OPTIMIZATION: Direct binary creation without intermediate copy when possible
        let mut binary = rustler::types::binary::OwnedBinary::new(child.len())
            .ok_or(Error::BadArg)?;
        binary.as_mut_slice().copy_from_slice(&child);
        result_binaries.push(binary.release(env));
    }
    
    Ok((atoms::ok(), result_binaries).encode(env))
}

#[rustler::nif]
fn match_pattern<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>,
    patterns: Vec<(Binary, Binary)>
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    
    // Return not_found if patterns is empty
    if patterns.is_empty() {
        return Ok(atoms::not_found().encode(env));
    }
    
    // Keep patterns as references for efficient comparison
    let patterns_vec: Vec<(&[u8], &[u8])> = patterns
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    
    // Only flush write buffer if there are pending writes
    if db_handle.has_pending_writes() {
        if let Err(error_msg) = db_handle.force_flush_buffer() {
            return Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env));
        }
    }

    let (live_env, live_db) = match db_handle.ensure_open_handles() {
        Ok(handles) => handles,
        Err(error_msg) => {
            return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
        }
    };
    
    // Create a read-only transaction
    let txn = match live_env.begin_ro_txn() {
        Ok(txn) => txn,
        Err(_) => {
            return Ok((atoms::error(), atoms::transaction_error(), "Failed to begin read transaction".to_string()).encode(env));
        }
    };
    
    // Open a cursor for the database
    let mut cursor = match txn.open_ro_cursor(live_db) {
        Ok(cursor) => cursor,
        Err(_) => {
            return Ok((atoms::error(), atoms::database_error(), "Failed to open cursor".to_string()).encode(env));
        }
    };
    
    // Data structures for tracking matches
    const MAX_RESULTS: usize = 100000;  // Reasonable limit to prevent unbounded memory growth
    let mut matching_ids: Vec<Vec<u8>> = Vec::new();
    let mut current_id: Option<Vec<u8>> = None;
    let mut seen_patterns: HashSet<usize> = HashSet::new();
    let total_patterns = patterns_vec.len();
    
    // Iterate through all key-value pairs in the database
    let iter = cursor.iter_start();
    for (key_bytes, value_bytes) in iter {
        // Parse key to extract ID and suffix
        // Find the position of the last '/' to extract the ID and suffix
        let last_slash_pos = key_bytes.iter().rposition(|&b| b == b'/');
        
        let (id, suffix) = if let Some(pos) = last_slash_pos {
            // Has hierarchy - split into ID and suffix
            let id = key_bytes[..pos].to_vec();
            let suffix = key_bytes[pos + 1..].to_vec();
            (id, suffix)
        } else {
            // No hierarchy - the entire key is the ID
            (key_bytes.to_vec(), Vec::new())
        };
        
        // Check if we've moved to a new ID
        if current_id.as_ref() != Some(&id) {
            // Check if previous ID matched all patterns
            if let Some(prev_id) = current_id.take() {
                if seen_patterns.len() == total_patterns {
                    matching_ids.push(prev_id);
                    // Stop if we've reached the maximum number of results
                    if matching_ids.len() >= MAX_RESULTS {
                        break;
                    }
                }
            }
            
            // Reset for new ID
            current_id = Some(id.clone());
            seen_patterns.clear();
        }
        
        // Check if this key-value pair matches any pattern
        for (pattern_idx, (pattern_key, pattern_value)) in patterns_vec.iter().enumerate() {
            // Check if suffix matches pattern key and value matches pattern value
            if suffix.as_slice() == *pattern_key && value_bytes == *pattern_value {
                seen_patterns.insert(pattern_idx);
            }
        }
    }
    
    // Check the final ID
    if let Some(final_id) = current_id {
        if seen_patterns.len() == total_patterns {
            matching_ids.push(final_id);
        }
    }
    
    // Return results
    if matching_ids.is_empty() {
        Ok(atoms::not_found().encode(env))
    } else {
        // Convert matching IDs to Erlang binaries
        let mut result_binaries = Vec::with_capacity(matching_ids.len());
        for id in matching_ids {
            let mut binary = OwnedBinary::new(id.len())
                .ok_or(Error::BadArg)?;
            binary.as_mut_slice().copy_from_slice(&id);
            result_binaries.push(binary.release(env));
        }
        
        Ok((atoms::ok(), result_binaries).encode(env))
    }
}

#[rustler::nif]
fn flush<'a>(
    env: Env<'a>,
    db_handle: ResourceArc<LmdbDatabase>
) -> NifResult<Term<'a>> {
    // Validate database and environment status
    if let Err(error_msg) = db_handle.validate_database() {
        return Ok((atoms::error(), atoms::database_error(), error_msg).encode(env));
    }
    
    match db_handle.force_flush_buffer() {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error_msg) => Ok((atoms::error(), atoms::transaction_error(), error_msg).encode(env))
    }
}

///===================================================================
/// Helper Functions
///===================================================================

fn encode_binary<'a>(env: Env<'a>, bytes: &[u8]) -> NifResult<Term<'a>> {
    let mut binary = OwnedBinary::new(bytes.len())
        .ok_or(Error::BadArg)?;
    binary.as_mut_slice().copy_from_slice(bytes);
    Ok(binary.release(env).encode(env))
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
        lmdb::Error::Other(24) => atoms::too_many_open_files(),
        lmdb::Error::Other(28) => atoms::no_space(),
        lmdb::Error::Other(_) => atoms::io_error(),
    }
}

fn parse_env_options(options: Vec<Term>) -> NifResult<EnvOptions> {
    let mut env_opts = EnvOptions::default();
    
    for option in options {
        if let Ok((atom, value)) = option.decode::<(rustler::Atom, Term)>() {
            let name = format!("{:?}", atom);
            // Remove quotes from debug output
            let name = name.trim_start_matches('"').trim_end_matches('"');
            match name {
                "map_size" => {
                    if let Ok(size) = value.decode::<u64>() {
                        env_opts.map_size = Some(size);
                    }
                }
                "max_readers" => {
                    if let Ok(readers) = value.decode::<u32>() {
                        env_opts.max_readers = Some(readers);
                    }
                }
                "batch_size" => {
                    if let Ok(size) = value.decode::<u64>() {
                        if size > 0 && size <= usize::MAX as u64 {
                            env_opts.batch_size = Some(size as usize);
                        }
                    }
                }
                _ => {} // Ignore unknown options for now
            }
        } else if let Ok(atom) = option.decode::<rustler::Atom>() {
            let name = format!("{:?}", atom);
            // Remove quotes from debug output
            let name = name.trim_start_matches('"').trim_end_matches('"');
            match name {
                "no_mem_init" => env_opts.no_mem_init = true,
                "no_sync" => env_opts.no_sync = true,
                "no_lock" => env_opts.no_lock = true,
                "write_map" => env_opts.write_map = true,
                _ => {} // Ignore unknown options
            }
        }
    }
    
    Ok(env_opts)
}

fn parse_db_options(options: Vec<Term>) -> NifResult<DbOptions> {
    let mut db_opts = DbOptions::default();
    
    for option in options {
        if let Ok(atom) = option.decode::<rustler::Atom>() {
            let name = format!("{:?}", atom);
            // Remove quotes from debug output
            let name = name.trim_start_matches('"').trim_end_matches('"');
            match name {
                "create" => db_opts.create = true,
                _ => {} // Ignore unknown options
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
    write_map: bool,
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

// Initialize the NIF module
// explicit fuctions are deprecated but here is a list
// [env_open, env_close, env_close_by_name, db_open, db_close, put, put_batch, get, list, flush, env_status]
rustler::init!("elmdb", load = init);

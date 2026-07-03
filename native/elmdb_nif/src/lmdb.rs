use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce, Tag};
use libc::{c_int, c_uint, c_void, size_t};
use lmdb_master3_sys as ffi;
use std::ffi::{CStr, CString};
use std::fmt;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;
use std::slice;
use std::sync::Mutex;

const CHACHA20_POLY1305_TAG_SIZE: c_uint = 16;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum Error {
    KeyExist,
    NotFound,
    PageNotFound,
    Corrupted,
    Panic,
    VersionMismatch,
    Invalid,
    MapFull,
    DbsFull,
    ReadersFull,
    TlsFull,
    TxnFull,
    CursorFull,
    PageFull,
    MapResized,
    Incompatible,
    BadRslot,
    BadTxn,
    BadValSize,
    BadDbi,
    CryptoFail,
    EnvEncryption,
    Other(c_int),
}

impl Error {
    pub fn from_err_code(err_code: c_int) -> Error {
        match err_code {
            ffi::MDB_KEYEXIST => Error::KeyExist,
            ffi::MDB_NOTFOUND => Error::NotFound,
            ffi::MDB_PAGE_NOTFOUND => Error::PageNotFound,
            ffi::MDB_CORRUPTED => Error::Corrupted,
            ffi::MDB_PANIC => Error::Panic,
            ffi::MDB_VERSION_MISMATCH => Error::VersionMismatch,
            ffi::MDB_INVALID => Error::Invalid,
            ffi::MDB_MAP_FULL => Error::MapFull,
            ffi::MDB_DBS_FULL => Error::DbsFull,
            ffi::MDB_READERS_FULL => Error::ReadersFull,
            ffi::MDB_TLS_FULL => Error::TlsFull,
            ffi::MDB_TXN_FULL => Error::TxnFull,
            ffi::MDB_CURSOR_FULL => Error::CursorFull,
            ffi::MDB_PAGE_FULL => Error::PageFull,
            ffi::MDB_MAP_RESIZED => Error::MapResized,
            ffi::MDB_INCOMPATIBLE => Error::Incompatible,
            ffi::MDB_BAD_RSLOT => Error::BadRslot,
            ffi::MDB_BAD_TXN => Error::BadTxn,
            ffi::MDB_BAD_VALSIZE => Error::BadValSize,
            ffi::MDB_BAD_DBI => Error::BadDbi,
            ffi::MDB_CRYPTO_FAIL => Error::CryptoFail,
            ffi::MDB_ENV_ENCRYPTION => Error::EnvEncryption,
            other => Error::Other(other),
        }
    }

    pub fn to_err_code(self) -> c_int {
        match self {
            Error::KeyExist => ffi::MDB_KEYEXIST,
            Error::NotFound => ffi::MDB_NOTFOUND,
            Error::PageNotFound => ffi::MDB_PAGE_NOTFOUND,
            Error::Corrupted => ffi::MDB_CORRUPTED,
            Error::Panic => ffi::MDB_PANIC,
            Error::VersionMismatch => ffi::MDB_VERSION_MISMATCH,
            Error::Invalid => ffi::MDB_INVALID,
            Error::MapFull => ffi::MDB_MAP_FULL,
            Error::DbsFull => ffi::MDB_DBS_FULL,
            Error::ReadersFull => ffi::MDB_READERS_FULL,
            Error::TlsFull => ffi::MDB_TLS_FULL,
            Error::TxnFull => ffi::MDB_TXN_FULL,
            Error::CursorFull => ffi::MDB_CURSOR_FULL,
            Error::PageFull => ffi::MDB_PAGE_FULL,
            Error::MapResized => ffi::MDB_MAP_RESIZED,
            Error::Incompatible => ffi::MDB_INCOMPATIBLE,
            Error::BadRslot => ffi::MDB_BAD_RSLOT,
            Error::BadTxn => ffi::MDB_BAD_TXN,
            Error::BadValSize => ffi::MDB_BAD_VALSIZE,
            Error::BadDbi => ffi::MDB_BAD_DBI,
            Error::CryptoFail => ffi::MDB_CRYPTO_FAIL,
            Error::EnvEncryption => ffi::MDB_ENV_ENCRYPTION,
            Error::Other(err_code) => err_code,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            let msg = CStr::from_ptr(ffi::mdb_strerror(self.to_err_code()));
            write!(f, "{}", msg.to_string_lossy())
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

fn lmdb_result(err_code: c_int) -> Result<()> {
    if err_code == ffi::MDB_SUCCESS {
        Ok(())
    } else {
        Err(Error::from_err_code(err_code))
    }
}

bitflags::bitflags! {
    #[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
    pub struct EnvironmentFlags: c_uint {
        const NO_SUB_DIR = ffi::MDB_NOSUBDIR;
        const WRITE_MAP = ffi::MDB_WRITEMAP;
        const READ_ONLY = ffi::MDB_RDONLY;
        const NO_SYNC = ffi::MDB_NOSYNC;
        const NO_LOCK = ffi::MDB_NOLOCK;
        const NO_READAHEAD = ffi::MDB_NORDAHEAD;
        const NO_MEM_INIT = ffi::MDB_NOMEMINIT;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
    pub struct DatabaseFlags: c_uint {
        const REVERSE_KEY = ffi::MDB_REVERSEKEY;
        const DUP_SORT = ffi::MDB_DUPSORT;
        const INTEGER_KEY = ffi::MDB_INTEGERKEY;
        const DUP_FIXED = ffi::MDB_DUPFIXED;
        const INTEGER_DUP = ffi::MDB_INTEGERDUP;
        const REVERSE_DUP = ffi::MDB_REVERSEDUP;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
    pub struct WriteFlags: c_uint {
        const NO_OVERWRITE = ffi::MDB_NOOVERWRITE;
        const NO_DUP_DATA = ffi::MDB_NODUPDATA;
        const CURRENT = ffi::MDB_CURRENT;
        const APPEND = ffi::MDB_APPEND;
        const APPEND_DUP = ffi::MDB_APPENDDUP;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Database {
    dbi: ffi::MDB_dbi,
}

impl Database {
    unsafe fn new(txn: *mut ffi::MDB_txn, name: Option<&str>, flags: c_uint) -> Result<Database> {
        let c_name = name.and_then(|name| CString::new(name).ok());
        let name_ptr = c_name.as_ref().map_or(ptr::null(), |name| name.as_ptr());
        let mut dbi: ffi::MDB_dbi = 0;
        lmdb_result(ffi::mdb_dbi_open(txn, name_ptr, flags, &mut dbi))?;
        Ok(Database { dbi })
    }

    pub fn dbi(&self) -> ffi::MDB_dbi {
        self.dbi
    }
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

pub struct Environment {
    env: *mut ffi::MDB_env,
    dbi_open_mutex: Mutex<()>,
}

impl Environment {
    pub fn new() -> EnvironmentBuilder {
        EnvironmentBuilder {
            flags: EnvironmentFlags::empty(),
            max_readers: None,
            max_dbs: None,
            map_size: None,
            encrypt_key: None,
        }
    }

    pub fn env(&self) -> *mut ffi::MDB_env {
        self.env
    }

    pub fn open_db(&self, name: Option<&str>) -> Result<Database> {
        let mutex_guard = self
            .dbi_open_mutex
            .lock()
            .map_err(|_| Error::Other(libc::EAGAIN))?;
        let txn = self.begin_ro_txn()?;
        let db = unsafe { txn.open_db(name)? };
        txn.commit()?;
        drop(mutex_guard);
        Ok(db)
    }

    pub fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> Result<Database> {
        let mutex_guard = self
            .dbi_open_mutex
            .lock()
            .map_err(|_| Error::Other(libc::EAGAIN))?;
        let txn = self.begin_rw_txn()?;
        let db = unsafe { txn.create_db(name, flags)? };
        txn.commit()?;
        drop(mutex_guard);
        Ok(db)
    }

    pub fn begin_ro_txn(&self) -> Result<RoTransaction<'_>> {
        RoTransaction::new(self)
    }

    pub fn begin_rw_txn(&self) -> Result<RwTransaction<'_>> {
        RwTransaction::new(self)
    }

    pub fn sync(&self, force: bool) -> Result<()> {
        unsafe { lmdb_result(ffi::mdb_env_sync(self.env(), i32::from(force))) }
    }
}

unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

impl fmt::Debug for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Environment").finish()
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        unsafe { ffi::mdb_env_close(self.env) }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct EnvironmentBuilder {
    flags: EnvironmentFlags,
    max_readers: Option<c_uint>,
    max_dbs: Option<c_uint>,
    map_size: Option<size_t>,
    encrypt_key: Option<[u8; 32]>,
}

impl EnvironmentBuilder {
    pub fn open(&self, path: &Path) -> Result<Environment> {
        self.open_with_permissions(path, 0o644)
    }

    pub fn open_with_permissions(&self, path: &Path, mode: ffi::mdb_mode_t) -> Result<Environment> {
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_env_create(&mut env))?;

            if let Some(key) = self.encrypt_key {
                let key_val = ffi::MDB_val {
                    mv_size: key.len(),
                    mv_data: key.as_ptr() as *mut c_void,
                };
                if let Err(err) = lmdb_result(ffi::mdb_env_set_encrypt(
                    env,
                    Some(chacha20_poly1305_crypt),
                    &key_val,
                    CHACHA20_POLY1305_TAG_SIZE,
                )) {
                    ffi::mdb_env_close(env);
                    return Err(err);
                }
            }

            if let Some(max_readers) = self.max_readers {
                if let Err(err) = lmdb_result(ffi::mdb_env_set_maxreaders(env, max_readers)) {
                    ffi::mdb_env_close(env);
                    return Err(err);
                }
            }
            if let Some(max_dbs) = self.max_dbs {
                if let Err(err) = lmdb_result(ffi::mdb_env_set_maxdbs(env, max_dbs)) {
                    ffi::mdb_env_close(env);
                    return Err(err);
                }
            }
            if let Some(map_size) = self.map_size {
                if let Err(err) = lmdb_result(ffi::mdb_env_set_mapsize(env, map_size)) {
                    ffi::mdb_env_close(env);
                    return Err(err);
                }
            }

            let c_path = match CString::new(path_bytes(path)) {
                Ok(path) => path,
                Err(_) => {
                    ffi::mdb_env_close(env);
                    return Err(Error::Invalid);
                }
            };
            if let Err(err) = lmdb_result(ffi::mdb_env_open(
                env,
                c_path.as_ptr(),
                self.flags.bits(),
                mode,
            )) {
                ffi::mdb_env_close(env);
                return Err(err);
            }
        }
        Ok(Environment {
            env,
            dbi_open_mutex: Mutex::new(()),
        })
    }

    pub fn set_flags(&mut self, flags: EnvironmentFlags) -> &mut EnvironmentBuilder {
        self.flags = flags;
        self
    }

    pub fn set_max_readers(&mut self, max_readers: c_uint) -> &mut EnvironmentBuilder {
        self.max_readers = Some(max_readers);
        self
    }

    pub fn set_map_size(&mut self, map_size: size_t) -> &mut EnvironmentBuilder {
        self.map_size = Some(map_size);
        self
    }

    pub fn set_encrypt_key(&mut self, key: [u8; 32]) -> &mut EnvironmentBuilder {
        self.encrypt_key = Some(key);
        self
    }
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().as_bytes().to_vec()
}

pub trait Transaction: Sized {
    fn txn(&self) -> *mut ffi::MDB_txn;

    fn commit(self) -> Result<()> {
        unsafe {
            let result = lmdb_result(ffi::mdb_txn_commit(self.txn()));
            std::mem::forget(self);
            result
        }
    }

    unsafe fn open_db(&self, name: Option<&str>) -> Result<Database> {
        Database::new(self.txn(), name, 0)
    }

    fn get<'txn, K>(&'txn self, database: Database, key: &K) -> Result<&'txn [u8]>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let mut key_val = ffi::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut c_void,
        };
        let mut data_val = ffi::MDB_val {
            mv_size: 0,
            mv_data: ptr::null_mut(),
        };
        unsafe {
            match ffi::mdb_get(self.txn(), database.dbi(), &mut key_val, &mut data_val) {
                ffi::MDB_SUCCESS => Ok(slice::from_raw_parts(
                    data_val.mv_data as *const u8,
                    data_val.mv_size,
                )),
                err_code => Err(Error::from_err_code(err_code)),
            }
        }
    }

    fn open_ro_cursor(&self, db: Database) -> Result<RoCursor<'_>> {
        RoCursor::new(self, db)
    }
}

pub struct RoTransaction<'env> {
    txn: *mut ffi::MDB_txn,
    _marker: std::marker::PhantomData<&'env ()>,
}

impl<'env> RoTransaction<'env> {
    fn new(env: &'env Environment) -> Result<RoTransaction<'env>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_txn_begin(
                env.env(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?;
        }
        Ok(RoTransaction {
            txn,
            _marker: std::marker::PhantomData,
        })
    }
}

impl Transaction for RoTransaction<'_> {
    fn txn(&self) -> *mut ffi::MDB_txn {
        self.txn
    }
}

impl Drop for RoTransaction<'_> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_txn_abort(self.txn) }
    }
}

pub struct RwTransaction<'env> {
    txn: *mut ffi::MDB_txn,
    _marker: std::marker::PhantomData<&'env ()>,
}

impl<'env> RwTransaction<'env> {
    fn new(env: &'env Environment) -> Result<RwTransaction<'env>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_txn_begin(env.env(), ptr::null_mut(), 0, &mut txn))?;
        }
        Ok(RwTransaction {
            txn,
            _marker: std::marker::PhantomData,
        })
    }

    pub unsafe fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> Result<Database> {
        Database::new(self.txn(), name, flags.bits() | ffi::MDB_CREATE)
    }

    pub fn put<K, D>(
        &mut self,
        database: Database,
        key: &K,
        data: &D,
        flags: WriteFlags,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let data = data.as_ref();
        let mut key_val = ffi::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut c_void,
        };
        let mut data_val = ffi::MDB_val {
            mv_size: data.len(),
            mv_data: data.as_ptr() as *mut c_void,
        };
        unsafe {
            lmdb_result(ffi::mdb_put(
                self.txn(),
                database.dbi(),
                &mut key_val,
                &mut data_val,
                flags.bits(),
            ))
        }
    }
}

impl Transaction for RwTransaction<'_> {
    fn txn(&self) -> *mut ffi::MDB_txn {
        self.txn
    }
}

impl Drop for RwTransaction<'_> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_txn_abort(self.txn) }
    }
}

pub trait Cursor<'txn> {
    fn cursor(&self) -> *mut ffi::MDB_cursor;

    fn get(
        &self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        op: c_uint,
    ) -> Result<(Option<&'txn [u8]>, &'txn [u8])> {
        unsafe {
            let mut key_val = slice_to_val(key);
            let mut data_val = slice_to_val(data);
            let key_ptr = key_val.mv_data;
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor(),
                &mut key_val,
                &mut data_val,
                op,
            ))?;
            let key_out = if key_ptr != key_val.mv_data {
                Some(val_to_slice(key_val))
            } else {
                None
            };
            Ok((key_out, val_to_slice(data_val)))
        }
    }

    fn iter_start(&mut self) -> Iter<'txn> {
        self.get(None, None, ffi::MDB_FIRST).unwrap();
        Iter::new(self.cursor(), ffi::MDB_GET_CURRENT, ffi::MDB_NEXT)
    }

    fn iter_from<K>(&mut self, key: K) -> Iter<'txn>
    where
        K: AsRef<[u8]>,
    {
        self.get(Some(key.as_ref()), None, ffi::MDB_SET_RANGE)
            .unwrap();
        Iter::new(self.cursor(), ffi::MDB_GET_CURRENT, ffi::MDB_NEXT)
    }
}

pub struct RoCursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    _marker: std::marker::PhantomData<fn() -> &'txn ()>,
}

impl<'txn> RoCursor<'txn> {
    fn new<T>(txn: &'txn T, db: Database) -> Result<RoCursor<'txn>>
    where
        T: Transaction,
    {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_cursor_open(txn.txn(), db.dbi(), &mut cursor))?;
        }
        Ok(RoCursor {
            cursor,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<'txn> Cursor<'txn> for RoCursor<'txn> {
    fn cursor(&self) -> *mut ffi::MDB_cursor {
        self.cursor
    }
}

impl Drop for RoCursor<'_> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.cursor) }
    }
}

pub struct Iter<'txn> {
    cursor: *mut ffi::MDB_cursor,
    op: c_uint,
    next_op: c_uint,
    _marker: std::marker::PhantomData<fn(&'txn ())>,
}

impl Iter<'_> {
    fn new(cursor: *mut ffi::MDB_cursor, op: c_uint, next_op: c_uint) -> Self {
        Self {
            cursor,
            op,
            next_op,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'txn> Iterator for Iter<'txn> {
    type Item = (&'txn [u8], &'txn [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = ffi::MDB_val {
            mv_size: 0,
            mv_data: ptr::null_mut(),
        };
        let mut data = ffi::MDB_val {
            mv_size: 0,
            mv_data: ptr::null_mut(),
        };

        unsafe {
            let err_code = ffi::mdb_cursor_get(self.cursor, &mut key, &mut data, self.op);
            self.op = self.next_op;
            if err_code == ffi::MDB_SUCCESS {
                Some((val_to_slice(key), val_to_slice(data)))
            } else {
                None
            }
        }
    }
}

unsafe fn slice_to_val(slice: Option<&[u8]>) -> ffi::MDB_val {
    match slice {
        Some(slice) => ffi::MDB_val {
            mv_size: slice.len(),
            mv_data: slice.as_ptr() as *mut c_void,
        },
        None => ffi::MDB_val {
            mv_size: 0,
            mv_data: ptr::null_mut(),
        },
    }
}

unsafe fn val_to_slice<'a>(val: ffi::MDB_val) -> &'a [u8] {
    slice::from_raw_parts(val.mv_data as *const u8, val.mv_size)
}

unsafe extern "C" fn chacha20_poly1305_crypt(
    src: *const ffi::MDB_val,
    dst: *mut ffi::MDB_val,
    key_ptr: *const ffi::MDB_val,
    encdec: c_int,
) -> c_int {
    let result = std::panic::catch_unwind(|| {
        let input = slice::from_raw_parts((*src).mv_data as *const u8, (*src).mv_size);
        let output = slice::from_raw_parts_mut((*dst).mv_data as *mut u8, (*dst).mv_size);
        let key = slice::from_raw_parts((*key_ptr).mv_data as *const u8, (*key_ptr).mv_size);
        let iv = slice::from_raw_parts(
            (*key_ptr.add(1)).mv_data as *const u8,
            (*key_ptr.add(1)).mv_size,
        );
        let auth = slice::from_raw_parts_mut(
            (*key_ptr.add(2)).mv_data as *mut u8,
            (*key_ptr.add(2)).mv_size,
        );

        if key.len() != 32 || iv.len() < 12 || auth.len() != CHACHA20_POLY1305_TAG_SIZE as usize {
            return Err(());
        }

        output.copy_from_slice(input);
        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
        let nonce = Nonce::from_slice(&iv[..12]);
        if encdec == 1 {
            let tag = cipher
                .encrypt_in_place_detached(nonce, b"", output)
                .map_err(|_| ())?;
            auth.copy_from_slice(&tag);
            Ok(())
        } else {
            cipher
                .decrypt_in_place_detached(nonce, b"", output, Tag::from_slice(auth))
                .map_err(|_| ())
        }
    });

    match result {
        Ok(Ok(())) => 0,
        _ => 1,
    }
}

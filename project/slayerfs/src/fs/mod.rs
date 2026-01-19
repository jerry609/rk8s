//! FileSystem layer: Path-based file system operations (JuiceFS pkg/fs/fs.go style).
//!
//! This module provides a higher-level API that is *parallel* to VFS:
//! - Path-based operations with symlink handling
//! - `File` wrapper with lazy reader/writer
//! - `FileStat` for file metadata (similar to os.FileInfo)
//! - Optional access logging
//!
//! The design mirrors JuiceFS's FileSystem layer and talks directly to
//! MetaClient + Chunk IO, instead of wrapping VFS.

use crate::chuck::chunk::ChunkLayout;
use crate::chuck::store::BlockStore;
use crate::file_io::{ChunkIoFactory, FileRegistry, Inode};
use crate::meta::client::MetaClient;
use crate::meta::config::MetaClientConfig;
use crate::meta::store::{
    DirEntry, FileAttr, FileType, MetaError, SetAttrFlags, SetAttrRequest, StatFsSnapshot,
};
use crate::meta::MetaStore;
use dashmap::Entry;
use libc::{getegid, geteuid};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::info;

/// File system statistics similar to POSIX statvfs.
#[derive(Debug, Clone)]
pub struct StatFs {
    pub total_space: u64,
    pub avail_space: u64,
    pub used_space: u64,
    pub total_inodes: u64,
    pub avail_inodes: u64,
    pub used_inodes: u64,
}

impl From<StatFsSnapshot> for StatFs {
    fn from(s: StatFsSnapshot) -> Self {
        Self {
            total_space: s.total_space,
            avail_space: s.available_space,
            used_space: s.total_space.saturating_sub(s.available_space),
            total_inodes: s.used_inodes.saturating_add(s.available_inodes),
            avail_inodes: s.available_inodes,
            used_inodes: s.used_inodes,
        }
    }
}

/// File metadata similar to `os.FileInfo` in Go.
#[derive(Debug, Clone)]
pub struct FileStat {
    name: String,
    inode: i64,
    attr: FileAttr,
}

impl FileStat {
    pub fn new(name: String, inode: i64, attr: FileAttr) -> Self {
        Self { name, inode, attr }
    }

    pub fn inode(&self) -> i64 {
        self.inode
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.attr.size
    }

    pub fn mode(&self) -> u32 {
        self.attr.mode
    }

    pub fn mod_time(&self) -> SystemTime {
        let nanos = self.attr.mtime as u64;
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    pub fn access_time(&self) -> SystemTime {
        let nanos = self.attr.atime as u64;
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    pub fn is_dir(&self) -> bool {
        self.attr.kind == FileType::Dir
    }

    pub fn is_file(&self) -> bool {
        self.attr.kind == FileType::File
    }

    pub fn is_symlink(&self) -> bool {
        self.attr.kind == FileType::Symlink
    }

    pub fn file_type(&self) -> FileType {
        self.attr.kind
    }

    pub fn uid(&self) -> u32 {
        self.attr.uid
    }

    pub fn gid(&self) -> u32 {
        self.attr.gid
    }

    pub fn nlink(&self) -> u32 {
        self.attr.nlink
    }

    pub fn attr(&self) -> &FileAttr {
        &self.attr
    }
}

/// Open file flags.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub create: bool,
    pub truncate: bool,
    pub exclusive: bool,
}

impl OpenFlags {
    pub const fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            append: false,
            create: false,
            truncate: false,
            exclusive: false,
        }
    }

    pub const fn write_only() -> Self {
        Self {
            read: false,
            write: true,
            append: false,
            create: false,
            truncate: false,
            exclusive: false,
        }
    }

    pub const fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            append: false,
            create: false,
            truncate: false,
            exclusive: false,
        }
    }

    pub const fn create_write() -> Self {
        Self {
            read: false,
            write: true,
            append: false,
            create: true,
            truncate: false,
            exclusive: false,
        }
    }

    pub const fn create_new() -> Self {
        Self {
            read: false,
            write: true,
            append: false,
            create: true,
            truncate: false,
            exclusive: true,
        }
    }

    pub fn with_append(mut self) -> Self {
        self.append = true;
        self
    }

    pub fn with_truncate(mut self) -> Self {
        self.truncate = true;
        self
    }
}

/// Access log entry for tracking file operations.
#[derive(Debug, Clone)]
pub struct AccessLogEntry {
    pub timestamp: SystemTime,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub op: String,
    pub path: String,
    pub result: String,
    pub duration_us: u64,
}

impl std::fmt::Display for AccessLogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = self
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        write!(
            f,
            "{:.6} [uid:{},gid:{},pid:{}] {} ({}) -> {} <{:.6}s>",
            ts,
            self.uid,
            self.gid,
            self.pid,
            self.op,
            self.path,
            self.result,
            self.duration_us as f64 / 1_000_000.0
        )
    }
}

/// Log context for tracking operation timing.
pub struct LogContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    start: std::time::Instant,
}

impl LogContext {
    pub fn new(uid: u32, gid: u32, pid: u32) -> Self {
        Self {
            uid,
            gid,
            pid,
            start: std::time::Instant::now(),
        }
    }

    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

fn make_log_context() -> LogContext {
    let uid = unsafe { geteuid() as u32 };
    let gid = unsafe { getegid() as u32 };
    let pid = std::process::id();
    LogContext::new(uid, gid, pid)
}

/// FileSystem configuration.
#[derive(Debug, Clone)]
pub struct FileSystemConfig {
    /// Enable access logging.
    pub access_log: bool,
    /// Access log buffer size.
    pub access_log_buffer_size: usize,
}

impl Default for FileSystemConfig {
    fn default() -> Self {
        Self {
            access_log: false,
            access_log_buffer_size: 1024,
        }
    }
}

fn access_log_sender(config: &FileSystemConfig) -> Option<mpsc::Sender<AccessLogEntry>> {
    if !config.access_log {
        return None;
    }

    let (tx, mut rx) = mpsc::channel::<AccessLogEntry>(config.access_log_buffer_size);
    // Spawn background task to process access logs
    tokio::spawn(async move {
        while let Some(entry) = rx.recv().await {
            // For now, just log to tracing; can be extended to write to file
            info!(target: "access_log", "{}", entry);
        }
    });
    Some(tx)
}

fn meta_error_to_io(path: &str, err: MetaError) -> io::Error {
    let kind = match err {
        MetaError::NotFound(_) | MetaError::ParentNotFound(_) => io::ErrorKind::NotFound,
        MetaError::AlreadyExists { .. } => io::ErrorKind::AlreadyExists,
        MetaError::NotDirectory(_) => io::ErrorKind::NotADirectory,
        MetaError::DirectoryNotEmpty(_) => io::ErrorKind::DirectoryNotEmpty,
        MetaError::InvalidPath(_) => io::ErrorKind::InvalidInput,
        MetaError::TooManySymlinks => io::ErrorKind::InvalidInput,
        MetaError::NotSupported(_) | MetaError::NotImplemented => io::ErrorKind::Unsupported,
        MetaError::InvalidHandle(_) => io::ErrorKind::InvalidInput,
        MetaError::LockConflict { .. } => io::ErrorKind::WouldBlock,
        MetaError::LockNotFound { .. } => io::ErrorKind::NotFound,
        MetaError::Io(e) => e.kind(),
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("{path}: {err}"))
}

/// High-level path-based file system API.
///
/// This struct talks directly to MetaClient + Chunk IO and provides JuiceFS
/// FileSystem-style operations:
/// - Path-based operations (Open, Stat, Mkdir, etc.)
/// - `File` handle with lazy reader/writer
/// - Optional access logging
pub struct FileSystem<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    meta_layer: Arc<MetaClient<M>>,
    chunk_io: Arc<ChunkIoFactory<S, MetaClient<M>>>,
    files: Arc<FileRegistry<S, MetaClient<M>>>,
    config: FileSystemConfig,
    access_log_tx: Option<mpsc::Sender<AccessLogEntry>>,
    next_file_id: AtomicU64,
}

impl<S, M> FileSystem<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    /// Create a new FileSystem with default meta/client configuration.
    pub async fn new(layout: ChunkLayout, store: S, meta: M) -> Result<Self, String> {
        Self::with_configs(
            layout,
            store,
            meta,
            MetaClientConfig::default(),
            FileSystemConfig::default(),
        )
        .await
    }

    /// Create a new FileSystem with FileSystem configuration.
    pub async fn with_config(
        layout: ChunkLayout,
        store: S,
        meta: M,
        config: FileSystemConfig,
    ) -> Result<Self, String> {
        Self::with_configs(layout, store, meta, MetaClientConfig::default(), config).await
    }

    /// Create a new FileSystem with MetaClient configuration.
    pub async fn with_meta_client_config(
        layout: ChunkLayout,
        store: S,
        meta: M,
        meta_config: MetaClientConfig,
    ) -> Result<Self, String> {
        Self::with_configs(layout, store, meta, meta_config, FileSystemConfig::default()).await
    }

    async fn with_configs(
        layout: ChunkLayout,
        store: S,
        meta: M,
        meta_config: MetaClientConfig,
        config: FileSystemConfig,
    ) -> Result<Self, String> {
        let store = Arc::new(store);
        let meta = Arc::new(meta);
        let meta_client = MetaClient::with_options(
            Arc::clone(&meta),
            meta_config.capacity.clone(),
            meta_config.effective_ttl(),
            meta_config.options.clone(),
        );
        meta_client.initialize().await.map_err(|e| e.to_string())?;
        let meta_layer: Arc<MetaClient<M>> = meta_client.clone();
        Ok(Self::from_components(
            layout,
            store,
            meta,
            meta_layer,
            config,
        ))
    }

    /// Create a new FileSystem from components and an existing meta layer.
    pub fn from_components(
        layout: ChunkLayout,
        store: Arc<S>,
        meta: Arc<M>,
        meta_layer: Arc<MetaClient<M>>,
        config: FileSystemConfig,
    ) -> Self {
        let access_log_tx = access_log_sender(&config);
        let _ = meta;
        let chunk_io = Arc::new(ChunkIoFactory::new(
            layout,
            Arc::clone(&store),
            Arc::clone(&meta_layer),
        ));
        let backend = Arc::clone(chunk_io.backend());
        Self {
            meta_layer,
            chunk_io,
            files: Arc::new(FileRegistry::new(layout, backend)),
            config,
            access_log_tx,
            next_file_id: AtomicU64::new(1),
        }
    }

    /// Log an access entry if access logging is enabled.
    fn log_access(&self, ctx: &LogContext, op: &str, path: &str, result: &str) {
        if let Some(ref tx) = self.access_log_tx {
            let entry = AccessLogEntry {
                timestamp: SystemTime::now(),
                uid: ctx.uid,
                gid: ctx.gid,
                pid: ctx.pid,
                op: op.to_string(),
                path: path.to_string(),
                result: result.to_string(),
                duration_us: ctx.elapsed_us(),
            };
            let _ = tx.try_send(entry);
        }
    }

    fn log_context(&self) -> Option<LogContext> {
        if self.access_log_tx.is_some() {
            Some(make_log_context())
        } else {
            None
        }
    }

    fn log_result<T>(&self, ctx: Option<&LogContext>, op: &str, path: &str, result: &io::Result<T>) {
        if let Some(ctx) = ctx {
            let outcome = match result {
                Ok(_) => "ok".to_string(),
                Err(err) => err.to_string(),
            };
            self.log_access(ctx, op, path, &outcome);
        }
    }

    /// Get file system statistics.
    pub async fn stat_fs(&self) -> io::Result<StatFs> {
        let snapshot = self
            .meta_layer
            .stat_fs()
            .await
            .map_err(|e| meta_error_to_io("stat_fs", e))?;
        Ok(StatFs::from(snapshot))
    }

    /// Resolve a path to FileStat, following symlinks.
    pub async fn stat(&self, path: &str) -> io::Result<FileStat> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = self.resolve(&path, true).await;
        self.log_result(log_ctx.as_ref(), "stat", &path, &result);
        result
    }

    /// Resolve a path to FileStat without following the final symlink (lstat).
    pub async fn lstat(&self, path: &str) -> io::Result<FileStat> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = self.resolve(&path, false).await;
        self.log_result(log_ctx.as_ref(), "lstat", &path, &result);
        result
    }

    /// Internal path resolution with symlink handling.
    async fn resolve(&self, path: &str, follow_last: bool) -> io::Result<FileStat> {
        let ino = if follow_last {
            self.meta_layer
                .resolve_path_follow(path)
                .await
                .map_err(|e| meta_error_to_io(path, e))?
        } else {
            self.meta_layer
                .resolve_path(path)
                .await
                .map_err(|e| meta_error_to_io(path, e))?
        };

        let attr = self
            .meta_layer
            .stat(ino)
            .await
            .map_err(|e| meta_error_to_io(path, e))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{path}: not found")))?;

        let name = path.rsplit('/').next().unwrap_or("");
        Ok(FileStat::new(name.to_string(), attr.ino, attr))
    }

    /// Normalize a path by removing redundant slashes.
    fn normalize_path(path: &str) -> String {
        if path.is_empty() {
            return "/".to_string();
        }
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", parts.join("/"))
        }
    }

    /// Split a normalized path into parent directory and basename.
    fn split_dir_file(path: &str) -> (String, String) {
        let n = path.rfind('/').unwrap_or(0);
        if n == 0 {
            ("/".into(), path[1..].into())
        } else {
            (path[..n].into(), path[n + 1..].into())
        }
    }

    /// Open a file with the given flags.
    pub async fn open(&self, path: &str, flags: OpenFlags) -> io::Result<File<S, M>> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            // Handle file creation
            if flags.create {
                match self.resolve(&path, true).await {
                    Ok(fi) => {
                        if flags.exclusive {
                            return Err(io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                "file already exists",
                            ));
                        }
                        if fi.is_dir() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "cannot open directory as file",
                            ));
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        // Create the file
                        self.create_file_in_existing_dir(&path, flags.exclusive)
                            .await?;
                    }
                    Err(e) => return Err(e),
                }
            }

            let fi = self.resolve(&path, true).await?;
            if fi.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot open directory as file",
                ));
            }

            // Handle truncate
            if flags.truncate && flags.write {
                self.truncate(&path, 0).await?;
            }

            let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);

            // Initialize reader/writer in FileRegistry if needed
            // This is done lazily when the File is first read/written

            Ok(File {
                id: file_id,
                path: path.clone(),
                inode: fi.inode(),
                info: fi,
                flags,
                offset: AtomicU64::new(0),
                meta_layer: Arc::clone(&self.meta_layer),
                chunk_io: Arc::clone(&self.chunk_io),
                files: Arc::clone(&self.files),
                access_log_tx: self.access_log_tx.clone(),
            })
        }
        .await;
        self.log_result(log_ctx.as_ref(), "open", &path, &result);
        result
    }

    /// Open a file without following symlinks.
    pub async fn lopen(&self, path: &str, flags: OpenFlags) -> io::Result<File<S, M>> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let fi = self.resolve(&path, false).await?;

            if fi.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot open directory as file",
                ));
            }

            let file_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);

            Ok(File {
                id: file_id,
                path: path.clone(),
                inode: fi.inode(),
                info: fi,
                flags,
                offset: AtomicU64::new(0),
                meta_layer: Arc::clone(&self.meta_layer),
                chunk_io: Arc::clone(&self.chunk_io),
                files: Arc::clone(&self.files),
                access_log_tx: self.access_log_tx.clone(),
            })
        }
        .await;
        self.log_result(log_ctx.as_ref(), "lopen", &path, &result);
        result
    }

    /// Create a new file (O_CREAT | O_EXCL).
    pub async fn create(&self, path: &str) -> io::Result<File<S, M>> {
        self.open(path, OpenFlags::create_new()).await
    }

    /// Create a regular file (mkdir -p on its parent if needed).
    pub async fn create_file(&self, path: &str) -> io::Result<i64> {
        let path = Self::normalize_path(path);
        if path == "/" {
            return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
        }
        let (dir, name) = Self::split_dir_file(&path);
        let dir_ino = self.mkdir_p(&dir).await?;

        if let Ok(Some(ino)) = self.meta_layer.lookup(dir_ino, &name).await {
            if let Some(attr) = self.meta_layer.stat(ino).await.map_err(|e| meta_error_to_io(&path, e))? {
                if attr.kind == FileType::Dir {
                    return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
                }
                return Ok(ino);
            }
        }

        let ino = self
            .meta_layer
            .create_file(dir_ino, name)
            .await
            .map_err(|e| meta_error_to_io(&path, e))?;
        Ok(ino)
    }

    /// Check if a path exists.
    pub async fn exists(&self, path: &str) -> bool {
        let path = Self::normalize_path(path);
        matches!(self.meta_layer.lookup_path(&path).await, Ok(Some(_)))
    }

    /// Create a directory.
    pub async fn mkdir(&self, path: &str) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            if path == "/" {
                return Ok(());
            }
            let (dir, name) = Self::split_dir_file(&path);
            if name.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "empty directory name",
                ));
            }

            let parent_ino = if dir == "/" {
                self.meta_layer.root_ino()
            } else {
                let (ino, kind) = self
                    .meta_layer
                    .lookup_path(&dir)
                    .await
                    .map_err(|e| meta_error_to_io(&dir, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?;
                if kind != FileType::Dir {
                    return Err(io::Error::new(io::ErrorKind::NotADirectory, dir));
                }
                ino
            };

            if let Some(ino) = self
                .meta_layer
                .lookup(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
            {
                let attr = self
                    .meta_layer
                    .stat(ino)
                    .await
                    .map_err(|e| meta_error_to_io(&path, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
                if attr.kind == FileType::Dir {
                    return Ok(());
                }
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, path));
            }

            self.meta_layer
                .mkdir(parent_ino, name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "mkdir", &path, &result);
        result
    }

    /// Create directories recursively (mkdir -p).
    pub async fn mkdir_all(&self, path: &str) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = self.mkdir_p(&path).await.map(|_| ());
        self.log_result(log_ctx.as_ref(), "mkdir_all", &path, &result);
        result
    }

    /// Remove a file (unlink).
    pub async fn unlink(&self, path: &str) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let (dir, name) = Self::split_dir_file(&path);
            let parent_ino = if dir == "/" {
                self.meta_layer.root_ino()
            } else {
                let (ino, kind) = self
                    .meta_layer
                    .lookup_path(&dir)
                    .await
                    .map_err(|e| meta_error_to_io(&dir, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?;
                if kind != FileType::Dir {
                    return Err(io::Error::new(io::ErrorKind::NotADirectory, dir));
                }
                ino
            };

            let ino = self
                .meta_layer
                .lookup(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            let attr = self
                .meta_layer
                .stat(ino)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            if attr.kind == FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
            }
            self.meta_layer
                .unlink(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "unlink", &path, &result);
        result
    }

    /// Remove an empty directory.
    pub async fn rmdir(&self, path: &str) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            if path == "/" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot remove root",
                ));
            }
            let (dir, name) = Self::split_dir_file(&path);
            let parent_ino = if dir == "/" {
                self.meta_layer.root_ino()
            } else {
                let (ino, kind) = self
                    .meta_layer
                    .lookup_path(&dir)
                    .await
                    .map_err(|e| meta_error_to_io(&dir, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?;
                if kind != FileType::Dir {
                    return Err(io::Error::new(io::ErrorKind::NotADirectory, dir));
                }
                ino
            };

            let ino = self
                .meta_layer
                .lookup(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            let attr = self
                .meta_layer
                .stat(ino)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            if attr.kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, path));
            }
            let children = self
                .meta_layer
                .readdir(ino)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            if !children.is_empty() {
                return Err(io::Error::new(io::ErrorKind::DirectoryNotEmpty, path));
            }
            self.meta_layer
                .rmdir(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "rmdir", &path, &result);
        result
    }

    /// Remove a file or empty directory.
    pub async fn remove(&self, path: &str) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let fi = self.resolve(&path, false).await?;
        if fi.is_dir() {
            self.rmdir(&path).await
        } else {
            self.unlink(&path).await
        }
    }

    /// Rename a file or directory.
    pub async fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        let old = Self::normalize_path(old_path);
        let new = Self::normalize_path(new_path);
        let log_ctx = self.log_context();
        let op_path = format!("{old} -> {new}");
        let result = async {
            let (old_dir, old_name) = Self::split_dir_file(&old);
            let (new_dir, new_name) = Self::split_dir_file(&new);

            let old_parent_ino = if &old_dir == "/" {
                self.meta_layer.root_ino()
            } else {
                self.meta_layer
                    .lookup_path(&old_dir)
                    .await
                    .map_err(|e| meta_error_to_io(&old_dir, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, old_dir.clone()))?
                    .0
            };

            let src_ino = self
                .meta_layer
                .lookup(old_parent_ino, &old_name)
                .await
                .map_err(|e| meta_error_to_io(&old, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, old.clone()))?;
            let src_attr = self
                .meta_layer
                .stat(src_ino)
                .await
                .map_err(|e| meta_error_to_io(&old, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, old.clone()))?;

            if let Ok(Some((dest_ino, dest_kind))) = self.meta_layer.lookup_path(&new).await {
                let new_dir_ino = if &new_dir == "/" {
                    self.meta_layer.root_ino()
                } else {
                    self.meta_layer
                        .lookup_path(&new_dir)
                        .await
                        .map_err(|e| meta_error_to_io(&new_dir, e))?
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, new_dir.clone()))?
                        .0
                };

                if dest_kind == FileType::Dir {
                    if src_attr.kind != FileType::Dir {
                        return Err(io::Error::new(io::ErrorKind::NotADirectory, new.clone()));
                    }
                    let children = self
                        .meta_layer
                        .readdir(dest_ino)
                        .await
                        .map_err(|e| meta_error_to_io(&new, e))?;
                    if !children.is_empty() {
                        return Err(io::Error::new(io::ErrorKind::DirectoryNotEmpty, new.clone()));
                    }
                    self.meta_layer
                        .rmdir(new_dir_ino, &new_name)
                        .await
                        .map_err(|e| meta_error_to_io(&new, e))?;
                } else {
                    if src_attr.kind == FileType::Dir {
                        return Err(io::Error::new(io::ErrorKind::NotADirectory, new.clone()));
                    }
                    self.meta_layer
                        .unlink(new_dir_ino, &new_name)
                        .await
                        .map_err(|e| meta_error_to_io(&new, e))?;
                }
            }

            let new_dir_ino = self
                .mkdir_p(&new_dir)
                .await
                .map_err(|e| io::Error::new(e.kind(), e.to_string()))?;
            self.meta_layer
                .rename(old_parent_ino, &old_name, new_dir_ino, new_name)
                .await
                .map_err(|e| meta_error_to_io(&new, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "rename", &op_path, &result);
        result
    }

    /// Create a hard link.
    pub async fn link(&self, existing: &str, link_path: &str) -> io::Result<()> {
        let existing = Self::normalize_path(existing);
        let link = Self::normalize_path(link_path);
        let log_ctx = self.log_context();
        let op_path = format!("{existing} -> {link}");
        let result = async {
            if existing == "/" {
                return Err(io::Error::new(io::ErrorKind::IsADirectory, existing));
            }
            if link == "/" {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, link));
            }
            let (src_ino, src_kind) = self
                .meta_layer
                .lookup_path(&existing)
                .await
                .map_err(|e| meta_error_to_io(&existing, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, existing.clone()))?;
            if src_kind == FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::IsADirectory, existing));
            }

            let (parent_path, name) = Self::split_dir_file(&link);
            if name.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, link));
            }

            let parent_ino = if &parent_path == "/" {
                self.meta_layer.root_ino()
            } else {
                self.meta_layer
                    .lookup_path(&parent_path)
                    .await
                    .map_err(|e| meta_error_to_io(&parent_path, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, parent_path.clone()))?
                    .0
            };

            let parent_attr = self
                .meta_layer
                .stat(parent_ino)
                .await
                .map_err(|e| meta_error_to_io(&parent_path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, parent_path.clone()))?;
            if parent_attr.kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, parent_path));
            }

            if self
                .meta_layer
                .lookup(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&link, e))?
                .is_some()
            {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, link));
            }

            self.meta_layer
                .link(src_ino, parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&link, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "link", &op_path, &result);
        result
    }

    /// Create a symbolic link.
    pub async fn symlink(&self, link_path: &str, target: &str) -> io::Result<()> {
        let link = Self::normalize_path(link_path);
        let log_ctx = self.log_context();
        let op_path = format!("{link} -> {target}");
        let result = async {
            if link == "/" {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, link));
            }
            let (dir, name) = Self::split_dir_file(&link);
            if name.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, link));
            }

            let parent_ino = if &dir == "/" {
                self.meta_layer.root_ino()
            } else {
                self.meta_layer
                    .lookup_path(&dir)
                    .await
                    .map_err(|e| meta_error_to_io(&dir, e))?
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?
                    .0
            };

            let parent_attr = self
                .meta_layer
                .stat(parent_ino)
                .await
                .map_err(|e| meta_error_to_io(&dir, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?;
            if parent_attr.kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, dir));
            }

            if self
                .meta_layer
                .lookup(parent_ino, &name)
                .await
                .map_err(|e| meta_error_to_io(&link, e))?
                .is_some()
            {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, link));
            }

            self.meta_layer
                .symlink(parent_ino, &name, target)
                .await
                .map_err(|e| meta_error_to_io(&link, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "symlink", &op_path, &result);
        result
    }

    /// Read the target of a symbolic link.
    pub async fn readlink(&self, path: &str) -> io::Result<String> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let (ino, kind) = self
                .meta_layer
                .lookup_path(&path)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            if kind != FileType::Symlink {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, path));
            }
            self.meta_layer
                .read_symlink(ino)
                .await
                .map_err(|e| meta_error_to_io(&path, e))
        }
        .await;
        self.log_result(log_ctx.as_ref(), "readlink", &path, &result);
        result
    }

    /// Truncate a file to the given size.
    pub async fn truncate(&self, path: &str, size: u64) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let (ino, kind) = self
                .meta_layer
                .lookup_path(&path)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            match kind {
                FileType::File => {}
                FileType::Dir => {
                    return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
                }
                _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, path)),
            }

            if let Some(inode) = self.files.inode(ino) {
                inode.update_size(size);
            }
            self.meta_layer
                .set_file_size(ino, size)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "truncate", &path, &result);
        result
    }

    /// Set file attributes.
    pub async fn set_attr(
        &self,
        path: &str,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> io::Result<FileAttr> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let fi = self.resolve(&path, true).await?;
            let attr = self
                .meta_layer
                .set_attr(fi.inode(), req, flags)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?;
            if let Some(size) = req.size {
                if let Some(inode) = self.files.inode(fi.inode()) {
                    inode.update_size(size);
                }
            }
            Ok(attr)
        }
        .await;
        self.log_result(log_ctx.as_ref(), "set_attr", &path, &result);
        result
    }

    /// Read directory entries.
    pub async fn readdir(&self, path: &str) -> io::Result<Vec<DirEntry>> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let (ino, kind) = self
                .meta_layer
                .lookup_path(&path)
                .await
                .map_err(|e| meta_error_to_io(&path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.clone()))?;
            if kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, path));
            }
            self.meta_layer
                .readdir(ino)
                .await
                .map_err(|e| meta_error_to_io(&path, e))
        }
        .await;
        self.log_result(log_ctx.as_ref(), "readdir", &path, &result);
        result
    }

    /// Read data at a specific offset (path-based).
    pub async fn read_at(&self, path: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let fi = self.resolve(&path, true).await?;
            read_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                fi.inode(),
                offset,
                len,
                &path,
            )
            .await
        }
        .await;
        self.log_result(log_ctx.as_ref(), "read_at", &path, &result);
        result
    }

    /// Write data at a specific offset (path-based).
    pub async fn write_at(&self, path: &str, offset: u64, data: &[u8]) -> io::Result<usize> {
        let path = Self::normalize_path(path);
        let log_ctx = self.log_context();
        let result = async {
            let fi = self.resolve(&path, true).await?;
            write_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                fi.inode(),
                offset,
                data,
                &path,
            )
            .await
        }
        .await;
        self.log_result(log_ctx.as_ref(), "write_at", &path, &result);
        result
    }

    /// Get file lock information for a given path and query.
    pub async fn get_plock(
        &self,
        path: &str,
        query: &crate::meta::file_lock::FileLockQuery,
    ) -> io::Result<crate::meta::file_lock::FileLockInfo> {
        let path = Self::normalize_path(path);
        let fi = self.resolve(&path, true).await?;
        self.meta_layer
            .get_plock(fi.inode(), query)
            .await
            .map_err(|e| meta_error_to_io(&path, e))
    }

    /// Set file lock for a given path.
    pub async fn set_plock(
        &self,
        path: &str,
        owner: i64,
        block: bool,
        lock_type: crate::meta::file_lock::FileLockType,
        range: crate::meta::file_lock::FileLockRange,
        pid: u32,
    ) -> io::Result<()> {
        let path = Self::normalize_path(path);
        let fi = self.resolve(&path, true).await?;
        self.meta_layer
            .set_plock(fi.inode(), owner, block, lock_type, range, pid)
            .await
            .map_err(|e| meta_error_to_io(&path, e))
    }

    async fn mkdir_p(&self, path: &str) -> io::Result<i64> {
        let path = Self::normalize_path(path);
        if path == "/" {
            return Ok(self.meta_layer.root_ino());
        }

        if let Some((ino, kind)) = self
            .meta_layer
            .lookup_path(&path)
            .await
            .map_err(|e| meta_error_to_io(&path, e))?
        {
            if kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, path));
            }
            return Ok(ino);
        }

        let mut cur_ino = self.meta_layer.root_ino();
        let mut cur_path = String::new();
        for part in path.trim_start_matches('/').split('/') {
            if part.is_empty() {
                continue;
            }
            cur_path.push('/');
            cur_path.push_str(part);

            match self
                .meta_layer
                .lookup(cur_ino, part)
                .await
                .map_err(|e| meta_error_to_io(&cur_path, e))?
            {
                Some(ino) => {
                    let attr = self
                        .meta_layer
                        .stat(ino)
                        .await
                        .map_err(|e| meta_error_to_io(&cur_path, e))?
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, cur_path.clone()))?;
                    if attr.kind != FileType::Dir {
                        return Err(io::Error::new(io::ErrorKind::NotADirectory, cur_path));
                    }
                    cur_ino = ino;
                }
                None => {
                    let ino = self
                        .meta_layer
                        .mkdir(cur_ino, part.to_string())
                        .await
                        .map_err(|e| meta_error_to_io(&cur_path, e))?;
                    cur_ino = ino;
                }
            }
        }
        Ok(cur_ino)
    }

    async fn create_file_in_existing_dir(
        &self,
        path: &str,
        create_new: bool,
    ) -> io::Result<i64> {
        if path == "/" {
            return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
        }

        let (dir, name) = Self::split_dir_file(path);
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty file name",
            ));
        }

        let parent_ino = if dir == "/" {
            self.meta_layer.root_ino()
        } else {
            let (ino, kind) = self
                .meta_layer
                .lookup_path(&dir)
                .await
                .map_err(|e| meta_error_to_io(&dir, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, dir.clone()))?;
            if kind != FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::NotADirectory, dir));
            }
            ino
        };

        if let Some(existing) = self
            .meta_layer
            .lookup(parent_ino, &name)
            .await
            .map_err(|e| meta_error_to_io(path, e))?
        {
            let attr = self
                .meta_layer
                .stat(existing)
                .await
                .map_err(|e| meta_error_to_io(path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.to_string()))?;
            if attr.kind == FileType::Dir {
                return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
            }
            if create_new {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, path));
            }
            return Ok(existing);
        }

        let ino = self
            .meta_layer
            .create_file(parent_ino, name)
            .await
            .map_err(|e| meta_error_to_io(path, e))?;
        Ok(ino)
    }
}

async fn ensure_inode_registered<S, M>(
    meta_layer: &MetaClient<M>,
    files: &FileRegistry<S, MetaClient<M>>,
    ino: i64,
    path: &str,
) -> io::Result<Arc<Inode>>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    if let Some(inode) = files.inode(ino) {
        return Ok(inode);
    }

    match files.inode.entry(ino) {
        Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
        Entry::Vacant(entry) => {
            let attr = meta_layer
                .stat(ino)
                .await
                .map_err(|e| meta_error_to_io(path, e))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, path.to_string()))?;
            match attr.kind {
                FileType::File => {}
                FileType::Dir => {
                    return Err(io::Error::new(io::ErrorKind::IsADirectory, path));
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, path));
                }
            }

            let inode = Inode::new(ino, attr.size);
            files.ensure_init(Arc::clone(&inode));
            entry.insert(inode.clone());
            Ok(inode)
        }
    }
}

async fn read_inode<S, M>(
    meta_layer: &MetaClient<M>,
    files: &FileRegistry<S, MetaClient<M>>,
    ino: i64,
    offset: u64,
    len: usize,
    path: &str,
) -> io::Result<Vec<u8>>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    if len == 0 {
        return Ok(Vec::new());
    }
    ensure_inode_registered(meta_layer, files, ino, path).await?;
    let reader = files.reader(ino).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "file reader is not initialized",
        )
    })?;
    reader
        .read(offset, len)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

async fn write_inode<S, M>(
    meta_layer: &MetaClient<M>,
    files: &FileRegistry<S, MetaClient<M>>,
    ino: i64,
    offset: u64,
    data: &[u8],
    path: &str,
) -> io::Result<usize>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    let inode = ensure_inode_registered(meta_layer, files, ino, path).await?;
    let writer = files.writer(ino).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            "file writer is not initialized",
        )
    })?;
    let guard = writer.write().await;
    let written = guard
        .write(offset, data)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    let target_size = offset + data.len() as u64;
    if target_size > inode.file_size() {
        inode.update_size(target_size);
        meta_layer
            .set_file_size(ino, target_size)
            .await
            .map_err(|e| meta_error_to_io(path, e))?;
    }

    Ok(written)
}

/// An open file handle.
///
/// Similar to JuiceFS's File struct in pkg/fs/fs.go.
/// Provides read/write operations with automatic offset tracking.
pub struct File<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    id: u64,
    path: String,
    inode: i64,
    info: FileStat,
    flags: OpenFlags,
    offset: AtomicU64,
    meta_layer: Arc<MetaClient<M>>,
    chunk_io: Arc<ChunkIoFactory<S, MetaClient<M>>>,
    files: Arc<FileRegistry<S, MetaClient<M>>>,
    access_log_tx: Option<mpsc::Sender<AccessLogEntry>>,
}

impl<S, M> File<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + 'static,
{
    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the file inode.
    pub fn inode(&self) -> i64 {
        self.inode
    }

    /// Get file metadata.
    pub fn info(&self) -> &FileStat {
        &self.info
    }

    /// Get current offset.
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Seek to a position.
    pub fn seek(&self, offset: u64) {
        self.offset.store(offset, Ordering::Relaxed);
    }

    fn log_access(&self, ctx: &LogContext, op: &str, result: &str) {
        if let Some(ref tx) = self.access_log_tx {
            let entry = AccessLogEntry {
                timestamp: SystemTime::now(),
                uid: ctx.uid,
                gid: ctx.gid,
                pid: ctx.pid,
                op: op.to_string(),
                path: self.path.clone(),
                result: result.to_string(),
                duration_us: ctx.elapsed_us(),
            };
            let _ = tx.try_send(entry);
        }
    }

    fn log_context(&self) -> Option<LogContext> {
        if self.access_log_tx.is_some() {
            Some(make_log_context())
        } else {
            None
        }
    }

    fn log_result<T>(&self, ctx: Option<&LogContext>, op: &str, result: &io::Result<T>) {
        if let Some(ctx) = ctx {
            let outcome = match result {
                Ok(_) => "ok".to_string(),
                Err(err) => err.to_string(),
            };
            self.log_access(ctx, op, &outcome);
        }
    }

    /// Read data at the current offset and advance offset.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.read {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "file not opened for reading",
                ));
            }

            let offset = self.offset.load(Ordering::Relaxed);
            let data = read_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                self.inode,
                offset,
                buf.len(),
                &self.path,
            )
            .await?;

            let n = data.len();
            buf[..n].copy_from_slice(&data);
            self.offset.fetch_add(n as u64, Ordering::Relaxed);

            Ok(n)
        }
        .await;
        self.log_result(log_ctx.as_ref(), "read", &result);
        result
    }

    /// Read data at a specific offset (pread).
    pub async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.read {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "file not opened for reading",
                ));
            }

            let data = read_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                self.inode,
                offset,
                buf.len(),
                &self.path,
            )
            .await?;

            let n = data.len();
            buf[..n].copy_from_slice(&data);

            Ok(n)
        }
        .await;
        self.log_result(log_ctx.as_ref(), "read_at", &result);
        result
    }

    /// Write data at the current offset and advance offset.
    pub async fn write(&self, data: &[u8]) -> io::Result<usize> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.write {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "file not opened for writing",
                ));
            }

            let offset = if self.flags.append {
                // For append mode, always write at end
                self.size().await?
            } else {
                self.offset.load(Ordering::Relaxed)
            };

            let n = write_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                self.inode,
                offset,
                data,
                &self.path,
            )
            .await?;

            if self.flags.append {
                self.offset.store(offset + n as u64, Ordering::Relaxed);
            } else {
                self.offset.fetch_add(n as u64, Ordering::Relaxed);
            }

            Ok(n)
        }
        .await;
        self.log_result(log_ctx.as_ref(), "write", &result);
        result
    }

    /// Write data at a specific offset (pwrite).
    pub async fn write_at(&self, data: &[u8], offset: u64) -> io::Result<usize> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.write {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "file not opened for writing",
                ));
            }

            let n = write_inode(
                self.meta_layer.as_ref(),
                self.files.as_ref(),
                self.inode,
                offset,
                data,
                &self.path,
            )
            .await?;
            Ok(n)
        }
        .await;
        self.log_result(log_ctx.as_ref(), "write_at", &result);
        result
    }

    /// Truncate the file to the given size.
    pub async fn truncate(&self, size: u64) -> io::Result<()> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.write {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "file not opened for writing",
                ));
            }

            if let Some(inode) = self.files.inode(self.inode) {
                inode.update_size(size);
            }
            self.meta_layer
                .set_file_size(self.inode, size)
                .await
                .map_err(|e| meta_error_to_io(&self.path, e))?;
            self.info.attr.size = size;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "truncate", &result);
        result
    }

    /// Sync file data to storage.
    pub async fn sync(&self) -> io::Result<()> {
        let log_ctx = self.log_context();
        let result = async {
            if !self.flags.write {
                return Ok(());
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                .as_nanos() as i64;
            let req = SetAttrRequest {
                mtime: Some(now),
                ctime: Some(now),
                ..Default::default()
            };
            self.meta_layer
                .set_attr(self.inode, &req, SetAttrFlags::empty())
                .await
                .map_err(|e| meta_error_to_io(&self.path, e))?;
            self.info.attr.mtime = now;
            self.info.attr.ctime = now;
            Ok(())
        }
        .await;
        self.log_result(log_ctx.as_ref(), "sync", &result);
        result
    }

    /// Get current file size.
    pub async fn size(&self) -> io::Result<u64> {
        let attr = self
            .meta_layer
            .stat(self.inode)
            .await
            .map_err(|e| meta_error_to_io(&self.path, e))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode not found"))?;
        Ok(attr.size)
    }

    /// Refresh file metadata.
    pub async fn refresh_info(&mut self) -> io::Result<()> {
        let attr = self
            .meta_layer
            .stat(self.inode)
            .await
            .map_err(|e| meta_error_to_io(&self.path, e))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode not found"))?;
        self.info = FileStat::new(self.info.name.clone(), self.inode, attr);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chuck::store::ObjectBlockStore;
    use crate::meta::factory::create_meta_store_from_url;
    use tempfile::tempdir;

    async fn create_test_fs() -> FileSystem<ObjectBlockStore<LocalFsBackend>, Arc<dyn MetaStore>> {
        let tmp = tempdir().unwrap();
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let metadata: Arc<dyn MetaStore> = meta_handle.store();
        let store = ObjectBlockStore::new(client);
        FileSystem::new(layout, store, metadata).await.unwrap()
    }

    #[tokio::test]
    async fn test_filesystem_basic() {
        let fs = create_test_fs().await;

        // Create directory
        fs.mkdir_all("/test/subdir").await.unwrap();

        // Create and write file
        let file = fs.create("/test/hello.txt").await.unwrap();
        file.write(b"Hello, World!").await.unwrap();

        // Read file
        let file = fs.open("/test/hello.txt", OpenFlags::read_only()).await.unwrap();
        let mut buf = vec![0u8; 20];
        let n = file.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"Hello, World!");

        // Stat file
        let fi = fs.stat("/test/hello.txt").await.unwrap();
        assert!(fi.is_file());
        assert_eq!(fi.size(), 13);

        // List directory
        let entries = fs.readdir("/test").await.unwrap();
        assert!(entries.iter().any(|e| e.name == "hello.txt"));
        assert!(entries.iter().any(|e| e.name == "subdir"));
    }

    #[tokio::test]
    async fn test_file_read_write() {
        let fs = create_test_fs().await;
        fs.mkdir("/data").await.unwrap();

        // Create file with write access
        let file = fs
            .open("/data/test.bin", OpenFlags::create_write())
            .await
            .unwrap();

        // Write data
        let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let n = file.write(&data).await.unwrap();
        assert_eq!(n, 1024);

        // Read it back
        let file = fs
            .open("/data/test.bin", OpenFlags::read_only())
            .await
            .unwrap();
        let mut buf = vec![0u8; 1024];
        let n = file.read(&mut buf).await.unwrap();
        assert_eq!(n, 1024);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_file_pread_pwrite() {
        let fs = create_test_fs().await;
        fs.mkdir("/ptest").await.unwrap();

        let file = fs
            .open("/ptest/random.dat", OpenFlags::create_write().with_truncate())
            .await
            .unwrap();

        // Write at specific offsets
        file.write_at(b"HELLO", 100).await.unwrap();
        file.write_at(b"WORLD", 200).await.unwrap();

        // Read at specific offsets
        let file = fs
            .open("/ptest/random.dat", OpenFlags::read_only())
            .await
            .unwrap();

        let mut buf = vec![0u8; 5];
        let n = file.read_at(&mut buf, 100).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"HELLO");

        let n = file.read_at(&mut buf, 200).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"WORLD");
    }

    #[tokio::test]
    async fn test_lstat_stat_symlink() {
        let fs = create_test_fs().await;
        fs.mkdir("/links").await.unwrap();

        let file = fs.create("/links/target.txt").await.unwrap();
        file.write(b"data").await.unwrap();

        fs.symlink("/links/link.symlink", "/links/target.txt")
            .await
            .unwrap();

        let lstat = fs.lstat("/links/link.symlink").await.unwrap();
        assert!(lstat.is_symlink());

        let stat = fs.stat("/links/link.symlink").await.unwrap();
        assert!(stat.is_file());
    }

    #[tokio::test]
    async fn test_append_writes() {
        let fs = create_test_fs().await;
        fs.mkdir("/append").await.unwrap();

        let file = fs
            .open("/append/log.txt", OpenFlags::create_write().with_append())
            .await
            .unwrap();
        file.write(b"one").await.unwrap();
        file.write(b"two").await.unwrap();

        let file = fs
            .open("/append/log.txt", OpenFlags::read_only())
            .await
            .unwrap();
        let mut buf = vec![0u8; 6];
        let n = file.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"onetwo");
    }
}

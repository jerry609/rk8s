use crate::chuck::chunk::ChunkLayout;
use crate::chuck::reader::DataFetcher;
use crate::chuck::writer::DataUploader;
use crate::chuck::{BlockStore, ChunkSpan};
use crate::meta::MetaLayer;
use crate::vfs::backend::Backend;
use crate::vfs::config::{DEFAULT_PAGE_SIZE, ReadConfig, WriteConfig};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

mod inode;
mod reader;
mod writer;

pub use inode::Inode;
pub(crate) use reader::DataReader;
pub(crate) use reader::FileReader;
pub(crate) use writer::DataWriter;
pub(crate) use writer::FileWriter;

const CHUNK_ID_BASE: u64 = 1_000_000_000u64;

pub fn chunk_id_for(ino: i64, chunk_index: u64) -> u64 {
    let ino_u64 = u64::try_from(ino).expect("inode must be non-negative");
    ino_u64
        .checked_mul(CHUNK_ID_BASE)
        .and_then(|v| v.checked_add(chunk_index))
        .unwrap_or_else(|| {
            panic!(
                "chunk_id overflow for inode {} chunk_index {}",
                ino, chunk_index
            )
        })
}

/// Extracts the inode number and chunk index from a chunk_id.
/// This is the inverse operation of `chunk_id_for`.
///
/// # Returns
/// A tuple of (inode, chunk_index) where:
/// - inode = chunk_id / CHUNK_ID_BASE
/// - chunk_index = chunk_id % CHUNK_ID_BASE
pub fn extract_ino_and_chunk_index(chunk_id: u64) -> (i64, u64) {
    let ino = (chunk_id / CHUNK_ID_BASE) as i64;
    let chunk_index = chunk_id % CHUNK_ID_BASE;
    (ino, chunk_index)
}

pub(crate) fn split_chunk_spans(layout: ChunkLayout, offset: u64, len: usize) -> Vec<ChunkSpan> {
    if len == 0 {
        return Vec::new();
    }

    let mut spans = Vec::new();
    let mut remaining = len as u64;
    let mut cursor = offset;

    while remaining > 0 {
        let chunk_index = layout.chunk_index_of(cursor);
        let chunk_offset = layout.within_chunk_offset(cursor) as u32;
        let avail = layout.chunk_size.saturating_sub(chunk_offset as u64);
        let take = std::cmp::min(remaining, avail) as u32;

        spans.push(ChunkSpan::new(chunk_index, chunk_offset, take));
        remaining -= take as u64;
        cursor += take as u64;
    }

    spans
}

pub struct ChunkIoFactory<S, M>
where
    S: BlockStore + Send + Sync,
    M: MetaLayer + Send + Sync,
{
    layout: ChunkLayout,
    backend: Arc<Backend<S, M>>,
}

impl<S, M> ChunkIoFactory<S, M>
where
    S: BlockStore + Send + Sync,
    M: MetaLayer + Send + Sync,
{
    pub fn new(layout: ChunkLayout, store: Arc<S>, meta: Arc<M>) -> Self {
        Self {
            layout,
            backend: Arc::new(Backend::new(store, meta)),
        }
    }

    pub fn layout(&self) -> ChunkLayout {
        self.layout
    }

    pub(crate) fn backend(&self) -> &Arc<Backend<S, M>> {
        &self.backend
    }

    pub(crate) fn reader(&self, chunk_id: u64) -> DataFetcher<'_, S, M> {
        DataFetcher::new(self.layout, chunk_id, self.backend.as_ref())
    }

    pub(crate) fn writer(&self, chunk_id: u64) -> DataUploader<'_, S, M> {
        DataUploader::new(self.layout, chunk_id, self.backend.as_ref())
    }
}

pub struct FileRegistry<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub inode: DashMap<i64, Arc<Inode>>,
    pub(crate) writers: DashMap<i64, Arc<RwLock<FileWriter<B, M>>>>,
    pub(crate) readers: DashMap<i64, Arc<FileReader<B, M>>>,
    backend: Arc<Backend<B, M>>,
    write_config: Arc<WriteConfig>,
    reader_core: Arc<DataReader<B, M>>,
}

impl<B, M> FileRegistry<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(layout: ChunkLayout, backend: Arc<Backend<B, M>>) -> Self {
        let page_size = if layout.block_size.is_multiple_of(DEFAULT_PAGE_SIZE) {
            DEFAULT_PAGE_SIZE
        } else {
            layout.block_size
        };
        let read_config = Arc::new(ReadConfig::new(layout));
        let write_config = Arc::new(WriteConfig::new(layout, page_size));
        let reader_core = Arc::new(DataReader::new(read_config, Arc::clone(&backend)));
        Self {
            inode: DashMap::new(),
            writers: DashMap::new(),
            readers: DashMap::new(),
            backend,
            write_config,
            reader_core,
        }
    }

    // Protect by inode entry.
    pub fn ensure_init(&self, inode: Arc<Inode>) {
        let ino = inode.ino();

        // Only initialize writer/reader if not already present.
        let _writer_arc = self
            .writers
            .entry(ino)
            .or_insert_with(|| {
                Arc::new(RwLock::new(FileWriter::new(
                    Arc::clone(&inode),
                    Arc::clone(&self.write_config),
                    Arc::clone(&self.backend),
                    Arc::clone(&self.reader_core),
                )))
            })
            .clone();
        self.readers
            .entry(ino)
            .or_insert_with(|| self.reader_core.open_for_handle(inode, ino as u64));
    }

    pub fn inode(&self, ino: i64) -> Option<Arc<Inode>> {
        self.inode.get(&ino).map(|entry| Arc::clone(entry.value()))
    }

    pub(crate) fn writer(&self, ino: i64) -> Option<Arc<RwLock<FileWriter<B, M>>>> {
        self.writers
            .get(&ino)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub(crate) fn reader(&self, ino: i64) -> Option<Arc<FileReader<B, M>>> {
        self.readers
            .get(&ino)
            .map(|entry| Arc::clone(entry.value()))
    }
}

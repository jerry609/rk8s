use crate::chuck::{BlockStore, ChunkSpan, ChunkTag};
use crate::meta::MetaStore;
use crate::vfs::chunk_id_for;
use crate::vfs::fs::ChunkIoFactory;
use crate::vfs::inode::Inode;
use std::convert::TryFrom;
use std::sync::Arc;

pub struct FileWriter<B, M>
where
    B: BlockStore,
    M: MetaStore,
{
    inode: Arc<Inode>,
    chunk_io: Arc<ChunkIoFactory<B, M>>,
}

impl<B, M> FileWriter<B, M>
where
    B: BlockStore,
    M: MetaStore,
{
    pub fn new(inode: Arc<Inode>, chunk_io: Arc<ChunkIoFactory<B, M>>) -> Self {
        FileWriter { inode, chunk_io }
    }

    pub async fn write(&self, offset: u64, buf: &[u8]) -> anyhow::Result<usize> {
        let layout = self.chunk_io.layout();
        let chunk_span = ChunkSpan::new(
            layout.chunk_index_of(offset),
            u32::try_from(layout.within_chunk_offset(offset))
                .expect("chunk offset must fit within u32 for spans"),
            u32::try_from(buf.len()).expect("write length must fit within u32 for spans"),
        );
        let spans: Vec<ChunkSpan> = chunk_span
            .split_into::<ChunkTag>(layout.chunk_size, layout.chunk_size, false)
            .collect();

        let mut cursor = 0usize;
        for sp in spans {
            let cid = chunk_id_for(self.inode.ino(), sp.index);
            let writer = self.chunk_io.writer(cid);
            let take = sp.len as usize;
            let buf = &buf[cursor..cursor + take];
            writer.write(sp.offset, buf).await?;
            cursor += take;
        }
        Ok(buf.len())
    }
}

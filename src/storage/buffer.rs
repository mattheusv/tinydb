use std::{
    collections::HashMap,
    io::{self, Seek, Write},
    sync::Arc,
};

use anyhow::{bail, Result};
use log::debug;
use std::sync::{Mutex, RwLock};

use crate::{lru::LRU, relation::Relation, Oid, INVALID_OID};

use super::{smgr::StorageManager, Page, PageNumber, INVALID_PAGE_NUMBER, PAGE_SIZE};

/// Buffer identifiers.
///
/// Zero is invalid, positive is the index of a shared buffer (1..NBuffers).
pub type Buffer = usize;

/// Identifies which disk block the buffer contains.
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
struct BufferTag {
    tablespace: Oid,
    db: Oid,
    relation: Oid,
    page_number: PageNumber,
}

impl BufferTag {
    fn new(page_number: PageNumber, rel: &Relation) -> Self {
        Self {
            page_number,
            tablespace: rel.locator.tablespace,
            relation: rel.locator.oid,
            db: rel.locator.database,
        }
    }
}

impl Default for BufferTag {
    fn default() -> Self {
        Self {
            tablespace: INVALID_OID,
            db: INVALID_OID,
            relation: INVALID_OID,
            page_number: INVALID_PAGE_NUMBER,
        }
    }
}

/// Shared descriptor/state data for a single shared buffer.
struct BufferDesc {
    /// Buffer index number (from 1).
    id: Buffer,

    /// Tag identifier.
    tag: BufferTag,

    /// Number of refereces that this buffer had.
    refcount: usize,

    /// Flag informing if buffer should be writen to disk if is dirty or not.
    is_dirty: bool,

    /// Relation that this buffer belongs. None if buffer is free to use on buffer pool.
    rel: Option<Relation>,

    /// Raw page from buffer.
    page: BufferPage,
}

impl BufferDesc {
    fn new(id: Buffer, tag: BufferTag) -> Self {
        Self {
            id,
            tag,
            refcount: 0,
            is_dirty: false,
            rel: None,
            page: BufferPage::default(),
        }
    }

    fn relation(&self) -> Result<Relation> {
        match &self.rel {
            Some(rel) => Ok(rel.clone()),
            None => bail!("buffer descriptor don't have a relation"),
        }
    }
}

/// Shared buffer pool manager interface used by almost all other database components.
///
/// It encapsulatates the BufferPoolState allowing multiple referances to it.
///
/// BufferPool is reference counted and clonning will just increase the reference counter.
pub struct BufferPool(Arc<Mutex<BufferPoolState>>);

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(size: usize, smgr: StorageManager) -> Self {
        Self(Arc::new(Mutex::new(BufferPoolState::new(size, smgr))))
    }

    /// Returns the buffer number for the buffer containing the block read.
    /// The returned buffer has been pinned.
    pub fn fetch_buffer(&self, rel: &Relation, page_num: PageNumber) -> Result<Buffer> {
        let mut buffer_pool = self.0.lock().unwrap();
        buffer_pool.fetch_buffer(rel, page_num)
    }

    /// Physically write out a shared page to disk.
    ///
    /// Return error if the page could not be found in the page table, None otherwise.
    pub fn flush_buffer(&self, buffer: &Buffer) -> Result<()> {
        let mut buffer_pool = self.0.lock().unwrap();
        buffer_pool.flush_buffer(buffer)
    }

    /// Return the page contents from a buffer.
    pub fn get_page(&self, buffer: &Buffer) -> Result<BufferPage> {
        let buffer_pool = self.0.lock().unwrap();
        buffer_pool.get_page(buffer)
    }

    /// Allocate a new empty page block on disk on the given relation. If the buffer pool is at full capacity,
    /// alloc_page will select a replacement victim to allocate the new page.
    ///
    /// The returned buffer is pinned and is already marked as holding the new page.
    ///
    /// Return error if no new pages could be created, otherwise the buffer.
    pub fn alloc_buffer(&self, rel: &Relation) -> Result<Buffer> {
        let mut buffer_pool = self.0.lock().unwrap();
        buffer_pool.alloc_buffer(rel)
    }

    /// Make the buffer available for replacement. The buffer is also unpined on lru if the ref count is 0.
    ///
    /// Return error if the buffer does not exists on buffer pool, None otherwise.
    pub fn unpin_buffer(&self, buffer: Buffer, is_dirty: bool) -> Result<()> {
        let mut buffer_pool = self.0.lock().unwrap();
        buffer_pool.unpin_buffer(buffer, is_dirty)
    }

    /// Return the number of pages of the given relation.
    pub fn size_of_relation(&self, rel: &Relation) -> Result<u32> {
        let mut buffer_pool = self.0.lock().unwrap();
        buffer_pool.size_of_relation(rel)
    }
}

struct BufferPoolState {
    /// Storage manager used to fetch pages from disk.
    smgr: StorageManager,

    /// Replacer used to find a page that can be removed from memory.
    lru: LRU<Buffer>,

    /// Fixed array all pages.
    pages: Vec<Arc<RwLock<BufferDesc>>>,

    /// List of free buffers.
    free_list: Vec<Buffer>,

    /// Map of page numers to buffer indexes.
    page_table: HashMap<BufferTag, Buffer>,
}

impl BufferPoolState {
    /// Create a new buffer pool initializing the page array with the given size.
    fn new(size: usize, smgr: StorageManager) -> Self {
        let mut free_list = Vec::with_capacity(size);
        let mut pages = Vec::with_capacity(size);

        // Buffer ids start at 1. Buffer id 0 means invalid.
        for buffer in 1..size + 1 {
            free_list.push(buffer);
            pages.push(Arc::new(RwLock::new(BufferDesc::new(
                buffer,
                BufferTag::default(),
            ))))
        }

        Self {
            free_list,
            pages,
            smgr,
            lru: LRU::new(size),
            page_table: HashMap::with_capacity(size),
        }
    }

    /// Return the given page number buffer, fetching from disk if it's not in memory.
    fn fetch_buffer(&mut self, rel: &Relation, page_num: PageNumber) -> Result<Buffer> {
        let buf_tag = BufferTag::new(page_num, rel);
        if let Some(buffer) = self.page_table.get(&buf_tag) {
            debug!(
                "Page {} exists on memory on buffer {} for relation {}",
                page_num, buffer, rel.rel_name,
            );

            let buf_desc = self.get_buffer_descriptor(*buffer)?;
            let bufid = buf_desc.read().unwrap().id;

            self.pin_buffer(&buf_desc);

            Ok(bufid)
        } else {
            debug!(
                "Fething page {} from disk for relation {}",
                page_num, rel.rel_name
            );

            // Find a new buffer id for page.
            let new_buffer = self.new_free_buffer()?;
            let new_buf_desc = self.get_buffer_descriptor(new_buffer)?;

            {
                let mut new_buf_desc = new_buf_desc.write().unwrap();
                new_buf_desc.tag = buf_tag.clone();
                new_buf_desc.refcount = 0;
                new_buf_desc.is_dirty = false;
                new_buf_desc.rel = Some(rel.clone());
                // new_buf_desc.page.0.replace([0; PAGE_SIZE]);
            }

            // Read page from disk and store inside buffer descriptor.
            {
                let new_buf_desc = new_buf_desc.read().unwrap();
                let mut page = new_buf_desc.page.0.lock().unwrap();
                self.smgr.read(rel, page_num, &mut page)?;
            }

            // Add buffer descriptior on cache and pinned.
            self.page_table.insert(buf_tag, new_buffer);
            self.pin_buffer(&new_buf_desc);

            Ok(new_buffer)
        }
    }

    // Write the page of the given buffer out to disk
    fn flush_buffer(&mut self, buffer: &Buffer) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(*buffer)?;
        let buf_desc = buf_desc.read().unwrap();
        debug!(
            "Flushing buffer {} of relation {} to disk",
            buffer,
            buf_desc.relation()?.rel_name
        );
        let page = self.get_page(&buffer)?;

        let page = page.0.lock().unwrap();
        self.smgr
            .write(&buf_desc.relation()?, buf_desc.tag.page_number, &page)?;

        Ok(())
    }

    fn get_page(&self, buffer: &Buffer) -> Result<BufferPage> {
        Ok(self
            .get_buffer_descriptor(*buffer)?
            .read()
            .unwrap()
            .page
            .clone())
    }

    /// Return a new allocated page buffer on the given relation. The buffer returned is pinned.
    fn alloc_buffer(&mut self, rel: &Relation) -> Result<Buffer> {
        let page_num = self.smgr.extend(rel)?;
        debug!(
            "New page {} allocated for relation {}",
            page_num, rel.rel_name
        );
        self.fetch_buffer(rel, page_num)
    }

    /// Return a new free buffer from free list or victim if there is no more free buffers to use.
    fn new_free_buffer(&mut self) -> Result<Buffer> {
        assert!(
            self.page_table.len() < self.page_table.capacity(),
            "Buffer pool exceeded the maximum capacity"
        );
        match self.free_list.pop() {
            Some(buffer) => Ok(buffer),
            None => self.victim(),
        }
    }

    /// Use the LRU replacement policy to choose a page to victim. This function panic if the LRU
    /// don't have any page id to victim. Otherwise the page will be removed from page table. If
    /// the choosen page is dirty victim will flush to disk before removing from page table.
    fn victim(&mut self) -> Result<Buffer> {
        let buffer = self
            .lru
            .victim()
            .expect("replacer does not contain any page id to victim");

        debug!("Page {} was chosen for victim", buffer);

        let buf_desc = self.get_buffer_descriptor(buffer)?;
        let buf_desc = buf_desc.read().unwrap();

        if buf_desc.is_dirty {
            debug!(
                "Flusing dirty page {} to disk before victim",
                buf_desc.tag.page_number,
            );
            self.flush_buffer(&buffer)?;
        }

        self.page_table.remove(&buf_desc.tag);

        Ok(buffer)
    }

    fn get_buffer_descriptor(&self, buffer: Buffer) -> Result<Arc<RwLock<BufferDesc>>> {
        Ok(self.pages.get(buffer - 1).unwrap().clone())
    }

    /// Make buffer unavailable for replacement.
    fn pin_buffer(&mut self, buffer: &Arc<RwLock<BufferDesc>>) {
        let mut buffer = buffer.write().unwrap();
        buffer.refcount += 1;
        self.lru.pin(&buffer.id);
    }

    fn unpin_buffer(&mut self, buffer: Buffer, is_dirty: bool) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(buffer)?;
        let mut buf_desc = buf_desc.write().unwrap();

        buf_desc.is_dirty = buf_desc.is_dirty || is_dirty;
        buf_desc.refcount -= 1;

        if buf_desc.refcount == 0 {
            self.lru.unpin(&buffer);
        }
        Ok(())
    }

    // TODO: call flush_buffer instead of duplicate the code.
    pub fn flush_all_buffers(&mut self) -> Result<()> {
        for buffer in self.page_table.values() {
            let buf_desc = self.get_buffer_descriptor(*buffer)?;
            let buf_desc = buf_desc.read().unwrap();
            debug!(
                "Flushing buffer {} of relation {} to disk",
                buffer,
                buf_desc.relation()?.rel_name
            );
            let page = self.get_page(&buffer)?;

            let page = page.0.lock().unwrap();
            self.smgr
                .write(&buf_desc.relation()?, buf_desc.tag.page_number, &page)?;
        }
        Ok(())
    }

    fn size_of_relation(&mut self, rel: &Relation) -> Result<u32> {
        self.smgr.size(rel)
    }
}

impl Drop for BufferPoolState {
    fn drop(&mut self) {
        log::info!("flushing all buffers to disk");
        self.flush_all_buffers()
            .expect("failed to flush all buffers to disk");
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// A mutable reference counter to a buffer page.
///
/// BufferPage is reference counted and clonning will just increase
/// the reference counter.
///
/// Buffer page is a read only instance of a page. To write
/// data on buffer page call the writer method, that will
/// create a new buffer page writer, writing incomming buffer
/// data in a mutable shared reference of a page.
///
/// It mostly used by buffer pool and access methods.
pub struct BufferPage(Arc<std::sync::Mutex<Page>>);

impl BufferPage {
    /// Create a new page writer, writing new data to
    /// the same reference of a page.
    pub fn writer(&mut self) -> BufferPageWriter {
        BufferPageWriter {
            pos: 0,
            page: self.0.clone(),
        }
    }

    /// Return a slice of page on the given range.
    pub fn slice(&self, start: usize, end: usize) -> Vec<u8> {
        let page = self.0.lock().unwrap();
        page[start..end].to_vec()
    }
}

/// A buffer page writer.
///
/// BufferPageWriter implements std::io::Write and std::io::Seek traits
/// so it can be used as a writer parameter when serializing data.
pub struct BufferPageWriter {
    /// Current position of writer to write incommig buffer data.
    pos: usize,

    /// Mutable shared reference to write incomming data.
    page: Arc<std::sync::Mutex<Page>>,
}

impl io::Write for BufferPageWriter {
    /// Write the incomming buf on in memory referente of page.
    ///
    /// The incomming buf lenght can not exceed the PAGE_SIZE.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut page = self.page.lock().unwrap();

        let new_size = self.pos + buf.len();
        if new_size > page.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Size of buffer {} can not be greater than {}",
                    new_size,
                    page.len(),
                ),
            ));
        }

        let mut current_pos = self.pos;
        for b in buf {
            page[current_pos] = b.clone();
            current_pos += 1;
        }

        self.pos = current_pos;

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl BufferPageWriter {
    /// An wrapper around seek and write calls.
    ///
    /// Start to write the incomming buf data that the given offset.
    pub fn write_at(&mut self, buf: &[u8], offset: io::SeekFrom) -> Result<usize> {
        self.seek(offset)?;
        let size = self.write(buf)?;
        Ok(size)
    }
}

impl io::Seek for BufferPageWriter {
    /// Change the current position of buffer page writer.
    fn seek(&mut self, pos: io::SeekFrom) -> std::io::Result<u64> {
        let page = self.page.lock().unwrap();

        let page_size = page.len();
        match pos {
            std::io::SeekFrom::Start(pos) => {
                self.pos = pos as usize;
            }
            std::io::SeekFrom::End(pos) => {
                self.pos = page_size + pos as usize;
            }
            std::io::SeekFrom::Current(pos) => {
                self.pos += pos as usize;
            }
        };

        if self.pos >= page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Can not seek for a position {} that is greater than page size {}.",
                    self.pos, page_size,
                ),
            ));
        }

        Ok(self.pos as u64)
    }
}

impl Default for BufferPage {
    fn default() -> Self {
        Self(Arc::new(std::sync::Mutex::new([0; PAGE_SIZE])))
    }
}

impl Clone for BufferPage {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

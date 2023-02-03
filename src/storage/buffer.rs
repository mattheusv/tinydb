use std::{
    collections::HashMap,
    sync::{atomic, Arc},
};

use anyhow::{bail, Result};
use log::debug;
use std::sync::{Mutex, RwLock};

use crate::{lru::LRU, relation::Relation, Oid, INVALID_OID};

use super::{smgr::StorageManager, Page, PageNumber, INVALID_PAGE_NUMBER};

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

    /// Relation that this buffer belongs. None if buffer is free to use on
    /// buffer pool.
    rel: Option<Relation>,

    /// Raw page from buffer.
    page: Page,
}

impl BufferDesc {
    fn new(id: Buffer, tag: BufferTag) -> Self {
        Self {
            id,
            tag,
            refcount: 0,
            is_dirty: false,
            rel: None,
            page: Page::default(),
        }
    }

    fn relation(&self) -> Result<Relation> {
        match &self.rel {
            Some(rel) => Ok(rel.clone()),
            None => bail!("buffer descriptor don't have a relation"),
        }
    }
}

/// Shared buffer pool manager interface used by almost all other database
/// components.
///
/// It encapsulatates the BufferPoolState allowing multiple referances to it.
///
/// BufferPool is reference counted and clonning will just increase the
/// reference counter.
pub struct BufferPool {
    /// Storage manager used to fetch pages from disk.
    smgr: Arc<Mutex<StorageManager>>,

    /// Replacer used to find a page that can be removed from memory.
    lru: Arc<Mutex<LRU<Buffer>>>,

    /// Fixed array all pages.
    pages: Arc<RwLock<Vec<Arc<RwLock<BufferDesc>>>>>,

    /// List of free buffers.
    free_list: Arc<Mutex<Vec<Buffer>>>,

    /// Map of page numers to buffer indexes.
    page_table: Arc<RwLock<HashMap<BufferTag, Buffer>>>,

    /// How many strong references. Dirty pages will be written back to disk
    /// once there are no more references.
    refs: Arc<atomic::AtomicUsize>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(size: usize, smgr: StorageManager) -> Self {
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
            free_list: Arc::new(Mutex::new(free_list)),
            pages: Arc::new(RwLock::new(pages)),
            smgr: Arc::new(Mutex::new(smgr)),
            lru: Arc::new(Mutex::new(LRU::new(size))),
            page_table: Arc::new(RwLock::new(HashMap::with_capacity(size))),
            refs: Arc::new(atomic::AtomicUsize::new(1)),
        }
    }

    /// Returns the buffer number for the buffer containing the block read. The
    /// returned buffer has been pinned.
    pub fn fetch_buffer(&self, rel: &Relation, page_num: PageNumber) -> Result<Buffer> {
        let buf_tag = BufferTag::new(page_num, rel);
        let page_table = self.page_table.read().unwrap();
        let buffer = page_table.get(&buf_tag);

        match buffer {
            Some(buffer) => {
                debug!(
                    "page {} exists on memory on buffer {} for relation {}",
                    page_num, buffer, rel.rel_name,
                );

                let buf_desc = self.get_buffer_descriptor(*buffer)?;
                let bufid = buf_desc.read().unwrap().id;

                drop(page_table);
                self.pin_buffer(&buf_desc);

                Ok(bufid)
            }
            None => {
                debug!(
                    "fething page {} from disk for relation {}",
                    page_num, rel.rel_name
                );
                drop(page_table);

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
                    let mut smgr = self.smgr.lock().unwrap();
                    smgr.read(rel, page_num, &new_buf_desc.page)?;
                }

                // Add buffer descriptior on cache and pinned.
                {
                    let mut page_table = self.page_table.write().unwrap();
                    page_table.insert(buf_tag, new_buffer);
                }
                self.pin_buffer(&new_buf_desc);

                Ok(new_buffer)
            }
        }
    }

    /// Physically write out a shared page to disk.
    ///
    /// Return error if the page could not be found in the page table, None
    /// otherwise.
    pub fn flush_buffer(&self, buffer: &Buffer) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(*buffer)?;
        let buf_desc = buf_desc.read().unwrap();
        debug!(
            "flushing buffer {} of relation {} to disk",
            buffer,
            buf_desc.relation()?.rel_name
        );
        let page = self.get_page(&buffer)?;

        let mut smgr = self.smgr.lock().unwrap();
        smgr.write(&buf_desc.relation()?, buf_desc.tag.page_number, &page)?;

        Ok(())
    }

    /// Return the page contents from a buffer.
    pub fn get_page(&self, buffer: &Buffer) -> Result<Page> {
        Ok(self
            .get_buffer_descriptor(*buffer)?
            .read()
            .unwrap()
            .page
            .clone())
    }

    /// Allocate a new empty page block on disk on the given relation. If the
    /// buffer pool is at full capacity, alloc_page will select a replacement
    /// victim to allocate the new page.
    ///
    /// The returned buffer is pinned and is already marked as holding the new
    /// page.
    ///
    /// Return error if no new pages could be created, otherwise the buffer.
    pub fn alloc_buffer(&self, rel: &Relation) -> Result<Buffer> {
        let mut smgr = self.smgr.lock().unwrap();
        let page_num = smgr.extend(rel)?;
        // Force drop to avoid trying use multiple mutable references of self.
        drop(smgr);

        debug!(
            "new page {} allocated for relation {}",
            page_num, rel.rel_name
        );
        self.fetch_buffer(rel, page_num)
    }

    /// Return a new free buffer from free list or victim if there is no more
    /// free buffers to use.
    fn new_free_buffer(&self) -> Result<Buffer> {
        let page_table = self.page_table.read().unwrap();
        assert!(
            page_table.len() < page_table.capacity(),
            "Buffer pool exceeded the maximum capacity"
        );
        drop(page_table);

        let mut free_list = self.free_list.lock().unwrap();
        let buffer = free_list.pop();
        // Force drop to avoid trying use multiple mutable references of self.
        drop(free_list);

        match buffer {
            Some(buffer) => Ok(buffer),
            None => self.victim(),
        }
    }

    /// Use the LRU replacement policy to choose a page to victim. This function
    /// panic if the LRU don't have any page id to victim. Otherwise the page
    /// will be removed from page table. If the choosen page is dirty victim
    /// will flush to disk before removing from page table.
    fn victim(&self) -> Result<Buffer> {
        let buffer = self
            .lru
            .lock()
            .unwrap()
            .victim()
            .expect("replacer does not contain any page id to victim");

        debug!("page {} was chosen for victim", buffer);

        let buf_desc = self.get_buffer_descriptor(buffer)?;
        let buf_desc = buf_desc.read().unwrap();

        if buf_desc.is_dirty {
            debug!(
                "flusing dirty page {} to disk before victim",
                buf_desc.tag.page_number,
            );
            self.flush_buffer(&buffer)?;
        }

        let mut page_table = self.page_table.write().unwrap();
        page_table.remove(&buf_desc.tag);

        Ok(buffer)
    }

    fn get_buffer_descriptor(&self, buffer: Buffer) -> Result<Arc<RwLock<BufferDesc>>> {
        let pages = self.pages.read().unwrap();
        Ok(pages.get(buffer - 1).unwrap().clone())
    }

    /// Make buffer unavailable for replacement.
    fn pin_buffer(&self, buffer: &Arc<RwLock<BufferDesc>>) {
        let mut buffer = buffer.write().unwrap();
        buffer.refcount += 1;
        self.lru.lock().unwrap().pin(&buffer.id);
    }

    /// Make the buffer available for replacement. The buffer is also unpined on
    /// lru if the ref count is 0.
    ///
    /// Return error if the buffer does not exists on buffer pool, None
    /// otherwise.
    pub fn unpin_buffer(&self, buffer: Buffer, is_dirty: bool) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(buffer)?;
        let mut buf_desc = buf_desc.write().unwrap();

        buf_desc.is_dirty = buf_desc.is_dirty || is_dirty;
        buf_desc.refcount -= 1;

        if buf_desc.refcount == 0 {
            self.lru.lock().unwrap().unpin(&buffer);
        }
        Ok(())
    }

    // TODO: call flush_buffer instead of duplicate the code.
    pub fn flush_all_buffers(&self) -> Result<()> {
        for buffer in self.page_table.read().unwrap().values() {
            let buf_desc = self.get_buffer_descriptor(*buffer)?;
            let buf_desc = buf_desc.read().unwrap();
            debug!(
                "flushing buffer {} of relation {} to disk",
                buffer,
                buf_desc.relation()?.rel_name
            );
            let page = self.get_page(&buffer)?;

            let mut smgr = self.smgr.lock().unwrap();
            smgr.write(&buf_desc.relation()?, buf_desc.tag.page_number, &page)?;
        }
        Ok(())
    }

    /// Return the number of pages of the given relation.
    pub fn size_of_relation(&self, rel: &Relation) -> Result<u32> {
        self.smgr.lock().unwrap().size(rel)
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);

        log::trace!("buffer Pool de-referenced; original_ref: {} ", refs);

        if self.refs.load(atomic::Ordering::SeqCst) == 0 {
            log::info!("flushing all buffers to disk");
            self.flush_all_buffers()
                .expect("failed to flush all buffers to disk");
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        let refs = &self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        log::trace!("buffer Pool referenced; original_ref: {} ", refs);

        Self {
            smgr: self.smgr.clone(),
            lru: self.lru.clone(),
            pages: self.pages.clone(),
            free_list: self.free_list.clone(),
            page_table: self.page_table.clone(),
            refs: self.refs.clone(),
        }
    }
}

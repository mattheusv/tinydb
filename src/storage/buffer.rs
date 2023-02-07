use std::{
    collections::HashMap,
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use log::debug;
use std::sync::{Mutex, RwLock};

use crate::{lru::LRU, relation::Relation, Oid, INVALID_OID};

use super::{smgr::StorageManager, Page, PageNumber, INVALID_PAGE_NUMBER};

/// Buffer identifiers.
///
/// Zero is invalid, positive is the index of a shared buffer (1..NBuffers).
pub type BufferID = usize;

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
///
/// Buffer represents a a page that is mapped by buffer pool in memory. Each
/// buffer points to a block page on disk.
///
/// Buffer is reference counted and clonning will just increase the reference
/// counter.
pub struct Buffer {
    /// Buffer index number (from 1).
    pub id: Arc<RwLock<BufferID>>,

    /// Raw page from buffer.
    pub page: Page,

    /// Tag identifier.
    tag: Arc<RwLock<BufferTag>>,

    /// Number of strong references. Dirty pages will be written back to disk
    /// once there are no more references.
    refs: Arc<atomic::AtomicUsize>,

    /// Flag informing if buffer should be writen to disk if is dirty or not.
    is_dirty: Arc<atomic::AtomicBool>,

    /// Relation that this buffer belongs. None if buffer is free to use on
    /// buffer pool.
    rel: Arc<RwLock<Option<Relation>>>,
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            tag: self.tag.clone(),
            refs: self.refs.clone(),
            is_dirty: self.is_dirty.clone(),
            rel: self.rel.clone(),
            page: self.page.clone(),
        }
    }
}

impl Buffer {
    fn new(id: BufferID, tag: BufferTag) -> Self {
        Self {
            id: Arc::new(RwLock::new(id)),
            tag: Arc::new(RwLock::new(tag)),
            refs: Arc::new(atomic::AtomicUsize::new(0)),
            is_dirty: Arc::new(atomic::AtomicBool::new(false)),
            rel: Arc::new(RwLock::new(None)),
            page: Page::default(),
        }
    }

    fn relation(&self) -> Result<Relation> {
        let rel = self.rel.read().unwrap();
        // Match the reference from the de-refenrece value of RwLock
        match &*rel {
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
    lru: Arc<Mutex<LRU<BufferID>>>,

    /// Fixed array all pages.
    pages: Arc<RwLock<Vec<Buffer>>>,

    /// List of free buffers.
    free_list: Arc<Mutex<Vec<BufferID>>>,

    /// Map of page numers to buffer indexes.
    page_table: Arc<RwLock<HashMap<BufferTag, BufferID>>>,

    /// How many strong references the buffer pool had.
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
            pages.push(Buffer::new(buffer, BufferTag::default()))
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

                let buffer = self.get_buffer(buffer)?;

                drop(page_table);
                self.pin_buffer(&buffer);

                Ok(buffer)
            }
            None => {
                debug!(
                    "fething page {} from disk for relation {}",
                    page_num, rel.rel_name
                );
                drop(page_table);

                // Find a new buffer id for page.
                let new_buffer = self.new_free_buffer()?;
                let new_buffer = self.get_buffer(&new_buffer)?;

                {
                    // Crate a short live write mutex for buffer desc tag.
                    let mut new_buffer_tag = new_buffer.tag.write().unwrap();
                    new_buffer_tag.tablespace = buf_tag.tablespace;
                    new_buffer_tag.db = buf_tag.db;
                    new_buffer_tag.relation = buf_tag.relation;
                    new_buffer_tag.page_number = buf_tag.page_number;
                }

                {
                    // Create a short live write mutex for buffer desc relation.
                    let mut new_buffer_rel = new_buffer.rel.write().unwrap();
                    let _ = new_buffer_rel.take();
                    *new_buffer_rel = Some(rel.clone());
                }

                new_buffer.refs.store(0, Ordering::SeqCst);
                new_buffer.is_dirty.store(false, atomic::Ordering::SeqCst);

                // Read page from disk and store inside buffer descriptor.
                {
                    let mut smgr = self.smgr.lock().unwrap();
                    smgr.read(rel, page_num, &new_buffer.page)?;
                }

                // Add buffer descriptior on cache and pinned.
                {
                    let mut page_table = self.page_table.write().unwrap();
                    page_table.insert(buf_tag, *new_buffer.id.read().unwrap());
                }
                self.pin_buffer(&new_buffer);

                Ok(new_buffer)
            }
        }
    }

    /// Physically write out a shared page to disk.
    ///
    /// Return error if the page could not be found in the page table, None
    /// otherwise.
    pub fn flush_buffer(&self, buffer: &Buffer) -> Result<()> {
        debug!(
            "flushing buffer {} of relation {} to disk",
            buffer.id.read().unwrap(),
            buffer.relation()?.rel_name
        );
        let mut smgr = self.smgr.lock().unwrap();
        smgr.write(
            &buffer.relation()?,
            buffer.tag.read().unwrap().page_number,
            &buffer.page,
        )?;

        Ok(())
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
    fn new_free_buffer(&self) -> Result<BufferID> {
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
    fn victim(&self) -> Result<BufferID> {
        let bufid = self
            .lru
            .lock()
            .unwrap()
            .victim()
            .expect("replacer does not contain any page id to victim");

        debug!("page {} was chosen for victim", bufid);

        let buffer = self.get_buffer(&bufid)?;
        let buf_tag = buffer.tag.read().unwrap();

        if buffer.is_dirty.load(Ordering::SeqCst) {
            debug!(
                "flusing dirty page {} to disk before victim",
                buf_tag.page_number,
            );
            self.flush_buffer(&buffer)?;
        }

        let mut page_table = self.page_table.write().unwrap();
        page_table.remove(&buf_tag);

        Ok(bufid)
    }

    fn get_buffer(&self, buffer: &BufferID) -> Result<Buffer> {
        let pages = self.pages.read().unwrap();
        let buffer = pages.get(buffer - 1).unwrap();
        Ok(buffer.clone())
    }

    /// Make buffer unavailable for replacement.
    fn pin_buffer(&self, buffer: &Buffer) {
        let bufid = buffer.id.read().unwrap();

        let refs = buffer.refs.fetch_add(1, Ordering::SeqCst);
        log::trace!("page {} referenced; original_ref: {}", bufid, refs);
        self.lru.lock().unwrap().pin(&bufid);
    }

    /// Make the buffer available for replacement. The buffer is also unpined on
    /// lru if the ref count is 0.
    ///
    /// Return error if the buffer does not exists on buffer pool, None
    /// otherwise.
    pub fn unpin_buffer(&self, buffer: &Buffer, is_dirty: bool) -> Result<()> {
        let bufid = buffer.id.read().unwrap();
        let buffer = self.get_buffer(&bufid)?;

        // Change the is_dirty flag to false only if the current value is false.
        buffer.is_dirty.fetch_or(is_dirty, Ordering::SeqCst);
        let refs = buffer.refs.fetch_sub(1, Ordering::SeqCst);
        log::trace!(
            "page {} de-referenced; original_ref: {}",
            buffer.id.read().unwrap(),
            refs
        );

        if self.refs.load(Ordering::SeqCst) == 0 {
            self.lru.lock().unwrap().unpin(&bufid);
        }
        Ok(())
    }

    pub fn flush_all_buffers(&self) -> Result<()> {
        for bufid in self.page_table.read().unwrap().values() {
            let buffer = self.get_buffer(bufid)?;
            self.flush_buffer(&buffer)?;
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
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        log::trace!("buffer Pool de-referenced; original_ref: {} ", refs);

        if self.refs.load(Ordering::SeqCst) == 0 {
            log::debug!("flushing all buffers to disk");
            self.flush_all_buffers()
                .expect("failed to flush all buffers to disk");
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        let refs = &self.refs.fetch_add(1, Ordering::SeqCst);

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

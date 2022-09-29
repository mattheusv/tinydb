use std::{cell::RefCell, collections::HashMap, convert::TryInto, rc::Rc};

use anyhow::{bail, Result};
use log::debug;

use crate::{lru::LRU, relation::Relation, Oid, INVALID_OID};

use super::{smgr::StorageManager, Page, PageNumber, INVALID_PAGE_NUMBER, PAGE_SIZE};

/// A mutable reference to a page.
///
/// It mostly used by buffer pool and access methods.
pub type MemPage = Rc<RefCell<Bytes>>;

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
        let rel = rel.borrow();
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
    page: MemPage,
}

impl BufferDesc {
    fn new(id: Buffer, tag: BufferTag) -> Self {
        Self {
            id,
            tag,
            refcount: 0,
            is_dirty: false,
            rel: None,
            page: Rc::new(RefCell::new(Bytes::new())),
        }
    }

    fn relation(&self) -> Result<Relation> {
        match &self.rel {
            Some(rel) => Ok(rel.clone()),
            None => bail!("buffer descriptor don't have a relation"),
        }
    }
}

pub struct BufferPool {
    smgr: StorageManager,

    /// Replacer used to find a page that can be removed from memory.
    lru: LRU<Buffer>,

    /// Fixed array all pages.
    pages: Vec<Rc<RefCell<BufferDesc>>>,

    /// List of free buffers.
    free_list: Vec<Buffer>,

    /// Map of page numers to buffer indexes.
    page_table: HashMap<BufferTag, Buffer>,
}

impl BufferPool {
    /// Create a new buffer pool with a given size.
    pub fn new(size: usize, smgr: StorageManager) -> Self {
        let mut free_list = Vec::with_capacity(size);
        let mut pages = Vec::with_capacity(size);

        // Buffer ids start at 1. Buffer id 0 means invalid.
        for buffer in 1..size + 1 {
            free_list.push(buffer);
            pages.push(Rc::new(RefCell::new(BufferDesc::new(
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

    /// Returns the buffer number for the buffer containing the block read.
    /// The returned buffer has been pinned.
    pub fn fetch_buffer(&mut self, rel: &Relation, page_num: PageNumber) -> Result<Buffer> {
        let buf_tag = BufferTag::new(page_num, rel);
        if let Some(buffer) = self.page_table.get(&buf_tag) {
            debug!(
                "Page {} exists on memory on buffer {} for relation {}",
                page_num,
                buffer,
                rel.borrow().rel_name,
            );

            let buf_desc = self.get_buffer_descriptor(*buffer)?;
            let bufid = buf_desc.borrow().id;

            self.pin_buffer(&buf_desc);

            Ok(bufid)
        } else {
            debug!(
                "Fething page {} from disk for relation {}",
                page_num,
                rel.borrow().rel_name
            );

            // Find a new buffer id for page.
            let new_buffer = self.new_free_buffer()?;
            let new_buf_desc = self.get_buffer_descriptor(new_buffer)?;

            {
                let mut new_buf_desc = new_buf_desc.borrow_mut();
                new_buf_desc.tag = buf_tag.clone();
                new_buf_desc.refcount = 0;
                new_buf_desc.is_dirty = false;
                new_buf_desc.rel = Some(rel.clone());
                new_buf_desc.page.borrow_mut().reset();
            }

            // Read page from disk and store inside buffer descriptor.
            self.smgr.read(
                rel,
                page_num,
                &mut new_buf_desc.borrow().page.borrow_mut().bytes_mut(),
            )?;

            // Add buffer descriptior on cache and pinned.
            self.page_table.insert(buf_tag, new_buffer);
            self.pin_buffer(&new_buf_desc);

            Ok(new_buffer)
        }
    }

    /// Physically write out a shared page to disk.
    ///
    /// Return error if the page could not be found in the page table, None otherwise.
    pub fn flush_buffer(&mut self, buffer: &Buffer) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(*buffer)?;
        let buf_desc = buf_desc.borrow();
        debug!(
            "Flushing buffer {} of relation {} to disk",
            buffer,
            buf_desc.relation()?.borrow().rel_name
        );
        let page = self.get_page(&buffer)?;

        self.smgr.write(
            &buf_desc.relation()?,
            buf_desc.tag.page_number,
            &page.borrow().bytes(),
        )?;

        Ok(())
    }

    /// Return the page contents from a buffer.
    pub fn get_page(&self, buffer: &Buffer) -> Result<MemPage> {
        Ok(self.get_buffer_descriptor(*buffer)?.borrow().page.clone())
    }

    /// Allocate a new empty page block on disk on the given relation. If the buffer pool is at full capacity,
    /// alloc_page will select a replacement victim to allocate the new page.
    ///
    /// The returned buffer is pinned and is already marked as holding the new page.
    ///
    /// Return error if no new pages could be created, otherwise the buffer.
    pub fn alloc_buffer(&mut self, rel: &Relation) -> Result<Buffer> {
        let page_num = self.smgr.extend(rel)?;
        debug!(
            "New page {} allocated for relation {}",
            page_num,
            rel.borrow().rel_name
        );
        self.fetch_buffer(rel, page_num)
    }

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

        if buf_desc.borrow().is_dirty {
            debug!(
                "Flusing dirty page {} to disk before victim",
                buf_desc.borrow().tag.page_number,
            );
            self.flush_buffer(&buffer)?;
        }

        self.page_table.remove(&buf_desc.borrow().tag);

        Ok(buffer)
    }

    fn get_buffer_descriptor(&self, buffer: Buffer) -> Result<Rc<RefCell<BufferDesc>>> {
        Ok(self.pages.get(buffer - 1).unwrap().clone())
    }

    /// Make buffer unavailable for replacement.
    fn pin_buffer(&mut self, buffer: &Rc<RefCell<BufferDesc>>) {
        let mut buffer = buffer.borrow_mut();
        buffer.refcount += 1;
        self.lru.pin(&buffer.id);
    }

    /// Make the buffer available for replacement. The buffer is also unpined on lru if the ref count is 0.
    ///
    /// Return error if the buffer does not exists on buffer pool, None otherwise.
    pub fn unpin_buffer(&mut self, buffer: Buffer, is_dirty: bool) -> Result<()> {
        let buf_desc = self.get_buffer_descriptor(buffer)?;
        let mut buf_desc = buf_desc.borrow_mut();

        buf_desc.is_dirty = buf_desc.is_dirty || is_dirty;
        buf_desc.refcount -= 1;

        if buf_desc.refcount == 0 {
            self.lru.unpin(&buffer);
        }
        Ok(())
    }

    /// Physically write out a all shared pages stored on buffer pool to disk.
    //
    // TODO: call flush_buffer instead of duplicate the code.
    pub fn flush_all_buffers(&mut self) -> Result<()> {
        debug!("Flushing all buffers to disk");
        for buffer in self.page_table.values() {
            let buf_desc = self.get_buffer_descriptor(*buffer)?;
            let buf_desc = buf_desc.borrow();
            debug!(
                "Flushing buffer {} of relation {} to disk",
                buffer,
                buf_desc.relation()?.borrow().rel_name
            );
            let page = self.get_page(&buffer)?;

            self.smgr.write(
                &buf_desc.relation()?,
                buf_desc.tag.page_number,
                &page.borrow().bytes(),
            )?;
        }
        Ok(())
    }

    /// Return the number of pages of the given relation.
    pub fn size_of_relation(&mut self, rel: &Relation) -> Result<u32> {
        self.smgr.size(rel)
    }
}

/// Bytes is a wrapper over a byte array that makes it easy to write, overwrite and reset that byte array.
#[derive(PartialEq, Debug)]
pub struct Bytes {
    page: Page,
}

impl Bytes {
    /// Create a new empty bytes buffer.
    pub fn new() -> Self {
        Self {
            page: [0; PAGE_SIZE],
        }
    }

    /// Override the current bytes from buffer to the incoming data.
    pub fn write(&mut self, data: [u8; PAGE_SIZE]) {
        self.page = data;
    }

    /// Write at bytes buffer from a vec. Panic if data.len() > N.
    pub fn write_from_vec(&mut self, data: Vec<u8>) {
        self.write(self.vec_to_array(data));
    }

    /// Write the comming data overrinding the bytes buffer starting at the given offset.
    pub fn write_at(&mut self, data: &Vec<u8>, offset: usize) {
        assert!(
            data.len() <= self.page.len() + offset,
            "Data overflow the current buffer size"
        );

        let mut idx_outer = 0;
        for idx in offset..self.page.len() {
            if idx_outer >= data.len() {
                break;
            }
            self.page[idx] = data[idx_outer];
            idx_outer += 1;
        }
    }

    /// Return the current bytes inside buffer.
    pub fn bytes(&self) -> [u8; PAGE_SIZE] {
        self.page
    }

    /// Return a mutable reference to override.
    pub fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.page
    }

    /// Resets the buffer to be empty, but it retains the underlying storage for use by future writes.
    pub fn reset(&mut self) {
        self.page = [0; PAGE_SIZE];
    }

    fn vec_to_array<T>(&self, v: Vec<T>) -> [T; PAGE_SIZE] {
        v.try_into().unwrap_or_else(|v: Vec<T>| {
            panic!(
                "Expected a Vec of length {} but it was {}",
                PAGE_SIZE,
                v.len()
            )
        })
    }
}

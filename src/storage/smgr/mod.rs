mod disk;

use std::{cell::RefCell, rc::Rc};

use crate::{relation::locator::RelationLocator, storage::smgr::disk::Disk};

use anyhow::Result;

use super::{MemPage, PageNumber};

/// Represents a storage manger of a relation.
pub struct SMgrRelationData {
    disk: Disk,
}

pub type SMgrRelation = Rc<RefCell<SMgrRelationData>>;

impl SMgrRelationData {
    /// Return a new SMgrRelation object.
    pub fn open(locator: &RelationLocator) -> Result<Self> {
        Ok(Self {
            disk: Disk::open(&locator.relation_path()?)?,
        })
    }

    /// Write the supplied page at the appropriate location.
    pub fn write(&mut self, page_number: PageNumber, page: &MemPage) -> Result<()> {
        self.disk.write_page(page_number, page)
    }

    /// Read the specified block from the storage manager relation.
    pub fn read(&mut self, page_number: PageNumber, page: &mut MemPage) -> Result<()> {
        self.disk.read_page(page_number, page)
    }

    /// Add a new page block to a file.
    pub fn extend(&mut self) -> Result<PageNumber> {
        self.disk.allocate_page()
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32> {
        self.disk.size()
    }
}

mod pager;

use std::{cell::RefCell, rc::Rc};

use crate::storage::{rel::RelationLocator, smgr::pager::Pager};

use anyhow::Result;

use super::{MemPage, PageNumber};

/// Represents a storage manger of a relation.
pub struct SMgrRelationData {
    pager: Pager,
}

pub type SMgrRelation = Rc<RefCell<SMgrRelationData>>;

impl SMgrRelationData {
    /// Return a new SMgrRelation object.
    ///
    /// Note that this does not attempt to actually open the underlying file.
    pub fn open(locator: &RelationLocator) -> Result<Self> {
        Ok(Self {
            pager: Pager::open(&locator.relation_path()?)?,
        })
    }

    /// Write the supplied page at the appropriate location.
    pub fn write(&mut self, page_number: PageNumber, page: &MemPage) -> Result<()> {
        self.pager.write_page(page_number, page)
    }

    /// Read the specified block from the storage manager relation.
    pub fn read(&mut self, page_number: PageNumber, page: &mut MemPage) -> Result<()> {
        self.pager.read_page(page_number, page)
    }

    /// Add a new page block to a file.
    pub fn extend(&mut self) -> Result<PageNumber> {
        self.pager.allocate_page()
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32> {
        self.pager.size()
    }
}

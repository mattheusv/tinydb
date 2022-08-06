mod pager;

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::storage::{rel::RelationLocator, smgr::pager::Pager};

use anyhow::Result;

use super::{MemPage, PageNumber};

/// Represents a storage manger of a relation.
pub struct SMgrRelationData {
    locator: RelationLocator,
}

pub type SMgrRelation = Rc<RefCell<SMgrRelationData>>;

impl SMgrRelationData {
    /// Return a new SMgrRelation object.
    ///
    /// Note that this does not attempt to actually open the underlying file.
    pub fn open(locator: &RelationLocator) -> Self {
        Self {
            locator: locator.clone(),
        }
    }

    /// Write the supplied page at the appropriate location.
    pub fn write(&self, page_number: PageNumber, page: &MemPage) -> Result<()> {
        let mut pager = Pager::open(
            &Path::new(&self.locator.db_data)
                .join(&self.locator.db_name)
                .join(&self.locator.oid.to_string()),
        )?;
        pager.write_page(page_number, page)
    }

    /// Read the specified block from the storage manager relation.
    pub fn read(&mut self, page_number: PageNumber, page: &mut MemPage) -> Result<()> {
        let mut pager = Pager::open(
            &Path::new(&self.locator.db_data)
                .join(&self.locator.db_name)
                .join(&self.locator.oid.to_string()),
        )?;
        pager.read_page(page_number, page)
    }

    /// Add a new page block to a file.
    pub fn extend(&self) -> Result<PageNumber> {
        let mut pager = Pager::open(
            &Path::new(&self.locator.db_data)
                .join(&self.locator.db_name)
                .join(&self.locator.oid.to_string()),
        )?;
        pager.allocate_page()
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32> {
        let pager = Pager::open(
            &Path::new(&self.locator.db_data)
                .join(&self.locator.db_name)
                .join(&self.locator.oid.to_string()),
        )?;
        pager.size()
    }
}

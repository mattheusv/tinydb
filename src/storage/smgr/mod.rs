mod disk;

use std::env;

use std::path::Path;
use std::{cell::RefCell, collections::HashMap, path::PathBuf, rc::Rc};

use crate::relation::Relation;

use anyhow::Result;

use self::disk::Disk;

use super::{
    relation_locator::{relation_path, RelationLocator},
    MemPage, PageNumber,
};

/// A SMgrRelationData is used as StorageManager entry key to store a physical disk page handler of
/// a relation.
#[derive(Eq, PartialEq, Hash)]
pub struct SMgrRelationData {
    locator: RelationLocator,
}

pub type SMgrRelation = Rc<SMgrRelationData>;

impl SMgrRelationData {
    /// Create a new storage manager relation for the given relation locator.
    pub fn new(locator: &RelationLocator) -> Self {
        Self {
            locator: locator.clone(),
        }
    }
}

/// Storage manager that handle read and write page operations.
///
/// The storge manager also have his own cache to store disk page handler to a given relation to
/// avoid re-open a file every time that an read/write operation is requested.
///
/// TODO: Add a configuration to limit the cache size.
pub struct StorageManager {
    /// Base data directory where database files are stored.
    ///
    /// At startup the current working directory is the path where the tinydb binary was started,
    /// so this field is used to open the disk page handler of relations, after startup the current
    /// working directory is changed to data_dir absolute path, so all functions and read/write
    /// files from database directory without needing the base data_dir path.
    data_dir: PathBuf,

    /// Hashmap to store the disk page handler for each relation.
    relation_smgr: HashMap<SMgrRelation, Rc<RefCell<Disk>>>,
}

impl StorageManager {
    /// Create a new storage manager using the given data_dir as base data directory path.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            relation_smgr: HashMap::new(),
        }
    }

    /// Write the supplied page at the appropriate location.
    pub fn write(&mut self, rel: &Relation, page_number: PageNumber, page: &MemPage) -> Result<()> {
        let disk = self.smgr_from_relation(rel)?;
        let mut disk = disk.borrow_mut();
        disk.write_page(page_number, page)
    }

    /// Read the specified block from the storage manager relation.
    pub fn read(
        &mut self,
        rel: &Relation,
        page_number: PageNumber,
        page: &mut MemPage,
    ) -> Result<()> {
        let disk = self.smgr_from_relation(rel)?;
        let mut disk = disk.borrow_mut();
        disk.read_page(page_number, page)
    }

    /// Add a new page block to a file.
    pub fn extend(&mut self, rel: &Relation) -> Result<PageNumber> {
        let disk = self.smgr_from_relation(rel)?;
        let mut disk = disk.borrow_mut();
        disk.allocate_page()
    }

    /// Computes the number of pages in a file.
    pub fn size(&mut self, rel: &Relation) -> Result<u32> {
        self.smgr_from_relation(rel)?.borrow().size()
    }

    /// Return a cached page handler for the given relation. If a page handler does not exists for
    /// relation, create a new one and cached it.
    fn smgr_from_relation(&mut self, rel: &Relation) -> Result<Rc<RefCell<Disk>>> {
        let smgr = &rel.borrow().smgr;
        match self.relation_smgr.get(smgr) {
            Some(disk) => Ok(disk.clone()),
            None => {
                let relpath = self.relation_path(rel)?;
                let disk = Rc::new(RefCell::new(Disk::open(&self.data_dir.join(relpath))?));
                self.relation_smgr.insert(smgr.clone(), disk.clone());
                Ok(disk)
            }
        }
    }

    fn relation_path(&self, rel: &Relation) -> Result<PathBuf> {
        let locator = &rel.borrow().locator;

        let relpath = &relation_path(&locator.tablespace, &locator.database, &locator.oid)?;
        if env::current_dir()? == self.data_dir {
            Ok(relpath.to_path_buf())
        } else {
            Ok(self.data_dir.join(relpath))
        }
    }
}

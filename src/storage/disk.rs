use anyhow::{bail, Result};
use log::debug;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use crate::storage::{Page, PageNumber, PAGE_SIZE};

/// Represents the tinydb header size.
const HEADER_SIZE: usize = 100;

/// Represents the first N bytes of the file.
const MAGIC_BYTES_SIZE: usize = 6;

/// Represents the first [MAGIC_BYTES_SIZE] of file.
const MAGIC_BYTES: &[u8; MAGIC_BYTES_SIZE] = b"Tinydb";

/// HeaderData is a type that represents the array of bytes
/// containing the header data from database file.
type HeaderData = [u8; HEADER_SIZE];

/// Represents errors that disk can have.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    /// Represents an invalid page number on database file.
    #[error("Incorret page number {0}")]
    IncorrectPageNumber(PageNumber),

    /// The database file is corrupted. Mostly the magic bytes
    /// is different than [MAGIC_BYTES].
    #[error("Corrupted database file")]
    CorruptedFile,
}

/// A in memory representation of a tinydb file header.
///
/// Note that Header instances are in-memory copy of current
/// page header data, if change was made is necessary to write
/// back to disk using [write_header](Disk::write_header) function.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Header {
    magic: [u8; MAGIC_BYTES_SIZE],
}

impl Header {
    /// Return the fixed size byte representation of header file.
    ///
    /// Note that if [Header] size is less than [HEADER_SIZE] the
    /// bytes data will be resized and 0 values will be added in
    /// the end of array. If [Header] size is greater thatn [HEADER_SZIE]
    /// the function will panic. Truncate the slice can't be done because
    /// can lost values and generate bugs.
    pub fn serialize(&self) -> Result<HeaderData> {
        let mut data = bincode::serialize(self)?;
        if data.len() < HEADER_SIZE {
            data.resize(HEADER_SIZE, 0);
        }
        Ok(data.try_into().unwrap_or_else(|v: Vec<u8>| {
            panic!(
                "Expected a Header of length {} but it was {}",
                HEADER_SIZE,
                v.len()
            )
        }))
    }

    /// Convert a fixed size byte array to Header.
    pub fn deserialize(data: &HeaderData) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

impl Default for Header {
    fn default() -> Self {
        Self {
            magic: MAGIC_BYTES.clone(),
        }
    }
}

/// Disk handle all read/write operations on database file.
///
/// If you want to modify the file, you need to modify the page returned by
/// the disk and instruct the disk to write it back to disk.
///
/// The Disk is very simple and always creates an in-memory copy of any page
/// that is read (even if that page has already been read before).
/// More specifically, pages are read into a MemPage structure.
#[derive(Debug)]
pub struct Disk {
    file: Mutex<File>,
    total_pages: AtomicU32,
}

impl Disk {
    /// Open a file for paged access.
    ///
    /// This function opens a database file and verifies that the file
    /// header is correct. If the file is empty (which will happen if the
    /// Disk is given a filename for a file that does not exist) then this
    /// function will initialize the file header using the default values.
    pub fn open(filename: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)?;
        let disk = Self {
            file: Mutex::new(file),
            total_pages: AtomicU32::new(0),
        };
        disk.total_pages.store(disk.size()?, Ordering::Relaxed);

        if disk.is_empty()? {
            disk.initialize_header()?;
        } else {
            disk.validate_header()?;
        }
        Ok(disk)
    }

    /// Read a page from file.  
    ///
    /// Reads a page from the disk, and updates the given in-memory Page struct. Any changes done
    /// to a Page will not be effective until call the [write_page](Disk::write_page] with that
    /// Page.
    pub fn read_page(&self, page_number: PageNumber, page: &Page) -> Result<()> {
        self.validate_page(page_number)?;
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(self.offset(page_number)))?;

        let mut page = page.0.lock().unwrap();
        let count = file.read(page.as_mut())?;
        debug!("Read {} bytes from page {}", count, page_number);

        Ok(())
    }

    /// Write a page to file.
    ///
    /// Writes the given in-memory copy of a page back to disk.
    pub fn write_page(&self, number: PageNumber, page: &Page) -> Result<()> {
        self.validate_page(number)?;

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(self.offset(number)))?;

        let page = page.0.lock().unwrap();
        let count = file.write(page.as_ref())?;
        debug!("Wrote {} bytes to page {}", count, number);

        Ok(())
    }

    /// Allocate an extra page on the file and returns the page number
    pub fn allocate_page(&self) -> Result<u32> {
        let new_page = self.total_pages.fetch_add(1, Ordering::SeqCst) + 1;
        self.write_page(new_page, &Page::default())?;
        Ok(new_page)
    }

    /// Reads the header of database file and returns it in a byte array.
    /// Note that this function can be called even if the page size is unknown,
    /// since the chidb header always occupies the first 100 bytes of the file.
    fn read_header(&self) -> Result<Header> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; HEADER_SIZE];
        file.read(&mut header)?;
        Ok(Header::deserialize(&header)?)
    }

    /// Write the header on database file. Note that the write_header function will
    /// always override the current header data if exists.
    fn write_header(&self, header: &Header) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.write(&header.serialize()?)?;
        Ok(())
    }

    /// Computes the number of pages in a file.
    pub fn size(&self) -> Result<u32> {
        let file = self.file.lock().unwrap();
        let len = file.metadata()?.len();
        if len == 0 || len as usize - HEADER_SIZE == 0 {
            // If len is equal 0 means that the file is empty.
            // If len - HEADER_SIZE is equal 0 means that the
            // file doest not have any page, so in both case
            // return 0.
            return Ok(0);
        }
        // Otherwise we calculate the total of
        // pages in file and finally substract with the
        // HEADER_SIZE to get the total of pages in file.
        Ok((len as u32 / PAGE_SIZE as u32) - HEADER_SIZE as u32)
    }

    /// Check if a page number is valid to this database file buffer.
    fn validate_page(&self, page: PageNumber) -> Result<()> {
        if page > self.total_pages.load(Ordering::Relaxed) || page <= 0 {
            bail!(Error::IncorrectPageNumber(page));
        }
        Ok(())
    }

    /// Returns the offset on database file where a Page start given a page number.
    fn offset(&self, page: PageNumber) -> u64 {
        // Start reading pages after page header; pages start reading at 0.
        (HEADER_SIZE as u32 + page - 1) as u64 * PAGE_SIZE as u64
    }

    /// Check if file buffer is empty.
    fn is_empty(&self) -> Result<bool> {
        let file = self.file.lock().unwrap();
        Ok(file.metadata()?.len() == 0)
    }

    /// Check if the header data is valid on disk.
    fn validate_header(&self) -> Result<()> {
        let header = self.read_header()?;

        // TODO: This is right? Seems not.
        if header.magic != MAGIC_BYTES.clone() {
            bail!(Error::CorruptedFile);
        }

        Ok(())
    }

    /// Initialize the default header values.
    fn initialize_header(&self) -> Result<()> {
        Ok(self.write_header(&Header::default())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_first_page_not_override_header() -> Result<()> {
        let disk = open_test_disk()?;
        let page_number = disk.allocate_page()?;
        let mem_page = Page::new([1; PAGE_SIZE]);
        disk.write_page(page_number, &mem_page)?;

        let page = Page::default();
        disk.read_page(page_number, &page)?;

        assert_eq!(disk.read_header()?, Header::default());
        assert_eq!(mem_page, page);

        Ok(())
    }

    #[test]
    fn test_open_existed_database_file() -> Result<()> {
        let file = NamedTempFile::new()?;
        {
            // Open empty database file and create a page.
            let disk = Disk::open(file.path())?;
            let page_number = disk.allocate_page()?;
            let page_data = Page::default();
            disk.write_page(page_number, &page_data)?;
        }

        // Open an already existed database file and create a new page.
        let disk = Disk::open(file.path())?;
        let page_number = disk.allocate_page()?;
        let page_data = Page::default();
        disk.write_page(page_number, &page_data)?;

        assert_eq!(2, disk.size()?);
        Ok(())
    }

    #[test]
    fn test_disk_file_page_size() -> Result<()> {
        let disk = open_test_disk()?;
        let total_pages = 20;

        for i in 0..total_pages {
            let page_number: PageNumber = disk.allocate_page()?;
            let page_data = Page::new([i; PAGE_SIZE]);
            disk.write_page(page_number, &page_data)?;
        }

        assert_eq!(total_pages as u32, disk.size()?);

        Ok(())
    }

    #[test]
    fn test_write_read_pages() -> Result<()> {
        let disk = open_test_disk()?;

        let total_pages = 20;

        // Test creating and reading multiple pages to assert
        // that the disk read the correct offset.
        for i in 0..total_pages {
            let page_number: PageNumber = disk.allocate_page()?;
            let page_data = Page::new([i; PAGE_SIZE]);
            disk.write_page(page_number, &page_data)?;

            let mut page = Page::default();
            disk.read_page(page_number, &mut page)?;

            assert_eq!(page_data, page);
        }

        Ok(())
    }

    #[test]
    fn test_read_invalid_page() -> Result<()> {
        let disk = open_test_disk()?;
        let mut page = Page::default();
        let result = disk.read_page(1, &mut page);

        let err = result.unwrap_err();
        assert_eq!(
            Error::IncorrectPageNumber(1),
            err.downcast::<Error>().unwrap()
        );
        Ok(())
    }

    #[test]
    fn test_read_corrupted_header() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        file.write(&[0; HEADER_SIZE])?;
        let result = Disk::open(file.path());

        let err = result.unwrap_err();
        assert_eq!(Error::CorruptedFile, err.downcast::<Error>().unwrap());
        Ok(())
    }

    #[test]
    fn test_open_new_disk() -> Result<()> {
        let disk = open_test_disk()?;
        let header = disk.read_header()?;
        assert_eq!(header, Header::default());
        Ok(())
    }

    fn open_test_disk() -> Result<Disk> {
        let file = NamedTempFile::new()?;
        Disk::open(file.path())
    }
}

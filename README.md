# tinydb
Tinydb is a simple database implementation for study purpose.

## Milestones
- [x] Disk storage manager
    - [x] Read header
    - [x] Write header
    - [x] Validate header consistency
    - [x] Read page from disk
    - [x] Write page to disk

- [x] LRU  Replacement Policy

- [x] Buffer Pool Manager
    - [x] Fetch page
        - [x] From disk
        - [x] From memory
        - [x] From memory reusing unused in memory pages from cache
    - [x] Victim page from cache to reuse
    - [x] Flush pages to disk
    - [ ] Parallel Buffer Pool Manager

- [ ] System Catalog
    - [ ] pg_tables
    - [ ] pg_attribute
    - [ ] pg_type
    - [ ] others...

- [ ] Write/Read values to/from tables using the system catalog

- [ ] Postgres wire protocol

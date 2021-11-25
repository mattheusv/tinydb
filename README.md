# tinydb
Tinydb is a simple database implementation for study purpose.

## Milestones
- [x] Disk manager
    - [x] Read header
    - [x] Write header
    - [x] Validate header consistency
    - [x] Read from disk
    - [x] Write to disk

- [x] LRU  Replacement Policy

- [ ] Buffer Pool Manager
    - [x] Fetch page
        - [x] From disk
        - [x] From memory
        - [x] From memory reusing unused in memory pages from cache
    - [x] Victim page from cache to reuse
    - [x] Flush pages to disk
    - [ ] Delete page from cache.

- [ ] Parallel Buffer Pool Manager

# tinydb
A database implementation from scratch in Rust for study purpose.

---
The most implementations is based on Postgresql implementation itself. Some design choices are:

- Buffer pool manager written from scratch (Tinydb don't use mmap)
- [LRU](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU) algorithm implementation for buffer victim 
- [Heap](https://en.wikipedia.org/wiki/Heap_(data_structure)) file format is used to store database files.
- [NULL values are handled using a bitmap](https://www.highgo.ca/2020/10/20/the-way-to-store-null-value-in-pg-record/) 
- [Postgres Wire Protocol implementation](https://www.postgresql.org/docs/current/protocol-flow.html) 


## Building
Tinydb is develop in Rust, so it's necessary to have the [Rust build toolchain installed](https://www.rust-lang.org/tools/install).

Once you have installed the Rust toolchanin, just clone the repository, build the binary and run.

- `git clone https://github.com/msAlcantara/tinydb`
- `cargo install --path .`

## Usage
 Tinydb is a server database that implements the [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol-flow.html) so any PostgreSQL client can be used with tinydb.

 The database directory should be initialized when running tinydb for the first time: `tinydb --init`

 For second run, you can just type `tinydb` to start the server with default configurations.

 And them you can connect using psql or any other Postgres client:

 `psql -h localhost -p 6379 -d tinydb`

## Data types

 The supported data types are 
 - INT
 - VARCHAR
 - BOOL

## Example

```sql
CREATE TABLE t(a int, b varchar, c boolean);

INSERT INTO t(a, b, c) VALUES(42, 'tinydb', true);

SELECT * FROM t;

``` 

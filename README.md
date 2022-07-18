# tinydb
A database implementation from scratch in Rust for study purpose.

---
The most implementations is based on Postgresql implementation itself. Some design choices are:

- Buffer pool manager written from scratch (Tinydb don't use mmap)
- [LRU](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU) algorithm implementation for buffer victim 
- [Heap](https://en.wikipedia.org/wiki/Heap_(data_structure)) file format is used to store database files.
- [NULL values are handled using a bitmap](https://www.highgo.ca/2020/10/20/the-way-to-store-null-value-in-pg-record/) on heap tuple header. So each NULL value takes up only 1 bit of space.


## Building
Tinydb is develop in Rust, so it's necessary to have the [Rust build toolchain installed](https://www.rust-lang.org/tools/install).

Once you have installed the Rust toolchanin, just clone the repository, build the binary and run.

- `git clone https://github.com/msAlcantara/tinydb`
- `cargo run`

For now, tinydb is just a REPL, so `cargo run` will put it connected to the default database.


## Usage
For now, tinydb only support creating table and inserting values with an int type:

- `create table t (a int, b int, c int);`
- `insert into t(a, b, c) values (10, 20, 30);`
- `select * from t;`

## Next steps
- [ ] Make tinydb a client/server application
- [ ] Add support for b+tree indexes
- [ ] Add support for UPDATE
- [ ] Add support for DELETE
- [ ] Add support for basic data types such as string, float, booleans, etc

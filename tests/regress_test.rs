use std::{cell::RefCell, fs, io, path::Path, rc::Rc};

use tinydb::{
    catalog::pg_database,
    initdb::init_database,
    sql::{ConnectionExecutor, ExecutorConfig},
    storage::{smgr::StorageManager, BufferPool},
};

#[test]
fn test_regress() {
    let db_oid = pg_database::TINYDB_OID;

    let sql_entries = fs::read_dir(Path::new("tests").join("regress").join("sql"))
        .expect("Failed to read regress sql dir")
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()
        .expect("");

    let expected_path = Path::new("tests").join("regress").join("expected");
    let output_path = Path::new("tests").join("regress").join("output");

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir to regress tests");

    // TODO: Make the buffer pool configurable via SQL.
    let buffer = Rc::new(RefCell::new(BufferPool::new(
        5,
        StorageManager::new(&temp_dir.path()),
    )));

    // Create a default tinydb database.
    init_database(&mut buffer.borrow_mut(), &temp_dir.path())
        .expect("Failed init default database");
    let config = ExecutorConfig { database: db_oid };
    let mut conn_executor = ConnectionExecutor::new(config, buffer);

    for sql_file in sql_entries {
        let mut output = Vec::new();

        let sql_name = sql_file
            .file_name()
            .expect("Failed to get filename from sql file")
            .to_str()
            .expect("Failed to get name of sql file");

        let expected_sql =
            fs::read_to_string(expected_path.join(format!("{}.out", sql_name))).expect("");

        let sql = fs::read_to_string(&sql_file).expect("Failed to read expected sql file");
        for sql in sql.lines() {
            output.extend_from_slice(&sql.as_bytes().to_vec());
            if sql != "" {
                output.extend_from_slice("\n".as_bytes());
            }
            conn_executor
                .run(&mut output, &sql)
                .expect(&format!("failed to execute {}", sql));
        }

        let output =
            std::str::from_utf8(&output.as_slice()).expect("Failed to convert output to string");

        fs::write(output_path.join(sql_name), output).unwrap();

        assert_eq!(expected_sql, output, "Failed to match file {:?}", sql_file);
    }
}

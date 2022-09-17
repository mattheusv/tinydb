use std::{cell::RefCell, fs, io, path::Path, rc::Rc};

use tinydb::{catalog::pg_database, engine::Engine, initdb::init_database, storage::BufferPool};

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

    // TODO: Make the buffer pool configurable via SQL.
    let mut buffer = BufferPool::new(5);

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir to regress tests");
    let db_data = temp_dir
        .path()
        .to_str()
        .expect("Failed to convert temp dir to string")
        .to_string();

    // Create a default tinydb database.
    init_database(&mut buffer, &db_data).expect("Failed init default database");
    let mut engine = Engine::new(Rc::new(RefCell::new(buffer)), &db_data);

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
        for sql in sql.split_inclusive(";").collect::<Vec<&str>>() {
            output.extend_from_slice(&sql.as_bytes().to_vec());
            output.extend_from_slice("\n".as_bytes());
            engine
                .exec(&mut output, &sql, &db_oid)
                .expect(&format!("failed to execute {}", sql));
        }

        let output =
            std::str::from_utf8(&output.as_slice()).expect("Failed to convert output to string");

        fs::write(output_path.join(sql_name), output).unwrap();

        assert_eq!(expected_sql, output, "Failed to match file {:?}", sql_file);
    }
}

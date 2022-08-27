use std::{fs, io, path::Path};

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
    for sql_file in sql_entries {
        let mut buffer = BufferPool::new(120);

        // Create a default tinydb database.
        init_database(&mut buffer, &"data").expect("Failed init default database");

        let mut output = Vec::new();
        let mut engine = Engine::new(buffer, "data");

        let expected_sql = fs::read_to_string(expected_path.join(format!(
            "{}.out",
            sql_file
                .file_name()
                .expect("Failed to get filename from sql file")
                .to_str()
                .expect("Failed to get name of sql file")
        )))
        .expect("");

        let sql = fs::read_to_string(&sql_file).expect("Failed to read expected sql file");
        for sql in sql.split_inclusive(";").collect::<Vec<&str>>() {
            output.extend_from_slice(&sql.as_bytes().to_vec());
            engine.exec(&mut output, &sql, &db_oid).expect("");
        }

        let output =
            std::str::from_utf8(&output.as_slice()).expect("Failed to convert output to string");

        assert_eq!(expected_sql, output);
    }
}

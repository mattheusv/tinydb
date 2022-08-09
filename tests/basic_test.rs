use tinydb::{engine::Engine, initdb::init_database, storage::BufferPool};

#[test]
fn test_insert_select() {
    let default_db_name = "tinydb";

    let mut buffer = BufferPool::new(120);

    // Create a default tinydb database.
    init_database(&mut buffer, &"data", &default_db_name).expect("Failed init default database");

    let mut output = Vec::new();
    let mut engine = Engine::new(buffer, "data");

    engine
        .exec(
            &mut output,
            "CREATE TABLE t(a int, b int, c int);",
            default_db_name,
        )
        .expect("Error to create table");

    // Add value for all columns
    engine
        .exec(
            &mut output,
            "INSERT INTO t(a, b, c) VALUES(10, 20, 30)",
            default_db_name,
        )
        .expect("Error to insert data on table");

    // Add NULL values for some fields
    engine
        .exec(
            &mut output,
            "INSERT INTO t(a, c) VALUES(40, 50)",
            default_db_name,
        )
        .expect("Error to insert data on table");
    engine
        .exec(&mut output, "INSERT INTO t(b) VALUES(60)", default_db_name)
        .expect("Error to insert data on table");

    assert!(
        output.is_empty(),
        "Expected empty output after creating and inserting into table"
    );

    engine
        .exec(&mut output, "SELECT * FROM t;", default_db_name)
        .expect("Error to insert data on table");

    let output =
        std::str::from_utf8(&output.as_slice()).expect("Failed to convert output to string");
    let expected = "  a   |  b   |  c   
------+------+------
  10  |  20  |  30  
  40  | NULL |  50  
 NULL |  60  | NULL 

";
    assert_eq!(output, expected);
}

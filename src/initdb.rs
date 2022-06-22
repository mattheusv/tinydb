use std::{fs::create_dir_all, path::PathBuf};

use anyhow::Result;

/// Initialize a empty database at the db_data path using db_name as the database name.
pub fn init_database(db_data: &PathBuf, db_name: &str) -> Result<()> {
    let db_path = db_data.join(db_name);

    if !db_path.exists() {
        create_dir_all(db_path)?;
    }
    Ok(())
}

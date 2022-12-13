use std::{
    env, fs, io,
    path::Path,
    process::{Child, Command},
};

#[tokio::test]
async fn test_regress() -> anyhow::Result<()> {
    build()?;

    let sql_entries = fs::read_dir(Path::new("tests").join("regress").join("sql"))
        .expect("Failed to read regress sql dir")
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let expected_path = Path::new("tests").join("regress").join("expected");
    let output_path = Path::new("tests").join("regress").join("output");

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir to regress tests");

    // tinydb command will be killed when _tinydb is dropped.
    let _tinydb = TinyDBCommand::start(&temp_dir.path())?;

    // Wait the server to start completely.
    std::thread::sleep(std::time::Duration::from_millis(5));

    for sql_file in sql_entries {
        let mut output = Vec::new();

        let sql_name = sql_file
            .file_name()
            .expect("Failed to get filename from sql file")
            .to_str()
            .expect("Failed to get name of sql file");

        let expected_sql = fs::read_to_string(expected_path.join(format!("{}.out", sql_name)))?;

        let sql = fs::read_to_string(&sql_file)?;
        for sql in sql.lines() {
            if sql.is_empty() || !sql.ends_with(";") {
                continue;
            }
            output.extend_from_slice(&sql.as_bytes().to_vec());
            if sql != "" {
                output.extend_from_slice("\n".as_bytes());
            }

            let result = Command::new("psql")
                .arg("-h")
                .arg("localhost")
                .arg("-p")
                .arg("6379")
                .arg("-X")
                .arg("-d")
                .arg("tinydb")
                .arg("-c")
                .arg(sql)
                .output()?;
            output.extend_from_slice(&result.stdout);

            assert_eq!(
                result.stderr.len(),
                0,
                "Failed to execute psql: {}",
                std::str::from_utf8(&result.stderr.as_slice())?
            );
        }

        let output =
            std::str::from_utf8(&output.as_slice()).expect("Failed to convert output to string");

        fs::write(output_path.join(sql_name), output).unwrap();

        assert_eq!(expected_sql, output, "Failed to match file {:?}", sql_file);
    }

    Ok(())
}

fn build() -> anyhow::Result<()> {
    let mut child = Command::new("cargo").arg("build").spawn()?;
    let exit_status = child.wait()?;
    if !exit_status.success() {
        anyhow::bail!("failed to compile tinydb binary");
    }
    Ok(())
}

struct TinyDBCommand {
    cmd: Child,
}

impl TinyDBCommand {
    fn start(data_dir: &Path) -> anyhow::Result<Self> {
        let cmd = Command::new(
            env::current_dir()?
                .join("target")
                .join("debug")
                .join("tinydb"),
        )
        .arg("--init")
        .arg("--data-dir")
        .arg(data_dir)
        .spawn()?;

        Ok(Self { cmd })
    }
}

impl Drop for TinyDBCommand {
    fn drop(&mut self) {
        self.cmd.kill().expect("Failed to kill tinydb server");
    }
}

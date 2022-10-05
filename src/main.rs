use std::cell::RefCell;
use std::env;
use std::io;
use std::path::Path;
use std::rc::Rc;

use rustyline::error::ReadlineError;
use rustyline::Editor;
use tinydb::catalog::pg_database;
use tinydb::initdb::init_database;
use tinydb::sql::ConnectionExecutor;
use tinydb::sql::ExecutorConfig;
use tinydb::storage::BufferPool;

use structopt::StructOpt;
use tinydb::storage::smgr::StorageManager;

/// Command line arguments
#[derive(StructOpt)]
#[structopt()]
struct Flags {
    /// Silence all output
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Initialize the database directory.
    #[structopt(long = "init")]
    init: bool,

    /// Path to store database files.
    #[structopt(long = "data-dir", default_value = "data")]
    data_dir: String,

    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: usize,
}

fn main() {
    let flags = Flags::from_args();

    stderrlog::new()
        .module(module_path!())
        .quiet(flags.quiet)
        .verbosity(flags.verbose)
        .init()
        .unwrap();

    let default_db_name = "tinydb";

    let cwd = env::current_dir().expect("Failed to get current working directory");

    let data_dir = cwd.join(&flags.data_dir);

    let buffer = Rc::new(RefCell::new(BufferPool::new(
        120,
        StorageManager::new(&data_dir),
    )));

    if flags.init {
        init_database(&mut buffer.borrow_mut(), &data_dir).expect("Failed init default database");
    }

    let mut rl = Editor::<()>::new();
    if rl.load_history(&cwd.join("history.txt")).is_err() {
        println!("No previous history.");
    }

    env::set_current_dir(Path::new(&flags.data_dir)).unwrap();

    let config = ExecutorConfig {
        database: pg_database::TINYDB_OID,
    };
    let mut conn_executor = ConnectionExecutor::new(config, buffer);

    println!("Connected at {} database", default_db_name);

    let mut stdout = io::stdout();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if let Err(err) = conn_executor.run(&mut stdout, &line) {
                    eprintln!("Error: {:?}", err);
                    continue;
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(&cwd.join("history.txt")).unwrap();
}

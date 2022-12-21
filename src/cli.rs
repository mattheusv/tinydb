use structopt::StructOpt;

/// Command line arguments
#[derive(StructOpt)]
#[structopt()]
pub struct Flags {
    /// Silence all output
    #[structopt(short = "q", long = "quiet")]
    pub quiet: bool,

    /// Initialize the database directory.
    #[structopt(long = "init")]
    pub init: bool,

    /// Path to store database files.
    #[structopt(long = "data-dir", default_value = "data")]
    pub data_dir: String,

    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: usize,

    /// Host name or IP address to listen on.
    #[structopt(long = "hostname", default_value = "127.0.0.1")]
    pub hostname: String,

    /// Database server port.
    #[structopt(short = "p", long = "port", default_value = "6379")]
    pub port: u32,
}

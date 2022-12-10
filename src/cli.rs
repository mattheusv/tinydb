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
}

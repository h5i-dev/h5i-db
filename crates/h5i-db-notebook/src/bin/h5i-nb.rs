//! `h5i-nb`: the in-terminal notebook command line.

use clap::Parser;
use h5i_db_notebook::cli::{Cli, report_error, run};

fn main() {
    let cli = Cli::parse();
    let format = cli.format;

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error[internal]: could not start the async runtime: {error}");
            std::process::exit(5);
        }
    };

    let code = match runtime.block_on(run(cli)) {
        Ok(()) => 0,
        Err(error) => report_error(&error, format),
    };
    // Exit rather than fall out of main: the supervisor-spawning path leaves
    // background tasks that would otherwise delay teardown.
    std::process::exit(code);
}

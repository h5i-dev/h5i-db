//! `h5i-nb`: the in-terminal notebook command line.

#[cfg(not(unix))]
fn main() {
    // A binary that exists and explains itself, rather than a build that
    // fails or a command that is silently absent.
    eprintln!(
        "error[unsupported]: h5i-nb needs a Unix-like platform. The session \
         supervisor is built on Unix domain sockets, flock, and POSIX signals."
    );
    std::process::exit(3);
}

#[cfg(unix)]
use clap::Parser;
#[cfg(unix)]
use h5i_db_notebook::cli::{Cli, report_error, run};

#[cfg(unix)]
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

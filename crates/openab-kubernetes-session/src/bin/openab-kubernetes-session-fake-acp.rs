#[path = "../fake_acp.rs"]
mod fake_acp;

use std::io;

fn main() {
    std::panic::set_hook(Box::new(|_| {
        eprintln!("fake ACP terminated unexpectedly");
    }));

    if std::env::args_os().nth(1).is_some() {
        eprintln!("fake ACP does not accept arguments");
        std::process::exit(2);
    }

    let stdin = io::stdin();
    let stdout = io::stdout();
    if let Err(error) = fake_acp::run(stdin.lock(), stdout.lock()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

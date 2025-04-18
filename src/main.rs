mod storage;
mod bubble;
mod query;
mod schema;
mod server;
mod cli;
use std::thread;

fn main() {
    let server = server::Server::new(); 
    let cli_thread = thread::spawn(|| cli::start_cli());
    server.start(10).expect("failed to start server");
    let _ = cli_thread.join();
}


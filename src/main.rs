mod storage;
mod bubble;
mod query;
mod schema;
mod server;

fn main() {
    let server = server::Server::new(); 
    server.start().expect("failed to start server");
}


use rust_client::*;
use std::{io::{self, Write}, sync::{Arc, atomic::AtomicBool}};
use std::thread;
use std::time::Duration;
use crate::bubble::*;

pub fn start_cli(terminate : Arc<AtomicBool>) {
    thread::sleep(Duration::from_millis(100));
    let mut connection = Connection::new(String::from("127.0.0.1:4321")).expect("cli failed to connect to server");
    loop {
        print!("<d-bee>: ");
        io::stdout().flush().unwrap(); // Ensure the prompt is displayed before input
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read line");
        match input.trim() {
            "exit" => {
                terminate.swap(true, std::sync::atomic::Ordering::Relaxed);
                let _ = Connection::new(String::from("127.0.0.1:4321"));
                return;
            },
            _ => (),
        }
        match connection.query(input) {
            Ok(Some(mut res)) => {
                let bubble = Bubble::new(vec![10; res.row.len()].to_vec());
                println!("{}", bubble.get_divider());
                loop {
                    println!("{}", bubble.format_line(res.row.iter().map(|value| value.to_string()).collect()));
                    if !match connection.next(&mut res) {
                        Ok(val) => val,
                        _ => false,
                    } {
                        break;
                    }
                }
                println!("{}", bubble.get_divider());
            },
            Ok(None) => println!("success"),
            Err(e) => println!("{}", e),
        }
    }
}

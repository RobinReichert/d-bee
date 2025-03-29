use rust_client::*;
use std::{io::{self, Write}, sync::{Arc, atomic::AtomicBool}};
use crate::bubble::*;

pub fn start_cli(terminate : Arc<AtomicBool>) {
    loop {
        print!("<d-bee>: ");
        io::stdout().flush().unwrap(); // Ensure the prompt is displayed before input
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read line");
        match input.trim() {
            "exit" => {
                terminate.swap(true, std::sync::atomic::Ordering::Relaxed);
                let _ = query(String::new());
                return;
            },
            _ => (),
        }
        match query(input) {
            Ok(Some(mut res)) => {
                let bubble = Bubble::new(vec![10; res.row.len()].to_vec());
                println!("{}", bubble.get_divider());
                loop {
                    println!("{}", bubble.format_line(res.row.iter().map(|value| value.to_string()).collect()));
                    if !match next(&mut res) {
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

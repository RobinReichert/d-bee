use rust_client::*;
use std::io::{self, Write};
use std::thread;
use std::time::Duration;
use std::net::TcpStream;
use std::io::Read;
use crate::bubble::*;


const KEY : &[u8] = b"adminkey";

const NEW_DATABASE_FLAG : u8 = 0x02;
const GET_KEY_FLAG : u8 = 0x03;
const TERMINATE_FLAG : u8 = 0x04;

pub fn start_cli() {
    thread::sleep(Duration::from_millis(100));
    if let Ok(mut connection) = TcpStream::connect("127.0.0.1:4322") {
        if !connection.write_all(KEY).is_ok() {
            println!("not ok");
            return;
        }
        let mut buffer = [0u8; 512];
        let len = connection.read(&mut buffer).expect("failed to read from connection"); //errorhandling
                                                                                         //not done
        match buffer[..len] {
            [0] => (),
            _ => {
                println!("failed to connect to server");
                return;},
        }
        let mut database : Option<(String, Connection)> = None;
        let mut disconnect : bool = false;
        loop {
            if let Some((ref db, _)) = database {
                print!("<d-bee/{}>: ", db);
            }else{
                print!("<d-bee>: ");
            }
            let mut command = String::new();
            io::stdout().flush().unwrap(); // Ensure the prompt is displayed before input
            io::stdin().read_line(&mut command).expect("Failed to read line");
            command.truncate(command.len() - 1);
            if let Some((_, ref mut database_connection)) = database.as_mut() {
                match command.trim() {
                    "exit" => {
                        disconnect = true;
                    },
                    _ => {
                        match database_connection.query(command) {
                            Ok(Some(mut res)) => {
                                let bubble = Bubble::new(vec![10; res.row.len()].to_vec());
                                println!("{}", bubble.get_divider());
                                loop {
                                    println!("{}", bubble.format_line(res.row.iter().map(|value| value.to_string()).collect()));
                                    if !match database_connection.next(&mut res) {
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
                    },
                }
            }else{
                let tokens : Vec<&str> = command.split(" ").collect();
                match tokens[0] {
                    "connect" => {
                        if tokens.len() != 2 {
                            println!("wrong usage of connect. Use it like this: connect <database name>");
                            continue;
                        }
                        let database_name = tokens[1];
                        let mut message : Vec<u8> = vec![];
                        message.push(GET_KEY_FLAG);
                        message.extend(database_name.as_bytes());
                        if !connection.write_all(&message).is_ok() {
                            println!("failed to send request");
                            continue;
                        };
                        let mut buffer = vec![0; 1024];
                        if let Ok(len) = connection.read(&mut buffer) {
                            buffer.truncate(len);
                            if len < 1 {
                                println!("response from server was empty");
                                continue;
                            }
                            match buffer.remove(0) {
                                0 => {
                                    let key = String::from_utf8_lossy(&buffer);
                                    match Connection::new("127.0.0.1:4321".to_string(), database_name.to_string(), key.to_string()) { 
                                        Ok(database_connection) => database = Some((database_name.to_string(), database_connection)),
                                        Err(e) => println!("{}", e),
                                    }
                                },
                                1 => {println!("{}", String::from_utf8_lossy(&buffer));},
                                _ => {println!("invalid status code returned from server");},
                            }
                        }
                    },
                    "new" => {
                        if tokens.len() != 2 {
                            println!("wrong usgae of new. Use it like this: new <database name>");
                            continue;
                        }
                        let database_name = tokens[1];
                        let mut message : Vec<u8> = vec![];
                        message.push(NEW_DATABASE_FLAG);
                        message.extend(database_name.as_bytes());
                        if !connection.write_all(&message).is_ok() {
                            println!("failed to send request");
                            continue;
                        };
                        let mut buffer = vec![0; 1024];
                        if let Ok(len) = connection.read(&mut buffer) {
                            buffer.truncate(len);
                            if len < 1 {
                                println!("response from server was empty");
                                continue;
                            }
                            match buffer.remove(0) {
                                0 => {println!("{}", String::from_utf8_lossy(&buffer));},
                                1 => {println!("successfull");},
                                _ => {println!("invalid status code returned from server");},
                            }
                        }
                    },
                    "exit" => {
                        let _ = connection.write_all(&[TERMINATE_FLAG; 1]);
                        let _ = connection.flush();
                        return;
                    },
                    _ => {
                        println!("Invalid Token");
                        continue;
                    },
                }
            }
            if disconnect {
                if let Some((_, database_connection)) = database {
                    disconnect = false;
                    database_connection.close();
                    database = None;
                }
            }
        }
    }
    else {
        println!("failed to connect");
    }
}

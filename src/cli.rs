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

    //Sleep till server has started.
    thread::sleep(Duration::from_millis(100));

    //Try to connect to server on the port designated for admins. Otherwise print error.
    if let Ok(mut connection) = TcpStream::connect("127.0.0.1:4322") {

        //Authenticate as admin
        if !connection.write_all(KEY).is_ok() {
            println!("not ok");
            return;
        }

        //Check response and exit if authentication failed.
        let mut buffer = [0u8; 512];
        let len = connection.read(&mut buffer).expect("failed to read from connection");
        match buffer[..len] {
            [0] => (),
            _ => {
                println!("failed to connect to server");
                return;},
        }

        //Database is used for connection to one database.
        let mut database : Option<(String, Connection)> = None;

        //Disconnect is used to exit the connection to one database. This has to be done since a
        //reference to database is held while exit is called.
        let mut disconnect : bool = false;

        //Continuously print path to the terminal and wait for new inputs.
        loop {
            if let Some((ref db, _)) = database {
                print!("<d-bee/{}>: ", db);
            }else{
                print!("<d-bee>: ");
            }
            let mut command = String::new();
            io::stdout().flush().unwrap(); // Ensure the prompt is displayed before input
            io::stdin().read_line(&mut command).expect("Failed to read line");
            
            //remove <\n> character
            command.truncate(command.len() - 1);

            //Check if the CLI is currently connected to a specific database and than treat the
            //command accordingly.
            if let Some((_, ref mut database_connection)) = database.as_mut() {

                //When connected to a specific database check for the exit command, otherwise
                //forward the input to the server via the rust client.
                match command.trim() {
                    "exit" => {
                        disconnect = true;
                    },
                    _ => {
                        match database_connection.query(command) {
                            
                            //Print result as a bubble if there is one
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

                            //If the result is empty print success so the user is not confused
                            Ok(None) => println!("success"),
                            Err(e) => println!("{}", e),
                        }
                    },
                }
            }else{

                //While not connected to a specific database the input is split into tokens. The
                //first token defines the further behavior
                let tokens : Vec<&str> = command.split(" ").collect();
                match tokens[0] {
                    "connect" => {
                        //Valid length for a connection attempt is 2
                        if tokens.len() != 2 {
                            println!("wrong usage of connect. Use it like this: connect <database name>");
                            continue;
                        }

                        //The right key for the database is requested with admin privilege
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
                            //The key is constructed from the servers response or errors are
                            //ignored and input is skipped
                            if len < 1 {
                                println!("response from server was empty");
                                continue;
                            }
                            match buffer.remove(0) {
                                0 => {
                                    let key = String::from_utf8_lossy(&buffer);

                                    //Tries to set database to a rust client connection with the
                                    //requested key
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
                        //Valid length for new is 2
                        if tokens.len() != 2 {
                            println!("wrong usgae of new. Use it like this: new <database name>");
                            continue;
                        }

                        //Request for new database is sent to server
                        let database_name = tokens[1];
                        let mut message : Vec<u8> = vec![];
                        message.push(NEW_DATABASE_FLAG);
                        message.extend(database_name.as_bytes());
                        if !connection.write_all(&message).is_ok() {
                            println!("failed to send request");
                            continue;
                        };

                        //Response is handled
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

                        //The server is notified about exit command and handles shutdown gracefully
                        let _ = connection.write_all(&[TERMINATE_FLAG; 1]);
                        let _ = connection.flush();

                        //CLI stops printing any newlines
                        return;
                    },
                    _ => {

                        //Any invalid tokens are handled by notifying the user
                        println!("Invalid Token");
                        continue;
                    },
                }
            }

            //When a database connection should be ended the disconnect flag is set to true
            if disconnect {

                //Should always be some
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

#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

fn main() {    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                // Send 8 bytes of data to the client
                // 4 bytes of "size" with any value and 4 bytes of data representing num 7
                let data: [u8; 8] = [0, 0, 0, 8, 0, 0, 0, 7];
                _stream.write(&data).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

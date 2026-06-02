#![allow(unused_imports)]
use std::{io::{Write, Read}, net::TcpListener};


struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    // client_id: String,
    // tag_buffer: Vec<u8>,
}


fn parse_request_header(buffer: &[u8]) -> RequestHeaderV2 {
    // We need to ignore the msg size (first 4 bytes) and then read the next 8 bytes for the request header
    let request_api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
    let request_api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
    let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);

    RequestHeaderV2 {
        request_api_key,
        request_api_version,
        correlation_id,
    }
}

fn main() {    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                // Read request size
                let mut size_buffer: [u8; 4] = [0; 4];
                _stream.read(&mut size_buffer).unwrap();
                let request_size = u32::from_be_bytes(size_buffer);

                let mut buffer: Vec<u8> = vec![0; request_size as usize];
                _stream.read(&mut buffer).unwrap();

                let request_header = parse_request_header(&buffer);

                // Send 8 bytes of data to the client
                // 4 bytes of "size" with any value and 4 bytes of data representing num 7
                let message_size_data: [u8; 4] = [0, 0, 0, 0];
                let correlation_id_data: [u8; 4] = request_header.correlation_id.to_be_bytes();
                _stream.write(&message_size_data).unwrap();
                _stream.write(&correlation_id_data).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

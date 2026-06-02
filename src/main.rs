use std::{io::Write, net::TcpListener};
use std::io::Read;
use bytes::{BufMut, BytesMut};

trait KafkaEncode {
    fn encode(&self, buf: &mut BytesMut);
}

fn write_response(stream: &mut impl Write, correlation_id: i32, response: &dyn KafkaEncode) {
    let mut body = BytesMut::new();
    response.encode(&mut body);

    let size: [u8; 4] = [0, 0, 0, 0];
    stream.write_all(&size).unwrap();
    stream.write_all(&correlation_id.to_be_bytes()).unwrap();
    stream.write_all(&body).unwrap();
}

struct RequestHeaderV2 {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
}

fn parse_request_header(buffer: &[u8]) -> RequestHeaderV2 {
    let request_api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
    let request_api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
    let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
    RequestHeaderV2 { request_api_key, request_api_version, correlation_id }
}

struct ErrorResponse {
    error_msg: String,
}

impl KafkaEncode for ErrorResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(35); // Assuming 35 is the error code for unsupported API key
        buf.extend_from_slice(self.error_msg.as_bytes());
    }
}

struct ApiVersionsResponse {
    error_code: i16,
}

impl KafkaEncode for ApiVersionsResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.error_code);
    }
}

fn handle_api_versions_request(version: i16) -> ApiVersionsResponse {
    let error_code = if version < 0 || version > 4 { 35 } else { 0 };
    ApiVersionsResponse { error_code }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // Read the request size (4 bytes)
                let mut size_buffer = [0u8; 4];
                stream.read_exact(&mut size_buffer).unwrap();
                let request_size = u32::from_be_bytes(size_buffer);
                
                // Read the rest of the request based on the size
                let mut buffer = vec![0u8; request_size as usize];
                stream.read_exact(&mut buffer).unwrap();

                // Parse the request header
                let header = parse_request_header(&buffer);

                // Handle the request based on the API key
                let response: Box<dyn KafkaEncode> = match header.request_api_key {
                    18 => Box::new(handle_api_versions_request(header.request_api_version)),
                    key => {
                        println!("Unsupported API key: {key}");
                        Box::new(ErrorResponse { error_msg: "Unsupported API key".into() })
                    }
                };

                write_response(&mut stream, header.correlation_id, response.as_ref());
            }
            Err(e) => println!("error: {e}"),
        }
    }
}

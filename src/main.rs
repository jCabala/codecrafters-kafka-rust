use std::{io::Write, net::TcpListener};
use std::io::Read;
use bytes::{BufMut, BytesMut};

trait KafkaEncode {
    fn encode(&self, buf: &mut BytesMut);
}

fn write_response(stream: &mut impl Write, correlation_id: i32, response: &dyn KafkaEncode) {
    let mut body = BytesMut::new();
    response.encode(&mut body);

    let size = (4 + body.len()) as i32; // correlation_id (4) + body
    stream.write_all(&size.to_be_bytes()).unwrap();
    stream.write_all(&correlation_id.to_be_bytes()).unwrap();
    stream.write_all(&body).unwrap();
}

#[derive(Clone, Copy)]
enum ApiKey {
    ApiVersions = 18,
}

impl ApiKey {
    fn version_range(self) -> (i16, i16) {
        match self {
            ApiKey::ApiVersions => (0, 4),
        }
    }

    fn all() -> &'static [ApiKey] {
        &[ApiKey::ApiVersions]
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            other => Err(other),
        }
    }
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

struct ApiKeyEntry {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl KafkaEncode for ApiKeyEntry {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.min_version);
        buf.put_i16(self.max_version);
        buf.put_u8(0); // TAG_BUFFER
    }
}

struct ApiVersionsResponse {
    error_code: i16,
    api_keys: Vec<ApiKeyEntry>,
    throttle_time_ms: i32,
}

impl KafkaEncode for ApiVersionsResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.error_code);
        // COMPACT_ARRAY: length encoded as (n + 1) unsigned varint
        buf.put_u8(self.api_keys.len() as u8 + 1);
        for entry in &self.api_keys {
            entry.encode(buf);
        }
        buf.put_i32(self.throttle_time_ms);
        buf.put_u8(0); // TAG_BUFFER
    }
}

fn handle_api_versions_request(version: i16) -> ApiVersionsResponse {
    let error_code = if version < 0 || version > 4 { 35 } else { 0 };
    let api_keys = ApiKey::all()
        .iter()
        .map(|&key| {
            let (min_version, max_version) = key.version_range();
            ApiKeyEntry { api_key: key as i16, min_version, max_version }
        })
        .collect();
    ApiVersionsResponse { error_code, api_keys, throttle_time_ms: 0 }
}


fn handle_connection(mut stream: std::net::TcpStream) {
    loop {
        let mut size_buffer = [0u8; 4];
        if stream.read_exact(&mut size_buffer).is_err() {
            break;
        }
        let request_size = u32::from_be_bytes(size_buffer);

        let mut buffer = vec![0u8; request_size as usize];
        if stream.read_exact(&mut buffer).is_err() {
            break;
        }

        let header = parse_request_header(&buffer);

        let response: Box<dyn KafkaEncode> = match ApiKey::try_from(header.request_api_key) {
            Ok(ApiKey::ApiVersions) => Box::new(handle_api_versions_request(header.request_api_version)),
            Err(key) => {
                println!("Unsupported API key: {key}");
                Box::new(ErrorResponse { error_msg: "Unsupported API key".into() })
            }
        };

        write_response(&mut stream, header.correlation_id, response.as_ref());
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || handle_connection(stream));
            }
            Err(e) => println!("error: {e}"),
        }
    }
}

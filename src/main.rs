use std::{io::{Read, Write}, net::TcpListener};
use bytes::{BufMut, BytesMut};

mod endpoints;
use endpoints::api_versions::handle_api_versions_request;
use endpoints::describe_topic_partitions::{handle_topic_partitions_request, parse_describe_topic_partitions_request};

pub trait KafkaEncode {
    fn encode(&self, buf: &mut BytesMut);
}

#[derive(Clone, Copy)]
pub enum ApiKey {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

impl ApiKey {
    pub fn version_range(self) -> (i16, i16) {
        match self {
            ApiKey::ApiVersions => (0, 4),
            ApiKey::DescribeTopicPartitions => (0, 0),
        }
    }

    pub fn all() -> &'static [ApiKey] {
        &[ApiKey::ApiVersions, ApiKey::DescribeTopicPartitions]
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            18 => Ok(ApiKey::ApiVersions),
            75 => Ok(ApiKey::DescribeTopicPartitions),
            other => Err(other),
        }
    }
}

#[allow(dead_code)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tag_buffer: Vec<u8>,
}

pub fn parse_request_header(buffer: &[u8]) -> (RequestHeaderV2, usize) {
    let request_api_key = i16::from_be_bytes([buffer[0], buffer[1]]);
    let request_api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
    let correlation_id = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
    let client_id_length = buffer[8] as usize;
    let client_id = if client_id_length > 0 {
        Some(String::from_utf8_lossy(&buffer[9..9 + client_id_length]).to_string())
    } else {
        None
    };
    let tag_buffer_start = 9 + client_id_length;
    let tag_buffer_length = buffer[tag_buffer_start] as usize;
    let tag_buffer = buffer[tag_buffer_start + 1..tag_buffer_start + 1 + tag_buffer_length].to_vec();
    (
        RequestHeaderV2 { request_api_key, request_api_version, correlation_id, client_id, tag_buffer },
        tag_buffer_start + 1 + tag_buffer_length,
    )
}

struct ErrorResponse;

impl KafkaEncode for ErrorResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(35);
    }
}

fn write_response(stream: &mut impl Write, correlation_id: i32, response: &dyn KafkaEncode) {
    let mut body = BytesMut::new();
    response.encode(&mut body);

    let size = (5 + body.len()) as i32; // correlation_id (4) + tag_buffer (1) + body
    stream.write_all(&size.to_be_bytes()).unwrap();
    stream.write_all(&correlation_id.to_be_bytes()).unwrap();
    stream.write_all(&[0]).unwrap(); // empty TAG_BUFFER for response header v0
    stream.write_all(&body).unwrap();
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

        let (header, body_offset) = parse_request_header(&buffer);

        let response: Box<dyn KafkaEncode> = match ApiKey::try_from(header.request_api_key) {
            Ok(ApiKey::ApiVersions) => Box::new(handle_api_versions_request(header.request_api_version)),
            Ok(ApiKey::DescribeTopicPartitions) => {
                let request = parse_describe_topic_partitions_request(&buffer, body_offset);
                Box::new(handle_topic_partitions_request(request))
            }
            Err(key) => {
                println!("Unsupported API key: {key}");
                Box::new(ErrorResponse)
            }
        };

        write_response(&mut stream, header.correlation_id, response.as_ref());
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => { std::thread::spawn(move || handle_connection(stream)); }
            Err(e) => println!("error: {e}"),
        }
    }
}

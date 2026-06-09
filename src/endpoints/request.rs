#[allow(dead_code)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tag_buffer: Vec<u8>,
}

pub fn parse_request_header(buffer: &[u8]) -> (RequestHeader, usize) {
    let request_api_key     = i16::from_be_bytes([buffer[0], buffer[1]]);
    let request_api_version = i16::from_be_bytes([buffer[2], buffer[3]]);
    let correlation_id      = i32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
    let client_id_length    = i16::from_be_bytes([buffer[8], buffer[9]]);
    let (client_id, tag_buffer_start) = if client_id_length < 0 {
        (None, 10)
    } else {
        let len = client_id_length as usize;
        (Some(String::from_utf8_lossy(&buffer[10..10 + len]).to_string()), 10 + len)
    };
    let tag_buffer_length = buffer[tag_buffer_start] as usize;
    let tag_buffer = buffer[tag_buffer_start + 1..tag_buffer_start + 1 + tag_buffer_length].to_vec();
    (
        RequestHeader { request_api_key, request_api_version, correlation_id, client_id, tag_buffer },
        tag_buffer_start + 1 + tag_buffer_length,
    )
}

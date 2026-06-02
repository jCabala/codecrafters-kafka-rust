use bytes::{BufMut, BytesMut};
use crate::{ApiKey, KafkaEncode};

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

pub struct ApiVersionsResponse {
    error_code: i16,
    api_keys: Vec<ApiKeyEntry>,
    throttle_time_ms: i32,
}

impl KafkaEncode for ApiVersionsResponse {
    fn response_header_version(&self) -> u8 { 0 }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.error_code);
        buf.put_u8(self.api_keys.len() as u8 + 1); // COMPACT_ARRAY: n+1
        for entry in &self.api_keys {
            entry.encode(buf);
        }
        buf.put_i32(self.throttle_time_ms);
        buf.put_u8(0); // TAG_BUFFER
    }
}

pub fn handle_api_versions_request(version: i16) -> ApiVersionsResponse {
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

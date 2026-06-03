use bytes::{BufMut, BytesMut};
use crate::protocol_types::api_key::ApiKey;
use crate::protocol_types::kafka_encode::KafkaEncode;

pub struct ApiKeyEntry {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
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
    pub error_code: i16,
    pub api_keys: Vec<ApiKeyEntry>,
    pub throttle_time_ms: i32,
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

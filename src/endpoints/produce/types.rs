use bytes::{BufMut, BytesMut};
use crate::endpoints::encode::KafkaEncode;

// ── Request ───────────────────────────────────────────────────────────────────

pub struct ProducePartitionData {
    pub index: i32,
}

pub struct ProduceTopicData {
    pub name: String,
    pub partitions: Vec<ProducePartitionData>,
}

pub struct ProduceRequest {
    pub topics: Vec<ProduceTopicData>,
}

// ── Response ──────────────────────────────────────────────────────────────────

struct PartitionResponse {
    index: i32,
    error_code: i16,
    base_offset: i64,
    log_start_offset: i64,
}

pub(super) struct TopicResponse {
    name: String,
    partitions: Vec<PartitionResponse>,
}

impl TopicResponse {
    pub(super) fn found(topic: &ProduceTopicData) -> Self {
        Self {
            name: topic.name.clone(),
            partitions: topic.partitions.iter().map(|p| PartitionResponse {
                index: p.index,
                error_code: 0,
                base_offset: 0,
                log_start_offset: 0,
            }).collect(),
        }
    }

    pub(super) fn not_found(topic: &ProduceTopicData) -> Self {
        Self {
            name: topic.name.clone(),
            partitions: topic.partitions.iter().map(|p| PartitionResponse {
                index: p.index,
                error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                base_offset: -1,
                log_start_offset: -1,
            }).collect(),
        }
    }
}

pub struct ProduceResponse {
    topics: Vec<TopicResponse>,
}

impl ProduceResponse {
    pub(super) fn new(topics: Vec<TopicResponse>) -> Self {
        Self { topics }
    }
}

impl KafkaEncode for ProduceResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.topics.len() as u8 + 1); // COMPACT_ARRAY: n+1
        for topic in &self.topics {
            buf.put_u8(topic.name.len() as u8 + 1); // COMPACT_STRING: n+1
            buf.extend_from_slice(topic.name.as_bytes());
            buf.put_u8(topic.partitions.len() as u8 + 1); // COMPACT_ARRAY: n+1
            for p in &topic.partitions {
                buf.put_i32(p.index);
                buf.put_i16(p.error_code);
                buf.put_i64(p.base_offset);
                buf.put_i64(-1); // log_append_time_ms
                buf.put_i64(p.log_start_offset);
                buf.put_u8(1);   // record_errors: empty COMPACT_ARRAY
                buf.put_u8(0);   // error_message: null
                buf.put_u8(0);   // TAG_BUFFER
            }
            buf.put_u8(0); // TAG_BUFFER
        }
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0);  // TAG_BUFFER
    }
}

// ── Parsing ───────────────────────────────────────────────────────────────────

pub fn parse_produce_request(buffer: &[u8], body_offset: usize) -> ProduceRequest {
    let mut pos = body_offset;

    skip_compact_nullable_string(buffer, &mut pos); // transactional_id
    pos += 2; // acks: INT16
    pos += 4; // timeout_ms: INT32

    let topics_count = read_compact_array_len(buffer, &mut pos);
    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        let name = read_compact_string(buffer, &mut pos);

        let parts_count = read_compact_array_len(buffer, &mut pos);
        let mut partitions = Vec::with_capacity(parts_count);
        for _ in 0..parts_count {
            let index = read_i32(buffer, &mut pos);
            skip_compact_nullable_bytes(buffer, &mut pos); // records
            skip_tag_buffer(buffer, &mut pos);
            partitions.push(ProducePartitionData { index });
        }

        skip_tag_buffer(buffer, &mut pos);
        topics.push(ProduceTopicData { name, partitions });
    }

    ProduceRequest { topics }
}

fn read_unsigned_varint(data: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = data[*pos] as u64;
        *pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 { return result; }
        shift += 7;
    }
}

fn read_compact_array_len(data: &[u8], pos: &mut usize) -> usize {
    let n = read_unsigned_varint(data, pos) as usize;
    if n == 0 { 0 } else { n - 1 } // COMPACT_ARRAY stores n+1
}

fn read_i32(data: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    v
}

fn read_compact_string(data: &[u8], pos: &mut usize) -> String {
    let len = read_unsigned_varint(data, pos) as usize;
    if len == 0 { return String::new(); }
    let len = len - 1; // COMPACT_STRING stores n+1
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
    *pos += len;
    s
}

fn skip_compact_nullable_string(data: &[u8], pos: &mut usize) {
    let len = read_unsigned_varint(data, pos) as usize;
    if len > 0 { *pos += len - 1; }
}

fn skip_compact_nullable_bytes(data: &[u8], pos: &mut usize) {
    let len = read_unsigned_varint(data, pos) as usize;
    if len > 0 { *pos += len - 1; }
}

fn skip_tag_buffer(data: &[u8], pos: &mut usize) {
    let count = read_unsigned_varint(data, pos) as usize;
    for _ in 0..count {
        read_unsigned_varint(data, pos); // field_id
        let data_len = read_unsigned_varint(data, pos) as usize;
        *pos += data_len;
    }
}

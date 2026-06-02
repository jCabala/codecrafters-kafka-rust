use bytes::{BufMut, BytesMut};
use crate::KafkaEncode;

pub struct ReqTopic {
    pub name: String,
    pub tag_buffer: Vec<u8>,
}

pub struct DescribeTopicPartitionsRequest {
    pub topics_array: Vec<ReqTopic>,
}

struct RespPartition {}

struct RespTopic {
    error_code: i16,
    topic_name: String,
    topic_id: uuid::Uuid,
    is_internal: bool,
    partitions: Vec<RespPartition>,
    topic_authorized_operations: i32,
}

pub struct DescribeTopicPartitionsResponse {
    topics: Vec<RespTopic>,
}

impl KafkaEncode for DescribeTopicPartitionsResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(self.topics.len() as u8 + 1); // COMPACT_ARRAY: n+1
        for topic in &self.topics {
            buf.put_i16(topic.error_code);
            buf.put_u8(topic.topic_name.len() as u8 + 1); // COMPACT_NULLABLE_STRING: n+1
            buf.extend_from_slice(topic.topic_name.as_bytes());
            buf.extend_from_slice(topic.topic_id.as_bytes());
            buf.put_u8(topic.is_internal as u8);
            buf.put_u8(topic.partitions.len() as u8 + 1); // COMPACT_ARRAY: n+1
            buf.put_i32(topic.topic_authorized_operations);
            buf.put_u8(0); // TAG_BUFFER
        }
        buf.put_u8(0xFF); // next_cursor: null
        buf.put_u8(0); // TAG_BUFFER
    }
}

pub fn parse_describe_topic_partitions_request(buffer: &[u8], body_offset: usize) -> DescribeTopicPartitionsRequest {
    let topics_array_length = buffer[body_offset] as usize - 1; // COMPACT_ARRAY: stored as n+1
    let mut offset = body_offset + 1;
    let mut topics_array = Vec::with_capacity(topics_array_length);
    for _ in 0..topics_array_length {
        let name_length = buffer[offset] as usize - 1; // COMPACT_STRING: stored as n+1
        offset += 1;
        let name = String::from_utf8_lossy(&buffer[offset..offset + name_length]).to_string();
        offset += name_length;
        let tag_buffer_length = buffer[offset] as usize;
        offset += 1;
        let tag_buffer = buffer[offset..offset + tag_buffer_length].to_vec();
        offset += tag_buffer_length;
        topics_array.push(ReqTopic { name, tag_buffer });
    }
    DescribeTopicPartitionsRequest { topics_array }
}

pub fn handle_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribeTopicPartitionsResponse {
    DescribeTopicPartitionsResponse {
        topics: request.topics_array.into_iter().map(|req_topic| RespTopic {
            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
            topic_name: req_topic.name,
            topic_id: uuid::Uuid::nil(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0,
        }).collect(),
    }
}

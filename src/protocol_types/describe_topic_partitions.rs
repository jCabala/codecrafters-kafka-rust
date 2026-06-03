use bytes::{BufMut, BytesMut};
use crate::protocol_types::kafka_encode::KafkaEncode;

pub struct ReqTopic {
    pub name: String,
    pub tag_buffer: Vec<u8>,
}

pub struct DescribeTopicPartitionsRequest {
    pub topics_array: Vec<ReqTopic>,
}

pub struct RespPartition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

pub struct RespTopic {
    pub error_code: i16,
    pub topic_name: String,
    pub topic_id: uuid::Uuid,
    pub is_internal: bool,
    pub partitions: Vec<RespPartition>,
    pub topic_authorized_operations: i32,
}

pub struct DescribeTopicPartitionsResponse {
    pub topics: Vec<RespTopic>,
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
            for p in &topic.partitions {
                buf.put_i16(p.error_code);
                buf.put_i32(p.partition_index);
                buf.put_i32(p.leader_id);
                buf.put_i32(p.leader_epoch);
                buf.put_u8(p.replica_nodes.len() as u8 + 1);
                for &r in &p.replica_nodes { buf.put_i32(r); }
                buf.put_u8(p.isr_nodes.len() as u8 + 1);
                for &r in &p.isr_nodes { buf.put_i32(r); }
                buf.put_u8(1); // eligible_leader_replicas: empty
                buf.put_u8(1); // last_known_elr: empty
                buf.put_u8(1); // offline_replicas: empty
                buf.put_u8(0); // TAG_BUFFER
            }
            buf.put_i32(topic.topic_authorized_operations);
            buf.put_u8(0); // TAG_BUFFER
        }
        buf.put_u8(0xFF); // next_cursor: null
        buf.put_u8(0);    // TAG_BUFFER
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

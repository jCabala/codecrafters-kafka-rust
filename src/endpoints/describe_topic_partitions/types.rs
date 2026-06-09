use bytes::{BufMut, BytesMut};
use uuid::Uuid;
use crate::endpoints::encode::KafkaEncode;
use crate::cluster::partition::Partition;
use crate::cluster::topic::Topic;

pub struct ReqTopic {
    pub name: String,
}

pub struct DescribeTopicPartitionsRequest {
    pub topics_array: Vec<ReqTopic>,
}

struct RespPartition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
}

impl From<&Partition> for RespPartition {
    fn from(p: &Partition) -> Self {
        Self {
            error_code: 0,
            partition_index: p.index(),
            leader_id: p.leader_id(),
            leader_epoch: p.leader_epoch(),
            replica_nodes: p.replica_nodes().to_vec(),
            isr_nodes: p.isr_nodes().to_vec(),
        }
    }
}

pub(super) struct RespTopic {
    error_code: i16,
    topic_name: String,
    topic_id: Uuid,
    is_internal: bool,
    partitions: Vec<RespPartition>,
    topic_authorized_operations: i32,
}

impl RespTopic {
    pub(super) fn found(name: String, topic: &Topic) -> Self {
        Self {
            error_code: 0,
            topic_name: name,
            topic_id: topic.id(),
            is_internal: false,
            partitions: topic.partitions().iter().map(RespPartition::from).collect(),
            topic_authorized_operations: 0,
        }
    }

    pub(super) fn not_found(name: String) -> Self {
        Self {
            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
            topic_name: name,
            topic_id: Uuid::nil(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0,
        }
    }
}

pub struct DescribeTopicPartitionsResponse {
    topics: Vec<RespTopic>,
}

impl DescribeTopicPartitionsResponse {
    pub(super) fn new(topics: Vec<RespTopic>) -> Self {
        Self { topics }
    }
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
        offset += 1 + tag_buffer_length;
        topics_array.push(ReqTopic { name });
    }
    DescribeTopicPartitionsRequest { topics_array }
}

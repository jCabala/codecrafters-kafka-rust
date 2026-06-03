use crate::protocol_types::describe_topic_partitions::{
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, RespPartition, RespTopic,
};
use crate::protocol_types::metadata::parse_cluster_metadata;

const METADATA_LOG_PATH: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn handle_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribeTopicPartitionsResponse {
    let data = std::fs::read(METADATA_LOG_PATH).unwrap_or_default();
    let mut metadata = parse_cluster_metadata(&data);

    let topics = request.topics_array.into_iter().map(|req_topic| {
        match metadata.remove(&req_topic.name) {
            Some(topic_meta) => RespTopic {
                error_code: 0,
                topic_name: req_topic.name,
                topic_id: topic_meta.topic_id,
                is_internal: false,
                partitions: topic_meta.partitions.into_iter().map(|p| RespPartition {
                    error_code: 0,
                    partition_index: p.partition_index,
                    leader_id: p.leader_id,
                    leader_epoch: p.leader_epoch,
                    replica_nodes: p.replica_nodes,
                    isr_nodes: p.isr_nodes,
                }).collect(),
                topic_authorized_operations: 0,
            },
            None => RespTopic {
                error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                topic_name: req_topic.name,
                topic_id: uuid::Uuid::nil(),
                is_internal: false,
                partitions: vec![],
                topic_authorized_operations: 0,
            },
        }
    }).collect();

    DescribeTopicPartitionsResponse { topics }
}

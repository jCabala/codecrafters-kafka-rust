mod types;
pub use types::{parse_describe_topic_partitions_request, DescribeTopicPartitionsResponse};
use types::{DescribeTopicPartitionsRequest, RespTopic};

use crate::cluster::metadata::load_all;

pub fn handle_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribeTopicPartitionsResponse {
    let mut metadata = load_all();

    let topics = request.topics_array.into_iter().map(|req_topic| {
        match metadata.remove(&req_topic.name) {
            Some(topic) => RespTopic::found(req_topic.name, &topic),
            None => RespTopic::not_found(req_topic.name),
        }
    }).collect();

    DescribeTopicPartitionsResponse::new(topics)
}

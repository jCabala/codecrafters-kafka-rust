mod types;
pub use types::{parse_produce_request, ProduceResponse};
use types::{ProduceRequest, TopicResponse};

pub fn handle_produce_request(request: ProduceRequest) -> ProduceResponse {
    let topics = request.topics.iter().map(TopicResponse::from_request).collect();
    ProduceResponse::new(topics)
}

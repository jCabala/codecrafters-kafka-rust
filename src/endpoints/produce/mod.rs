mod types;
pub use types::{parse_produce_request, ProduceResponse};
use types::{ProduceRequest, TopicResponse};

pub fn handle_produce_request(request: ProduceRequest) -> ProduceResponse {
    let topics = request.topics.iter().map(TopicResponse::not_found).collect();
    ProduceResponse::new(topics)
}

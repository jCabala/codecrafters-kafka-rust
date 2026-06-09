use bytes::BytesMut;

pub trait KafkaEncode {
    fn encode(&self, buf: &mut BytesMut);
    fn response_header_version(&self) -> u8 { 1 }
}

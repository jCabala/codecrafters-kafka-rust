use bytes::{BufMut, BytesMut};
use crate::endpoints::encode::KafkaEncode;

pub struct ErrorResponse;

impl KafkaEncode for ErrorResponse {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(35);
    }
}

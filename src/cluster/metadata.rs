use std::collections::HashMap;
use uuid::Uuid;
use super::partition::Partition;
use super::topic::Topic;

const METADATA_LOG_PATH: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn load_all() -> HashMap<String, Topic> {
    let data = std::fs::read(METADATA_LOG_PATH).unwrap_or_default();
    parse(&data)
}

fn parse(data: &[u8]) -> HashMap<String, Topic> {
    let mut topic_names: HashMap<Uuid, String> = HashMap::new();
    let mut topic_ids: HashMap<String, Uuid> = HashMap::new();
    let mut partitions_by_topic: HashMap<Uuid, Vec<Partition>> = HashMap::new();

    let mut pos = 0;

    while pos + 12 <= data.len() {
        pos += 8; // baseOffset: INT64
        let batch_length = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let batch_end = pos + batch_length;
        if batch_end > data.len() { break; }

        pos += 4; // partitionLeaderEpoch: INT32
        let magic = data[pos]; pos += 1;

        if magic != 2 {
            pos = batch_end;
            continue;
        }

        pos += 4 + 2 + 4; // crc, attributes, lastOffsetDelta
        pos += 8 + 8 + 8; // baseTimestamp, maxTimestamp, producerId
        pos += 2 + 4;      // producerEpoch, baseSequence

        let records_count = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        for _ in 0..records_count {
            let rec_len = read_varint(data, &mut pos);
            let rec_end = pos + rec_len as usize;

            pos += 1;                       // attributes: INT8
            read_varlong(data, &mut pos);   // timestampDelta
            read_varint(data, &mut pos);    // offsetDelta

            let key_len = read_varint(data, &mut pos);
            if key_len > 0 { pos += key_len as usize; }

            let val_len = read_varint(data, &mut pos);

            if val_len > 0 {
                let _frame_version = data[pos]; pos += 1;
                let record_type    = data[pos]; pos += 1;
                let _version       = data[pos]; pos += 1;

                match record_type {
                    2 => { // TopicRecord
                        let name = read_compact_string(data, &mut pos);
                        let topic_id = read_uuid(data, &mut pos);
                        topic_names.insert(topic_id, name.clone());
                        topic_ids.insert(name, topic_id);
                        partitions_by_topic.entry(topic_id).or_default();
                    }
                    3 => { // PartitionRecord
                        let partition_index = read_i32(data, &mut pos);
                        let topic_id        = read_uuid(data, &mut pos);
                        let replica_nodes   = read_compact_array_i32(data, &mut pos);
                        let isr_nodes       = read_compact_array_i32(data, &mut pos);
                        let _removing       = read_compact_array_i32(data, &mut pos);
                        let _adding         = read_compact_array_i32(data, &mut pos);
                        let leader_id       = read_i32(data, &mut pos);
                        let leader_epoch    = read_i32(data, &mut pos);
                        partitions_by_topic.entry(topic_id).or_default().push(
                            Partition::new(partition_index, leader_id, leader_epoch, replica_nodes, isr_nodes)
                        );
                    }
                    _ => {}
                }
            }

            pos = rec_end;
        }

        pos = batch_end;
    }

    topic_ids.into_iter().filter_map(|(name, topic_id)| {
        let partitions = partitions_by_topic.remove(&topic_id).unwrap_or_default();
        Some((name, Topic::new(topic_id, partitions)))
    }).collect()
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

fn read_varint(data: &[u8], pos: &mut usize) -> i32 {
    let n = read_unsigned_varint(data, pos) as u32;
    ((n >> 1) as i32) ^ -((n & 1) as i32)
}

fn read_varlong(data: &[u8], pos: &mut usize) -> i64 {
    let n = read_unsigned_varint(data, pos);
    ((n >> 1) as i64) ^ -((n & 1) as i64)
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

fn read_compact_array_i32(data: &[u8], pos: &mut usize) -> Vec<i32> {
    let count = read_unsigned_varint(data, pos) as usize;
    if count == 0 { return vec![]; }
    let count = count - 1; // COMPACT_ARRAY stores n+1
    (0..count).map(|_| read_i32(data, pos)).collect()
}

fn read_uuid(data: &[u8], pos: &mut usize) -> Uuid {
    let bytes: [u8; 16] = data[*pos..*pos + 16].try_into().unwrap();
    *pos += 16;
    Uuid::from_bytes(bytes)
}

use std::collections::HashMap;
use uuid::Uuid;

pub struct PartitionMetadata {
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

pub struct TopicMetadata {
    pub topic_id: Uuid,
    pub partitions: Vec<PartitionMetadata>,
}

pub fn parse_cluster_metadata(data: &[u8]) -> HashMap<String, TopicMetadata> {

    let mut topic_names: HashMap<Uuid, String> = HashMap::new();
    let mut topic_ids: HashMap<String, Uuid> = HashMap::new();
    let mut partitions_by_topic: HashMap<Uuid, Vec<PartitionMetadata>> = HashMap::new();

    let mut pos = 0;

    while pos + 12 <= data.len() {
        // baseOffset: INT64
        pos += 8;
        // batchLength: INT32 (bytes remaining in batch after this field)
        let batch_length = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let batch_end = pos + batch_length;
        if batch_end > data.len() {
            break;
        }

        // partitionLeaderEpoch: INT32
        pos += 4;
        // magic: INT8
        let magic = data[pos];
        pos += 1;

        if magic != 2 {
            pos = batch_end;
            continue;
        }

        // crc: UINT32, attributes: INT16, lastOffsetDelta: INT32
        pos += 4 + 2 + 4;
        // baseTimestamp: INT64, maxTimestamp: INT64, producerId: INT64
        pos += 8 + 8 + 8;
        // producerEpoch: INT16, baseSequence: INT32
        pos += 2 + 4;

        let records_count = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        for _ in 0..records_count {
            let rec_len = read_varint(&data, &mut pos);
            let rec_end = pos + rec_len as usize;

            // attributes: INT8
            pos += 1;
            read_varlong(&data, &mut pos); // timestampDelta
            read_varint(&data, &mut pos);  // offsetDelta

            let key_len = read_varint(&data, &mut pos);
            if key_len > 0 {
                pos += key_len as usize;
            }

            let val_len = read_varint(&data, &mut pos);
            let val_start = pos;

            if val_len > 0 {
                let _frame_version = data[pos]; pos += 1;
                let record_type  = data[pos]; pos += 1;
                let _version     = data[pos]; pos += 1;

                match record_type {
                    2 => { // TopicRecord
                        let name = read_compact_string(&data, &mut pos);
                        let topic_id = read_uuid(&data, &mut pos);
                        topic_names.insert(topic_id, name.clone());
                        topic_ids.insert(name, topic_id);
                        partitions_by_topic.entry(topic_id).or_default();
                    }
                    3 => { // PartitionRecord
                        let partition_index = read_i32(&data, &mut pos);
                        let topic_id       = read_uuid(&data, &mut pos);
                        let replica_nodes  = read_compact_array_i32(&data, &mut pos);
                        let isr_nodes      = read_compact_array_i32(&data, &mut pos);
                        let _removing      = read_compact_array_i32(&data, &mut pos);
                        let _adding        = read_compact_array_i32(&data, &mut pos);
                        let leader_id      = read_i32(&data, &mut pos);
                        let leader_epoch   = read_i32(&data, &mut pos);
                        // skip partitionEpoch + any version-specific fields by jumping to val_end
                        partitions_by_topic.entry(topic_id).or_default().push(PartitionMetadata {
                            partition_index,
                            leader_id,
                            leader_epoch,
                            replica_nodes,
                            isr_nodes,
                        });
                    }
                    _ => {}
                }
            }

            pos = rec_end; // jump past value remainder + headers
        }

        pos = batch_end;
    }

    topic_ids.into_iter().filter_map(|(name, topic_id)| {
        let partitions = partitions_by_topic.remove(&topic_id).unwrap_or_default();
        Some((name, TopicMetadata { topic_id, partitions }))
    }).collect()
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn read_unsigned_varint(data: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = data[*pos] as u64;
        *pos += 1;
        result |= (byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return result;
        }
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
    if len == 0 {
        return String::new();
    }
    let len = len - 1; // COMPACT_STRING stores n+1
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
    *pos += len;
    s
}

fn read_compact_array_i32(data: &[u8], pos: &mut usize) -> Vec<i32> {
    let count = read_unsigned_varint(data, pos) as usize;
    if count == 0 {
        return vec![];
    }
    let count = count - 1; // COMPACT_ARRAY stores n+1
    (0..count).map(|_| read_i32(data, pos)).collect()
}

fn read_uuid(data: &[u8], pos: &mut usize) -> Uuid {
    let bytes: [u8; 16] = data[*pos..*pos + 16].try_into().unwrap();
    *pos += 16;
    Uuid::from_bytes(bytes)
}

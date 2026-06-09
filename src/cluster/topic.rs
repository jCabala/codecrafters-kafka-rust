use uuid::Uuid;
use super::partition::Partition;

pub struct Topic {
    id: Uuid,
    partitions: Vec<Partition>,
}

impl Topic {
    pub fn new(id: Uuid, partitions: Vec<Partition>) -> Self {
        Self { id, partitions }
    }

    pub fn id(&self) -> Uuid { self.id }
    pub fn partitions(&self) -> &[Partition] { &self.partitions }
}

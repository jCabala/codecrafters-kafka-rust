pub struct Partition {
    index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
}

impl Partition {
    pub fn new(
        index: i32,
        leader_id: i32,
        leader_epoch: i32,
        replica_nodes: Vec<i32>,
        isr_nodes: Vec<i32>,
    ) -> Self {
        Self { index, leader_id, leader_epoch, replica_nodes, isr_nodes }
    }

    pub fn index(&self) -> i32 { self.index }
    pub fn leader_id(&self) -> i32 { self.leader_id }
    pub fn leader_epoch(&self) -> i32 { self.leader_epoch }
    pub fn replica_nodes(&self) -> &[i32] { &self.replica_nodes }
    pub fn isr_nodes(&self) -> &[i32] { &self.isr_nodes }
}

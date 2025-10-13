use std::collections::HashMap;
use crate::Shared;
pub struct IncrLocalRefOp {
    pub incr: HashMap<u64,u64>,
    pub shared: Shared,
}

impl IncrLocalRefOp {
    pub fn run(&self) -> anyhow::Result<()>{
        for (k, v) in self.incr.iter() {
            self.shared.ref_count.increment_local_incremental_count(*k, *v);
        }
        Ok(())
    }
}
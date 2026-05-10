// use std::collections::HashMap;

// use anyhow::Ok;

// use crate::Shared;
// use crate::types::UpdateKvOp;

// pub struct UpdateKvMetaOp {
//     pub kv_meta: Vec<[u8; 32]>,
//     pub kv_ref: HashMap<[u8; 32], u64>,
//     pub update_op: Vec<UpdateKvOp>,
//     pub shared: Shared,
// }

// impl UpdateKvMetaOp {
//     pub async fn run(&self) -> anyhow::Result<()>{
//         // 更新本地kv元数据
//         for block in self.kv_meta.iter() {
//             self.shared.insert_global_kvcache(block.clone());
//         }
//         // 更新引用计数
//         for (k, v) in self.kv_ref.iter() {
//             self.shared.ref_count.increment_global_ref_count(*k,*v);
//         }
//         for op in self.update_op.iter(){
//             let res = self.shared.execute_update_kvop(op.clone()).await;
//             if let Err(e) = res {
//                 tracing::error!("UpdateKvMetaOp: Failed to execute update kv op: {:?}", e);
//                 return Err(e.into());
//             }
//         }

        
//         tracing::info!("UpdateKvMetaOp: Updated {} remote KV block metas and {} global block counts.",
//             self.kv_meta.len(), self.kv_ref.len());
//         Ok(())
//     }
// }
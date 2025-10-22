use std::collections::HashMap;

use anyhow::Ok;

use crate::Shared;
use crate::types::{KvBlockMeta, UpdateKvOp};

pub struct UploadKvMetaOp {
    pub kv_meta: Vec<KvBlockMeta>,
    pub kv_ref: HashMap<u64, u64>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub fn run(&self) -> anyhow::Result<()>{
        // 更新本地kv元数据
        for block in self.kv_meta.iter() {
            if block.phy_size == 0{
                let _ = Self::delete_replica(self, block.clone());
            }
            else{
                self.shared.insert_local_kvcache(block.clone());
            }
            
        }
        // 更新引用计数
        for (k, v) in self.kv_ref.iter() {
            self.shared.ref_count.insert_or_update_local_ref_count(*k, *v);
        }
        tracing::info!("UploadKvMetaOp: Uploaded {} local KV block metas and {} local block counts.",
            self.kv_meta.len(), self.kv_ref.len());
        Ok(())
    }

    pub fn delete_replica(&self, mut meta: KvBlockMeta)-> anyhow::Result<()>{
        let local_data_server = self.shared.config.local_data_server.clone();
        //判断server_socket中是否包含本地节点的ip和port，并删除该server_socket
        meta.server_id.retain(|s| *s != local_data_server.id );
        if !meta.server_id.is_empty(){
            self.shared.insert_remote_kvcache(meta.clone());
        }
        self.shared.remove_local_kvcache(meta.block_id);

        let update_op = UpdateKvOp{
            block_id: meta.block_id,
            operation: 2, //删除副本操作
            server_id: local_data_server.id,
        };
        self.shared.update_kvop_table.insert(meta.block_id, update_op);
        tracing::info!("UploadKvMetaOp: Deleted replica of block_id {}, remaining replicas: {}.",
            meta.block_id, meta.server_id.len());
        Ok(())
    }
}
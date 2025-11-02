use cedfs_proto::kvcache::UploadKvBlockMeta;

use anyhow::Ok;

use crate::Shared;

pub struct UploadKvMetaOp {
    pub kv_meta: Vec<UploadKvBlockMeta>,
    pub shared: Shared,
}

impl UploadKvMetaOp {
    pub fn run(&self) -> anyhow::Result<()>{
        // 更新本地kv元数据
        for block in self.kv_meta.iter() {
            // if block.phy_size == 0{
            //     let _ = Self::delete_replica(self, block.clone());
            // }
            // else{
            //     let block_id = self.shared.find_or_create_kv_block(block.token_hash, block.tokens.clone());
            //     self.shared.ref_count.increment_local_incremental_count(block_id, block.kv_ref);
            // }
            let block_id = self.shared.find_or_create_kv_block(block.token_hash, block.tokens.clone());
            self.shared.ref_count.increment_local_incremental_count(block_id, block.kv_ref);
            
        }
        tracing::info!("UploadKvMetaOp: Uploaded {} local KV block metas.",self.kv_meta.len());
        Ok(())
    }

    // pub fn delete_replica(&self, delete_meta: UploadKvBlockMeta)-> anyhow::Result<()>{
    //     let local_data_server = self.shared.config.local_data_server.clone();
    //     //判断server_socket中是否包含本地节点的ip和port，并删除该server_socket
    //     let block_id = self.shared.find_kv_block(delete_meta.token_hash, &delete_meta.tokens);
    //     if block_id.is_none(){
    //         tracing::warn!("UploadKvMetaOp: Tried to delete replica of non-existing block token_hash {}.", delete_meta.token_hash);
    //         return Ok(());
    //     }
    //     let block_id = block_id.unwrap();
    //     let meta = self.shared.get_local_kvcache(block_id);
    //     if meta.is_none(){
    //         tracing::warn!("UploadKvMetaOp: Tried to delete replica of non-existing local block_id {}.",
    //             block_id);
    //         return Ok(());
    //     }
    //     let mut meta = meta.unwrap();
    //     meta.server_id.retain(|s| *s != local_data_server.id );
    //     if !meta.server_id.is_empty(){
    //         self.shared.insert_remote_kvcache(meta.clone());
    //     }
    //     self.shared.remove_local_kvcache(meta.block_id);

    //     let update_op = UpdateKvOp{
    //         block_id: meta.block_id,
    //         operation: 2, //删除副本操作
    //         server_id: local_data_server.id,
    //     };
    //     self.shared.update_kvop_table.insert(meta.block_id, update_op);
    //     tracing::info!("UploadKvMetaOp: Deleted replica of block_id {}, remaining replicas: {}.",
    //         meta.block_id, meta.server_id.len());
    //     Ok(())
    // }
}
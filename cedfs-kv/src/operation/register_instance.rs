use anyhow::Ok;

use crate::Shared;
use crate::types::DataServer;

pub struct RegisterInstanceOp {
    pub data_server: DataServer,
    pub shared: Shared,
}

impl RegisterInstanceOp {
    pub async fn run(&self) -> anyhow::Result<()>{
        // 将data_server插入shared的数据服务器集合中
        let mut data_server_vec = self.shared.data_server_collect.write().await;
        // 避免重复插入相同id的数据服务器
        if !data_server_vec.iter().any(|ds| ds.id == self.data_server.id) {
            data_server_vec.push(self.data_server.clone());
        }
        Ok(())
    }
}
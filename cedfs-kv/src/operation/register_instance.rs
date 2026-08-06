use anyhow::Ok;

use crate::types::DataServer;
use crate::Shared;

pub struct RegisterInstanceOp {
    pub data_server: DataServer,
    pub shared: Shared,
}

impl RegisterInstanceOp {
    pub async fn run(&self) -> anyhow::Result<()> {
        let data_server_id = self.data_server.id;
        let meta_server_id = self.shared.config.local_meta_server.id;

        // 1. 将data_server插入本地数据服务器集合中
        {
            let mut local_data_servers = self.shared.local_data_server_collect.write().await;
            if let Some(existing) = local_data_servers
                .iter_mut()
                .find(|server| server.id == data_server_id)
            {
                *existing = self.data_server.clone();
                tracing::info!("Updated data_server {} endpoint", data_server_id);
            } else {
                local_data_servers.push(self.data_server.clone());
                tracing::info!(
                    "Added data_server {} to local_data_server_collect",
                    data_server_id
                );
            }
        }

        // 2. 将data_server插入全局数据服务器集合中（按meta_server_id分组）
        {
            self.shared.global_data_server_collect
                .entry(meta_server_id)
                .and_modify(|servers| {
                    if let Some(existing) =
                        servers.iter_mut().find(|server| server.id == data_server_id)
                    {
                        *existing = self.data_server.clone();
                    } else {
                        servers.push(self.data_server.clone());
                        tracing::info!(
                            "Added data_server {} to global_data_server_collect under meta_server {}",
                            data_server_id,
                            meta_server_id
                        );
                    }
                })
                .or_insert_with(|| {
                    tracing::info!(
                        "Created new entry in global_data_server_collect for meta_server {} with data_server {}",
                        meta_server_id,
                        data_server_id
                    );
                    vec![self.data_server.clone()]
                });
        }

        // 3. 建立data_server到meta_server的映射
        {
            self.shared
                .data_server_to_meta_server
                .insert(data_server_id, meta_server_id);
            tracing::info!(
                "Mapped data_server {} to meta_server {} in data_server_to_meta_server",
                data_server_id,
                meta_server_id
            );
        }

        tracing::info!(
            "Successfully registered data_server {} (model: {}) under meta_server {}",
            data_server_id,
            self.data_server.model_name,
            meta_server_id
        );

        Ok(())
    }
}

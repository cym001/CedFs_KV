//! 推理实例负载跟踪：记录每个实例当前未完成推理请求的 prompt 长度总和，用于最小负载调度

use crate::types::DataServer;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// 推理实例的负载跟踪器：key 为 "ip:http_port"，value 为当前未完成请求的 prompt 长度总和
#[derive(Debug, Default)]
pub struct InferenceLoadTracker {
    /// 每个推理实例当前未完成请求的 prompt 长度总和
    loads: DashMap<u32, AtomicU64>,
}

impl InferenceLoadTracker {
    pub fn new() -> Self {
        Self {
            loads: DashMap::new(),
        }
    }

    /// 增加某实例的未完成负载（新请求开始时调用）
    pub fn add_load(&self, server_key: u32, prompt_len: usize) {
        self.loads
            .entry(server_key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(prompt_len as u64, Ordering::Relaxed);
    }

    /// 减少某实例的未完成负载（请求完成或失败时调用）
    pub fn sub_load(&self, server_key: &u32, prompt_len: usize) {
        if let Some(load) = self.loads.get(server_key) {
            load.fetch_sub(prompt_len as u64, Ordering::Relaxed);
        }
    }

    /// 获取某实例当前未完成负载
    pub fn get_load(&self, server_key: &u32) -> u64 {
        self.loads
            .get(server_key)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 在候选实例中选出当前未完成负载最小的一个；若多个相同则取第一个
    pub fn select_server_with_min_load(&self, servers: &[DataServer]) -> Option<DataServer> {
        if servers.is_empty() {
            return None;
        }
        let mut min_load = u64::MAX;
        let mut selected: Option<DataServer> = None;
        for server in servers {
            let key = server.id;
            let load = self.get_load(&key);
            if load < min_load {
                min_load = load;
                selected = Some(server.clone());
            }
        }
        selected
    }

        
}

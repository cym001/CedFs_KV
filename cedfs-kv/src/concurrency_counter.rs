use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

/// 并发度记录项
#[derive(Debug, Clone)]
struct ConcurrencyEntry {
    /// 当前并发访问数量
    concurrent_count: usize,
    /// 该 token_hash 对应的副本数
    replica_count: u32,
}

impl ConcurrencyEntry {
    fn new() -> Self {
        Self {
            concurrent_count: 0,
            replica_count: 0,
        }
    }

    /// 增加一次并发访问（请求开始）
    fn add_access(&mut self) {
        self.concurrent_count = self.concurrent_count.saturating_add(1);
    }

    /// 减少一次并发访问（请求结束）
    fn remove_access(&mut self) {
        self.concurrent_count = self.concurrent_count.saturating_sub(1);
    }

    /// 获取当前并发访问数量
    fn get_concurrency(&self) -> usize {
        self.concurrent_count
    }

    /// 设置副本数
    fn set_replica_count(&mut self, count: u32) {
        self.replica_count = count;
    }

    /// 获取副本数
    fn get_replica_count(&self) -> u32 {
        self.replica_count
    }

    /// 是否应触发域内迁移：并发数 > 副本数*2 且副本数>0，且域内实例数 != 副本数（相等时不触发）
    pub fn should_trigger_migration(&self, intra_instance_count: u32) -> bool {
        self.replica_count > 0
            && self.concurrent_count >= (self.replica_count as usize).saturating_mul(2)
            && intra_instance_count != self.replica_count
    }
}

/// KV Cache 并发度统计器
#[derive(Debug)]
pub struct ConcurrencyCounter {
    intra_instance_count: u32,
    /// 存储每个 token_hash 的并发度与副本数信息
    counters: Arc<DashMap<[u8; 32], ConcurrencyEntry>>,
    // 是否迁移
    is_transfer: bool,
}

impl ConcurrencyCounter {
    /// 创建新的并发度统计器
    pub fn new(is_transfer: bool) -> Self {
        Self {
            intra_instance_count: 0,
            counters: Arc::new(DashMap::new()),
            is_transfer
        }
    }

    /// 增加指定 token_hash 的并发度，并更新其副本数。
    /// 若更新后满足 concurrent_count > replica_count*2 且 replica_count>0，返回 true 表示建议触发域内迁移。
    pub fn increment(&self, token_hash: [u8; 32], replica_count: u32) -> bool {
        self.counters
            .entry(token_hash)
            .and_modify(|entry| {
                entry.add_access();
                entry.set_replica_count(replica_count);
            })
            .or_insert_with(|| {
                let mut entry = ConcurrencyEntry::new();
                entry.add_access();
                entry.set_replica_count(replica_count);
                entry
            });
        if !self.is_transfer {
            return false;
        }
        self.counters
            .get(&token_hash)
            .map(|e| e.should_trigger_migration(self.intra_instance_count))
            .unwrap_or(false)
    }

    /// 批量增加多个 token_hash 的并发度，并更新各自副本数。
    /// 返回满足“建议触发域内迁移”条件的 token_hash 列表（concurrent_count > replica_count*2）。
    pub fn batch_increment(&self, items: &[([u8; 32], u32)]) -> Vec<[u8; 32]> {
        let mut to_migrate = Vec::new();
        for &(token_hash, replica_count) in items {
            if self.increment(token_hash, replica_count) {
                to_migrate.push(token_hash);
            }
        }
        to_migrate
    }

    /// 减少指定 token_hash 的并发度（请求结束时调用）
    pub fn decrement(&self, token_hash: [u8; 32]) {
        self.counters
            .entry(token_hash)
            .and_modify(|entry| entry.remove_access());
    }

    /// 获取指定 token_hash 的当前并发度
    ///
    /// # 参数
    /// - `token_hash`: 块的哈希值
    ///
    /// # 返回
    /// - 当前有效的并发度计数
    pub fn get_concurrency(&self, token_hash: [u8; 32]) -> usize {
        self.counters
            .get(&token_hash)
            .map(|entry| entry.get_concurrency())
            .unwrap_or(0)
    }

    /// 获取所有 token_hash 的并发度信息
    ///
    /// # 返回
    /// - Vec<(token_hash, concurrency)>: 所有块的并发度信息
    pub fn get_all_concurrency(&self) -> Vec<([u8; 32], usize)> {
        self.counters
            .iter()
            .map(|entry| (*entry.key(), entry.value().get_concurrency()))
            .filter(|(_, concurrency)| *concurrency > 0)
            .collect()
    }

    /// 移除并发度为 0 的条目（释放内存）
    fn cleanup_zero_entries(&self) {
        self.counters.retain(|_, entry| entry.get_concurrency() > 0);
    }

    /// 启动定期清理零并发条目的后台任务
    ///
    /// # 参数
    /// - `counter`: ConcurrencyCounter 的 Arc 引用
    /// - `cleanup_interval`: 清理间隔时间
    pub fn start_cleanup_task(counter: Arc<ConcurrencyCounter>, cleanup_interval: Duration) {
        tokio::spawn(async move {
            let mut interval_timer = time::interval(cleanup_interval);
            loop {
                interval_timer.tick().await;
                counter.cleanup_zero_entries();
                let active_entries = counter.counters.len();
                if active_entries > 0 {
                    tracing::debug!("ConcurrencyCounter: cleanup zero-concurrency entries, active entries: {}", active_entries);
                }
            }
        });
    }

    /// 获取统计信息
    ///
    /// # 返回
    /// - (total_entries, total_concurrency, max_concurrency): 
    ///   总条目数、总并发度、最大并发度
    pub fn get_statistics(&self) -> (usize, usize, usize) {
        let mut total_concurrency = 0;
        let mut max_concurrency = 0;
        let total_entries = self.counters.len();

        for entry in self.counters.iter() {
            let concurrency = entry.value().get_concurrency();
            total_concurrency += concurrency;
            max_concurrency = max_concurrency.max(concurrency);
        }

        (total_entries, total_concurrency, max_concurrency)
    }

    /// 清空所有并发度记录
    pub fn clear(&self) {
        self.counters.clear();
    }
}

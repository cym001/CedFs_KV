use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

/// 并发度记录项
#[derive(Debug, Clone)]
struct ConcurrencyEntry {
    /// 访问时间戳列表
    timestamps: Vec<Instant>,
}

impl ConcurrencyEntry {
    fn new() -> Self {
        Self {
            timestamps: Vec::new(),
        }
    }

    /// 添加一次访问记录
    fn add_access(&mut self) {
        self.timestamps.push(Instant::now());
    }

    /// 清理过期的访问记录（超过指定过期时间）
    fn cleanup_expired(&mut self, expiration: Duration) {
        let now = Instant::now();
        self.timestamps.retain(|&timestamp| now.duration_since(timestamp) < expiration);
    }

    /// 获取当前有效的并发度
    fn get_concurrency(&self) -> usize {
        self.timestamps.len()
    }
}

/// KV Cache 并发度统计器
#[derive(Debug)]
pub struct ConcurrencyCounter {
    /// 存储每个 token_hash 的并发度信息
    counters: Arc<DashMap<[u8; 32], ConcurrencyEntry>>,
    /// 过期时间
    expiration: Duration,
}

impl ConcurrencyCounter {
    /// 创建新的并发度统计器
    ///
    /// # 参数
    /// - `expiration`: 访问记录的过期时间
    pub fn new(expiration: Duration) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            expiration,
        }
    }

    /// 增加指定 token_hash 的并发度
    ///
    /// # 参数
    /// - `token_hash`: 块的哈希值
    pub fn increment(&self, token_hash: [u8; 32]) {
        self.counters
            .entry(token_hash)
            .and_modify(|entry| entry.add_access())
            .or_insert_with(|| {
                let mut entry = ConcurrencyEntry::new();
                entry.add_access();
                entry
            });
    }

    /// 批量增加多个 token_hash 的并发度
    ///
    /// # 参数
    /// - `token_hashes`: 块哈希值列表
    pub fn batch_increment(&self, token_hashes: &[[u8; 32]]) {
        for &token_hash in token_hashes {
            self.increment(token_hash);
        }
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

    /// 清理所有过期的访问记录
    fn cleanup_expired_entries(&self) {
        // 清理每个条目中的过期时间戳
        for mut entry in self.counters.iter_mut() {
            entry.value_mut().cleanup_expired(self.expiration);
        }

        // 移除没有任何有效访问记录的条目
        self.counters.retain(|_, entry| entry.get_concurrency() > 0);
    }

    /// 启动定期清理过期记录的后台任务
    ///
    /// # 参数
    /// - `counter`: ConcurrencyCounter 的 Arc 引用
    /// - `cleanup_interval`: 清理间隔时间
    ///
    /// # 示例
    /// ```
    /// let counter = Arc::new(ConcurrencyCounter::new(Duration::from_secs(5)));
    /// ConcurrencyCounter::start_cleanup_task(counter.clone(), Duration::from_secs(1));
    /// ```
    pub fn start_cleanup_task(counter: Arc<ConcurrencyCounter>, cleanup_interval: Duration) {
        tokio::spawn(async move {
            let mut interval_timer = time::interval(cleanup_interval);
            loop {
                interval_timer.tick().await;
                counter.cleanup_expired_entries();
                
                // 记录清理统计信息
                let active_entries = counter.counters.len();
                if active_entries > 0 {
                    tracing::debug!("ConcurrencyCounter: Cleaned up expired entries, active entries: {}", active_entries);
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_increment_and_get() {
        let counter = ConcurrencyCounter::new(Duration::from_secs(5));
        let token_hash = [1u8; 32];

        counter.increment(token_hash);
        assert_eq!(counter.get_concurrency(token_hash), 1);

        counter.increment(token_hash);
        assert_eq!(counter.get_concurrency(token_hash), 2);
    }

    #[tokio::test]
    async fn test_batch_increment() {
        let counter = ConcurrencyCounter::new(Duration::from_secs(5));
        let token_hashes = vec![[1u8; 32], [2u8; 32], [1u8; 32]];

        counter.batch_increment(&token_hashes);

        assert_eq!(counter.get_concurrency([1u8; 32]), 2);
        assert_eq!(counter.get_concurrency([2u8; 32]), 1);
    }

    #[tokio::test]
    async fn test_expiration() {
        let counter = ConcurrencyCounter::new(Duration::from_millis(100));
        let token_hash = [1u8; 32];

        counter.increment(token_hash);
        assert_eq!(counter.get_concurrency(token_hash), 1);

        // 等待过期
        sleep(Duration::from_millis(150)).await;
        counter.cleanup_expired_entries();

        assert_eq!(counter.get_concurrency(token_hash), 0);
    }

    #[tokio::test]
    async fn test_statistics() {
        let counter = ConcurrencyCounter::new(Duration::from_secs(5));
        
        counter.increment([1u8; 32]);
        counter.increment([1u8; 32]);
        counter.increment([2u8; 32]);

        let (total_entries, total_concurrency, max_concurrency) = counter.get_statistics();
        
        assert_eq!(total_entries, 2);
        assert_eq!(total_concurrency, 3);
        assert_eq!(max_concurrency, 2);
    }
}

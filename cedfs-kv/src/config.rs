use std::path::PathBuf;
use std::collections::HashMap;

use derive_builder::Builder;

use crate::{MetaServer};

#[derive(Debug, Builder)]
pub struct Config {
    pub loaded_config: config::Config,

    //本地节点信息
    pub local_meta_server: MetaServer,

    // kvcache元数据同步间隔，单位秒
    pub sync_interval: u64,     

    // 请求超时时间，单位ms
    pub request_timeout: u64,   

    // 副本主动拉取策略
    pub replica_pull: bool,

    // 副本拉取间隔，单位秒
    pub replica_pull_interval: u64,

    // 一次拉取的副本数量
    pub replica_pull_count: u64,

    // 日志级别
    pub log_level: String,

    // 日志文件路径
    pub log_file: String,

    // kv_block大小
    pub block_size: usize,

    // 是否支持不满的kv块
    pub unfull_chunk: bool,

    // 采用的hash算法
    pub hash_algorithm: String,

    // hash种子值
    pub hash_seed: u64,

    // model_name到tokenizer_path的映射
    pub model_tokenizer_map: HashMap<String, String>,

    // 请求调度方式
    pub scheduler_strategy: String,

    // 是否迁移KV Cache
    pub transfer_strategy: bool,

}

impl Config {
    pub fn build_with_config(path: PathBuf) -> Result<Self, ConfigError> {
        let config = Self::load_from_file(&path)?;
        
        // 从配置中提取各个字段        
        let local_meta_server: MetaServer = config.get("local_meta_server")
            .map_err(|e| ConfigError::MissingField(format!("local_meta_server: {}", e)))?;
        
        let sync_interval: u64 = config.get("sync_interval")
            .map_err(|e| ConfigError::MissingField(format!("sync_interval: {}", e)))?;
        
        let request_timeout: u64 = config.get("request_timeout")
            .map_err(|e| ConfigError::MissingField(format!("request_timeout: {}", e)))?;
        
        let replica_pull: bool = config.get("replica_pull")
            .map_err(|e| ConfigError::MissingField(format!("replica_pull: {}", e)))?;
        
        let replica_pull_interval: u64 = config.get("replica_pull_interval")
            .map_err(|e| ConfigError::MissingField(format!("replica_pull_interval: {}", e)))?;

        let replica_pull_count: u64 = config.get("replica_pull_count")
            .map_err(|e| ConfigError::MissingField(format!("replica_pull_count: {}", e)))?;
        
        let log_level: String = config.get("log_level")
            .map_err(|e| ConfigError::MissingField(format!("log_level: {}", e)))?;
        
        let log_file: String = config.get("log_file")
            .map_err(|e| ConfigError::MissingField(format!("log_file: {}", e)))?;

        let block_size: usize = config.get("block_size")
            .map_err(|e| ConfigError::MissingField(format!("block_size: {}", e)))?;

        let unfull_chunk: bool = config.get("unfull_chunk")
            .map_err(|e| ConfigError::MissingField(format!("unfull_chunk: {}", e)))?;

        let hash_algorithm: String = config.get("hash_algorithm")
            .map_err(|e| ConfigError::MissingField(format!("hash_algorithm: {}", e)))?;

        let hash_seed: u64 = config.get("hash_seed")
            .map_err(|e| ConfigError::MissingField(format!("hash_seed: {}", e)))?;

        let model_tokenizer_map: HashMap<String, String> = config.get("model_tokenizer_map")
            .map_err(|e| ConfigError::MissingField(format!("model_tokenizer_map: {}", e)))?;

        let scheduler_strategy: String = config.get("scheduler_strategy")
            .map_err(|e| ConfigError::MissingField(format!("scheduler_strategy: {}", e)))?;

        let transfer_strategy: bool = config.get("transfer_strategy")
            .map_err(|e| ConfigError::MissingField(format!("transfer_strategy: {}", e)))?;
        
        Ok(ConfigBuilder::default()
            .loaded_config(config)
            .local_meta_server(local_meta_server)
            .sync_interval(sync_interval)
            .request_timeout(request_timeout)
            .replica_pull(replica_pull)
            .replica_pull_interval(replica_pull_interval)
            .replica_pull_count(replica_pull_count)
            .log_level(log_level)
            .log_file(log_file)
            .block_size(block_size)
            .unfull_chunk(unfull_chunk)
            .hash_algorithm(hash_algorithm)
            .hash_seed(hash_seed)
            .model_tokenizer_map(model_tokenizer_map)
            .scheduler_strategy(scheduler_strategy)
            .transfer_strategy(transfer_strategy)
            .build()
            .map_err(|e| ConfigError::BuildError(e.to_string()))?)
    }
    
    fn load_from_file(path: &PathBuf) -> Result<config::Config, ConfigError> {
        config::Config::builder()
            .add_source(config::File::with_name(path.to_str().unwrap()))
            .build()
            .map_err(|e| ConfigError::LoadError(e.to_string()))
    }

    pub fn load_remote_meta_from_config(&self) -> Result<Vec<MetaServer>, ConfigError> {
        self.loaded_config
            .get::<Vec<MetaServer>>("remote_meta_servers")
            .map_err(|e| ConfigError::MissingField(format!("remote_meta_servers: {}", e)))
    }
}

// 自定义错误类型
#[derive(Debug)]
pub enum ConfigError {
    LoadError(String),
    MissingField(String),
    BuildError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::LoadError(msg) => write!(f, "配置加载失败: {}", msg),
            ConfigError::MissingField(msg) => write!(f, "缺少配置字段: {}", msg),
            ConfigError::BuildError(msg) => write!(f, "配置构建失败: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}
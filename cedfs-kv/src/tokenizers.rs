use std::collections::HashMap;
use std::time::Duration;
use std::{path::Path, sync::Arc};
use dashmap::DashMap;
use tokenizers::{Tokenizer,Result};
use tokio::task;
use tokio::time::timeout;

#[derive(Clone)]
pub struct TokenizerManager {
    pub tokenizers: Arc<DashMap<String, Tokenizer>>,
    pub model_tokenizer_map: Arc<HashMap<String, String>>,
}

impl TokenizerManager{
    pub fn new(model_tokenizer_map: HashMap<String, String>) -> Self {
        Self {
            tokenizers: Arc::new(DashMap::new()),
            model_tokenizer_map: Arc::new(model_tokenizer_map)
        }
    }

    /// 异步初始化，加载所有配置的tokenizer
    pub async fn new_with_preload(model_tokenizer_map: HashMap<String, String>) -> Self {
        let manager = Self::new(model_tokenizer_map.clone());
        
        tracing::info!("Preloading {} tokenizers from configuration", model_tokenizer_map.len());
        
        // 并发加载所有tokenizer
        let mut load_tasks = Vec::new();
        for (model_name, _) in model_tokenizer_map.iter() {
            let model_name = model_name.clone();
            let manager_clone = manager.clone();
            
            let task = tokio::spawn(async move {
                match manager_clone.load_tokenizer(&model_name).await {
                    Ok(_) => {
                        tracing::info!("Successfully preloaded tokenizer for '{}'", model_name);
                    }
                    Err(e) => {
                        tracing::error!("Failed to preload tokenizer for '{}': {}", model_name, e);
                    }
                }
            });
            
            load_tasks.push(task);
        }
        
        // 等待所有加载任务完成
        for task in load_tasks {
            if let Err(e) = task.await {
                tracing::error!("Tokenizer preload task failed: {}", e);
            }
        }
        
        tracing::info!("Tokenizer preloading completed. Loaded {}/{} tokenizers", 
            manager.tokenizers.len(), 
            model_tokenizer_map.len()
        );
        
        manager
    }

    
    pub fn get_tokenizer(&self, model_name:&str) -> Option<Tokenizer> {
        self.tokenizers
            .get(model_name)
            .map(|tokenizer| tokenizer.clone())
    }

    pub async fn load_tokenizer(&self, model_name: &str) -> Result<()> {
        // 尝试从配置的映射中获取tokenizer路径
        if let Some(tokenizer_path) = self.model_tokenizer_map.get(model_name) {
            // 如果配置中存在，从文件加载
            let tokenizer_path = tokenizer_path.clone();
            tracing::info!(
                "Loading tokenizer for '{}' from configured path: {}",
                model_name,
                tokenizer_path
            );

            // spawn_blocking: because loading Tokenizer is CPU & IO heavy
            let load_result = task::spawn_blocking(move || {
                load_tokenizer_core(&tokenizer_path)
            })
            .await;

            match load_result {
                Ok(Ok(tokenizer)) => {
                    // 成功从文件加载
                    tracing::info!(
                        "Successfully loaded tokenizer '{}' from file: {}",
                        model_name,
                        self.model_tokenizer_map.get(model_name).unwrap()
                    );
                    self.tokenizers.insert(model_name.to_string(), tokenizer);
                    Ok(())
                }
                Ok(Err(e)) => {
                    // 从文件加载失败，尝试从pretrained加载
                    tracing::warn!(
                        "Failed to load tokenizer '{}' from file: {}. Falling back to pretrained.",
                        model_name,
                        e
                    );
                    self.load_from_http(model_name).await
                }
                Err(e) => {
                    // spawn_blocking失败，尝试从pretrained加载
                    tracing::warn!(
                        "spawn_blocking failed for tokenizer '{}': {}. Falling back to pretrained.",
                        model_name,
                        e
                    );
                    self.load_from_http(model_name).await
                }
            }
        } else {
            // 如果配置中不存在，调用load_from_http
            tracing::info!(
                "No tokenizer path configured for '{}', loading from pretrained",
                model_name
            );
            self.load_from_http(model_name).await
        }
    }

    pub async fn load_from_http(
        &self,
        model_name: &str,
    ) -> Result<()> {
        tracing::info!("Loading tokenizer '{}' from pretrained", model_name);
        
        let model_name_clone = model_name.to_string();
        
        // 使用60秒超时
        let load_result = timeout(
            Duration::from_secs(60),
            task::spawn_blocking(move || {
                Tokenizer::from_pretrained(&model_name_clone, None)
            })
        ).await;

        match load_result {
            Ok(Ok(Ok(tokenizer))) => {
                // 成功加载
                tracing::info!("Successfully loaded tokenizer '{}' from pretrained", model_name);
                self.tokenizers.insert(model_name.to_string(), tokenizer);
                Ok(())
            }
            Ok(Ok(Err(e))) => {
                // tokenizer加载失败
                tracing::error!(
                    "Failed to load tokenizer '{}' from pretrained: {}",
                    model_name,
                    e
                );
                Err(e)
            }
            Ok(Err(e)) => {
                // spawn_blocking失败
                let error_msg = format!("spawn_blocking failed for tokenizer '{}': {}", model_name, e);
                tracing::error!("{}", error_msg);
                Err(tokenizers::Error::from(error_msg))
            }
            Err(_) => {
                // 超时
                let error_msg = format!(
                    "Timeout (60s) loading tokenizer '{}' from pretrained",
                    model_name
                );
                tracing::error!("{}", error_msg);
                Err(tokenizers::Error::from(error_msg))
            }
        }
    }

    /// Encode prompts into token IDs
    /// 
    /// # Arguments
    /// - `model_name`: The name of the model/tokenizer to use
    /// - `prompts`: The input text to tokenize
    /// 
    /// # Returns
    /// - `Ok(Vec<u32>)`: Token IDs as u32 vector
    /// - `Err`: If tokenizer not found or encoding fails
    pub fn encode(&self, model_name: &str, prompts: &str) -> Result<Vec<u32>> {
        // Get tokenizer from cache
        let tokenizer = self.tokenizers
            .get(model_name)
            .ok_or_else(|| {
                tokenizers::Error::from(format!(
                    "Tokenizer '{}' not found. Please load it first using load_tokenizer() or load_from_http()",
                    model_name
                ))
            })?;

        // Encode the text
        let encoding = tokenizer.encode(prompts, true)?;
        
        // Get token IDs 
        let tokens: Vec<u32> = encoding.get_ids().to_vec();

        Ok(tokens)
    }

    /// Encode prompts into token IDs (async version with spawn_blocking for heavy workloads)
    /// 
    /// # Arguments
    /// - `model_name`: The name of the model/tokenizer to use
    /// - `prompts`: The input text to tokenize
    /// 
    /// # Returns
    /// - `Ok(Vec<u32>)`: Token IDs as u32 vector
    /// - `Err`: If tokenizer not found or encoding fails
    pub async fn encode_async(&self, model_name: &str, prompts: &str) -> tokenizers::Result<Vec<u32>> {
        let tokenizer = self.tokenizers
            .get(model_name)
            .ok_or_else(|| {
                tokenizers::Error::from(format!(
                    "Tokenizer '{}' not found. Load it first via load_from_http()",
                    model_name
                ))
            })?
            .clone();
    
        let prompts = prompts.to_string();
    
        let tokens = tokio::task::spawn_blocking(move || {
            let encoding = tokenizer.encode(prompts, true)?;
            // get_ids() -> &[u32] 需要 clone 成 Vec<u32>
            Ok::<Vec<u32>, tokenizers::Error>(encoding.get_ids().to_vec())
        })
        .await
        .map_err(|e| tokenizers::Error::from(format!("Tokio join error: {}", e)))??;
    
        Ok(tokens)
    }

}

/// Try loading tokenizer in the same order as vLLM/HF
fn load_tokenizer_core(model_path: &str) -> Result<Tokenizer> {
    // 1. tokenizer.json (full fast tokenizer)
    let tok_json = format!("{}/tokenizer.json", model_path);
    if Path::new(&tok_json).exists() {
        return Tokenizer::from_file(tok_json);
    }

    // 2. sentencepiece
    let spm = format!("{}/tokenizer.model", model_path);
    if Path::new(&spm).exists() {
        return Tokenizer::from_file(spm);
    }

    // 3. vocab.json + merges.txt
    let vocab = Path::new(model_path).join("vocab.json");
    if vocab.exists() {
        return Tokenizer::from_file(vocab);
    }

    Err("No valid tokenizer files found".into())
}

use sha2::{Sha256, Digest};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use serde::Serialize;

/// 哈希算法类型
#[derive(Debug, Clone, Copy)]
pub enum HashAlgorithm {
    Builtin,
    Sha256,
    Sha256Cbor,
}

/// 哈希结果类型 - 可以是 64 位或 256 位
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HashValue {
    U64(u64),
    U256([u8; 32]),
}

impl HashValue {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            HashValue::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            HashValue::U64(_) => {
                panic!("Cannot get bytes from U64 hash")
            }
            HashValue::U256(bytes) => bytes,
        }
    }
    /// 将哈希值转换为 u64，如果是 U256 则截断前 8 字节
    pub fn to_u64(&self) -> u64 {
        match self {
            HashValue::U64(v) => *v,
            HashValue::U256(bytes) => {
                // 截断前 8 字节转换为 u64
                u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ])
            }
        }
    }

    /// 将哈希值转换为 [u8; 32]，如果是 U64 则扩展
    pub fn to_u256(&self) -> [u8; 32] {
        match self {
            HashValue::U256(bytes) => *bytes,
            HashValue::U64(v) => {
                // 将 u64 扩展为 32 字节，前 8 字节为 u64 的大端表示，其余填充 0
                let mut result = [0u8; 32];
                result[0..8].copy_from_slice(&v.to_be_bytes());
                result
            }
        }
    }
    
    pub fn as_u256(&self) -> Option<[u8; 32]> {
        match self {
            HashValue::U256(bytes) => Some(*bytes),
            _ => None,
        }
    }
}

/// Token 哈希器配置
pub struct TokenHasher {
    algorithm: HashAlgorithm,
    none_hash: HashValue,
    unfull_chunk: bool,
    seed: u64,
}

impl TokenHasher {
    /// 创建新的 TokenHasher
    pub fn new(algorithm: HashAlgorithm, unfull_chunk: bool, seed: u64) -> Self {
        let none_hash = Self::compute_none_hash(algorithm, seed);
        Self {
            algorithm,
            none_hash,
            unfull_chunk,
            seed,
        }
    }

    /// 使用默认的 builtin 算法创建
    pub fn default() -> Self {
        Self::new(HashAlgorithm::Builtin, false, 0)
    }

    /// 计算 NONE_HASH 的初始值
    fn compute_none_hash(algorithm: HashAlgorithm, seed: u64) -> HashValue {
        match algorithm {
            HashAlgorithm::Builtin => {
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                None::<u32>.hash(&mut hasher);
                HashValue::U64(hasher.finish())
            }
            HashAlgorithm::Sha256 | HashAlgorithm::Sha256Cbor => {
                let mut hasher = Sha256::new();
                hasher.update(seed.to_le_bytes());
                hasher.update(b"None");
                HashValue::U256(hasher.finalize().into())
            }
        }
    }

    /// 获取初始哈希值
    pub fn get_init_hash(&self) -> HashValue {
        self.none_hash.clone()
    }

    /// 对 token 序列进行哈希 (Vec<u32> 输入)
    /// 
    /// # 参数
    /// - `tokens`: token 序列
    /// - `prefix_hash`: 可选的前缀哈希值
    /// - `extra_keys`: 可选的额外键（用于多模态输入等）
    pub fn hash_tokens(
        &self,
        tokens: &[u32],
        prefix_hash: Option<&HashValue>,
        extra_keys: Option<&[String]>,
    ) -> HashValue {
        match self.algorithm {
            HashAlgorithm::Builtin => {
                self.builtin_hash(tokens, prefix_hash, extra_keys)
            }
            HashAlgorithm::Sha256 => {
                self.sha256_hash(tokens, prefix_hash, extra_keys)
            }
            HashAlgorithm::Sha256Cbor => {
                self.sha256_cbor_hash(tokens, prefix_hash, extra_keys)
            }
        }
    }

    /// 使用 Rust 内置的哈希函数
    fn builtin_hash(
        &self,
        tokens: &[u32],
        prefix_hash: Option<&HashValue>,
        extra_keys: Option<&[String]>,
    ) -> HashValue {
        let mut hasher = DefaultHasher::new();
        
        self.seed.hash(&mut hasher);
        prefix_hash.hash(&mut hasher);
        tokens.hash(&mut hasher);
        extra_keys.hash(&mut hasher);
        
        HashValue::U64(hasher.finish())
    }

    /// 使用 SHA256 哈希
    /// 
    /// 该函数模拟 Python 的行为：
    /// ```python
    /// def sha256(input: Any) -> bytes:
    ///     input_bytes = pickle.dumps(input, protocol=pickle.HIGHEST_PROTOCOL)
    ///     return hashlib.sha256(input_bytes).digest()
    /// 
    /// self.hash_func((prefix_hash, tokens_tuple, extra_keys))
    /// ```
    fn sha256_hash(
        &self,
        tokens: &[u32],
        prefix_hash: Option<&HashValue>,
        extra_keys: Option<&[String]>,
    ) -> HashValue {
        // 创建一个可序列化的结构来匹配 Python 的 tuple
        #[derive(Serialize)]
        struct HashInput<'a> {
            prefix_hash: Option<Vec<u8>>,
            tokens: &'a [u32],
            extra_keys: Option<&'a [String]>,
        }
        
        // 将 prefix_hash 转换为字节数组（如果存在）
        let prefix_bytes = prefix_hash.map(|hash| {
            match hash {
                HashValue::U64(v) => v.to_le_bytes().to_vec(),
                HashValue::U256(bytes) => bytes.to_vec(),
            }
        });
        
        let input = HashInput {
            prefix_hash: prefix_bytes,
            tokens,
            extra_keys,
        };
        
        // 使用 bincode 序列化（类似于 pickle）
        let serialized = bincode::serialize(&input).expect("Failed to serialize hash input");
        
        // 计算 SHA256
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let result = hasher.finalize();
        
        HashValue::U256(result.into())
    }

    /// 使用 SHA256 + CBOR 哈希（简化版本）
    fn sha256_cbor_hash(
        &self,
        tokens: &[u32],
        prefix_hash: Option<&HashValue>,
        extra_keys: Option<&[String]>,
    ) -> HashValue {
        let mut hasher = Sha256::new();
        
        hasher.update(b"seed:");
        hasher.update(self.seed.to_le_bytes());
        
        if let Some(hash) = prefix_hash {
            hasher.update(b"prefix:");
            match hash {
                HashValue::U64(v) => hasher.update(v.to_le_bytes()),
                HashValue::U256(bytes) => hasher.update(bytes),
            }
        }
        
        hasher.update(b"tokens:");
        for token in tokens {
            hasher.update(token.to_le_bytes());
        }
        
        if let Some(keys) = extra_keys {
            hasher.update(b"keys:");
            for key in keys {
                hasher.update(key.as_bytes());
            }
        }
        
        let result = hasher.finalize();
        HashValue::U256(result.into())
    }

    /// SHA256 分块迭代哈希（返回所有中间哈希值和偏移量）
    /// 
    /// 将输入 tokens 按照 block_size 分成若干块，迭代计算它们的哈希
    /// 返回每一步的哈希值和对应的偏移量（块结束位置）
    /// 
    /// # 参数
    /// - `tokens`: 输入的 token 序列
    /// - `block_size`: 每个块的大小
    /// 
    /// # 返回
    /// 包含每个块计算后的哈希值和偏移量的 Vec<(HashValue, offset)>
    /// offset 表示该块在 tokens 中的结束位置（不包含）
    pub fn hash_tokens_with_blocks_all(
        &self,
        tokens: &[u32],
        block_size: usize,
    ) -> Vec<(HashValue, u32)> {
        if block_size == 0 {
            panic!("block_size must be greater than 0");
        }

        if !matches!(self.algorithm, HashAlgorithm::Sha256) {
            panic!("Block-based hashing only supported for Sha256 algorithm");
        }

        let mut results = Vec::new();
        let mut current_hash = self.get_init_hash();
        
        let chunks: Vec<&[u32]> = tokens.chunks(block_size).collect();
        let total_chunks = chunks.len();
        
        for (idx, chunk) in chunks.into_iter().enumerate() {
            let is_last_chunk = idx == total_chunks - 1;
            let is_unfull_chunk = chunk.len() < block_size;
            
            // 如果是最后一个块且不完整，根据参数决定是否计算
            if is_last_chunk && is_unfull_chunk && !self.unfull_chunk {
                break;
            }
            
            current_hash = self.sha256_hash(chunk, Some(&current_hash), None);
            let offset = chunk.len() as u32;
            results.push((current_hash.clone(), offset));
        }
        
        results
    }

    /// 计算前缀哈希序列
    /// 
    /// 对每个 token chunk 进行增量哈希，返回每个步骤的哈希值
    pub fn prefix_hash<'a>(
        &'a self,
        token_chunks: impl Iterator<Item = &'a [u32]> + 'a,
    ) -> impl Iterator<Item = HashValue> + 'a {
        let mut prefix_hash = self.get_init_hash();
        
        token_chunks.map(move |chunk| {
            prefix_hash = self.hash_tokens(chunk, Some(&prefix_hash), None);
            prefix_hash.clone()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;



    #[test]
    fn test_hash_tokens_with_blocks_all() {
        let hasher = TokenHasher::new(HashAlgorithm::Sha256, false, 0);
        let tokens: Vec<u32> = vec![1, 2, 3, 4, 5, 6];
        
        let results = hasher.hash_tokens_with_blocks_all(&tokens, 2);
        
        // 应该有 3 个哈希值和偏移量 (6 tokens / 2 = 3 blocks)
        assert_eq!(results.len(), 3);
        
        // 验证偏移量
        assert_eq!(results[0].1, 2);  // 第一个块结束于位置 2
        assert_eq!(results[1].1, 2);  // 第二个块结束于位置 4
        assert_eq!(results[2].1, 2);  // 第三个块结束于位置 6
    }

    #[test]
    fn test_builtin_hash_u32() {
        let hasher = TokenHasher::new(HashAlgorithm::Builtin, false, 0);
        let tokens: Vec<u32> = vec![1, 2, 3];
        
        let hash = hasher.hash_tokens(&tokens, None, None);
        assert!(matches!(hash, HashValue::U64(_)));
    }
}
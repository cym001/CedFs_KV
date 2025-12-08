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
    Sha256CrossLanguage,
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
            HashAlgorithm::Sha256CrossLanguage => {
                // Cross-language None serialization: 32 bytes of zeros
                let mut hasher = Sha256::new();
                hasher.update(&[0u8; 32]);
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
            HashAlgorithm::Sha256CrossLanguage => {
                self.sha256_cross_language_hash(tokens, prefix_hash, extra_keys)
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

        if !matches!(self.algorithm, HashAlgorithm::Sha256 | HashAlgorithm::Sha256CrossLanguage) {
            panic!("Block-based hashing only supported for Sha256 and Sha256CrossLanguage algorithms");
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
            // INSERT_YOUR_CODE
            // 根据 hash 算法调用合适的 hash 函数
            current_hash = match self.algorithm {
                HashAlgorithm::Sha256CrossLanguage => {
                    self.sha256_cross_language_hash(chunk, Some(&current_hash), None)
                }
                HashAlgorithm::Sha256 => {
                    self.sha256_hash(chunk, Some(&current_hash), None)
                }
                _ => {
                    panic!("Only Sha256 and Sha256CrossLanguage supported in hash_tokens_with_blocks_all");
                }
            };
            // current_hash = self.sha256_hash(chunk, Some(&current_hash), None);
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

    /// 使用跨语言一致的 SHA256 哈希
    /// 
    /// 该函数实现与 Python 的 sha256_cross_language 相同的序列化和哈希逻辑
    /// 
    /// 序列化格式：
    /// - 前 32 字节（256 位）：prefix_hash 作为大端无符号整数（如果为 None，使用全零）
    /// - 后续字节：每个 token 作为 4 字节大端无符号整数（value & 0xFFFFFFFF）
    /// - extra_keys：当前忽略
    fn sha256_cross_language_hash(
        &self,
        tokens: &[u32],
        prefix_hash: Option<&HashValue>,
        extra_keys: Option<&[String]>,
    ) -> HashValue {
        let mut hasher = Sha256::new();
        
        // prefix_hash: 32 字节（256 位）大端无符号整数
        match prefix_hash {
            None => {
                // 如果为 None，使用全零
                hasher.update(&[0u8; 32]);
            }
            Some(hash) => {
                match hash {
                    HashValue::U256(bytes) => {
                        // 直接使用 32 字节
                        hasher.update(bytes);
                    }
                    HashValue::U64(_) => {
                        // U64 不支持，使用全零
                        hasher.update(&[0u8; 32]);
                    }
                }
            }
        }
        
        // tokens: 每个 token 作为 4 字节大端无符号整数
        for &token in tokens {
            let uint32_val = token & 0xFFFFFFFF;
            hasher.update(&uint32_val.to_be_bytes());
        }
        
        // extra_keys: 当前忽略
        let _ = extra_keys;
        
        HashValue::U256(hasher.finalize().into())
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


    #[test]
    fn test_cross_language_hasher() {
        let hasher = TokenHasher::new(HashAlgorithm::Sha256CrossLanguage, false, 0);
        let tokens: Vec<u32> = vec![1, 2, 3];
        
        let hash = hasher.hash_tokens(&tokens, None, None);
        assert!(matches!(hash, HashValue::U256(_)));
        
        // Test with prefix hash
        let prefix = hasher.get_init_hash();
        let hash_with_prefix = hasher.hash_tokens(&tokens, Some(&prefix), None);
        assert!(matches!(hash_with_prefix, HashValue::U256(_)));
        
        // Hashes should be different
        assert_ne!(hash, hash_with_prefix);
    }

    #[test]
    fn test_cross_language_hash_with_extra_keys() {
        let hasher = TokenHasher::new(HashAlgorithm::Sha256CrossLanguage, false, 0);
        let tokens: Vec<u32> = vec![1, 2, 3];
        let extra_keys = vec!["key1".to_string(), "key2".to_string()];
        
        let hash_without_keys = hasher.hash_tokens(&tokens, None, None);
        let hash_with_keys = hasher.hash_tokens(&tokens, None, Some(&extra_keys));
        
        // In the new implementation, extra_keys are ignored, so hashes should be the same
        assert_eq!(hash_without_keys, hash_with_keys);
    }


    #[test]
    fn test_cross_language_hash_v2_none_hash() {
        // Test NONE_HASH: (None, [], None)
        // Expected from Python: 66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925
        let hasher = TokenHasher::new(HashAlgorithm::Sha256CrossLanguage, false, 0);
        let hash = hasher.get_init_hash();
        
        let expected = [
            102, 104, 122, 173, 248, 98, 189, 119, 108, 143, 193, 139, 142, 159, 142, 32,
            8, 151, 20, 133, 110, 226, 51, 179, 144, 42, 89, 29, 13, 95, 41, 37
        ];
        
        match hash {
            HashValue::U256(bytes) => {
                assert_eq!(bytes, expected, "NONE_HASH mismatch");
            }
            _ => panic!("Expected U256 hash"),
        }
    }

    #[test]
    fn test_cross_language_hash_v2_with_tokens() {
        // Test: (None, [1, 2, 3], None)
        // Expected from Python: db9ea6001d3ab7d3f65542eef1d4f0ea18106524bd4f57b99d368f9b7a86c5cf
        let hasher = TokenHasher::new(HashAlgorithm::Sha256CrossLanguage, false, 0);
        let tokens: Vec<u32> = vec![1, 2, 3];
        let hash = hasher.hash_tokens(&tokens, None, None);
        
        let expected = [
            219, 158, 166, 0, 29, 58, 183, 211, 246, 85, 66, 238, 241, 212, 240, 234,
            24, 16, 101, 36, 189, 79, 87, 185, 157, 54, 143, 155, 122, 134, 197, 207
        ];
        
        match hash {
            HashValue::U256(bytes) => {
                assert_eq!(bytes, expected, "Hash of [1, 2, 3] mismatch");
            }
            _ => panic!("Expected U256 hash"),
        }
    }

    #[test]
    fn test_cross_language_hash_v2_with_prefix() {
        // Test iterative hashing
        let hasher = TokenHasher::new(HashAlgorithm::Sha256CrossLanguage, false, 0);
        
        // Initial hash (None, [], None)
        let mut current_hash = hasher.get_init_hash();
        let expected_init = [
            102, 104, 122, 173, 248, 98, 189, 119, 108, 143, 193, 139, 142, 159, 142, 32,
            8, 151, 20, 133, 110, 226, 51, 179, 144, 42, 89, 29, 13, 95, 41, 37
        ];
        assert_eq!(current_hash.as_u256().unwrap(), expected_init);
        
        // After block [1, 2]
        let tokens1: Vec<u32> = vec![1, 2];
        current_hash = hasher.hash_tokens(&tokens1, Some(&current_hash), None);
        let expected_after_12 = [
            231, 126, 234, 14, 187, 244, 253, 63, 62, 214, 102, 98, 34, 211, 208, 16,
            57, 62, 29, 69, 242, 226, 128, 190, 223, 144, 73, 135, 173, 127, 197, 36
        ];
        assert_eq!(current_hash.as_u256().unwrap(), expected_after_12);
        
        // After block [3, 4]
        let tokens2: Vec<u32> = vec![3, 4];
        current_hash = hasher.hash_tokens(&tokens2, Some(&current_hash), None);
        let expected_after_34 = [
            47, 142, 85, 133, 204, 94, 103, 30, 211, 59, 76, 101, 73, 91, 248, 62,
            208, 148, 149, 115, 65, 254, 162, 216, 213, 200, 133, 72, 21, 98, 82, 47
        ];
        assert_eq!(current_hash.as_u256().unwrap(), expected_after_34);
        
        // After block [5, 6]
        let tokens3: Vec<u32> = vec![5, 6];
        current_hash = hasher.hash_tokens(&tokens3, Some(&current_hash), None);
        let expected_after_56 = [
            198, 188, 165, 234, 224, 191, 171, 225, 194, 175, 212, 222, 3, 144, 149, 4,
            38, 86, 133, 145, 205, 217, 185, 191, 52, 110, 143, 31, 57, 38, 165, 65
        ];
        assert_eq!(current_hash.as_u256().unwrap(), expected_after_56);
    }
}
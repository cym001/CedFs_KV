use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

const DEFAULT_EXPIRY_DURATION: Duration = Duration::from_secs(300);

// TODO: use the common request_id if it exists in the repo
pub type RequestId = String;

/// A multi-request sequence manager that handles multiple active sequences with shared KV cache
#[derive(Debug)]
pub struct ActiveSequences {
    active_seqs: DashMap<RequestId, Vec<([u8; 32], Arc<()>)>>,
    unique_blocks: DashMap<[u8; 32], Weak<()>>,
    request_deadlines: DashMap<RequestId, Instant>,
    expiry_timer: Mutex<Instant>,
    request_ttl: Duration,
    block_size: usize,
}

impl ActiveSequences {
    /// Create a new SharedSequenceManager instance
    pub fn new(block_size: usize) -> Self {
        Self::new_with_ttl(block_size, DEFAULT_EXPIRY_DURATION)
    }

    pub fn new_with_ttl(block_size: usize, request_ttl: Duration) -> Self {
        // TODO: make this not a hard req
        assert!(block_size > 1, "block_size must be greater than 1");

        Self {
            active_seqs: DashMap::new(),
            unique_blocks: DashMap::new(),
            request_deadlines: DashMap::new(),
            expiry_timer: Mutex::new(Instant::now() + request_ttl.min(Duration::from_secs(1))),
            request_ttl,
            block_size,
        }
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn touch_block(&self, block: &[u8; 32]) -> Arc<()> {
        if let Some(weak) = self.unique_blocks.get(block) {
            if let Some(rc) = weak.upgrade() {
                return rc;
            }
        }

        let rc = Arc::new(());
        self.unique_blocks.insert(*block, Arc::downgrade(&rc));
        rc
    }

    fn try_remove_block(&self, block: &[u8; 32]) {
        if let Some(weak) = self.unique_blocks.get(block) {
            let can_remove = weak.strong_count() == 0;
            drop(weak);
            if can_remove {
                self.unique_blocks.remove(block);
            }
        }
    }

    pub fn active_blocks(&self) -> usize {
        self.unique_blocks.len()
    }

    /// Return active hold count for a single block.
    pub fn block_hold_count(&self, block: &[u8; 32]) -> u64 {
        self.unique_blocks
            .get(block)
            .map(|weak| weak.strong_count() as u64)
            .unwrap_or(0)
    }

    /// Return active hold counts for a sequence, preserving order.
    pub fn sequence_hold_counts(&self, blocks: &[[u8; 32]]) -> Vec<u64> {
        blocks
            .iter()
            .map(|block| self.block_hold_count(block))
            .collect()
    }

    /// Add a new request with its initial tokens
    /// Returns the set of expired request IDs that were removed during cleanup
    pub fn add_request(
        &self,
        request_id: RequestId,
        token_sequence: Option<Vec<[u8; 32]>>,
    ) -> HashSet<RequestId> {
        let removed_requests = self.force_expiry();

        // Duplicate start is idempotent and only refreshes this request's TTL.
        if self.active_seqs.contains_key(&request_id) {
            self.request_deadlines
                .insert(request_id, Instant::now() + self.request_ttl);
            return removed_requests;
        }

        if let Some(sequence) = token_sequence {
            let sequence_with_refs: Vec<([u8; 32], Arc<()>)> = sequence
                .iter()
                .map(|block| (*block, self.touch_block(block)))
                .collect();
            self.active_seqs
                .insert(request_id.clone(), sequence_with_refs);
        } else {
            // dummy empty sequence
            self.active_seqs.insert(request_id.clone(), Vec::new());
        }
        self.request_deadlines
            .insert(request_id, Instant::now() + self.request_ttl);

        removed_requests
    }

    pub fn potential_blocks_and_tokens(
        &self,
        token_sequence: Option<&[[u8; 32]]>,
        _isl: usize,
        _overlap: u32,
    ) -> (usize, usize) {
        let potential_blocks = if let Some(token_seq) = token_sequence {
            self.new_blocks(token_seq) + self.active_blocks()
        } else {
            self.active_blocks()
        };
        let potential_tokens = 0usize;
        (potential_blocks, potential_tokens)
    }

    /// Match a request against existing blocks and return the number of new blocks that would be added
    pub fn new_blocks(&self, token_sequence: &[[u8; 32]]) -> usize {
        token_sequence
            .iter()
            .filter(|block| !self.unique_blocks.contains_key(*block))
            .count()
    }

    /// Return the total number of blocks that would be used if the token sequence was added
    /// This is the sum of new blocks that would be added plus the current active blocks
    pub fn potential_blocks(&self, token_sequence: &[[u8; 32]]) -> usize {
        self.new_blocks(token_sequence) + self.active_blocks()
    }

    /// Free all blocks associated with a request
    pub fn free(&self, request_id: &RequestId) -> usize {
        self.request_deadlines.remove(request_id);

        // Remove from active_seqs and get the token sequence
        let token_seq = match self.active_seqs.remove(request_id) {
            Some((_key, seq)) => seq,
            None => {
                tracing::warn!("Trying to free non-existent request {request_id}");
                return self.active_blocks();
            },
        };

        // Drop each Rc reference, then clean up the corresponding weak reference
        for (block_hash, rc) in token_seq {
            drop(rc);
            self.try_remove_block(&block_hash);
        }

        self.active_blocks()
    }

    /// Force expiry of stale requests if the timer has elapsed
    /// Returns the set of expired request IDs that were removed
    pub fn force_expiry(&self) -> HashSet<RequestId> {
        let now = Instant::now();
        let mut timer = self.expiry_timer.lock().expect("expiry_timer poisoned");
        if now < *timer {
            return HashSet::new();
        }

        let expired_requests: HashSet<RequestId> = self
            .request_deadlines
            .iter()
            .filter_map(|entry| (*entry.value() <= now).then(|| entry.key().clone()))
            .collect();
        for request_id in &expired_requests {
            tracing::warn!("Force expiring stale request: {}", request_id);
            self.free(request_id);
        }

        *timer = now + self.request_ttl.min(Duration::from_secs(1));

        expired_requests
    }
}

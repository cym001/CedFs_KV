# LMCache v0.5.2：Coding Agent DRAM LRU 针对性修改方案

审计基线：LMCache upstream `v0.5.2`（`cd2c0d6a6a98`），当前 CEDFS 适配分支 `v0.5.2-19-g53861679`。

## 1. 结论

当前不应直接把 `LRUCachePolicy.get_evict_candidates()` 改成一个复杂评分函数。最小且正确的修改顺序是：

1. **先修正 access accounting**：去掉 `LocalCPUBackend.keys_in_request` 这一进程级临时列表，改为 request-scoped、显式传递命中 keys。同步和异步 lookup 必须走同一个 touch 接口。
2. **新增而不是替换默认 LRU**：增加仅作用于 DRAM tier 的 `AGENT_LRU`，默认 `LRU` 行为保持不变，便于灰度和回滚。
3. **按 session 选择 victim、按 tail 释放 chunk**：先选择预计最晚再次访问的 session，再从它的非共享 tail 向 prefix 方向返回候选。
4. **生命周期只接受显式事件**：`WORKFLOW_END`/dead branch 才进入 retired-first；当前 vLLM `request_finished`/CEDFS `ReportRequestEnd` 只是一次推理请求结束，不能等同于 coding workflow 结束。
5. **元数据缺失时严格退化为原 LRU**：第一版不引入神经 predictor、全量 GDSF 或同步 CEDFS RPC；先使用显式 next-use hint 或 session inter-arrival EWMA。

建议的第一版策略名为 `AGENT_LRU`，而不是修改 `LRU` 的默认语义。

## 2. 现有源码中的直接问题

| 位置 | 当前行为 | 对 Agent 负载的影响 | 修改方向 |
| --- | --- | --- | --- |
| [`local_cpu_backend.py`](../../LMCache/lmcache/v1/storage_backend/local_cpu_backend.py) `keys_in_request` | 所有同步 lookup 共用一个列表，注释假设“同一时刻只有一个请求 lookup” | async loading/并发请求下无法可靠区分访问属于哪个 request | 删除隐式累积，显式传入本次命中的 keys |
| `LocalCPUBackend.batched_async_contains()` | `pin=True` 时向 `keys_in_request` 追加 key | async lookup 路径没有对应的 `touch_cache()`，LRU 顺序可能长期不更新，列表也可能持续积累 | async contains 返回后立即对该 backend 的实际 prefix hit 做 request-scoped touch |
| [`cache_engine.py`](../../LMCache/lmcache/v1/cache_engine.py) `lookup()` | finally 中无参数调用 `StorageManager.touch_cache()` | touch 依赖 backend 隐藏状态，`lookup_id/request_configs/block_mapping` 全部丢失 | 传递准确的 per-backend hit keys 与 request context |
| [`storage_manager.py`](../../LMCache/lmcache/v1/storage_backend/storage_manager.py) `async_lookup_and_prefetch()` | 已持有 `lookup_id`、backend 和 hit slice，但不更新 LRU | sync/async 的 recency 语义不一致 | 在确定 `backend_keys` 后调用同一 access hook |
| [`lru.py`](../../LMCache/lmcache/v1/storage_backend/cache_policy/lru.py) | 只维护全局 `OrderedDict` recency | `a,b,c,d,...` 中会驱逐下一次要访问的 session | 增加 session next-use heap 和 per-session tail 索引，LRU 保留为 fallback |
| [`base_policy.py`](../../LMCache/lmcache/v1/storage_backend/cache_policy/base_policy.py) | 只有单 key hit/put/remove/candidate 接口 | 看不到 request 边界、session、生命周期和有序 prefix | 增加带默认实现的 batch access/store 与 lifecycle hook，不破坏其他 policy |
| [`vllm_v1_adapter.py`](../../LMCache/lmcache/integration/vllm/vllm_v1_adapter.py) | `req_id` 和非 tag `lmcache.*` request config 已经存在 | 有可用的本地上下文，但没有传到 eviction policy | 构造一次 `CachePolicyRequestContext` 并沿 lookup/store 传递 |
| [`kvcache_v2.proto`](../cedfs-proto/proto/kvcache_v2.proto) | 只有 request start/end，没有 workflow/tool 生命周期 | 无法区分“本轮推理完成，正在跑工具”和“整个任务结束” | 后续新增独立 workload event；不要复用 request end |

两个已有机制必须保留：

- `MemoryObj.can_evict` 是物理安全边界；pin、refcount/in-flight 对象仍必须无条件跳过。
- 前台 allocation failure 和后台水位回收最终都调用 `LocalCPUBackend._evict_once()`，因此只要统一 `get_evict_candidates()`，两条回收路径就会得到相同策略，不应分别实现两套 victim logic。

## 3. Phase A：先修正 LRU 访问更新

这是 Agent-aware 评分之前的必要修复，应单独提交和验证。

### 3.1 新增 request-scoped context

在 `lmcache/v1/storage_backend/cache_policy/context.py` 增加最小数据结构：

```python
@dataclass(frozen=True, slots=True)
class CachePolicyRequestContext:
    request_id: str
    session_id: str | None = None
    workflow_id: str | None = None
    predicted_reuse_after_ms: int | None = None
    prediction_confidence: float = 0.0
```

第一版只保留这些字段。tool class、semantic role 和成本模型以后通过 lifecycle snapshot 扩展，不要一开始把所有论文字段都塞进热路径。

### 3.2 扩展 `BaseCachePolicy`，但保持兼容

增加两个**非 abstract** hook：

```python
def update_on_request_access(
    self,
    keys: Sequence[KeyType],
    cache_dict: MapType,
    context: CachePolicyRequestContext | None,
) -> None:
    for key in reversed(keys):
        self.update_on_hit(key, cache_dict)

def update_on_request_store(
    self,
    keys: Sequence[KeyType],
    cache_dict: MapType,
    context: CachePolicyRequestContext | None,
) -> None:
    return
```

默认 access 实现必须保持现有 reversed-touch 语义：同一连续 prefix 中，suffix 位于 prefix 之前成为 eviction candidate。现有 LRU/LFU/FIFO/MRU 不需要知道 Agent metadata。

`update_on_request_store()` 不能只接收新 key 后立即把它们当作普通 MRU。`AGENT_LRU` 要用本次 batch 的 prefix→suffix 顺序建立 session tail；否则新生成的 suffix 可能比旧 prefix 更晚进入 `OrderedDict`，在下一轮访问前反而受到错误保护。

为避免 put 与 batch hook 之间出现“已进入 hot cache、但还没有 policy metadata”的窗口，`AGENT_LRU.update_on_put()` 应先从 key 的非 tag `lmcache.policy.session_id` 建立最小 owner 状态；随后 `update_on_request_store()` 在同一 `cpu_lock` 临界区内补齐准确的 batch 顺序和 request context。`LocalCPUBackend.batched_submit_put_task()` 因此应将真正的 batch 插入与 request-store hook 合并在一次加锁中，而不是继续逐 key 重复加锁。

### 3.3 删除 `keys_in_request` 隐式状态

修改 `LocalCPUBackend`：

- `contains()` 和 `batched_async_contains()` 只负责 contains/pin，不再追加 `keys_in_request`。
- `touch_cache()` 改为接收 `keys + context`，在 `cpu_lock` 内调用 `update_on_request_access()`。
- 增加对应的 request-store hook；store keys 必须保持 prefix→suffix 顺序传入。
- 将 `local_cpu_keys_in_request_count` 替换为 request-access event counter 或删除。它不应继续表示一个已不存在的共享列表。

修改 `StorageManager`：

- 同步 `batched_contains()` 返回的 `block_mapping` 就是准确的 per-backend hit slice；`LMCacheEngine.lookup()` 应把它传给 `touch_cache(block_mapping, context)`。
- async `async_lookup_and_prefetch()` 在算出 `backend_keys` 后，为 `LocalCPUBackend` 调用相同 touch hook。第一版可保持“lookup 命中即 touch”的现有同步语义；若以后要排除 retrieve failure，再把 touch 移到 all-done callback。
- 不要再由 backend 猜测一次 request 的边界。

修改 `LMCacheEngine`：

- 从现有 `lookup_id/req_id + request_configs` 构造一次 context。
- `lookup()`、`async_lookup_and_prefetch()` 和 `store()` 使用同一个 parser。
- `store()` 把 context 和本次实际写入的有序 keys 传给 `batched_put()`；`StorageManager` 仅对 `LocalCPUBackend` 使用该 context，backend 在 batch 插入的同一个 `cpu_lock` 临界区内调用 request-store hook。

### 3.4 Phase A 验收条件

- 关闭 `AGENT_LRU` 时，单线程同步 LRU 的 victim 顺序与当前测试完全一致。
- async prefix hit 会更新 LRU，且不存在跨 request 共用的 pending-key 列表。
- 两个交错 lookup 只 touch 各自命中的 keys。
- pinned/refcount key 的行为不变。
- new suffix 的顺序信息可以由后续 policy 使用，不依赖 `CacheEngineKey` identity。

## 4. Phase B：新增 `AGENT_LRU`

### 4.1 配置必须只影响 LocalCPUBackend

当前 `cache_policy` 同时被 LocalCPU、LocalDisk 和部分 NIXL backend 使用。不能直接把全局 `cache_policy` 设置成只理解 Agent context 的实现。

建议增加：

```yaml
extra_config:
  local_cpu.cache_policy: AGENT_LRU
  local_cpu.agent_lru.mode: shadow   # shadow | enforce
```

- `local_cpu.cache_policy` 默认回退到现有 `config.cache_policy`。
- `AGENT_LRU` 仅由 `LocalCPUBackend` 实例化；LocalDisk/NIXL 继续使用原 policy。
- 默认模式保持现有 `LRU`。首次上线先用 `shadow`：实际仍驱逐 LRU victim，只记录 Agent policy 本会选择谁。

暂时不要加入大量可调权重。第一版固定使用下述硬规则和 EWMA，只保留 `mode` 作为运行开关。

### 4.2 内部状态

新增 `agent_lru.py`，继承 `LRUCachePolicy`，继续把 `hot_cache` 的 `OrderedDict` 作为 fallback 顺序。额外维护：

```text
BlockState
  owners: set[session_id]
  per_owner_offset: dict[session_id, prefix_ordinal]
  last_access_ns

SessionState
  workflow_id
  ordered_keys                  # prefix -> tail
  last_access_ns
  reuse_gap_ewma_ns
  reuse_samples
  explicit_next_use_ns
  prediction_confidence
  lifecycle                    # ACTIVE | SOFT_PROTECTED | RETIRED
  version                      # lazy heap invalidation
```

索引：

- `session_id -> SessionState`；
- `cache key -> BlockState`；
- `request_id -> session_id` 仅用于本轮关联，设置 TTL；
- active session max-heap，按预测 next-use 从远到近；
- retired session queue；
- 原 `OrderedDict` 继续保存全局 LRU fallback。

所有 policy 状态只在 `LocalCPUBackend.cpu_lock` 下更新，不再引入第二把 policy lock，避免锁顺序问题。

### 4.3 next-use 估计

优先级从高到低：

1. 调用方提供的、未过期的 `predicted_reuse_after_ms`，在接收时转换为本地 monotonic deadline；使用相对时长可避免跨进程时钟偏差；
2. tool lifecycle snapshot 给出的条件剩余时间；
3. 同一稳定 `session_id` 的 inter-arrival EWMA；
4. 数据不足时使用全局 LRU。

EWMA 只需要：

```text
gap = now - session.last_access
reuse_gap_ewma = alpha * gap + (1 - alpha) * reuse_gap_ewma
predicted_next_use = now + reuse_gap_ewma
```

至少观察两次 reuse gap 后才允许它覆盖 LRU。超出 metadata TTL、时间倒退、无稳定 session id 或低置信度时立即降级，不猜测。

### 4.4 victim 选择

`get_evict_candidates()` 的顺序为：

1. 跳过 `cache.can_evict == false`；这是不可突破的 hard constraint。
2. 从 `RETIRED/dead-branch` session 的**非共享 tail**开始选。
3. 当 Agent metadata 覆盖率足够且至少两个 session 有有效预测时，从 next-use 最远的 session 选 victim。
4. 在选中的 session 内，从 `ordered_keys` 尾部向前扫描；共享 block 的下一次使用取所有 active owner 中最早的时间，只要有 owner 即将使用就延后驱逐。
5. metadata 缺失、覆盖率不足或 predictor 未预热时，调用原 `LRUCachePolicy.get_evict_candidates()`。
6. `SOFT_PROTECTED` 只是最后选择，不得造成 allocation 永久返回零候选；如果所有可驱逐对象都受软保护，按最晚 next-use 逐步解除。只有 `can_evict=false` 才能硬阻止回收。

伪代码：

```text
select(n):
  victims = retired_exclusive_tail(n)
  if len(victims) == n:
      return victims

  if predictive_mode_ready():
      for session in sessions_by_farthest_next_use:
          victims += evictable_unshared_tail(session)
          if len(victims) == n:
              return victims

      victims += shared_blocks_by_earliest_active_owner_use()

  if len(victims) < n:
      victims += legacy_lru_excluding(victims)
  return victims[:n]
```

候选选择不能提前永久删除 heap/metadata 状态。`_detach_evictable_entries()` 成功从 `hot_cache` pop 后，现有 `update_on_force_evict()` 才是 commit 点；heap 使用 `version` 丢弃 stale entry。这样 pinned race 或 layerwise group 无法完整回收时不会丢失 policy 状态。

### 4.5 为什么能改善 `a,b,c,d,a...`

当 `a/b/c/d` 是四个稳定 session、容量只能覆盖三个 session 时：

- LRU 在访问 `a` 后把 `b` 放在最老位置，而 `b` 恰好是下一次访问。
- `AGENT_LRU` 在周期估计预热后会得到 `b < c < d < a` 的 next-use 顺序，因此选择最晚回来的 `a`，而不是最早回来的 `b`。
- session 含多个 chunk 时，只从 `a` 的 tail 向前释放所需 batch，不先打穿它的内部 prefix。

冷启动阶段仍可能与 LRU 相同；预期收益必须分别报告 warm-up 和 steady-state，不能只给平均命中率。

## 5. Metadata 传递约定

### 5.1 不改变 cache identity

现有 `extract_request_configs()` 会保留 `kv_transfer_params` 中所有 `lmcache.*` 字段，而 `CacheEngineKey` 只有 `lmcache.tag.*` 会进入 hash/equality。因此建议使用：

```text
lmcache.policy.session_id
lmcache.policy.workflow_id
lmcache.policy.predicted_reuse_after_ms
lmcache.policy.prediction_confidence
```

禁止使用：

```text
lmcache.tag.session_id
lmcache.tag.workflow_id
lmcache.tag.tool_class
```

否则相同 token prefix 会因 session/tool 不同生成不同 cache identity，直接破坏跨请求共享。

`session_id` 必须跨 coding-agent 多轮稳定；vLLM `request_id` 只表示一次 inference turn，不能替代它。

### 5.2 生命周期事件

增加独立的本地 public API，而不是让 eviction 快路径同步查询 CEDFS：

```text
LMCacheEngine.update_cache_policy(event)
  -> StorageManager.update_cache_policy(event)
  -> LocalCPUBackend.update_cache_policy(event)
  -> AgentLRUCachePolicy.update_lifecycle(event)
```

建议事件：

```text
SESSION_ACCESS
TOOL_START
TOOL_END
BRANCH_ABANDONED
WORKFLOW_END
```

语义：

- `TOOL_START`：根据 tool hint 更新 next-use，不 pin。
- `TOOL_END`：进入短期 `SOFT_PROTECTED`，等待下一轮请求。
- `BRANCH_ABANDONED`：只 retire 该分支独占 tail。
- `WORKFLOW_END`：移除 session owner；只有无其他 active owner 的 key 才 retired。
- inference `REQUEST_END`：不能 retire，只表示当前模型调用结束。
- event 带 `policy_epoch/expires_at`；过期事件降级为 unknown，不自动转 retired。

CEDFS V2 后续可新增 `ReportWorkloadEvent` 保存和传播这些事件，但 LMCache 本地 evictor 只消费异步、本地 snapshot。CEDFS 不可成为 allocation failure 路径上的同步依赖。

## 6. 需要修改的文件

| 文件 | 修改内容 |
| --- | --- |
| `lmcache/v1/storage_backend/cache_policy/context.py` | 新增 request context/lifecycle event 数据结构与非 tag 字段解析 |
| `lmcache/v1/storage_backend/cache_policy/base_policy.py` | 增加兼容的 batch access/store 和 lifecycle hook |
| `lmcache/v1/storage_backend/cache_policy/agent_lru.py` | session heap、owner/tail 索引、EWMA、retired-first、LRU fallback |
| `lmcache/v1/storage_backend/cache_policy/__init__.py` | 注册 `AGENT_LRU`，但仅允许 LocalCPU 选择 |
| `lmcache/v1/storage_backend/local_cpu_backend.py` | 去除 `keys_in_request`；显式 access/store/context；接收 lifecycle；保留统一 `_evict_once()` |
| `lmcache/v1/storage_backend/storage_manager.py` | 同步/异步传递 per-backend hit keys；向 LocalCPU batch put 传递有序 keys/context；转发 lifecycle |
| `lmcache/v1/cache_engine.py` | 从 `req_id/request_configs` 构造 context，并在 lookup/store 传递 |
| `lmcache/integration/vllm/vllm_v1_adapter.py` | 保留 `lmcache.policy.*`；稳定 session id；本地 lifecycle 入口 |
| `lmcache/observability.py` | 增加 shadow disagreement、fallback reason、retired/predictive victim 等指标 |
| `tests/v1/test_cache_policy.py` | `AGENT_LRU` 单元测试 |
| `tests/v1/storage_backend/test_local_cpu_backend.py` | sync/async access accounting、pin、batch/background、layerwise 测试 |
| `cedfs-proto/proto/kvcache_v2.proto`（后续） | additive workload-event RPC，不修改现有 request lifecycle 语义 |

不建议第一阶段修改 MP `lmcache/v1/distributed/eviction_policy/lru.py`。当前 CEDFS 使用的是 `LocalCPUBackend`；先在真实部署路径验证，之后再把相同 policy contract 移植到 distributed `EvictionPolicy`。

## 7. 可观测性

最少新增以下指标：

```text
lmcache:agent_lru_access_total{path=sync|async}
lmcache:agent_lru_metadata_coverage_ratio
lmcache:agent_lru_victim_total{reason=retired|next_use|lru_fallback|soft_break}
lmcache:agent_lru_shadow_disagreement_total
lmcache:agent_lru_prediction_error_ms
lmcache:agent_lru_evicted_before_reuse_total
lmcache:agent_lru_policy_latency_us
lmcache:agent_lru_metadata_entries
```

每次 eviction 记录轻量 reason code，不记录原始 prompt、tool arguments 或完整 session id。session id 只能做有界 hash，避免指标高基数和敏感信息泄漏。

## 8. 测试与验收矩阵

### 8.1 必须增加的测试

1. **原 LRU 兼容**：没有 metadata 时，put/hit/pin 后的候选与现有 LRU 完全一致。
2. **sync/async 一致**：相同 hit 序列经同步和异步路径得到相同 victim 顺序。
3. **并发隔离**：两个 interleaved lookup 不串用 keys，不残留 pending request 状态。
4. **周期访问**：`a,b,c,d`、容量 3；预热后 Agent policy 不再稳定驱逐下一次访问对象，并与 Belady oracle 报告差距。
5. **retired-first**：retired private tail 始终先于 active/unknown key。
6. **request end 语义**：一次 inference request end 不会 retire session；显式 workflow end 才会。
7. **共享 owner**：一个 owner end 不会 retire 仍被其他 active owner 使用的 prefix。
8. **tail-first**：同一 session 的 suffix 在 ancestor prefix 之前返回。
9. **pin/refcount**：预测分最高也不能返回 `can_evict=false` key。
10. **soft protection fail-open**：所有对象都 soft-protected 时仍能释放可驱逐对象，不导致 allocation busy-loop。
11. **foreground/background 一致**：allocation failure 与 watermark evict 得到相同 reason/victim 顺序。
12. **layerwise**：同一 chunk 的 layer group 不产生只删除部分 layer 的 policy metadata。
13. **stale event**：旧 epoch、过期 hint、时间倒退不会覆盖新状态。
14. **metadata cleanup**：evict、clear、session timeout、close 后无 owner/heap/request 映射泄漏。

### 8.2 上线门禁

- `shadow` 下 policy P99 CPU 时间低于一次 DRAM allocation/eviction 预算，且不延长 `cpu_lock` 的高分位持有时间。
- Agent metadata coverage 达到预设门槛后才能启用 enforce；否则保持 LRU。
- `evicted-before-reuse`、saved prefill tokens/ms 和连续 prefix hit length 优于 LRU；不能只比较 chunk hit count。
- P95/P99 TTFT、allocation wait 和 background eviction rate 无显著回退。
- 关闭 `local_cpu.cache_policy=AGENT_LRU` 后无需清空 cache 或改变 cache key，即可恢复原 LRU。

## 9. 推荐提交顺序

1. `fix(local-cpu): make cache touches request-scoped`：只修 sync/async accounting，不引入新策略。
2. `feat(cache-policy): add opt-in agent lru shadow policy`：session/tail 索引、EWMA、shadow metrics。
3. `feat(cache-policy): enforce retired-first and predictive victims`：在 metadata coverage 达标后开启 enforce。
4. `feat(agent): add local workload lifecycle events`：tool start/end、branch abandoned、workflow end。
5. `feat(cedfs): propagate versioned workload policy snapshots`：只做异步控制面增强。

其中第 1、2 步已经可以验证 `a,b,c,d` 周期负载是否存在可利用的 policy headroom；在没有 trace 和 shadow 数据前，不建议直接实施 cost-aware GDSF、神经 predictor 或 MP/distributed 双路径改造。

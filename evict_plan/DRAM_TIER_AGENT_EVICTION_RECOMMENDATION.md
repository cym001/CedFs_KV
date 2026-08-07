# CEDFS-KV + LMCache：Coding Agent DRAM Tier KV 驱逐专项调研

调研日期：2026-08-07。

LMCache 审计基线：官方 `v0.5.2` tag（`cd2c0d6a6a98`）；当前 CEDFS 适配分支为 `v0.5.2-19-g53861679`，即在 tag 之后叠加了 19 个 KV transfer/CEDFS 适配提交。本文将两类 DRAM 路径明确区分：当前 CEDFS 实际接入的进程内 `LocalCPUBackend`，以及 v0.5.2 新的 multiprocess/distributed L1/L2 路径。除非明确写为 MP/distributed，下面的落地建议均指前者。

对应当前源码的文件级修改、接口草案、并发约束和测试矩阵见 [LMCACHE_V052_AGENT_LRU_MODIFICATION_PLAN.md](./LMCACHE_V052_AGENT_LRU_MODIFICATION_PLAN.md)。

## 1. 结论

对 CEDFS-KV + LMCache 的 DRAM tier，最值得实现的不是单一的“更聪明 LRU”，而是一个分层决策：

1. **生命周期先行**：已结束 workflow 的 private KV、废弃分支和无 active owner 的块先回收。
2. **预测下一次使用**：对仍活跃的 coding-agent session，根据正在运行的 tool 及其参数估计剩余时间，优先驱逐最晚返回的 session；只需要相对顺序，不要求精确预测时间。
3. **保持 prefix 可用性**：先选低价值 session/前缀，再从其 tail 向前释放，避免产生无法命中的 prefix hole；共享 ancestor 聚合所有 active owner 的价值。
4. **按收益/字节排序**：在复用概率接近时，保留能节省更多 prefill 或下层读取时间的块；加入 admission，避免一次性大对象污染 DRAM。
5. **LRU 仅作 tie-breaker/fallback**：未知 tool、低置信度或没有 agent metadata 时回退到 LRU，而不是让 recency 主导所有请求。

建议的 DRAM retention value 为：

\[
V(b)=Lifecycle(b)\times P_{reuse}(b,H)\times Share(b)\times
\frac{MissPenalty(b)}{Bytes(b)}
\]

其中：

\[
MissPenalty=\left[\min(T_{prefill},T_{lower\ tier})-T_{DRAM\rightarrow GPU}\right]^+
\]

如果没有 SSD/远端 tier，则 miss 路径成本直接取 `T_prefill`。`Share` 对 private block 取 1，对共享 block 聚合 active owner 的复用价值，而不是简单用可能为 0 的 owner 数。如果保留 DRAM KV 比重算还慢，则该对象不应 admission。`H` 是由当前 DRAM 压力和预计可保留时间决定的有限 horizon。

## 2. 为什么不能只把 LRU 换成 LFU

若访问为 `a,b,c,d,a,b,c,d,...`，容量为 3：

- LRU 预热后会持续驱逐下一次即将访问的对象，稳态可以接近 0 命中。
- LFU 对四个对象得到相同频率，若同频按 FIFO/LRU 打破平局，仍可能持续抖动。
- MRU 在这个严格、均匀、容量为 `N-1` 的循环上可显著改善，但对热点共享前缀和非周期负载可能反向伤害，适合作为诊断 baseline，不适合直接作为生产默认策略。
- 真正能利用周期性的是 next-use/Belady 类排序；如果未来信息不完美，则应结合生命周期、tool metadata 和在线历史近似。

此外，eviction-only 不能解决所有 pollution。若一个只访问一次的大请求被无条件写入 DRAM，它仍会驱逐将被反复复用的 agent history，因此需要 admission/bypass。

## 3. 相关工作：按对 DRAM 场景的直接相关度排序

### 3.1 Bidaw：最直接的 host-DRAM 驱逐工作

[Bidaw（FAST '26）](https://www.usenix.org/conference/fast26/presentation/hu-shipeng)明确研究 host memory + SSD 的两级历史 KV 缓存。论文观察到交互 workload 的 KV 访问时间局部性很差；在其设置中，DRAM 平均只能容纳约 40.1% 的 KV 时，LRU、FIFO 和 queue-enhanced 策略的 DRAM 命中率都只有约 20%。

其驱逐策略可概括为：

- 用上一轮模型回答长度估计下一次访问的 weighted reuse-distance 下界；
- 维护按 reuse-distance 分桶的访问分布；
- 用后台 ghost cache 回放过去 trace，估计不同距离桶在 Belady 下的 hit potential；
- 驱逐估计 hit potential 最低的 KV；
- waiting request 对应的 KV 不参与驱逐。

对 coding agent，回答长度不是最合适的信号，应替换为 CacheWise 的 `tool_name + tool_args + elapsed time`。Bidaw 最值得借鉴的是 **DRAM victim 的概率化排序、ghost cache 在线校准以及 compute/storage 元数据互通**。

### 3.2 CacheWise：最匹配 coding-agent 访问模式

[CacheWise](https://arxiv.org/html/2606.16824)基于真实 coding-assistant traces，指出 coding agent 是长生命周期、prefix 单调增长、LLM 与工具交替的 closed loop；不同 tool 的执行时间跨多个数量级，使 LRU 在 tool gap 中产生 priority inversion。

其核心是近似 Belady：根据 tool name、tool arguments 和已经过去的时间，估计 tool 的条件剩余时间，只预测各 session 的相对 next-use 顺序。实现上把 session metadata 附到 block，使用 eviction heap，并周期性重建 heap 修正陈旧预测。论文在 GPU/XPU cache 上评估，因此结果不能直接外推到 DRAM，但 **预测信号和 session-level victim selection 可原样迁移到 LMCache DRAM**。

第一版不必复制其 TF-IDF + KMeans：可先按 tool 类别和命令族维护 P50/P90/条件剩余时间，例如 `read/grep/git status`、`test/typecheck`、`build/docker`、网络工具和人工等待。

### 3.3 PBKV：最有价值的是 retired-first 和共享度聚合

[PBKV](https://arxiv.org/html/2605.06472)把 workflow 结束后的 private cache 标为 retired，强制在 active cache 之前回收；仅这一确定性规则在论文实验中就使平均命中率最高提升 1.66 倍。对 active cache，它把多个 workflow 对同一 prefix 的未来访问概率聚合，保护全局和热门共享前缀。

这两个机制非常适合 DRAM 的最终删除：

- `workflow_end` 比访问时间更可靠，优先作为硬规则；
- 一个共享块不能因为某个 session 结束就删除，必须在所有 owner 都结束或价值都很低时才 retired；
- predictor 错误时也不应越过 retired-first guardrail。

PBKV 的 GraphSAGE/hidden-state 多步 predictor 可留到后期，不是第一阶段依赖。

### 3.4 KVCache Cache in the Wild：按 workload 分类学习复用分布

[KVCache Cache in the Wild（USENIX ATC '25）](https://www.usenix.org/conference/atc25/presentation/wang-jiahao)从大规模生产 trace 发现：全局 reuse time 很杂，但在具体请求类别内更可预测；LFU 也会受 KV 生命周期短暂影响。其 workload-aware policy 按请求类别拟合复用时间分布，计算未来有限 lifespan 内的复用概率，并用 block offset 打破平局。

对 CEDFS-KV 的借鉴是：不要训练一个全局时间模型，而应按 `tool class / agent type / project or tenant / turn phase` 分桶，并定期更新。`Life` 必须有限，避免重尾分布让过期对象长期占用 DRAM。

### 3.5 RAGCache 与 Marconi：成本密度和 admission

[RAGCache](https://arxiv.org/html/2404.12457)在 GPU/host hierarchy 使用 prefix-aware GDSF：

\[
Priority=Clock+\frac{Frequency\times ComputeCost}{Size}
\]

host memory 超容量时也按该价值驱逐。它说明 DRAM 策略不应只追求 chunk hit count，而应优化节省的 prefill 时间/字节，并保持 prefix tree 的父子约束。

[Marconi](https://arxiv.org/html/2411.19379)也把 recency 与 FLOP-saved/byte 结合，并包含 SWE-Agent trace。它主要面向 Hybrid/SSM 模型；对纯 GQA/MLA、固定 token chunk 和单一模型，块字节数往往近似相同，因此第一阶段的主要增益仍来自 `P_reuse` 和 lifecycle，而不是复杂 FLOP 模型。多模型、多 dtype、不同 chunk 长度或不同 lower tier 共用 DRAM 时，cost density 才更重要。

### 3.6 CachedAttention：调度队列是可靠的近期未来信息

[CachedAttention（USENIX ATC '24）](https://www.usenix.org/conference/atc24/presentation/gao-bin-cost)直接管理 DRAM + SSD。它保护调度队列 look-ahead window 中即将运行的 session，并优先把队尾、距离执行更远的 session 从 DRAM 下沉；驱逐粒度是完整 conversation session。

CEDFS-KV 即便不能控制 GPU scheduler，也应接收 `queued/running/tool_wait/workflow_end` 状态。已经进入 worker waiting queue 的请求属于高置信度近期访问，应覆盖统计预测。

### 3.7 五项 agent 工作在 DRAM 中的适用边界

| 工作 | 对 DRAM 可直接借鉴 | 不宜直接照搬 |
| --- | --- | --- |
| CacheWise | tool metadata、条件剩余时间、相对 next-use、session heap | GPU 调度收益数字 |
| PBKV | retired-first、active owner、跨 workflow 共享度 | 首版即部署多步神经 predictor |
| KVFlow | 显式 workflow 的 steps-to-execution、共享 ancestor 聚合 | CPU→GPU prefetch 作为 DRAM eviction 核心；无条件驱逐 active dynamic suffix |
| Continuum | 有界 protection lease、到期自动降权 | GPU pin 的 queueing cost 公式；把 TTL 到期等同于 DRAM 删除 |
| SAECache | role-aware multi-queue、在线 miss-after-eviction 反馈 | 首版引入 hidden-state classifier；不区分 active/retired 就驱逐 tool/output history |

重要修正：KVFlow 所说的动态 suffix 易失，是针对跨 workflow 的 agent fixed-prompt reuse；coding-agent 的 active session 会在下一轮复用包括 assistant output 和 tool result 在内的整段历史。因此 active dynamic history 应由 next-tool-return 时间保护，只有 retired、废弃分支或已经不在当前 continuation path 上的 suffix 才是 evict-first。

## 4. GQA 与 MLA 对策略的影响

驱逐的根因与 attention 架构无关，但成本模型不同：

- GQA 的每 token KV 字节数由 KV heads、head dimension、layer 数和 dtype 决定。
- MLA 保存的是压缩 latent KV 及相关状态，通常每 token DRAM 占用更小；相同 DRAM 可覆盖更长 reuse distance。
- 在固定模型、固定 chunk size 下，各块 `Bytes` 接近常量，优先把精力放在 lifecycle 和 next-use；不要为了理论上的 cost/byte 引入高额快路径开销。
- 跨 GQA/MLA 模型共享内存池时，必须使用实际 allocator bytes 和实测 `prefill/load` 曲线，不能只用 token 数比较价值。
- 对 full-attention、连续 exact-prefix lookup，无论 GQA 还是 MLA，驱逐内部 prefix block 都可能使后续 suffix 无法形成连续命中，应采用 tail-first/leaf-only 约束。LMCache v0.5.2 的 MP 路径还用 `object_group_id` 和 `AttnWindowDesc` 表示多 object group、sliding-window/Mamba 等布局；这些模式不应套用全局统一的 prefix-hole 假设，价值和可用性约束应按 object group/attention window 计算。

本专项讨论的是 **lossless、跨请求的 exact KV chunk retention**。IntentKV、H2O、StreamingLLM、MemDecay 等单请求内部 token pruning 会改变 attention 可见上下文和精度边界，不应与 LMCache DRAM chunk 驱逐混为一个问题。

## 5. 建议的 CEDFS-KV 策略

### 5.1 硬规则层

按以下顺序处理：

1. `can_evict=false`、in-flight load/store、running request：不可驱逐。
2. 已进入 waiting queue 或 tool 已实际完成：最高保护。
3. workflow ended 且没有其他 active owner：retired，最先驱逐。
4. 废弃 retry/branch tail、明确一次性且未共享的对象：evict-first。
5. 其余 active/shared 块进入价值评分层。

多租户公平性是另一条正交约束。若以后采用 MP/distributed L2，可复用 v0.5.2 的 `cache_salt` quota + `IsolatedLRU` 限制 noisy neighbor；但这只是在租户内继续做 LRU，不能解决同一 salt 内 coding-agent 的周期访问，不能替代 next-use/lifecycle 评分。

### 5.2 active value 层

建议按 session/owner 计算预测，避免每个 chunk 重复运行 predictor：

```text
next_use_rank(session) = conditional_remaining_time(
    tool_class, argument_class, project_class, elapsed
)

block_value = reuse_probability_within_horizon
            * active_owner_aggregation
            * miss_penalty
            / actual_bytes
```

选择最低价值 session 后，从其非共享 tail 向前批量释放；遇到共享 ancestor 时重新计算 owner 聚合价值。recency 仅用于同分 tie-break 和 metadata 缺失 fallback。

### 5.3 admission 层

- 第一次出现且无共享、无 active continuation、体积很大的 KV 可 bypass DRAM。
- active coding session 的新 tail 可 admission，但 protection lease 有上限；tool 完成时立即提升，而不是等预测 TTL。
- system/tool schema、repo instruction 等跨 session 共享 prefix 通过频率/owner 数获得 admission。
- 使用 ghost entry 记录被 bypass/驱逐对象的后续命中，在线调整阈值，不保留其 KV payload。

## 6. LMCache v0.5.2 的两条驱逐路径与实现缺口

### 6.1 当前 CEDFS 使用的进程内路径

当前 CEDFS migration plugin 仍围绕 `LocalCPUBackend` 的 store/remove、pin 和内存位置接口工作。对这条路径，静态检查确认：

- `cache_policy` 默认是 LRU；可配置实现仍为 LRU/LFU/FIFO/MRU。
- LRU 只根据命中移动 `OrderedDict`，候选扫描只跳过 `can_evict=false` 的对象。
- `LocalCPUBackend` 已支持 pin；当前 CEDFS 分支又在 upstream v0.5.2 之上增加了批量回收和可选后台高低水位回收，适合作为执行层。后台回收默认关闭，默认高/低水位为 0.92/0.82，不能把这部分写成官方 tag 原生能力。
- `touch_cache()` 会逆序 touch 同一请求的 keys，使较晚的 suffix chunk 比较早的 prefix chunk 更接近 LRU 端。也就是说，v0.5.2 已有基础的 request 内 tail-first 行为，新增策略应保留它，而不是重复实现另一套相反顺序。
- `BaseCachePolicy` 的事件只有 hit、put、force-evict 和 get-candidates，看不到 session/tool/lifecycle/offset/bytes。
- `CacheEngineKey` 的 `lmcache.tag.*` 会进入 hash/equality；不能把瞬态 session/tool priority 塞入 tag，否则会破坏相同 prefix 的跨请求共享。

### 6.2 v0.5.2 的 MP/distributed 路径

v0.5.2 还包含一套独立的 `StorageManager` 驱逐框架，旧文档将 LMCache DRAM 驱逐全部归到 `LocalCPUBackend.cache_policy` 并不完整：

- `EvictionConfig` 支持 `LRU`、`IsolatedLRU` 和 `noop`，默认 trigger watermark/eviction ratio 为 0.8/0.2。
- `EvictionPolicy` 通过 `on_keys_created/touched/removed` 接收 L1/L2 事件，并返回带 destination 的 `EvictionAction`；eligible filter 可跳过有读写锁的对象。当前 L1 controller 实际只执行 `DISCARD`，不能把它描述成自动 DRAM→L2 demotion。
- distributed LRU 同样逆序处理一次请求的 keys，已经保留“后部 suffix 先驱逐”的 exact-prefix 语义。
- `ObjectKey` 包含 `chunk_hash/model_name/kv_rank/object_group_id/cache_salt`。`cache_salt` 属于 cache identity，用于严格隔离；`IsolatedLRU` 配合 `QuotaManager` 的 per-salt quota 主要解决 L2 noisy-neighbor/fairness，并不预测同一租户内的 next use。
- IPC 输入的 `IPCCacheServerKey.request_id` 标注为 session tracking，且 `compare=False`，所以 v0.5.2 已经有一个不改变 cache identity 的请求句柄；但它转换成 `ObjectKey` 后不会进入现有 eviction callback。现有 policy 仍看不到 tool、workflow state、owner、priority、next-use 和实际 miss cost。

### 6.3 建议的接入方式

对当前 CEDFS 路径，建议在 `LocalCPUBackend`/migration reporter 附近增加独立的 `CachePolicyMetadataStore`，以 cache key/chunk hash 为索引但不改变 cache identity，至少记录：

```text
owners / active_owners
workflow_state
session_state
tool_class / argument_class / tool_start_time
predicted_next_use / confidence / expires_at
prefix_offset / parent / tail relation
semantic_role
actual_bytes / estimated_miss_penalty
policy_epoch
```

短期应实现一个使用该 side table 的 `BaseCachePolicy` 专用实现，并复用 `can_evict`、现有逆序 touch 与批量回收。若以后把 CEDFS DRAM tier 迁到 MP server，则应将相同评分实现为新的 distributed `EvictionPolicy`，并扩展 listener/event context，使 `request_id` 能关联 workload metadata；不要把瞬态 priority 塞进 `lmcache.tag.*` 或 `cache_salt`。

接口方向可参考仍处于 open RFC 状态的 [vLLM Context-Aware KV-Cache Retention API #37003](https://github.com/vllm-project/vllm/issues/37003)：orchestrator 提供 token-range priority、TTL 和 scope，cache engine 只负责高效执行。对 CEDFS-KV，可由 CEDFS/controller 持有策略智能，LMCache DRAM evictor 使用本地、带 epoch 的 metadata snapshot，不能在内存压力快路径同步依赖远端控制面。

## 7. 实施顺序

### Phase 0：先证明存在策略空间

- 当前 `LocalCPUBackend` 可先复用已有 eviction/reuse Prometheus 指标，并补齐 request、session/tool/workflow、bytes、连续 prefix、重算/下层读取成本；不要假设 MP 指标会自动覆盖当前 CEDFS 进程内路径。
- MP 路径已提供 rotating lookup-hash JSONL、`cache-simulator` LRU replay，以及 `lmcache_mp.l1_chunk_reuse_gap`、`lmcache_mp.l1_chunk_evict_reuse_gap`、`lmcache_mp.real_reuse_gap`/`objects` 等指标。若评估 MP 部署，应先复用这些设施，不必从零实现 trace recorder。
- 现有 simulator 只模拟 LRU/token hit rate；扩展它或写 companion replay，加入 MRU、LFU、S3-FIFO/ARC 类 baseline、agent-aware policy 和 Belady oracle。lookup log 已带 `request_id`，但仍需并入 tool/lifecycle 和成本事件。
- 若 Belady 相对 LRU 的 saved-prefill headroom 很小，先扩容/分层；若差距大，再实现预测策略。

### Phase 1：无模型版本

- retired-first；
- waiting/tool-completed protection；
- tool 类别 P50/P90 或条件剩余时间；
- session victim + tail-first；
- shared-owner 聚合；
- LRU fallback。

在当前 `LocalCPUBackend` 中，tail-first 应直接继承已有 reversed-touch 顺序，只改变跨 session victim 排序。

这是预计性价比最高的版本。

### Phase 2：成本和 admission

- 使用实测 prefill curve、DRAM→GPU 和 CEDFS/lower-tier load latency；
- 加入 value-per-byte 和 large-object admission；
- ghost metadata 反馈和按 workload 类别的在线分布更新。

### Phase 3：多步预测

- 有显式 graph 时使用 KVFlow STE；
- 动态分支数据充分后再引入 PBKV 式 K-step predictor；
- 低置信度只影响 soft score，不越过 hard lifecycle rule。

## 8. 验证指标

主指标不能只是 chunk hit rate：

- saved prefill tokens、estimated saved prefill milliseconds；
- DRAM byte hit ratio、连续 prefix hit length；
- eviction-before-reuse 次数和复用间隔；
- DRAM→GPU、lower-tier→DRAM 和重算字节/时间；
- coding-agent session completion time、token goodput、P50/P95 TTFT；
- policy CPU 时间、heap rebuild 时间和锁竞争；
- tenant/session fairness，以及高 priority/lease 的 DRAM 占用。

其中 v0.5.2 已直接提供的只是部分基础数据：进程内路径的 local CPU eviction/reuse 指标，以及 MP 路径的 L1/L2 hit、evicted、reuse-gap、per-`cache_salt` 指标和 lookup trace。saved-prefill、连续可用 prefix、tool/workflow 分桶、完整 tier miss cost、session completion time、policy overhead 与误预测原因仍需补充或跨组件关联。

最重要的消融顺序是：LRU → retired-first → tool next-use → shared/prefix constraint → cost/admission → predictor。这样才能区分是策略改善，还是调度、预取或更大容量带来的收益。

## 9. 建议优先级

1. **最高优先级**：PBKV retired-first + CacheWise tool-return next-use + session/tail-first。
2. **第二优先级**：Bidaw/KVCache-in-the-Wild 式按类别在线分布与 ghost feedback。
3. **第三优先级**：RAGCache/Marconi 式 miss-cost-per-byte 和 admission。
4. **条件启用**：显式 graph 的 KVFlow STE、PBKV 多步 predictor、SAECache 在线语义学习。
5. **只作 DRAM fallback/诊断**：LRU、MRU、LFU、通用 ARC/S3-FIFO；它们缺少 agent 生命周期和 future-use 信号。

最终建议是把方案定义为 **Lifecycle-aware Predictive GDSF**，而不是为某篇论文做一比一移植：硬生命周期规则保证正确方向，tool-aware next-use 解决 coding agent 周期性，GDSF/admission 负责 DRAM 的容量经济性，prefix/owner 约束保证驱逐后的剩余 KV 仍然可用。

实现路线应按部署形态分叉：当前先落在 `LocalCPUBackend` 的 policy + metadata side table；只有迁移到 MP server 后，才在 distributed `EvictionPolicy` 上实现等价策略。`IsolatedLRU`/quota 作为公平性 guardrail 保留，但不作为 agent-aware 驱逐本身。

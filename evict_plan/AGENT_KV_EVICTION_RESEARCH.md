# Agent 负载 KV Cache 管理调研：KVFlow、PBKV、CacheWise、Continuum 与 SAECache

> 范围更新（2026-08-07）：CEDFS-KV + LMCache 的当前目标是 **DRAM tier 的跨请求 KV chunk 管理**，而不是 GPU/HBM block residency。DRAM 专项结论、相关工作排序和实施建议见 [DRAM_TIER_AGENT_EVICTION_RECOMMENDATION.md](./DRAM_TIER_AGENT_EVICTION_RECOMMENDATION.md)。其中的建议优先于本文偏 GPU/分层缓存视角的阶段规划。特别是：active coding-agent session 的动态历史通常会在下一轮整体复用，不能无条件按“动态 suffix 优先驱逐”处理。

> LMCache v0.5.2 审计（2026-08-07）：核对基线为官方 `v0.5.2` tag（`cd2c0d6a6a98`），当前 CEDFS 适配分支为 `v0.5.2-19-g53861679`。本文中“当前 CEDFS + LMCache”的实现判断指进程内 `LocalCPUBackend` 集成；v0.5.2 的 multiprocess/distributed L1/L2 是另一套可选路径，不能把两套 policy、key 和 observability 接口混写。

## 1. 文档范围

本文调研五项面向跨请求 prefix KV cache 管理的工作，并评估其对当前 CEDFS + LMCache + Dynamo/vLLM/SGLang 架构的适用性：

- [KVFlow](https://arxiv.org/abs/2507.07400)：静态或可显式描述的多 Agent workflow。
- [PBKV](https://arxiv.org/abs/2605.06472)：运行时路径不确定、包含条件分支和重试环的动态 workflow。
- [CacheWise](https://arxiv.org/abs/2606.16824)：LLM 调用与外部工具交替执行的 coding-agent session。
- [Continuum](https://arxiv.org/abs/2511.02230)：tool-call gap 期间的 GPU KV 保留和 program-level 调度。
- [SAECache](https://arxiv.org/abs/2605.18825)：混合 chat、agent、模板化单轮请求下的语义感知、自适应驱逐。

调研时间为 2026-08-07。除特别说明外，论文数字均为作者在各自实验设置下报告的结果，不能直接横向比较，也不代表在 CEDFS 组合中的预期收益。

本文讨论的是请求结束后仍可复用的 prefix/session KV cache，而不是 H2O、SnapKV 等单个请求内部按 attention score 删除 token KV 的有损压缩。五项工作均不修改模型输出语义，主要改变 cache 保留、迁移、预取或请求调度。

## 2. 结论摘要

### 2.1 五项工作不是互斥的替代方案

它们利用的是不同信号：

| 工作 | 主要信号 | 解决的核心问题 | 主要动作 |
| --- | --- | --- | --- |
| KVFlow | 显式 Agent Step Graph、steps-to-execution | 固定循环或静态图中 LRU 驱逐即将复用的 Agent | node-level priority、CPU→GPU 预取、status-aware scheduling |
| PBKV | 全局 call graph、历史路径、当前 prefill 语义、workflow 生命周期 | 动态分支和 retry loop 无法预先给出确定执行序列 | retired-first、预测复用评分、保守预取 |
| CacheWise | session、tool name、tool arguments、历史 tool duration | coding agent 的下次访问时间取决于正在执行的工具 | prefix-aware scheduling、近似 Belady 驱逐 |
| Continuum | tool duration CDF、reload/prefill cost、queueing delay | 请求结束即释放导致每轮重载和重新排队 | 有界 TTL pin、TTL-aware priority、program-level FCFS |
| SAECache | token role、session 类型、块位置、在线命中/误驱逐反馈 | 不同语义块复用价值差异大，固定策略易受 workload drift 影响 | 多队列、语义权重、在线参数学习 |

最合理的 CEDFS 演进方向不是完整复刻其中一项，而是组合其低风险机制：

1. 先引入 PBKV 的 lifecycle guardrail：结束 workflow 的 private cache 优先回收。
2. 再引入 SAECache 的粗粒度语义分类：retired/废弃分支的动态 suffix 优先；active coding-agent session 的完整历史按 next-use 保护，不能仅因它是动态内容就优先驱逐。
3. 对 tool gap 使用 Continuum 的有界 TTL，而不是无限 pin。
4. 有历史数据后，用 CacheWise 的相对 next-use 排序替代纯 LRU。
5. 只有当调用图或预测置信度可靠时，才启用 KVFlow/PBKV 风格的预取。

### 2.2 当前 CEDFS 不能单独完成 GPU eviction

当前 CEDFS 维护的是 LMCache CPU KV 的全局位置、热度和跨实例迁移控制面；它不拥有 vLLM/SGLang GPU block allocator，也不知道本地 waiting/running queue。因此：

- GPU block 的驱逐、pin、调度和 prefetch 必须在 inference engine 的本地快路径执行。
- CEDFS 适合提供跨实例热度、生命周期、共享度、预测提示和目标位置等慢路径信号。
- Dynamo 负责把 session/workflow hint 送到正确 worker，并结合 KV locality 与负载路由。
- LMCache/CEDFS 负责 HBM miss 后的 CPU 命中、跨实例复制和更长生命周期的二级保留。

LMCache v0.5.2 新增/完善的 MP distributed eviction 并不改变这个控制面边界。它提供 L1/L2 `EvictionPolicy`、锁过滤、`cache_salt` 隔离、L2 quota 和 `IsolatedLRU`，但 `IsolatedLRU` 仍是租户内 LRU，只解决公平性，不解决 coding-agent 周期访问。当前 CEDFS migration plugin 仍接入 `LocalCPUBackend`，所以短期 DRAM 策略应落在该路径，而不是假设 distributed policy 已经生效。

仓库中的 Dynamo 已具备 `nvext.agent_hints.priority`、priority-aware routing，并可向 SGLang 的 priority radix eviction 传递优先级；`program_id`、`context_type` 和通用 cache prefetch 仍在文档中标为 planned/future work，参见 [agentic_workloads.md](../../dynamo/docs/features/agentic_workloads.md)。这提供了近期可落地的执行通道，但还不是五项论文策略的完整实现。

## 3. 问题模型

以容量只能容纳三个工作集、访问序列为：

```text
a, b, c, d, a, b, c, d, ...
```

LRU 会在每次访问时驱逐下一轮即将访问的条目，预热后可能持续 miss。这不是 GQA 或 MLA 特有问题；GQA/MLA 改变每个 token/block 的字节数、搬运时间和重算成本，但不会改变基于过去 recency 无法预测周期访问的根因。

当工作集大于 HBM 容量时，任何只使用 HBM 的策略都无法保证 100% 命中。因此实际目标应从单一 hit rate 扩展为：

- 少做多少重复 prefill；
- HBM miss 是否能命中 CPU/远端 cache；
- CPU→GPU 传输能否与 decode 重叠；
- 是否因为 pin/prefetch 挤占其他活跃请求；
- agent job completion time 和 token goodput 是否改善。

## 4. KVFlow

### 4.1 目标与核心机制

KVFlow 针对结构化多 Agent workflow。其出发点与周期访问例子完全一致：一个 Agent 很久没有访问，不代表它未来很久不用；在固定循环里，它反而可能是下一步。

KVFlow 将应用执行结构表示为 Agent Step Graph。每个 Agent 节点带一个 step aggregation function，用于计算 steps-to-execution（STE）：

- 多个前置步骤都必须完成时，使用类似 `max(E1, E2) + 1` 的聚合；
- 任一路径完成即可触发时，使用类似 `min(E1, E2) + 1` 的聚合；
- STE 越大，距离下一次执行越远，越适合驱逐。

驱逐不是 Agent 粒度，而是 radix-tree node 粒度：

1. 只给 Agent 的固定 prompt 部分赋 STE；动态 suffix 总是最高驱逐优先级。
2. Agent 固定前缀末端节点获得该 Agent 的 STE。
3. 优先级向树根传播；共享节点取所有相关 Agent 中最小 STE，确保只要有一个 Agent 即将使用，该共享前缀就受到保护。
4. 内存不足时先驱逐动态 suffix，再按 STE 从大到小驱逐固定前缀节点。

### 4.2 预取与调度

KVFlow 使用 host memory 保存从 GPU 驱逐的固定 prompt KV：

- 当前 Agent 生成时，从 Step Graph 推断下一批可能执行的 Agent，并异步 CPU→GPU prefetch。
- 条件分支下可保守地预取多个候选，但有并发数量上限。
- cache node 有 `GPU`、`CPU backup`、`loading`、`offloading` 四种状态。
- scheduler 遇到仍在 loading 的请求时先跳过，调度其他 ready request，以隐藏传输尾延迟并避免重复加载。

原型基于 SGLang v0.4.4，修改了 frontend、HTTP metadata、radix cache 和 scheduler。请求需要携带 client ID、当前 Agent ID、所有 Agent 的 STE；固定/动态边界可由调用方显式标注，也可从稳定命中前缀推断。

### 4.3 实验结果与边界

论文使用 Llama-3.1-8B/A10G 和 Qwen2.5-32B/H100，评估 10-Agent 顺序 workflow、并发 workflow 和 PEER 风格模拟：

- 在 A10G、`8192 fixed / 32 dynamic / 32 output` 设置下，相对 SGLang+HiCache 报告 1.83x speedup，相对 GPU-only SGLang 为 2.91x。
- 高并发实验中，相对 SGLang 最多 1.25x，相对 LRU HiCache 最多 2.19x。
- 更现实但 prompt 较短的 PEER 模拟中，相对 SGLang 和 HiCache 的最大收益分别约 1.12x 和 1.08x。
- output 越长，decode 在总时间中的占比越高，KVFlow 的相对收益越小。

主要局限：

- 强依赖可显式表达且足够准确的 workflow graph；运行时生成的新分支和 retry loop 难以提前给出 STE。
- 需要应用框架与 serving backend 共同传递 Agent 身份、图和固定 prompt 边界，耦合较强。
- 高并发实验明确排除了“所有显存都被 active request 占满、没有 reusable prefix 空间”的情况。
- 最大收益来自超长固定 prompt 和受限 PCIe/GPU 场景；更现实的短 prompt 实验收益明显较小。
- arXiv 页面截至调研日未链接独立公开代码仓库，工程成熟度应按论文原型看待。

### 4.4 对 CEDFS 的启示

KVFlow 最值得借鉴的是“上层已知的未来信息应进入 cache policy”，而不是复制 Step Graph 实现本身。CEDFS 可以接收 `workflow_id/agent_id/next_agents/steps_to_execution`，计算跨实例候选和 host 预放置；实际 GPU priority 和 prefetch 仍应由 Dynamo + engine 执行。

## 5. PBKV

### 5.1 相对 KVFlow 的扩展

PBKV 认为实际 workflow 只是全局 call graph 上的一条运行时路径。全局图可以包含所有允许的边和 retry loop，但当前任务究竟走哪条边取决于上下文和 LLM 输出，因此不能像 KVFlow 一样直接把静态图当作未来真值。

PBKV 对未来 `K` 步 Agent 调用输出概率分布。论文实现融合三类信号：

- GraphSAGE 编码的全局 Agent transition graph；
- 对已执行 Agent 历史进行 attention aggregation 得到的路径特征；
- 复用 serving LLM 最后一个 prefill token 的 hidden state，表示当前请求语义。

这些特征经过小型 MLP 一次性输出未来多步分布，避免 autoregressive predictor 自身累积误差。论文 predictor 约 350K 参数；在其 HoVer 设置下，使用 1K traces 训练后，1-step/3-step accuracy 分别为 0.94/0.77。

### 5.2 分层驱逐

PBKV 的驱逐分为确定性 guardrail 和概率评分两层。

第一层是 lifecycle-aware eviction：

- workflow 结束后，仅属于该 workflow 的 private cache 被标为 retired；
- retired block 在所有 active cache 之前驱逐；
- 多 workflow 共享的 popular prefix 不会因单个 workflow 结束而立即失去价值。

第二层对 active cache 计算多步 lookahead score：

\[
Score(c)=\sum_{k=1}^{K}\gamma^{k-1}
\sum_{w\in W_{act}(c)}s_w^{(k)} A_w(c)\cdot P_w^{(k)}
\]

其中：

- `A_w(c)` 表示 workflow `w` 的哪些 Agent 曾访问 block `c`；
- `P_w^(k)` 是第 `k` 步各 Agent 的预测概率；
- `s_w^(k)` 是 workflow 到第 `k` 步仍未结束的概率；
- `gamma` 对较远、较不可靠的预测衰减；
- 对所有 active workflow 求和会自然保护跨 workflow 的 popular prefix。

驱逐时先排空 retired cache，再从 active cache 中按 score 从低到高回收。相比“预测为 0 就直接驱逐”，这个层次保证预测错误时仍先执行确定无用的回收。

### 5.3 保守预取

PBKV 基于 SGLang+HiCache 的 GPU/host 两层 cache，但不允许为了 speculative prefetch 驱逐 active cache：

- 可用空间只取 GPU free space 与 retired cache 的并集。
- 只在 pure-decode batch 中预取，减少与 prefill 对 PCIe 的竞争。
- 传输预算为 `min(可用空间, 带宽 × decode step duration)`。
- 预取只使用 one-step value 排序；更远的候选可以下一步再取。

这使错误预取通常只浪费空闲带宽，不会把已知有价值的 active cache 挤出 HBM。

### 5.4 实验结果与边界

PBKV 使用 Qwen3-14B/32B、8×A6000、SGLang+HiCache，评估 HoVer+LangChain、SWE-bench+AutoGen 和静态 FinanceBench+CrewAI：

- HoVer 动态 workflow 上，相对 LRU 的 workflow latency 最多改善 1.85x，per-agent TTFT 最多改善 2.03x。
- 代表性 Qwen3-32B 设置中，GPU token hit rate 从 LRU 的 27.09% 提升到 69.10%；仅 retired-first 已提升到 44.91%。
- 静态 FinanceBench 上相对 KVFlow 最多约 1.26x；论文将收益归因于 lifecycle guardrail 和跨 workflow popularity aggregation。
- 论文观察到 `K=3` 最优；过短会近视，过长会引入远期预测噪声。

主要局限：

- predictor 需要 workload-specific traces，且论文未完整评估长期 distribution drift。
- 需要访问 serving model 的 prefill hidden state，并维护 Agent identity、workflow path 和 block-to-Agent access vector。
- message-passing 约定决定哪些 Agent 实际能复用哪些 private prefix，需要逐框架适配。
- full PBKV 相比“分层驱逐但无预取”的增益较小，说明预测首先应用于 eviction 比激进 prefetch 更可靠。
- arXiv 页面截至调研日未给出稳定的公开实现入口，应视为研究原型。

### 5.5 对 CEDFS 的启示

PBKV 最适合拆成三个独立阶段引入：

1. 不需要模型的 `workflow_end -> retired private cache first`。
2. 不需要神经网络的跨 workflow access-count/popularity aggregation。
3. 最后才增加可插拔 predictor；预测失败时必须回退到前两层，而不是回退为纯 LRU。

这比一步引入 GraphSAGE/hidden-state predictor 更符合 CEDFS 当前控制面的成熟度。

## 6. CacheWise

### 6.1 Workload 观察

CacheWise 针对 coding agent：一个长生命周期 session 在 LLM generation 与 shell、测试、文件、git 等 tool execution 之间交替；每一轮 prompt 通常是上一轮 prefix 的单调扩展。

论文认为两个默认策略共同造成 thrashing：

- FCFS 将多个长 session 交错运行，持续扩大 active KV working set。
- LRU 不知道一个 session 的工具即将结束，可能驱逐马上要返回的 session，同时保留仍将长时间执行工具的 session。

因此优化目标从单请求 TTFT/TBT 转为 session completion time 和 useful-token goodput。

### 6.2 Prefix-aware scheduling

对 waiting request `r_i`，CacheWise 计算当前还需新增的 block 数 `a_i(t)`，优先调度 `a_i(t)` 最小的请求。这同时：

- 最大化现有 HBM prefix 的使用；
- 减少为新请求腾空间所需的 eviction；
- 近似 shortest-job-first，但可能牺牲单请求公平性。

该机制说明 eviction 不能脱离调度单独优化：即使 victim 选择更准，FCFS 仍可能不断切换 session 并制造工作集抖动。

### 6.3 Predictive eviction

CacheWise 近似 Belady：优先驱逐预计下次使用时间 `tau_i(t)` 最远的 session block。它不追求准确预测绝对时长，只要求把各 session 的 next-use 相对顺序排对，尤其识别最晚返回的那个 session。

预测器使用：

- tool name；
- tool arguments；
- tool 开始时间；
- 相同用户、项目或环境中的历史执行时长。

对于同一种 tool 内部差异很大的命令，论文使用 tool arguments 的 TF-IDF embedding 和 KMeans 分簇，并从相似历史样本的 duration distribution 估计条件剩余时间。实现把预测值作为 eviction heap priority；共享同一 session metadata 的 blocks 只预测一次。由于条件剩余时间随已等待时长变化，heap 每若干 engine iteration 重建，论文取 `N_rebuild=3`。

### 6.4 实验结果与边界

原型约 2,500 行 Python，修改 vLLM scheduler 与 KV block manager；实验使用 2×H200、Qwen2.5-Coder-32B 和真实 CATraces replay：

- 高负载下相对 vLLM 和 InferCept，session completion time 报告降低 2.7x–3.5x。
- KV eviction 数量报告减少约 2x–2.6x。
- 低负载、没有明显 cache contention 时，各策略表现相近。
- 论文称轻量预测器接近使用 ground-truth tool latency 的 oracle variant。

主要局限：

- 依赖 tool name/args 和历史 duration trace；新项目、新工具或 workload drift 会降低预测质量。
- 论文明确把 drift robustness 留作未来工作。
- prefix-aware scheduling 优化整体完成时间，需额外处理 starvation 和交互式高优请求。
- 它预测 session 何时回来，不预测下一 Agent 或共享 prefix 的语义价值；与 PBKV/SAECache 是互补关系。
- 论文公开了 CATraces 数据仓库，但截至调研日未确认论文中的完整 vLLM fork 已作为稳定组件发布。

### 6.5 对 CEDFS 的启示

CEDFS 不必复制 TF-IDF/KMeans 才能获得第一阶段收益。可以先按 `(tool_name, command_class, compatibility_group)` 维护 duration histogram/CDF，只预测相对 next-use bucket：

```text
imminent / short / medium / long / unknown
```

本地 engine 使用 bucket 决定 HBM eviction；CEDFS 使用它决定 CPU 副本是否保留、是否提前迁移到 session affinity worker。

## 7. Continuum

### 7.1 与 CacheWise 的区别

Continuum 关注同一个 Agent program 在 tool gap 期间是否继续占用 GPU KV。即使 KV 已 offload 到 CPU，返回后的请求仍可能需要等待其他 active request 释放 HBM；这个 per-turn queueing delay 会在多轮 Agent 中累计。因此只比较 CPU reload 与 GPU residency cost 会低估 pin 的收益。

### 7.2 TTL utility model

Continuum 为产生 tool call 的已完成请求计算 TTL `tau`。GPU 占用成本近似为：

\[
Cost(\tau,r)=\frac{MemUsage(r)}{AverageMemUsage}\times\tau
\]

命中收益包括两部分：

\[
Benefit(r)=CacheMissCost(r)+OutOfOrderCost(r)
\]

- `CacheMissCost`：重新 prefill 或从 CPU reload 的时间。
- `OutOfOrderCost`：KV 被释放后，下一轮重新进入 waiting queue 造成的顺序破坏和排队时间。

根据历史 tool `f` 的执行时长经验 CDF，选择：

\[
\tau^*=\arg\max_{\tau}
P(tool_f\ finishes\ within\ \tau)\times Benefit(r)-Cost(\tau,r)
\]

若工具在 TTL 内返回，请求立即复用 pinned KV；若超时，自动 unpin/evict，避免长尾工具或失败工具无限占用显存。

### 7.3 调度与实现

Continuum 使用多级 request priority：

1. 已被 preempt 的请求；
2. 仍处于 TTL 内的返回请求；
3. program-level arrival order。

它通过 program-level FCFS 尽量保持整个 Agent program 的连续性。原型在 vLLM 上约增加 1K 行 Python，tool-call handler 在请求结束时识别工具并设置 TTL，在下一轮到达时记录真实工具完成时间以更新历史分布。

### 7.4 实验结果与边界

论文 v6 使用 SWE-Bench、BFCL、OpenHands/内部 coding-agent traces，以及 Llama-3.1-8B/70B、Gemma-3-12B、GLM-4.5-355B，覆盖 A100/H100/B200：

- 公开实验中报告延迟改善约 1.12x–3.66x、吞吐改善约 1.10x–3.22x。
- 内部真实 SWE-agent 场景报告最高约 8.18x。
- 摘要中的“over 8x”来自特定内部设置，不应作为普遍预期。

主要局限：

- 重点是同 program 下一轮立即复用，不直接处理多 Agent shared prefix、周期调用图或 token 语义。
- 需要准确识别 tool boundary、program identity 和终止状态。
- 经验 CDF 在 cold start、少样本和长尾工具下不稳定；论文需要专门的 cold-start fallback。
- pin 的收益依赖本地 queue pressure；只在 CEDFS 全局侧计算 TTL 会缺失 worker 实时队列状态。
- [预览代码](https://github.com/Hanchenli/vllm-continuum)可用，但仍是 vLLM 研究分支而非默认 upstream policy。

### 7.5 对 CEDFS 的启示

不要把当前 `ActiveSequences` 的 5 分钟过期机制当作 Continuum TTL。前者只是控制面陈旧请求清理，[squnence.rs](../cedfs-kv/src/transfer/squnence.rs) 中的 deadline 不会 pin GPU block，也没有 cost-benefit 决策。

Continuum TTL 应由本地 engine 根据 queue/memory 状态执行；CEDFS 只需保存 program/tool 生命周期和历史统计，并向 Dynamo/worker 下发建议 TTL 或 priority。最终 TTL 必须由 worker 限幅和拒绝，以避免全局提示导致本地死锁。

## 8. SAECache

### 8.1 Workload 观察

SAECache 认为 recency/frequency 仍忽略了 prompt 内部的语义差异：

- decode block 的跨请求精确复用率低，应优先回收；
- 单轮模板请求的前部 system/tool scaffolding 比尾部动态内容更可复用；
- 多轮请求的复用几乎都在同一 session 内；
- chat 与 agent 的 inter-turn time 分布不同；
- system prompt、user query、tool output、model response、chain-of-thought 等 token type 的复用率可相差数百倍。

### 8.2 多队列与评分

SAECache 将 block 路由到四类队列：

- `evict-first`：decode 和低复用、非模板化 prefill block；
- `structural`：模板化单轮请求的 prefill block；
- `chat`：多轮 chat session block；
- `agentic`：多轮 agent session block。

多轮 block 的局部保留概率来自在线拟合的 log-normal inter-turn distribution：

\[
p_q(b)=1-F_{LN}(\Delta t_b;\mu_q,\sigma_q)
\]

结构化 block 根据 prompt offset 衰减，越靠前优先级越高：

\[
p_q(b)=1-(offset_b/offset_{max})^\gamma
\]

最终 retention score 还乘以 token-type weight 和 queue hit-efficiency weight。算法先排空 `evict-first`，再从其他队列中选择最低分 block。log-normal 参数、position decay、token type weight、queue weight 及 EMA/temperature 等均从正常 cache hit、miss-after-eviction 等反馈在线更新。

对于没有 history 的首请求，SAECache 使用 serving model 最后一个 prefill token 的 hidden state，加约 1M 参数的三层 MLP，预测它是否会发展成 multi-turn session，从而决定进入 structural 还是 multi-turn queue。

### 8.3 实验结果需要谨慎解读

论文在 vLLM v0.8.5 V0 engine 上实现 drop-in cache evictor，并用 Qwen2.5-1.5B/A40 验证可运行性。主要 trace-driven 结果使用 ShareGPT、LMSys、Chatbot-Arena：

- 所有 12 个数据集/注入间隔组合中，hit ratio 相对最强 baseline 提高 4.8–5.9 个百分点。
- LMSys 上相对 LRU 的 mean TTFT 降低 4%–8%，相对 LPC 最多 16%。
- ShareGPT 上大致持平于最佳 baseline。
- 只有 12% multi-turn 的 Chatbot-Arena 上，尽管 hit ratio 更高，TTFT 反而比最佳 baseline 差 12%–34%，原因是多队列 bookkeeping 和 session predictor 开销超过 prefill 节省。

摘要报告的 1.4x–2.7x TTFT 改善主要需要结合论文的合成 workload/消融设置理解，不能覆盖性地概括真实 trace 主结果。该论文最有价值的结论不是固定 speedup，而是“语义类型与 workload composition 必须进入 policy，且固定权重可能在流量漂移后反向劣化”。

主要局限：

- 需要可靠的 token/segment role 标注；仅从 token 内容推断 role 容易混淆工具输出、代码和 reasoning。
- 首轮 session predictor 需要模型 hidden state，增加 engine 耦合。
- 多队列和在线学习在低复用流量上可能产生净负收益。
- 论文把 cluster-level routing 与 eviction 联合优化列为未来工作。
- arXiv 页面截至调研日未链接稳定公开代码仓库。

### 8.4 对 CEDFS 的启示

CEDFS 近期不应实现完整在线学习器。可以先采用稳定且可解释的硬规则：

```text
active/in-flight                 不驱逐
retired private / dead branch    最先驱逐
decode / retired dynamic suffix  高驱逐优先级
active tool output / reasoning   由 next-use/lifecycle 决定
active user/session history      由 next-use/lifecycle 决定
system / tool schema / shared    低驱逐优先级
```

只有在已有分类型 hit、miss-after-eviction、bytes-saved 和 policy-overhead 指标后，再用在线反馈调整权重。

## 9. 横向比较

### 9.1 未来信息强度

从确定性到不确定性，大致为：

```text
KVFlow 静态 Step Graph
  -> PBKV 动态多步调用概率
  -> CacheWise/Continuum tool-return 时间分布
  -> SAECache 语义与历史统计
  -> LRU 仅使用过去 recency
```

信号越强，理论上越接近未来访问，但集成成本和错误风险也越高。可靠的确定性生命周期信号应始终优先于概率预测。

### 9.2 决策粒度

| 工作 | 生命周期粒度 | 驱逐粒度 | 是否显式考虑共享 prefix |
| --- | --- | --- | --- |
| KVFlow | client / workflow / Agent | radix node | 是，父节点取最小 STE |
| PBKV | workflow / Agent | radix node | 是，跨 workflow 概率求和 |
| CacheWise | coding session / tool call | session-associated block | 主要关注同 session 增长前缀 |
| Continuum | program / turn | 整个请求的 KV pin | 否，重点是 program continuity |
| SAECache | session / semantic segment | block / queue | 通过结构位置和 session 类型间接考虑 |

### 9.3 预测失败时的风险

| 工作 | 错误后果 | 主要保护机制 |
| --- | --- | --- |
| KVFlow | 错误驱逐或错误预取分支 | 分支预取上限、status-aware scheduling |
| PBKV | active cache 被错误判断为低价值 | retired-first、置信度衰减、不为预取驱逐 active cache |
| CacheWise | 驱逐马上返回的 session | 只预测相对次序、周期重建 heap |
| Continuum | TTL 太长阻塞别人，太短白占显存后仍 miss | TTL 有界、基于经验 CDF 最大化净收益 |
| SAECache | 流量漂移后错误队列/权重 | 在线反馈和参数自适应；低复用场景仍有 overhead 风险 |

## 10. 当前 CEDFS 能力与缺口

### 10.1 已有能力

当前 [kv_radix.rs](../cedfs-kv/src/kv_radix.rs) 的 `RadixBlock` 已保存：

- `seq_hash/local_hash/position/offset/tokens`；
- 持有该块的 `servers`；
- `heat` 与 `last_access`；
- parent/children，可表达共享 prefix；
- 全局 block index 和 per-server lookup。

[kvcache_v2.proto](../cedfs-proto/proto/kvcache_v2.proto) 已增加实例 epoch、lease、capacity snapshot、compatibility fingerprint、逐块 mutation、inventory sync 和 request start/end。这些为可靠生命周期和跨实例决策提供了基础。

### 10.2 缺失信号

当前 V2 request lifecycle 只有 `request_id + blocks`，没有：

- `workflow_id/session_id/program_id/agent_id/step_id`；
- workflow 完成、Agent 完成、tool start/end；
- tool name/class/arguments fingerprint；
- fixed/dynamic 边界和 `context_type`；
- next Agent、STE 或预测概率；
- GPU/CPU tier hit、reload、recompute、queue delay；
- block 实际字节数和 GPU residency。

这里的缺口是 **CEDFS 协议和 eviction callback 没有消费 workload 语义**，而不是 LMCache v0.5.2 完全没有请求句柄：MP `IPCCacheServerKey.request_id` 已用于 session tracking 且不参与 equality/hash；问题在于它转换为 `ObjectKey` 后不会传入现有 eviction policy。相反，`cache_salt` 和进程内 `lmcache.tag.*` 都属于 cache identity，不能拿来承载瞬态 tool priority 或 workflow state。

当前 `heat += 1` 是累计访问次数，无法区分“过去很热但 workflow 已结束”与“访问不多但下一步马上使用”。这正是五项工作共同指出的缺口。

### 10.3 控制面边界

CEDFS 当前接收 LMCache CPU store/remove 事件，而 vLLM/SGLang GPU cache 可能有不同的 block 生命周期。把论文中的 HBM eviction score 直接加到 CEDFS radix tree，不会自动改变 GPU victim 选择。

建议拆成：

```text
Agent harness
  -> workflow/tool/semantic hints
  -> Dynamo：路由、session affinity、priority 传播
  -> Engine：GPU block pin/evict/prefetch、waiting queue
  -> LMCache：CPU/远端二级 cache、load/store
  -> CEDFS：全局位置、生命周期、热度/共享度、迁移与策略建议
```

## 11. 建议的 CEDFS 演进方案

### Phase 0：先建立可验证指标

在改变策略前，统一采集：

- GPU token/block hit、LMCache CPU hit、remote hit、full miss；
- avoided prefill tokens/FLOPs；
- CPU↔GPU 与跨实例传输 bytes、排队和 stall；
- eviction 后在不同时间窗口内再次访问的比例；
- request TTFT、queue time、session/job completion time、useful-token goodput；
- 按 context type、tool class、workflow state 分桶；
- prefetch accuracy、coverage、wasted bytes 和有害驱逐次数。

若评估 v0.5.2 MP 路径，优先复用其 lookup-hash JSONL、LRU `cache-simulator`、L1/L2 hit/evicted 和 reuse-gap 指标；现有 simulator 仍需扩展 agent-aware/Belady policy，lookup trace 也需与 tool/lifecycle/cost 事件关联。当前 CEDFS 的 `LocalCPUBackend` 路径不能自动获得这些 MP 指标，需要在既有 local CPU eviction/reuse metrics 上补齐。

不要只看逻辑 prefix hit rate；一个已经从 HBM 驱逐但仍在 CPU 的 block，与需要完整 recompute 的 miss 成本完全不同。

### Phase 1：确定性 lifecycle 与语义 guardrail

为 V2 协议添加独立、可选的 workload metadata，避免改变现有 block identity：

```text
workflow_id
session_id / program_id
agent_id / step_id
event: WORKFLOW_START | TOOL_START | TOOL_END | WORKFLOW_END
context_type: SYSTEM | TOOL_SCHEMA | USER | TOOL_OUTPUT | REASONING | RESPONSE
fixed_prefix_end
```

首版策略只使用确定性信息：

1. active/ref-counted block 不驱逐。
2. 已结束 workflow 的 private block 先驱逐。
3. decode 和已确认属于 retired/dead branch 的 unique dynamic suffix 次先驱逐；active session 的 tool output/reasoning 不能仅凭语义类型进入 evict-first。
4. 跨 workflow shared system/tool prefix 最后驱逐。
5. 同分时回退 LRU，保证确定行为。

这是 PBKV-LAE 与 SAECache evict-first 的组合，不需要 predictor，实施和回滚风险最低。

### Phase 2：Tool-aware TTL 与 next-use bucket

维护按 `tool_class + compatibility_group + environment class` 分桶的 duration CDF，并输出：

- 建议 TTL；
- expected remaining time bucket；
- 预测置信度与样本数。

worker 使用本地 queue/memory pressure 对 TTL 再裁剪：

```text
effective_ttl = min(global_suggestion, local_cap)
```

无样本或低置信度时回退到短 TTL/不 pin。该阶段结合 Continuum 的有界保留与 CacheWise 的相对 next-use 顺序。

### Phase 3：Priority contract 与本地执行

定义与后端极性无关的 retention contract，例如值越大越值得保留：

```text
retention_priority
expires_at
reason: LIFECYCLE | SEMANTIC | TOOL_RETURN | NEXT_AGENT | POPULARITY
confidence
```

Dynamo 将其映射到路由和 worker；SGLang 使用 priority radix eviction。vLLM 上游尚未具备同等成熟的 context-aware block eviction 时，不应在 CEDFS 中假设 priority 已真正生效。

### Phase 4：可选的 workflow lookahead 与保守预取

优先支持调用方显式给出的 `possible_next_agents/STE`，再考虑训练 predictor：

- 静态 workflow：采用 KVFlow STE。
- 动态 workflow：采用 PBKV 概率 score。
- prefetch 只占用 free/retired budget，不为预测数据驱逐 active cache。
- 加入 PCIe/NIXL 带宽预算、in-flight 去重和 cancel/version 检查。
- 预测缺失或过期时继续使用 Phase 1–3，不回退到纯 LRU。

### Phase 5：在线语义权重

只有当分类型反馈数据充分且 policy overhead 可观测时，再引入 SAECache 式在线权重：

\[
RetentionValue(b)=
LifecycleGuardrail\times
RoleWeight\times
ReuseProbability\times
SharedWorkflowCount\times
\frac{MissCost}{Bytes}
\]

其中 `MissCost` 应根据实际 tier 区分 GPU reload、CPU load、remote transfer 和 full prefill，GQA/MLA 使用真实 block bytes，而不是统一 token 常数。

## 12. 推荐验证矩阵

### 12.1 Workload

| 场景 | 目的 |
| --- | --- |
| `a,b,c,d` 周期，容量小于 working set | 复现 LRU 循环抖动 |
| 固定 DAG、多 Agent 共享部分 system prompt | 验证 KVFlow priority propagation |
| 含 retry loop/条件分支的动态 workflow | 验证 PBKV 预测错误与 guardrail |
| tool duration 为短/长双峰和重尾分布 | 验证 CacheWise/Continuum |
| 多 session、prefix 单调增长 | 验证 prefix-aware scheduling |
| chat/agent/模板单轮混合且比例漂移 | 验证 SAECache 语义与自适应 |
| workflow 结束后大量 private cache 残留 | 验证 retired-first |
| GQA 与 MLA、不同 KV dtype/block bytes | 验证 cost-per-byte 泛化 |

### 12.2 Baseline 与消融

- LRU。
- LFU 或现有 heat-based policy。
- lifecycle-only。
- lifecycle + semantic static weights。
- + TTL。
- + tool next-use ordering。
- + workflow prediction eviction。
- + conservative prefetch。

每一步单独启用，避免把调度、驱逐和预取的收益混在一起。

### 12.3 安全约束

- active block、loading/offloading block 不得被重复驱逐或迁移。
- priority/TTL 必须带版本和过期时间，防止陈旧预测长期生效。
- workflow end 应幂等，并与 instance epoch/lease 对齐。
- prefetch 不得占满为 active decode 预留的 HBM。
- 未知 context type、未知 tool、低置信度预测必须有保守 fallback。
- 任何策略都要有 per-tenant quota，防止高 priority/pin 被滥用。

## 13. 最终建议

对当前 CEDFS，优先级建议为：

1. **优先实现**：PBKV lifecycle-aware eviction、SAECache 的 evict-first/semantic hard rules，以及完整分层命中指标。
2. **随后实现**：Continuum 式有界 TTL 和 CacheWise 式 tool-return 相对排序；策略在 worker 本地结合 queue pressure 执行。
3. **按需实现**：调用方显式 Step Graph 时使用 KVFlow STE；动态 workflow 数据足够后再引入 PBKV predictor。
4. **暂缓实现**：SAECache 全量在线学习和 hidden-state session predictor，直到低复用 workload 上的 policy overhead 已能被准确测量和自动关闭。

这一路径先利用确定性生命周期和现有 Dynamo priority 通道获得可解释收益，再逐步引入预测，不会让 CEDFS 的全局控制面成为 GPU scheduler 的同步依赖。

## 14. 一手资料

- Zaifeng Pan et al., [KVFlow: Efficient Prefix Caching for Accelerating LLM-Based Multi-Agent Workflows](https://arxiv.org/html/2507.07400), 2025-07-10。
- Haoyu Zheng et al., [Efficient Serving for Dynamic Agent Workflows with Prediction-based KV-Cache Management](https://arxiv.org/html/2605.06472), 2026-05-07。
- Shubham Tiwari et al., [CacheWise: Understanding Workloads and Optimizing KVCache Management for Efficiently Serving LLM Coding Agents](https://arxiv.org/html/2606.16824), 2026-06-15。
- Hanchen Li et al., [Continuum: Efficient and Robust Multi-Turn LLM Agent Scheduling with KV Cache Time-to-Live, v6](https://arxiv.org/html/2511.02230), 2026-05-25。
- Shaoke Fang et al., [Not All Tokens Are Worth Caching: Learning Semantic-Aware Eviction for LLM Prefix Caches](https://arxiv.org/html/2605.18825), 2026-05-12。
- vLLM, [Automatic Prefix Caching design](https://docs.vllm.ai/en/stable/design/prefix_caching/)。
- NVIDIA Dynamo, [Agents: workload-aware inference](https://docs.nvidia.com/dynamo/latest/user-guides/agents)。
- NVIDIA Dynamo, [KV-cache-aware routing](https://docs.nvidia.com/dynamo/latest/user-guides/kv-cache-aware-routing)。

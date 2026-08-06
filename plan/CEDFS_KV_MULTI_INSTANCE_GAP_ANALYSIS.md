# CEDFS-KV 单 Meta、多实例场景缺口分析与演进建议

## 1. 结论摘要

本文在 [CEDFS_KV_ARCHITECTURE.md](CEDFS_KV_ARCHITECTURE.md) 的架构梳理基础上，进一步评估当前 `cedfs-kv` 是否足以承担“集群内多个 vLLM + LMCache 实例共享 KV cache、由单个 CEDFS meta 服务协调”的生产场景。

本次明确不讨论多个 meta 服务之间的元数据同步、选主、共识或跨 meta 分片。即使把这些问题全部排除，当前实现仍有三类生产阻塞项：

1. **元数据可能被写错**：LMCache 只上报本次实际新存储的 token suffix，CEDFS 却把它当作从根开始的完整 token 序列重新计算累计 hash，非首段 KV block 会得到错误身份。
2. **迁移的部分成功会被记成全部成功**：LMCache 允许只满足一部分 chunk 并返回正数，CEDFS 对任意正数都为目标实例登记本批全部 block，形成“meta 认为存在、目标实际不存在”的幽灵副本。
3. **实例和元数据没有恢复闭环**：实例无租约、心跳、注销和进程代次；meta 重启无持久化、实例也无 inventory 重放。同地址实例重启或 meta 重启后，元数据要么长期残留，要么整体丢失。

因此，当前版本适合受控验证，不宜直接作为生产级多实例 KV cache 控制面。建议先修复协议正确性，再补实例生命周期和状态重建，最后优化迁移策略、可观测性与安全性。

## 2. 分析范围与评判标准

### 2.1 场景边界

本文假设：

- 集群内只有一个活跃的 `cedfs-kv` meta 服务。
- 多个 vLLM + LMCache 实例向该 meta 注册和上报。
- KV tensor 保存在各 LMCache 实例，CEDFS 只保存位置、热度和迁移控制状态。
- CEDFS 可以命令源实例把 KV 复制到目标实例。
- Dynamo 继续维护自己的在线路由索引；本文不要求 Dynamo 改为直接查询 CEDFS。

本文不把下列事项列为当前缺口：

- meta-to-meta 全量或增量同步；
- 多 meta 的一致性协议、选主和脑裂处理；
- 跨集群、跨地域的 KV 复制。

单 meta 的本地持久化、实例重连和 inventory 重建仍在范围内，因为它们直接决定单 meta 重启后的可恢复性。

### 2.2 优先级定义

| 优先级 | 定义 |
| --- | --- |
| P0 | 会产生错误 KV 位置、跨不兼容实例迁移，或使控制面故障直接影响推理主路径；生产前必须修复 |
| P1 | 会导致故障恢复失败、长期状态漂移、迁移抖动或明显资源浪费；进入稳定压测前应修复 |
| P2 | 可运维性、扩展性、安全性或代码收敛问题；可以分阶段完善 |

## 3. 当前闭环及其薄弱点

当前理想闭环是：

```text
实例注册
  -> LMCache store/remove/request 事件上报
  -> CEDFS 更新 radix tree 和热度
  -> CEDFS 选择 source/target/block
  -> source LMCache 向 target LMCache 复制 KV
  -> CEDFS 提交 target 副本关系
  -> LMCache/vLLM 事件更新 Dynamo 路由索引
```

生产可用要求每一步都能回答四个问题：

- 这个实例是不是当前仍存活的同一个进程？
- 这个 block 身份是否与 LMCache 实际 cache key 完全一致？
- 这次状态变化是否能幂等、按序、可重放？
- meta 中的成功状态是否得到数据面的逐 block 确认？

当前实现对这四点都缺少完整约束。

## 4. P0：正确性与可用性缺口

### 4.1 增量 store 上报会生成错误的累计 hash

这是当前最优先的元数据正确性问题。

#### 证据链

1. vLLM adapter 对已有前缀设置 `store_mask=false`，只让 LMCache 保存 suffix：[`vllm_v1_adapter.py:1163`](LMCache/lmcache/integration/vllm/vllm_v1_adapter.py#L1163)。
2. LMCache 完成 store 后，只拼接本次实际保存的 `starts/ends` token 片段并调用 reporter：[`cache_engine.py:688`](LMCache/lmcache/v1/cache_engine.py#L688)。
3. metadata client 的 `UploadKvMetaRequest` 只发送这段 token，没有原始 position、parent hash 或真实 chunk hash：[`metadata_client.py:64`](LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py#L64)。
4. CEDFS 收到后从初始 hash 开始重新计算整段累计 hash：[`upload_kvmeta.rs:16`](CedFs_KV/cedfs-kv/src/operation/upload_kvmeta.rs#L16)。

假设真实序列是两个 block：

```text
A 的真实 key = H(seed, A)
B 的真实 key = H(H(seed, A), B)
```

当 A 已存在、这次只新存 B 时，LMCache 上报的是 `B.tokens`；CEDFS 登记成：

```text
B 的 CEDFS key = H(seed, B)
```

它与 LMCache 实际 key 不同。后续会出现连锁问题：

- CEDFS 查询无法发现真实 suffix；
- LMCache 淘汰上报携带真实 hash，可能无法删除 CEDFS 中的错误节点；
- CEDFS 使用错误 hash 请求迁移，源实例返回 not found；
- 热度、压力和副本数都被记到错误 block 上。

#### 建议

不要让 meta 根据“本次存储的 token 片段”猜测 cache key。上报协议应直接携带 LMCache 已经计算出的权威 block 信息：

```text
compatibility_group_id
instance_id
instance_epoch
event_seq
block.seq_hash
block.parent_seq_hash
block.position
block.offset
block.tokens        # 只有迁移事件确实需要时才保留
```

次优方案是上报完整 token 序列和明确的 stored ranges，但它传输量更大，也仍然复制了 LMCache 的 key 生成逻辑。

### 4.2 部分迁移成功会被提交为全部成功

#### 证据链

LMCache 的迁移 worker 明确允许 `0 < num_satisfied < requested_chunks`。这种情况下它只记录 warning，随后返回正的 `num_tokens`：[`migration.py:360`](LMCache/plugins/kv_transfer/lmcache_kv_transfer/migration.py#L360)。

CEDFS 则把任何正数解释为整批成功，并对请求中的全部 hash 执行 `add_server(target)`：[`lib.rs:495`](CedFs_KV/cedfs-kv/src/lib.rs#L495)。

同时，Rust 侧注释把普通正数描述成 satisfied chunk 数，Python 实现返回的却是 source lookup 的 token 数，协议语义也不一致。

#### 影响

- CEDFS 会返回实际不存在的目标副本。
- 下一轮压力计算会错误地按更高副本数分摊热度。
- Dynamo 依赖真实 store event，不会为未成功的 block 增加目标位置，CEDFS 与 Dynamo 的两份索引因此永久分叉。
- 以后 CEDFS 可能基于幽灵副本继续选择迁移 source 或停止必要复制。

#### 建议

迁移响应必须是逐 block 结果，而不是一个重载语义的整数：

```text
transfer_id
results[] = {
  seq_hash,
  status: COPIED | ALREADY_PRESENT | SOURCE_MISSING | FAILED,
  bytes_transferred
}
```

CEDFS 只能为 `COPIED` 和 `ALREADY_PRESENT` 的 hash 提交目标副本。`SOURCE_MISSING` 也只能删除对应 hash 的 source 关系，不能批量推断。

即使一次迁移全量成功，当前提交也没有 target epoch/block version 保护。target 若在传输完成后立即淘汰 block，remove 上报可能先于 source RPC 响应到达 CEDFS：remove 因副本尚未登记而成为 no-op，随后 CEDFS 再执行 `add_server(target)`，仍会重新制造幽灵副本。逐 block 响应还需要携带目标 epoch/version，mutation 和 transfer commit 必须在同一版本序列下比较。

### 4.3 缺少模型与 cache 布局命名空间

当前 `KvRadixTree` 的全局主键只有 32-byte `seq_hash`，block 的 server set 可以包含任意实例。注册信息虽然有 `model_name`，但压力极值、候选选择和 `TransferKv` 前都不校验模型或 cache 布局。

LMCache 的真实 cache key 还依赖模型、world size、worker/rank、KV dtype 和 request config；KV tensor 是否可直接复制还依赖模型 revision、tokenizer、chunk size、TP/PP 布局、MLA/普通 KV 格式等。仅 token hash 相同不代表 tensor 兼容。

#### 风险

- 相同 token ID 前缀可能在不同模型间合并到同一 radix 节点。
- 热度在模型间相互污染。
- CEDFS 可能命令模型 A 的实例向模型 B 的实例传输 KV。
- 即使模型名相同，不同 dtype、并行布局或模型 revision 仍可能不兼容。

#### 建议

注册时生成并校验 `compatibility_group_id`，至少覆盖：

- model name + immutable revision；
- tokenizer identity；
- hash algorithm、seed 和 chunk size；
- KV dtype、KV layout/MLA 标记；
- TP/PP/world-size/rank 语义；
- 影响 cache key 或 tensor shape 的 request config。

radix、热度、实例目录和迁移策略都按 compatibility group 隔离。部署侧“保证所有实例同构”只能作为当前临时约束，不能替代服务端校验。

### 4.4 metadata RPC 同步进入 cache 路径，且无 deadline、重试与隔离

`KvCacheClient` 使用同步 gRPC stub，所有调用都没有 deadline：[`metadata_client.py:51`](LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py#L51)。`on_kv_stored` 又在 LMCache store 完成路径中直接调用：[`cache_engine.py:688`](LMCache/lmcache/v1/cache_engine.py#L688)。

这意味着 meta 网络卡顿可能拉长 cache save 路径；异常是否影响请求取决于上层调用点的捕获方式。client 也没有：

- 有界异步事件队列；
- 指数退避重连；
- 失败事件重放；
- 队列满时的降级策略；
- 断线后的 inventory 对账。

#### 建议

将 metadata reporter 改成与推理路径隔离的异步组件：

- 推理线程只写入有界本地队列；
- RPC 设置较短 deadline 和退避重试；
- 每个实例维护单调 `event_seq`；
- meta 按 `(instance_id, epoch, event_seq)` 幂等处理；
- 队列溢出或断线过久时触发全量 inventory reconcile，而不是无限积压；
- meta 不可用时不得阻断 GPU 推理，只允许控制面功能降级。

## 5. P1：实例生命周期与恢复缺口

### 5.1 注册不是一个完整的实例生命周期协议

当前实例 ID 是 Python `hash("ip:http_port") & 0xffffffff`：[`metadata_client.py:43`](LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py#L43)。这存在以下问题：

- 只有 32 bit，存在碰撞空间；
- 依赖 `PYTHONHASHSEED` 才能跨进程稳定；
- 地址复用时无法区分旧进程和新进程；
- 容器 IP/端口变化会产生新 ID，但旧 ID 不会清理。

CEDFS 遇到重复 ID 只忽略新注册，不更新 IP、端口、URL 或模型信息：[`register_instance.rs:20`](CedFs_KV/cedfs-kv/src/operation/register_instance.rs#L20)。协议没有 heartbeat、lease、unregister，meta 也不会自动清理已失联实例及其全部 block 关系。

此外，handler 对缺失的 `data_server` 直接 `unwrap()`：[`kv_meta2data.rs:47`](CedFs_KV/cedfs-kv/src/network/kv_meta2data.rs#L47)，无效请求可能导致当前 RPC task panic，而不是返回 `invalid_argument`。

#### 建议

- 使用随机 UUID 或平台分配的稳定 `instance_id`。
- 每次进程启动生成新的 `instance_epoch/incarnation_id`。
- Register 返回 lease token、租约期限和 meta 接受的 compatibility group。
- Heartbeat 携带 epoch、容量、已用空间、队列负载和最后 event sequence。
- 租约过期后先标记 `UNAVAILABLE`，立即停止查询返回和迁移调度，再异步清理副本关系。
- 同 ID、新 epoch 注册时原子替换旧实例并清除旧进程 inventory。
- 增加显式 unregister，但正确性不能依赖优雅退出。

### 5.2 meta 重启和实例重启都没有状态重建

CEDFS 的实例目录、radix tree、热度和迁移状态均为内存对象。meta 重启后状态为空；LMCache client 初始化只重新注册实例，不会上报本地全部 cache inventory。

反方向也有问题：实例在相同地址重启后可能得到相同 server ID，但本地 cache 已空；CEDFS 会保留旧进程的全部 block 关系，并因为重复 ID 而忽略新的注册信息。

#### 建议

单 meta 场景不需要引入共识系统，但至少需要一个恢复方案：

1. CEDFS 将实例注册、inventory mutation 和最后 event sequence 写入本地 snapshot/WAL；
2. 每个实例注册成功后上报 inventory digest；
3. digest 不一致、epoch 变化或 meta generation 变化时，执行分页全量 inventory 重建；
4. meta 恢复期间把未完成实例标成 `SYNCING`，不对外宣称其 block 可用；
5. 定期抽样或分页 reconcile，修复丢事件和乱序事件。

如果暂时不做持久化，也可以把 CEDFS 明确定义为 soft-state 服务，但必须要求所有实例在 meta generation 变化后主动全量重放。当前两种路径都没有实现。

### 5.3 mutation 缺少 epoch、序号和幂等语义

`UploadKvMeta` 和 `RemoveKvMeta` 只携带 server ID 与内容，没有 event ID、进程 epoch、单调序号或版本。结果是：

- 旧进程延迟到达的 remove 可以删除新进程状态；
- store/remove 乱序可能让已淘汰 block 重新出现；
- client 无法安全重试并判断 meta 是否已提交；
- meta 无法发现事件丢失。

虽然当前 set insert/remove 在单次操作上大体幂等，但跨重启、乱序和批处理部分失败时并不具备收敛保证。

#### 建议

所有 mutation 使用 `(instance_id, instance_epoch, event_seq)`：

- epoch 不匹配的事件直接拒绝；
- `event_seq <= committed_seq` 的事件按幂等重复处理或确认；
- sequence gap 触发 reconcile；
- 批次响应返回逐项结果与 committed sequence。

### 5.4 请求 ID 未按实例隔离，重复 NewRequest 仍会重复加热

`ActiveSequences` 只用字符串 `request_id` 做 key，不包含 server ID 或实例 epoch。`RequestEnd` handler 甚至丢弃了请求中的 server ID：[`kv_meta2data.rs:99`](CedFs_KV/cedfs-kv/src/network/kv_meta2data.rs#L99)。不同实例使用相同 request ID 时会互相覆盖或释放。

更重要的是，`NewRequestOp` 先给所有 block 加热，再调用 `add_request` 检查重复：[`new_request.rs:16`](CedFs_KV/cedfs-kv/src/operation/new_request.rs#L16)。因此重复 RPC、调度器重复探测或重试仍会重复增加热度。

`RequestEnd` 对不存在的请求也继续增加 migration trigger 计数并可能启动 rebalance：[`request_end.rs:12`](CedFs_KV/cedfs-kv/src/operation/request_end.rs#L12)。

#### 建议

- 请求主键改为 `(instance_id, instance_epoch, request_id)`。
- 先原子执行 `insert-if-absent`，成功后再增加热度。
- `RequestEnd` 只有真正从 ACTIVE 转成 FINISHED 时才计入完成计数。
- 对重复 start/end 返回明确的幂等响应。

### 5.5 ActiveSequences 的过期逻辑不是逐请求 TTL，且未参与迁移决策

`force_expiry()` 每隔五分钟把 `expiry_requests` 中的所有请求全部释放，不记录每个请求的开始/最后更新时间：[`squnence.rs:166`](CedFs_KV/cedfs-kv/src/transfer/squnence.rs#L166)。刚好在全局 timer 到期前加入的请求也可能立即过期。

同时，`block_hold_count`、`sequence_hold_counts` 等信息没有被迁移候选或 eviction 路径使用。当前真正的数据传输安全主要依赖 LMCache source lookup 的 pin，而不是 CEDFS 的 ActiveSequences。

#### 建议

短期应二选一：

- 如果活跃引用不参与任何决策，删除这套控制面假保护，只保留请求幂等和热度统计；
- 如果需要避免迁移/淘汰活跃 block，就实现逐请求 deadline，并在候选选择中显式检查活跃引用。

## 6. P1：迁移与负载策略缺口

### 6.1 新注册的空实例不能成为迁移目标

`pressure_extremes()` 只遍历 radix tree 的 `lookup`：[`kv_radix.rs:417`](CedFs_KV/cedfs-kv/src/kv_radix.rs#L417)。而 `lookup` 只在实例 store block 或迁移成功后创建。刚注册、cache 为空的新实例不在压力集合中，因此最需要预热的空实例反而不会成为最低压力目标。

实例目录应该是候选实例的来源，block lookup 只用于计算其压力；没有 block 的健康实例压力应为 0。

### 6.2 热度只增不衰减，且 eviction 会错误改变需求量

每次 NewRequest 对已知 block 执行 `heat += 1`，没有时间窗口、衰减或周期 reset。运行越久，历史流量权重越大，近期热点越难影响决策。

另一方面，eviction 会按副本数扣除一份 `heat / replicas`：[`kv_radix.rs:529`](CedFs_KV/cedfs-kv/src/kv_radix.rs#L529)。如果 heat 表示用户需求，副本被淘汰不应让需求凭空下降。当前实现把“需求”和“副本承担的压力”混成了同一个可变值。

#### 建议

- block demand 独立保存，使用 EWMA 或固定时间窗口；
- `server_pressure` 在读取时根据 demand、replica、实例容量计算，不修改 demand；
- eviction 只修改位置关系，不修改 demand；
- 没有任何副本时可以保留短期 ghost entry，用于未来再预热，而不是立即丢失全部需求历史。

### 6.3 目标选择不考虑容量和真实负载

当前压力只有 `sum(block.heat / replica_count)`：[`kv_radix.rs:398`](CedFs_KV/cedfs-kv/src/kv_radix.rs#L398)。它没有考虑：

- 目标 cache 总容量、可用字节数和当前 eviction rate；
- 实例活跃请求数、prefill/decode 队列和 GPU 负载；
- 不同模型每 token 的真实 KV 字节数；
- source/target 的并发迁移和共享 NIC 带宽；
- 复制后目标为腾空间淘汰其他热点 block 的代价。

结果可能是把 hot block 复制到已满目标，随后立即被 LMCache 淘汰，形成迁移—淘汰振荡。

#### 建议

heartbeat 至少上报 `capacity_bytes`、`used_bytes`、`eviction_rate`、`active_requests`、`migration_in/out bytes`。策略需要先做硬约束过滤，再按收益/成本排序：

```text
eligible = healthy
        && compatible
        && free_bytes >= estimated_transfer_bytes + reserve
        && target_migration_slots > 0

benefit = expected_hit_saving - transfer_cost - eviction_cost
```

只有 `benefit > min_benefit` 才迁移。

### 6.4 迁移批次无明确字节上限，冷却模型过于粗糙

候选选择会沿着 parent-child 链持续选择 suffix：[`kv_radix.rs:473`](CedFs_KV/cedfs-kv/src/kv_radix.rs#L473)，没有 `max_blocks_per_transfer`、`max_tokens_per_transfer` 或 `max_bytes_per_transfer`。CEDFS 又把所有 hash、offset 和完整 token IDs 放入单个 gRPC 请求。

长序列会让请求依赖 gRPC 默认消息限制，也会长时间 pin source 内存并占用迁移 worker。当前冷却按固定 `96 KiB/token` 估算，只允许 500/1000/10000 Mbps 三个配置值，无法反映模型 KV 大小和实际链路吞吐。

#### 建议

- 按真实 tensor shape 估算 bytes；
- 设置每 RPC 的 block/token/byte 三重上限；
- 长 suffix 分页迁移，逐页提交；
- 设置 RPC deadline、transfer deadline 和取消语义；
- 分别限制每 source、每 target 和每 NIC 的并发/带宽；
- 用实际 transfer bytes/duration 更新 EWMA 带宽估计。

### 6.5 rebalance 并发控制粒度不足

每隔若干 RequestEnd 会 `tokio::spawn` 一个 rebalance。当前 in-flight key 只是 `(source, target)`，因此不同 pair 的多个 rebalance 可以同时读取非事务快照并修改同一批 block。pair cooldown 也只在普通正成功后更新，already-satisfied、失败和其他 pair 不受约束。

#### 建议

- 单 compatibility group 使用一个 reconcile/rebalance worker；
- 请求完成只发轻量 trigger，由 worker 合并触发；
- block 增加 migration state 和 `transfer_id`；
- source/target 都使用 semaphore；
- 提交前再次验证 instance epoch、block version 和 target lease。

### 6.6 只由 RequestEnd 触发，无法覆盖故障和容量事件

如果 RequestEnd 丢失、reporter 断线或业务流量很低，rebalance 不会运行。实例加入、lease 过期、容量骤降和大量 eviction 也不会直接触发重新评估。

建议改成“事件触发 + 周期兜底”的单 worker：注册/离线/容量阈值/热点变化发 trigger，周期 reconcile 防止漏事件。

## 7. P2：接口、可观测性、安全性与工程完整性

### 7.1 配置字段与实际行为不一致

`sync_interval`、`replica_pull*`、`scheduler_strategy`、`request_timeout` 等字段仍被强制读取，但当前主链路没有使用；`remote_meta_servers` 也仍是启动必需配置。对于明确的单 meta 场景，这些旧配置增加误解和部署失败面。

其他配置问题包括：

- `block_size` 没有在 Config 层校验，`ActiveSequences::new` 对 `<=1` 直接 assert；
- 未知 hash algorithm 会静默回退到 builtin，而不是 fail-fast；
- CLI `need_reset_storage` 没有实际行为；
- CEDFS 仓库没有提交一份与当前字段完全对应的示例配置。

建议删除失效字段或标为 optional/deprecated；所有影响 cache identity 的配置必须严格校验并在注册握手中回传 fingerprint。

### 7.2 CEDFS 与 LMCache proto 已漂移

CEDFS 当前 `kvcache.proto` 已移除 meta-to-meta 的 `GetKvMeta/UpdateKvMeta`，LMCache 的 `lmcache/v1/remote/kvcache.proto` 和 plugin 内生成的 `kvcache_pb2.py` 仍包含旧消息与旧 RPC。当前已使用 RPC 的字段编号仍兼容，因此不一定立刻报错，但这说明协议没有单一事实来源和兼容性检查。

建议把 proto 作为一个有版本的共享 artifact：

- 单一源文件生成 Rust/Python binding；
- 注册握手携带 protocol major/minor；
- CI 校验生成文件与 proto 同步；
- 破坏性变更升级 package/service version。

### 7.3 metrics 不是持续监控，并可能持续积压内存

metrics reporter 只 `sleep(600s)` 后采集一次，没有循环：[`lib.rs:240`](CedFs_KV/cedfs-kv/src/lib.rs#L240)。采集后新产生的 RPC/selection record 仍会追加到内存 Vec，但不会再次 drain，长期运行会持续增长。

同时当前只输出日志，缺少：

- 注册/健康/租约状态；
- mutation lag、event sequence gap 和 reconcile 进度；
- meta 与实例 inventory 差异；
- 迁移逐状态计数、字节、耗时、失败原因；
- phantom replica 修复数；
- radix block 数、token 内存占用和 group 维度；
- RPC 延迟、错误率和队列深度。

建议输出 Prometheus/OpenTelemetry 指标，使用固定容量 histogram/counter/gauge，不保存无界逐事件记录。

### 7.4 缺少 health/readiness 与降级状态

服务只暴露业务 gRPC，没有明确区分：

- 进程存活；
- tokenizer/config 已加载；
- inventory 恢复完成；
- 是否允许查询；
- 是否允许启动新迁移。

建议提供 liveness、readiness 和 control-plane status。meta 恢复期间可以接受注册和重放，但查询/迁移要等到对应 compatibility group 达到 ready。

### 7.5 身份认证、授权和输入限制缺失

所有 channel 都是 insecure，server 不验证调用方身份。任意可达客户端可以伪造 server ID，替其他实例 upload/remove 元数据，或用大量 token/prompt 请求消耗 CPU 和内存。

集群可信网络可以降低优先级，但仍建议：

- 至少使用服务身份 token 或 mTLS；
- mutation 只能操作证书/lease 绑定的 instance ID；
- 限制每 RPC token、hash、prompt 和批次数量；
- 配置并发、消息大小、deadline 和 rate limit；
- 日志避免输出原始 prompt/token 内容。

### 7.6 存在未接入的策略代码和历史代码

`InferenceLoadTracker`、`select_eviction_target`、ActiveSequences 的多数查询方法未被主链路使用；`client.rs`、`get_kvmeta.rs`、`update_kvmeta.rs` 和 popularity strategy 大量整体注释。它们增加了“功能似乎存在”的错觉。

建议在单 meta 目标设计确定后删除或隔离历史实现，只保留已经接入且有测试的路径。

### 7.7 测试集中在局部数据结构，缺少跨组件契约场景

当前 Rust 测试主要覆盖 hash、radix 基本行为和 RequestEnd interval，没有覆盖真实多实例闭环。至少需要增加以下契约/集成测试：

- 已有 prefix 后只 store suffix，meta hash 与 LMCache key 一致；
- transfer 1/3、2/3、3/3 成功时只提交真实成功 block；
- 同地址实例重启后旧 epoch 状态不可见；
- meta 重启后通过 inventory 重建；
- store/remove 重复、乱序、丢失后最终收敛；
- 空实例可成为迁移目标；
- 不同 compatibility group 永不互迁；
- meta 不可用时推理/cache save 不被无限阻塞；
- 超长序列按 byte budget 分页；
- 并发 rebalance 不重复迁移同一 block。

### 7.8 查询接口没有健康过滤，endpoint 语义也不一致

`SearchKvBlock` 根据注册 IP/HTTP port 临时拼接 `ip:port`，没有 scheme 和 API path；`SearchKvBlockByPrompts` 则直接返回注册的 `data_server.url`。LMCache 当前把该 URL 注册成 `http://localhost:<port>/v1`：[`metadata_client.py:101`](LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py#L101)，跨节点调用者无法使用 `localhost` 地址。

两种查询都不会检查实例 lease/health，且响应没有 meta generation、replica version 或 freshness，调用者无法判断结果是否来自恢复完成的 inventory。当前 Dynamo 不调用这组接口，所以它不是主链路 P0；如果未来让路由器直接依赖 CEDFS，必须先统一 endpoint 格式并只返回 `READY + PRESENT` 的副本。

## 8. 建议的单 Meta 目标模型

### 8.1 核心实体

| 实体 | 建议主键 | 关键状态 |
| --- | --- | --- |
| CompatibilityGroup | fingerprint hash | 模型、tokenizer、hash/chunk、dtype、layout、并行配置 |
| Instance | `(instance_id, epoch)` | lease、endpoint、capacity、load、last_event_seq、sync_state |
| Block | `(group_id, seq_hash)` | parent、offset、可选 tokens、windowed demand |
| Replica | `(group_id, seq_hash, instance_id, epoch)` | PRESENT/MIGRATING/STALE、version、last_confirmed |
| Transfer | `transfer_id` | source/target epoch、block results、deadline、bytes |

### 8.2 状态提交原则

```text
实例本地真实 inventory
        |
        | 带 epoch + event_seq 的 mutation / snapshot
        v
CEDFS desired + observed state
        |
        | 带 transfer_id 的复制命令
        v
source -> target 数据传输
        |
        | target 逐 block 确认
        v
CEDFS 提交 Replica=PRESENT
```

关键原则是：**CEDFS 可以决定 desired state，但只有目标实例的数据面确认才能形成 observed PRESENT state。**

### 8.3 一致性选择

单 meta 下不需要复杂分布式共识，可以使用较简单的语义：

- mutation：at-least-once 传输 + epoch/sequence 幂等提交；
- 查询：只返回 lease 有效、inventory synced、Replica=PRESENT 的实例；
- 迁移：逐 block commit，允许部分成功；
- 恢复：snapshot/WAL + 实例 inventory reconcile；
- 热度：允许最终一致，但不能污染 block identity 和 replica truth。

## 9. 分阶段改造建议

### 阶段 0：先阻断错误状态

目标：任何成功响应都不能制造错误 block 或幽灵副本。

1. Upload 协议改为上报真实 seq hash、parent hash、offset 和 position。
2. TransferKv 改为逐 block 结果；CEDFS 逐项提交。
3. 引入 compatibility group，迁移前做硬校验。
4. 修复注册输入校验和 duplicate ID 更新语义。
5. 为以上三条增加跨 Rust/Python 的协议 fixture 测试。

### 阶段 1：补实例生命周期与恢复闭环

目标：任一实例或 meta 重启后都能自动收敛到真实 inventory。

1. instance UUID + epoch + lease/heartbeat/unregister。
2. mutation event sequence 和幂等响应。
3. inventory digest + 分页全量 reconcile。
4. CEDFS snapshot/WAL，或明确采用 meta generation 驱动的全量重放。
5. reporter 异步队列、deadline、重连和降级。

### 阶段 2：让迁移策略具备容量意识

目标：复制能带来净收益，不造成 cache thrash。

1. 实例目录纳入空实例；按 group 计算 pressure。
2. demand 使用 EWMA/window，和 replica pressure 分离。
3. heartbeat 上报容量、eviction、请求负载和迁移负载。
4. 配置最大副本数、最小收益、hysteresis 和 byte budget。
5. 单 group rebalance worker + source/target 并发限制。

### 阶段 3：生产运维收口

目标：问题可发现、可定位、可安全降级。

1. Prometheus/OpenTelemetry 指标与告警。
2. health/readiness/recovery 状态。
3. mTLS 或服务身份鉴权、RPC 限流和消息限制。
4. 清理无效配置、旧 proto 和未接入代码。
5. 故障注入：meta 停机、网络分区、target 满、部分迁移、乱序事件。

## 10. 生产前验收标准

建议至少满足以下可验证条件：

| 场景 | 期望结果 |
| --- | --- |
| 已有 A，只新存 B | CEDFS 的 B hash 与 LMCache 实际 `H(H(seed,A),B)` 完全一致 |
| 迁移 5 个 block，只成功 2 个 | target 只新增这 2 个 Replica=PRESENT |
| target 在迁移完成前淘汰 block | 最终状态以 target epoch/version 的确认结果为准，不残留幽灵副本 |
| 实例同地址重启 | 新 epoch 原子替换旧 epoch，旧 inventory 不可查询 |
| meta 重启 | 在限定时间内通过 WAL/snapshot + reconcile 恢复，恢复前不返回未确认位置 |
| meta 不可达 | 推理继续；metadata 队列受限；恢复后自动对账 |
| 新增空实例 | 可作为健康、兼容且有容量的迁移目标 |
| 混合模型/dtype/layout | 不同 compatibility group 之间零迁移 |
| mutation 重复/乱序/丢失 | 通过 epoch/sequence/reconcile 最终收敛 |
| 长序列迁移 | 按 byte budget 分页，无超大单 RPC 和无限 pin |
| 长时间运行 metrics | 无无界事件 Vec，核心指标可持续采集和告警 |

## 11. 最终判断

当前 `cedfs-kv` 已具备一个有价值的原型骨架：全局 radix 索引、连续前缀查询、热度统计、source-target 选择和 LMCache P2P 迁移已经形成基本链路。

但在单 meta、多实例这个更窄的范围内，系统仍缺少生产控制面最关键的三条性质：

- **身份准确**：meta block key 必须等于数据面的真实 cache key；
- **状态可证实**：逐 block 的目标确认决定副本是否存在；
- **故障可收敛**：实例/meta 重启、丢事件和乱序后能自动回到真实 inventory。

建议不要先投入多 meta 同步，也不要先继续调优 pressure 公式。优先完成阶段 0 和阶段 1；否则更复杂的迁移策略只会更快地放大错误元数据。

## 12. 主要源码依据

- CEDFS 服务与迁移主循环：[CedFs_KV/cedfs-kv/src/lib.rs](CedFs_KV/cedfs-kv/src/lib.rs)
- CEDFS radix、压力与 eviction：[CedFs_KV/cedfs-kv/src/kv_radix.rs](CedFs_KV/cedfs-kv/src/kv_radix.rs)
- CEDFS store 上报处理：[CedFs_KV/cedfs-kv/src/operation/upload_kvmeta.rs](CedFs_KV/cedfs-kv/src/operation/upload_kvmeta.rs)
- CEDFS 实例注册：[CedFs_KV/cedfs-kv/src/operation/register_instance.rs](CedFs_KV/cedfs-kv/src/operation/register_instance.rs)
- CEDFS 请求生命周期：[CedFs_KV/cedfs-kv/src/operation/new_request.rs](CedFs_KV/cedfs-kv/src/operation/new_request.rs)、[request_end.rs](CedFs_KV/cedfs-kv/src/operation/request_end.rs)
- CEDFS 当前协议：[CedFs_KV/cedfs-proto/proto/kvcache.proto](CedFs_KV/cedfs-proto/proto/kvcache.proto)、[kvserver.proto](CedFs_KV/cedfs-proto/proto/kvserver.proto)
- LMCache metadata client：[LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py](LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py)
- LMCache migration worker：[LMCache/plugins/kv_transfer/lmcache_kv_transfer/migration.py](LMCache/plugins/kv_transfer/lmcache_kv_transfer/migration.py)
- LMCache store reporter 调用：[LMCache/lmcache/v1/cache_engine.py](LMCache/lmcache/v1/cache_engine.py)
- vLLM/LMCache suffix store mask：[LMCache/lmcache/integration/vllm/vllm_v1_adapter.py](LMCache/lmcache/integration/vllm/vllm_v1_adapter.py)

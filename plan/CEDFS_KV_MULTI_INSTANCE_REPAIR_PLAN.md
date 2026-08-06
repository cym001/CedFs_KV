# CEDFS-KV 单 Meta、多实例详细修复计划

## 1. 计划目标

本文承接 [CEDFS_KV_MULTI_INSTANCE_GAP_ANALYSIS.md](CEDFS_KV_MULTI_INSTANCE_GAP_ANALYSIS.md)，给出可直接拆分为研发任务的修复计划。

目标场景固定为：

- 一个活跃的 CEDFS meta 服务；
- 集群内多个 vLLM + LMCache cache 实例；
- CEDFS 负责副本位置、热度和复制决策；
- LMCache 是 KV tensor 和本地 inventory 的事实来源；
- 暂不实现多 meta 同步、选主、共识或跨地域复制；
- Dynamo 继续使用自己的 KV event 路由索引，不改成强依赖 CEDFS 查询。

修复完成后应具备以下性质：

1. CEDFS 中的 block key 与 LMCache 的真实 cache key 完全一致。
2. 迁移只提交目标实际确认成功的 block，不允许批量推断。
3. 实例重启、meta 重启、RPC 重复、乱序和短时断线后可以自动收敛。
4. meta 故障不会无限阻塞推理或 cache save 路径。
5. 迁移只发生在兼容、健康且有容量的实例之间。
6. 每个阶段都能独立灰度、观测和回滚。

## 2. 实施原则与已选技术方案

### 2.1 先修事实正确性，再调压力算法

执行顺序固定为：

```text
V2 协议与影子索引
  -> 权威 block metadata
  -> 逐 block 迁移结果与版本化提交
  -> instance epoch/lease/异步上报
  -> inventory full sync 与 meta 重建
  -> 容量感知和热点策略
  -> 可观测性、安全与旧代码清理
```

在权威 block identity、逐块确认和恢复闭环完成前，不继续复杂化 pressure 公式。

### 2.2 V1/V2 并行，不原地改变旧 RPC 语义

在现有 [`kvcache.proto`](../cedfs-proto/proto/kvcache.proto) 和 [`kvserver.proto`](../cedfs-proto/proto/kvserver.proto) 中新增独立的 V2 service 和 V2 message；保留现有 V1 service，避免旧 client 因同名 RPC 语义改变而误工作。

建议增加：

```text
kvcache.KvMeta2DataV2
lmcache.LmcacheServerV2
```

V2 与 V1 使用不同的 CEDFS 内部 index。影子阶段绝不把 V1 mutation 和 V2 mutation 混入同一 radix tree。

### 2.3 单 Meta 先采用 soft-state，不先引入数据库

本阶段选择：

- LMCache LocalCPUBackend inventory 是副本事实来源；
- CEDFS 启动生成新的 `meta_generation`；
- client 发现 generation 变化后执行全量 inventory sync；
- CEDFS 在对应实例 sync 完成前不返回其副本，也不向其调度迁移；
- 热度允许在 meta 重启后重新预热。

这样可以先实现正确恢复，避免同时维护“数据库状态”和“实例真实 cache”两套持久化事实。

后续如果 inventory 重建时间不能满足 RTO，再增加本地 snapshot 作为加速缓存；snapshot 仍不能替代实例 reconcile。多 meta 共识继续不在本计划内。

### 2.4 使用实例代次，而不是把地址当身份

实例身份采用：

```text
InstanceKey = (lmcache_instance_id, worker_id)
InstanceEpoch = 每次 LMCache engine 进程启动生成的 UUID
```

原因：LMCache 已有 `lmcache_instance_id` 和 `worker_id`，可以区分一个推理副本中的不同 rank；`InstanceEpoch` 用于隔离地址复用和旧进程延迟事件。

CEDFS 内部可以为每次注册分配紧凑的 `u64 instance_handle`，radix node 只保存 handle；外部 RPC 始终携带完整 InstanceKey、epoch 和 lease token。

### 2.5 compatibility group 由服务端计算

client 上报兼容性字段，CEDFS 使用固定顺序编码并计算 SHA-256 `compatibility_group_id`，不信任 client 自报 group ID。

第一版 fingerprint 至少包含：

- model name 和 immutable revision；
- tokenizer identity/revision；
- prefix hash algorithm、hash seed、`PYTHONHASHSEED`；
- chunk size、是否保存不足 chunk；
- KV dtype、MLA/普通 KV layout；
- TP/PP/world size 和 worker/rank 坐标；
- 会进入 `CacheEngineKey.tags` 的 request config。

注册字段不完整或服务端无法识别时，实例进入 `REJECTED_INCOMPATIBLE`，不能只记录 warning 后继续。

### 2.6 mutation 使用 epoch + 单调序号

每个 `(InstanceKey, InstanceEpoch)` 维护独立的 `event_seq`，从 1 开始。

- `seq <= committed_seq`：作为重复事件确认，不重复应用。
- `seq == committed_seq + 1`：正常应用。
- `seq > committed_seq + 1`：返回 sequence gap，实例进入 `SYNC_REQUIRED`。
- epoch 不匹配：拒绝旧进程事件。
- 一个 mutation event 内的 block 列表原子校验；任一字段非法时整事件不提交、不推进序号。

请求热度事件不进入 cache inventory sequence，使用独立的复合请求 ID 做幂等，避免热度上报失败阻塞副本 mutation。

## 3. 目标数据模型

### 3.1 CEDFS 核心结构

建议逐步把当前 `Shared` 中的 Vec/Map 收敛成以下逻辑结构：

```text
Shared
├── meta_generation: UUID
├── instances: DashMap<InstanceKey, InstanceRecord>
├── leases: DelayQueue/expiry index
├── groups: DashMap<GroupId, Arc<GroupState>>
├── inventory_syncs: DashMap<SyncId, StagingInventory>
└── rebalance_triggers: per-group channel

InstanceRecord
├── handle: u64
├── epoch: UUID
├── lease_id / lease_deadline
├── state: REGISTERING | SYNCING | READY | UNAVAILABLE | EXPIRED
├── endpoints
├── group_id
├── committed_event_seq
├── capacity/load snapshot
└── last_heartbeat

GroupState
├── radix: KvRadixTree
├── block_demand: windowed counters
├── replicas: block -> instance/version/state
├── active_requests
└── rebalance_worker_state

ReplicaRecord
├── instance_handle
├── instance_epoch
├── state: PRESENT | MIGRATING | STALE
├── replica_version       # target event_seq
└── last_confirmed_at
```

### 3.2 BlockDescriptorV2

LMCache 必须上报真实 chunk metadata，建议字段如下：

```text
bytes seq_hash              # 必须正好 32 bytes
optional bytes parent_hash  # root 无 parent；否则正好 32 bytes
uint32 position             # 从 0 开始的 chunk position
uint32 offset               # 当前 chunk token 数
repeated uint32 token_ids   # 长度必须等于 offset
```

服务端验证：

- `seq_hash` 和 `parent_hash` 长度；
- `offset > 0 && offset <= group.chunk_size`；
- `token_ids.len == offset`；
- root 必须 `position == 0 && parent_hash absent`；
- 非 root 必须有 parent；
- parent 在本事件前部、当前实例已有 inventory 或正在提交的 inventory page 中可解析；
- 同一个 `(group, seq_hash)` 的 parent/position/offset/tokens 必须与已有 block 一致。

CEDFS 不再根据 mutation 中的 suffix token 自己猜测累计 hash。开发/调试模式可以重算并核对 hash，生产路径以 LMCache 实际 `CacheEngineKey.chunk_hash` 为权威值。

### 3.3 LMCache 本地 metadata ledger

仅 `LocalCPUBackend.get_keys()` 只能得到 chunk hash，不能恢复 parent、position 和 token slice。V2 reporter 因此需要维护 sidecar ledger：

```text
chunk_hash -> {
  parent_hash,
  position,
  offset,
  token_ids,
  last_store_seq
}
```

ledger 更新规则：

- 普通 store 成功后登记实际写入的 chunk；
- P2P migration target 成功提交后登记新复制的 chunk；
- eviction/remove 后删除 ledger 条目并生成 REMOVE mutation；
- inventory snapshot 取 `LocalCPUBackend.get_keys()` 与 ledger 的交集；
- cache 中存在但 ledger 缺失的 key 标记为 `UNMANAGED`，不向 CEDFS 声明 PRESENT，并触发告警。

当前 LocalCPUBackend 随进程退出而丢失，因此 ledger 第一版可使用内存结构。若以后增加持久化 backend，需要 ledger 与 backend 一起持久化或能从 backend 元数据恢复。

## 4. V2 协议设计

### 4.1 注册与租约

建议 V2 RPC：

```text
KvMeta2DataV2.RegisterInstance
KvMeta2DataV2.Heartbeat
KvMeta2DataV2.UnregisterInstance
```

`RegisterInstanceV2Request`：

- protocol major/minor；
- InstanceKey、InstanceEpoch；
- HTTP、NIXL init、transfer RPC endpoint；
- compatibility fingerprint；
- capacity bytes、初始 used bytes；
- client 当前记录的 meta generation，可为空。

`RegisterInstanceV2Response`：

- accepted/error code；
- CEDFS 计算的 group ID；
- instance handle；
- lease ID 和 TTL；
- meta generation；
- `require_inventory_sync`；
- server protocol minor/capabilities。

建议默认：

- heartbeat interval：5 秒；
- lease TTL：20 秒；
- 连续两个 heartbeat deadline 后标记 `UNAVAILABLE`；
- lease 到期立即停止查询返回和新迁移，异步清理该 epoch 的 replica membership。

具体值必须配置化，并满足 `lease_ttl >= 3 * heartbeat_interval`。

### 4.2 Cache mutation

建议 RPC：

```text
KvMeta2DataV2.ReportCacheMutations
```

一个 batch 包含连续的 mutation events：

```text
CacheMutationEvent {
  uint64 event_seq;
  oneof payload {
    StoreBlocks { repeated BlockDescriptorV2 blocks; }
    RemoveBlocks { repeated bytes seq_hashes; }
  }
}
```

响应包含：

- `committed_through_seq`；
- `expected_next_seq`；
- `require_inventory_sync`；
- 第一个失败事件的 seq 和错误码。

错误处理：

- validation error：不重试同一坏事件，记录 fatal metric，进入 sync-required；
- transient/unavailable：按原 seq 重试；
- sequence gap：停止增量发送，开始 full sync；
- stale epoch/lease：重新注册，不允许直接换 epoch 重发旧事件。

### 4.3 Inventory full sync

建议 RPC：

```text
BeginInventorySync
UploadInventoryPage
CommitInventorySync
AbortInventorySync
```

第一版采用“短暂 freeze 取一致快照，网络上传期间继续运行”的方案，可复用 LMCache 已有 full-sync sender 的分页、重试和 jitter 思路：

1. reporter 暂停发送增量 mutation，但继续写入有界队列；
2. `LMCacheEngine.freeze(true)`，阻止新 store/evict，retrieval 继续；
3. 在 LocalCPUBackend lock 下取得 key snapshot，并与 ledger 生成 descriptor snapshot；
4. 记录 `base_event_seq`；
5. 在 `finally` 中立即 `freeze(false)`；之后产生的 mutation 序号必然大于 `base_event_seq`，继续进入队列；
6. Begin 后分页上传 snapshot，每页有 `page_id`、条目数和 checksum；
7. CEDFS 在 staging inventory 中校验，不直接污染 live radix；
8. Commit 验证页数、总数和整体 checksum，原子替换该实例 epoch 的 replica membership，并把 committed sequence 设置为 `base_event_seq`；
9. instance 状态切换为 READY；
10. reporter 丢弃已被 snapshot 覆盖的 `seq <= base_event_seq` 队列项，从 `base_event_seq + 1` 继续增量发送。

失败时 Abort staging、保留增量队列并按退避重新取 snapshot。freeze 只覆盖本地 snapshot capture，不能覆盖网络上传；所有异常路径都必须在 `finally` 中解除 freeze。

建议初始参数：

- page size：1000～2000 blocks；
- max snapshot freeze time：5 秒；
- full sync overall deadline：60 秒；
- sync RPC deadline：每页 5 秒；
- max retry：3；
- startup jitter：0～5 秒，避免 meta 重启后所有实例同时同步。

后续如果 5 秒内仍无法复制大 inventory 的 descriptor snapshot，再实现 copy-on-write snapshot；第一版不同时维护两套 snapshot 算法。

### 4.4 请求热度事件

建议 RPC：

```text
ReportRequestStartV2
ReportRequestEndV2
```

请求 key：

```text
(InstanceKey, InstanceEpoch, request_id)
```

处理规则：

- Start 使用 insert-if-absent；只有首次插入才增加 block demand；
- duplicate Start 返回 already-exists success，不重复加热；
- End 只有从 ACTIVE 成功删除时才增加 finished-request/rebalance trigger；
- duplicate/unknown End 不触发 rebalance；
- 每个请求记录自己的 deadline，后台按 deadline 清理，禁止使用全局五分钟一次性清空。

请求上报是 best-effort。meta 不可用时可以丢弃热度事件，但不能阻塞 cache mutation 和推理。

### 4.5 TransferKvV2

建议新增：

```text
LmcacheServerV2.TransferKvV2
```

请求包含：

- transfer ID；
- group ID；
- source InstanceKey/epoch；
- target InstanceKey/expected epoch；
- target init endpoint；
- 有序 BlockDescriptorV2 列表；
- copy/move 标记，当前只允许 copy；
- transfer deadline。

逐 block 状态枚举：

```text
COPIED
ALREADY_PRESENT
SOURCE_MISSING
TARGET_NO_CAPACITY
READ_FAILED
NOT_ATTEMPTED
INCOMPATIBLE
STALE_TARGET_EPOCH
```

响应每个请求 block 恰好返回一个结果：

```text
BlockTransferResult {
  seq_hash;
  status;
  target_replica_version;  # target mutation event_seq
  bytes_transferred;
  error_detail_code;
}
```

CEDFS 提交规则：

- `COPIED/ALREADY_PRESENT`：仅当 target epoch 仍匹配，且返回 version 不旧于当前 replica version 时提交 PRESENT；
- `SOURCE_MISSING`：只删除对应 block 的 source membership；
- `TARGET_NO_CAPACITY/READ_FAILED/NOT_ATTEMPTED`：不修改副本事实；
- `INCOMPATIBLE/STALE_TARGET_EPOCH`：终止本 transfer，其余未处理项保持不变；
- 未返回结果的 block 全部按协议错误处理，不得默认成功。

目标 LMCache 在提交 migrated block 时先从 reporter 预留一个 mutation seq，更新 ledger 并将该 version 返回给 source。随后同一 STORE mutation 异步送达 CEDFS；CEDFS 直接 transfer commit 与后到 mutation 使用相同 version 幂等合并。若更晚的 REMOVE 已先到达，旧 version 的 transfer commit 不能复活该副本。

## 5. 分阶段研发任务

### 5.1 阶段 A：协议基线与可回滚骨架

目标：V2 可以部署但不改变现有生产行为。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| A-01 | 确认 InstanceKey 与 TP/PP rank 对应关系 | LMCache 启动配置、`metadata.py` | 无 | 两个推理副本相同 rank 可互迁，不同 rank 不会共享身份 |
| A-02 | 在现有 proto 增加 V2 service/message | `cedfs-proto/proto/*.proto` | A-01 | V1 字段和 service 不变；V2 覆盖注册、mutation、sync、transfer |
| A-03 | 建立 proto 单一来源和生成校验 | `cedfs-proto/build.rs`、LMCache plugin proto binding | A-02 | Rust/Python descriptor checksum 一致；禁止手工漂移 |
| A-04 | 增加 protocol mode 配置 | CEDFS `config.rs`、LMCache plugin config | A-02 | 支持 `v1 / dual-shadow / v2`，默认仍为 v1 |
| A-05 | 加入 V2 空 service 和 capability 握手 | CEDFS network、LMCache client | A-02 | dual-shadow 部署不改变 V1 上报和迁移 |

建议配置：

```text
CEDFS protocol_mode = v1 | dual_shadow | v2
CEDFS enable_v2_transfer = false
LMCache globalkv_protocol = v1 | dual | v2
```

阶段门禁：V1 行为完全不变；V2 未启用时不创建影子 index、不启动额外后台任务。

### 5.2 阶段 B：修复 block metadata 身份

目标：V2 shadow index 中的每个 hash 与 LMCache CacheEngineKey 一致。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| B-01 | 定义 `KvBlockMetadata`/structured reporter API | [`kv_migration.py`](../../LMCache/lmcache/v1/plugin/kv_migration.py) | A | reporter 接收 hash/parent/position/offset/tokens，不再只接收 suffix tokens |
| B-02 | 从完整 token 序列构造实际 stored chunk descriptors | [`cache_engine.py`](../../LMCache/lmcache/v1/cache_engine.py)、[`kv_event_utils.py`](../../LMCache/lmcache/v1/kv_event_utils.py) | B-01 | 已有 A、只 store B 时上报真实 `H(H(seed,A),B)` |
| B-03 | 建立 LMCache metadata ledger | GlobalKV migration plugin、LocalCPUBackend 接口 | B-02 | 普通 store/remove 后 ledger 与实际 hot cache 收敛 |
| B-04 | 实现 V2 mutation client | [`metadata_client.py`](../../LMCache/plugins/kv_transfer/lmcache_kv_transfer/metadata_client.py) | A-05、B-01 | mutation 携带 epoch/seq/group 和 descriptor |
| B-05 | CEDFS 实现 instance registry、group state、V2 mutation handler | `cedfs-kv/src/state/*`、`network/*`、`operation/v2/*` | A-05 | 未注册/错 epoch/错 group/坏 descriptor 全部被拒绝 |
| B-06 | 建立 per-group radix shadow index | [`kv_radix.rs`](../cedfs-kv/src/kv_radix.rs) 外层 group manager | B-05 | 不同 group 相同 hash 不共享 servers/heat |
| B-07 | 双报与 shadow parity 指标 | CEDFS metrics、LMCache reporter | B-04～B-06 | 能比较 worker inventory、V2 index 和 V1 index 差异 |

实现注意：

- 不直接删除旧 `on_kv_stored(tokens)`，dual 模式同时生成 V1 和 V2 上报。
- structured descriptor 应复用现有 `_build_kv_event_chunk_infos()` / `build_full_sequence_chunk_infos()` 的 parent 计算，不再复制第三套 hash 遍历逻辑。
- `CacheEngineKey.chunk_hash` 是权威 hash；CEDFS debug 校验不应成为热路径必需步骤。
- B 阶段 V2 index 只观察，不发起迁移。

阶段门禁：连续压测窗口内，V2 inventory 与 LMCache `get_keys()` 对已管理块达到 100% 一致；任何 mismatch 都必须能定位到具体 mutation seq。

### 5.3 阶段 C：修复部分迁移与提交竞态

目标：数据面部分成功、target eviction 和响应乱序都不会产生幽灵副本。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| C-01 | target backend 生成逐 index 状态 | [`backend.py`](../../LMCache/plugins/kv_transfer/lmcache_kv_transfer/backend.py) | B-03 | 每个请求 hash 返回且只返回一个状态 |
| C-02 | ZMQ PUT response 携带逐块状态/version | `BatchedLookupAndPutRetMsg` 及 codec | C-01 | existing/copied/no-capacity/source-gap 可区分 |
| C-03 | migration worker 返回结构化结果 | [`migration.py`](../../LMCache/plugins/kv_transfer/lmcache_kv_transfer/migration.py) | C-02 | 不再用正整数复用 token/chunk/success 三种语义 |
| C-04 | GlobalKv gRPC 实现 TransferKvV2 | [`globalkv_server.py`](../../LMCache/plugins/kv_transfer/lmcache_kv_transfer/globalkv_server.py) | A-02、C-03 | gRPC 结果顺序和请求 hash 一一对应 |
| C-05 | CEDFS 逐块、条件式提交 | [`lib.rs`](../cedfs-kv/src/lib.rs)、`operation/transfer_kv.rs` | C-04、B-05 | 2/5 成功只增加 2 个 target replica |
| C-06 | transfer version 与 REMOVE 乱序保护 | CEDFS ReplicaRecord、LMCache ledger | C-01、C-05 | 较新的 remove 先到时，旧 transfer commit 不能复活 block |
| C-07 | 增加 transfer byte/block/token 上限与 deadline | CEDFS config、TransferKvV2 client/server | C-05 | 长 suffix 自动分页，单 RPC 有硬上限 |

target backend 逐块状态生成建议：

- contains 命中：`ALREADY_PRESENT`；
- source mem index unavailable：当前项 `SOURCE_MISSING`，后续项 `NOT_ATTEMPTED`；
- allocate 失败：当前项 `TARGET_NO_CAPACITY`，后续项 `NOT_ATTEMPTED`；
- NIXL read 成功并提交：`COPIED`；
- read exception：已选择但未提交项 `READ_FAILED`；
- 所有 finally 分支必须释放已分配 MemoryObj/ref/pin。

阶段门禁：故障注入覆盖 0%、部分、100%、already-present、target-full、target-evict-race；CEDFS 与 target inventory 最终一致。

阶段 C 实施记录（2026-08-06）：C-01～C-07 已落入代码。LMCache 的 PUT、worker 和 V2 gRPC 均保留逐块有序结果；target STORE mutation 的提交序号作为 `target_replica_version`。CEDFS 对响应基数、顺序、hash、instance epoch 和 compatibility group 做校验，只提交 `COPIED/ALREADY_PRESENT`，并以每实例 `last_versions` 阻止旧 transfer 覆盖较新的 REMOVE。V2 client 同时实施 block/token/估算 byte 分页和 RPC deadline。按仓库开发约束，本阶段仅完成静态检查；故障注入门禁留待允许执行构建与测试的环境验证。

### 5.4 阶段 D：实例生命周期、异步 reporter 和恢复

目标：实例/meta 重启和网络抖动后自动恢复，meta 不阻断推理。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| D-01 | instance epoch、register response、lease token | CEDFS registry、LMCache client | B-05 | 同地址新 epoch 原子替换旧 epoch |
| D-02 | heartbeat/lease expiry/unregister | CEDFS background task、LMCache reporter | D-01 | 失联实例在 TTL 后不再查询/迁移，旧 replica 被清理 |
| D-03 | 有界异步 mutation queue | GlobalKV metadata reporter | B-04、D-01 | meta 不可用时 store 路径只做有界 enqueue，不无限等待 |
| D-04 | retry、deadline、sequence gap 处理 | metadata client/reporter | D-03 | transient 原 seq 重试；gap 自动切换 full sync |
| D-05 | inventory ledger snapshot API | LocalCPUBackend + migration plugin | B-03 | 可以输出实际 key 与完整 descriptor 的一致快照 |
| D-06 | CEDFS staging inventory/full-sync state machine | CEDFS network/state | D-05 | 未 Commit 的 page 不进入 live index；Commit 原子替换实例 inventory |
| D-07 | meta generation 与启动重建 | CEDFS startup、Register/Heartbeat | D-06 | meta 重启后所有实例被要求 sync；sync 前不返回副本 |
| D-08 | composite request lifecycle/idempotency | `new_request.rs`、`request_end.rs`、ActiveSequences 替代实现 | D-01 | duplicate start 不重复加热；unknown end 不触发迁移 |
| D-09 | 逐请求 TTL 清理 | request tracker | D-08 | 只清理真正超时请求，不再每 5 分钟清空全部 |

异步队列策略：

- STORE/REMOVE mutation 不能静默丢弃；队列到高水位时标记 `sync_required`，合并后续状态并准备 inventory sync。
- RequestStart/End 可 best-effort 丢弃，单独计数。
- close 时设置短 drain deadline；进程退出正确性依赖 epoch/full sync，不依赖所有事件成功 flush。
- gRPC deadline 必须显式配置；不使用无限等待默认值。

阶段门禁：

- meta 停 60 秒后恢复，推理继续，所有实例自动 full sync；
- 实例 crash 后 TTL 内从 READY 变 UNAVAILABLE；
- 同 InstanceKey 新 epoch 注册后旧事件全部拒绝；
- sync 失败不会污染 live index，也不会无限 freeze cache store。

### 5.5 阶段 E：迁移策略与调度稳定性

目标：正确状态之上实现有容量约束、可收敛的复制策略。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| E-01 | 从 instance registry 枚举 target | CEDFS rebalance | D-02 | READY 的空实例压力为 0 且可成为目标 |
| E-02 | demand 与 replica pressure 分离 | [`kv_radix.rs`](../cedfs-kv/src/kv_radix.rs) / GroupState | D-08 | eviction 不再修改 block demand |
| E-03 | 热度时间窗口/EWMA | demand tracker | E-02 | 旧热点在配置窗口后衰减 |
| E-04 | heartbeat 上报容量和负载 | LMCache LocalCPUBackend、CEDFS registry | D-02 | target 选择可获取 total/used/free bytes、eviction rate |
| E-05 | eligibility 硬过滤 | candidate selector | E-01、E-04 | 不健康、不兼容、容量不足实例永不入选 |
| E-06 | byte cost、max replicas、hysteresis | candidate selector/config | C-07、E-03 | 无收益或达到副本上限时不复制 |
| E-07 | 单 group rebalance worker | CEDFS background worker | D、E-05 | trigger 合并；同 block 不存在并发 transfer |
| E-08 | source/target/NIC 并发限制 | transfer scheduler | E-07 | 不同 pair 也受 source/target semaphore 约束 |

第一版热度建议使用固定时间桶而不是复杂在线模型：

```text
score = current_window_count + 0.5 * previous_window_count
```

每 5 分钟轮换一次，配置化窗口长度。它容易验证、无浮点累计漂移，足以解决“历史热度永久占主导”的问题。

第一版迁移 eligibility：

```text
instance.state == READY
&& same compatibility_group
&& target.free_bytes >= estimated_bytes + reserve_bytes
&& replica_count < max_replicas
&& source/target transfer slots available
&& expected_benefit > min_benefit
```

阶段门禁：实例扩容时空节点能被预热；target 接近高水位时不再接收迁移；稳定流量下迁移率和 eviction rate 不持续振荡。

### 5.6 阶段 F：可观测性、配置、安全和收口

目标：生产问题可发现、可诊断、可安全降级。

| ID | 任务 | 主要文件 | 依赖 | 完成标准 |
| --- | --- | --- | --- | --- |
| F-01 | 替换一次性 metrics reporter | `metrics.rs`、`lib.rs` | B～E | 不保存无界逐事件 Vec，指标持续输出 |
| F-02 | health/readiness/status endpoint | CEDFS server | D-07 | liveness 与 group readiness 可区分 |
| F-03 | 核心告警 | deployment docs | F-01 | lease expired、seq gap、sync failed、phantom repair、transfer failure 有告警 |
| F-04 | 配置严格校验 | `config.rs` | A～E | block size/hash/group/TTL/budget 非法时 fail-fast |
| F-05 | endpoint 统一 | register/search ops | D-01 | 返回可跨节点使用的 scheme/host/port/path |
| F-06 | RPC 身份绑定与限流 | tonic/grpc interceptors | D-01 | mutation 只能操作 lease 绑定实例；请求有大小/并发限制 |
| F-07 | 清理旧配置和未接入代码 | config、注释模块、旧 proto | V2 全量稳定后 | 至少保留一个发布周期 deprecation，再删除 V1 |

核心指标禁止使用 block hash 作为 label，避免高基数：

- `cedfs_instances{group,state}`；
- `cedfs_mutation_total{op,result}`；
- `cedfs_mutation_sequence_gap_total`；
- `cedfs_inventory_sync_total{result}`、duration、blocks；
- `cedfs_replicas{group}`、`cedfs_blocks{group}`；
- `cedfs_transfer_blocks_total{status}`、bytes、duration；
- `cedfs_rebalance_total{result}`；
- `cedfs_reporter_queue_depth`、drops by event class；
- `cedfs_lease_expired_total`；
- `cedfs_reconcile_mismatch_total`。

最安全的运行时降级开关是 `enable_v2_transfer=false`：停止主动复制，但保留本地 LMCache cache、Dynamo 路由和 metadata 观测。

## 6. 文件级改动清单

### 6.1 CedFs_KV

| 文件/目录 | 计划改动 |
| --- | --- |
| `cedfs-proto/proto/kvcache.proto` | 新增 V2 identity、fingerprint、lease、mutation、inventory、request lifecycle message/service |
| `cedfs-proto/proto/kvserver.proto` | 新增 TransferKvV2 和逐 block result |
| `cedfs-proto/build.rs` | 固定 proto 输入和生成检查，避免遍历顺序/旧 binding 漂移 |
| `cedfs-kv/src/types.rs` | InstanceKey、epoch、group ID、ReplicaRecord、BlockDescriptorV2 |
| `cedfs-kv/src/config.rs` | protocol mode、lease、queue/sync、transfer budget、window 配置与校验 |
| `cedfs-kv/src/state/instance_registry.rs` | 注册、epoch、lease、READY/SYNCING 状态 |
| `cedfs-kv/src/state/group_state.rs` | per-group radix、demand、replica、rebalance channel |
| `cedfs-kv/src/state/inventory_sync.rs` | staging pages、checksum、atomic commit/abort |
| `cedfs-kv/src/network/kv_meta2data_v2.rs` | V2 gRPC service |
| `cedfs-kv/src/operation/v2/*` | 注册、heartbeat、mutation、inventory、request handlers |
| `cedfs-kv/src/operation/transfer_kv.rs` | V2 client 与逐 block response |
| `cedfs-kv/src/kv_radix.rs` | InstanceHandle、descriptor validation、demand/replica 语义拆分 |
| `cedfs-kv/src/lib.rs` | V2 service 装配、per-group workers、条件式 transfer commit |
| `cedfs-kv/src/metrics.rs` | 持续 counter/gauge/histogram，删除无界 record Vec |

目录名是建议组织方式；实现时可根据代码量合并，但不要继续把全部 V2 状态机堆入 `lib.rs`。

### 6.2 LMCache core 与 GlobalKV plugin

| 文件/目录 | 计划改动 |
| --- | --- |
| `lmcache/v1/plugin/kv_migration.py` | structured metadata interface/dataclass；V1 compatibility adapter |
| `lmcache/v1/cache_engine.py` | store 成功后上报实际 chunk descriptors；避免只上传 suffix tokens |
| `lmcache/v1/kv_event_utils.py` | 复用完整 parent chain 构造，输出 metadata descriptor |
| `lmcache/v1/storage_backend/local_cpu_backend.py` | inventory snapshot/capacity public API；保持 remove callback |
| `plugins/kv_transfer/.../metadata_client.py` | V2 register/lease/mutation/sync RPC 和 deadline |
| `plugins/kv_transfer/.../migration.py` | async reporter、event seq、ledger、heartbeat/full-sync worker、结构化 transfer result |
| `plugins/kv_transfer/.../backend.py` | target per-block result、ledger mutation version、迁移容量状态 |
| `plugins/kv_transfer/.../globalkv_server.py` | TransferKvV2 handler |
| `plugins/kv_transfer/.../proto/*` | 只由共享 proto 生成，不手工维护 |

优先把通用的 chunk descriptor 构造留在 LMCache core，把 CEDFS 专属 RPC、ledger、heartbeat 留在 GlobalKV plugin，避免 core 直接依赖 CEDFS proto。

## 7. 测试计划

### 7.1 协议契约测试

Rust 和 Python 使用同一组二进制 fixture：

- RegisterInstanceV2 request/response；
- root/non-root BlockDescriptor；
- STORE/REMOVE mutation batch；
- inventory page/commit；
- 9 种 transfer block status；
- unknown minor field 的前后兼容；
- 坏 hash 长度、坏 offset、缺 parent、重复 seq、sequence gap。

门禁：两侧 encode/decode 字节一致，proto descriptor checksum 一致。

### 7.2 CEDFS 单元测试

至少新增：

- duplicate registration same epoch 幂等；new epoch 替换旧 epoch；
- lease expiry 后查询和候选均排除；
- group 隔离；
- mutation seq duplicate/gap/stale epoch；
- inventory staging 不可见、commit 原子替换、abort 无污染；
- per-block transfer commit；
- newer remove 胜过 older transfer commit；
- request start/end 幂等；
- per-request TTL；
- empty READY instance 参与 target；
- heat window rotation；
- capacity/max-replica/hysteresis 过滤；
- rebalance trigger 合并和 block in-flight 去重。

### 7.3 LMCache 单元测试

至少新增：

- full prompt 初次 store 的 descriptors；
- prefix A 已有、suffix B 新存时 B 的真实累计 hash/parent/position；
- allocation 只成功部分 chunks 时只上报实际 stored chunks；
- removal 更新 ledger 并保留原 seq 重试；
- target migration 的 COPIED/ALREADY/SOURCE_MISSING/NO_CAPACITY 状态对齐；
- reporter queue 高水位切换 sync-required；
- meta deadline 不阻塞调用线程；
- generation change 触发 full sync；
- full sync 失败必定解除 freeze；
- ledger missing key 进入 UNMANAGED 而不是虚报。

### 7.4 无 GPU 控制面集成测试

构建一个 fake LMCache data server 和真实 CEDFS 的测试拓扑：

```text
CEDFS
├── fake instance A
└── fake instance B
```

覆盖：

- register -> mutation -> search；
- A -> B 迁移 2/5 成功；
- B remove 先于 CEDFS transfer commit；
- instance crash/lease expiry；
- meta restart/generation change/full sync；
- duplicate/reorder/drop mutation；
- group mismatch；
- oversized transfer 自动分页。

这组测试不依赖 CUDA/NIXL，用于 CI 快速验证控制面状态机。

### 7.5 两实例真实链路测试

在有 NIXL/CPU allocator 的环境验证：

- 两个同构实例复制后 tensor 可命中；
- target 满时返回部分结果且无幽灵副本；
- 复制后立即 eviction 最终收敛；
- 连续请求下 Dynamo event index 与 CEDFS V2 inventory 一致；
- meta 停机/恢复不影响推理正确性；
- 长序列迁移 obey byte budget；
- source/target 并发上限有效。

### 7.6 长稳与故障注入

至少执行：

- 24 小时 shadow soak：V2 不迁移，只比较 inventory；
- 24 小时 canary transfer soak；
- meta 每 30 分钟重启；
- 随机丢 1% mutation response，验证幂等重试；
- 随机延迟/乱序 target remove 与 transfer response；
- target 容量维持在高水位附近，观察是否振荡；
- reporter queue、CEDFS 内存和 metrics cardinality 长时间稳定。

## 8. 灰度发布与回滚

### 8.1 发布顺序

1. **代码就绪，V1 默认**：部署含 V2 代码的 CEDFS/LMCache，所有开关仍为 V1。
2. **dual-shadow 1% 实例**：LMCache 双报，CEDFS 建 shadow index，V2 transfer 关闭。
3. **dual-shadow 100%**：至少 24 小时验证 inventory、seq gap、queue 和内存。
4. **V2 metadata authoritative，transfer 关闭**：查询/指标使用 V2 READY inventory，Dynamo 仍独立工作。
5. **单 compatibility group canary transfer**：只允许一个同构组主动复制。
6. **10% -> 50% -> 100% group**：每档至少观察一个完整热点窗口。
7. **停止 V1 mutation 上报**：V2 稳定至少一个发布周期后执行。
8. **删除 V1**：再经过一个 deprecation 周期，且确认无旧 client。

### 8.2 每档观察指标

- inventory mismatch 必须为 0；
- stale epoch mutation 必须可解释；
- sequence gap 触发 sync 后必须恢复；
- transfer result 数必须等于 request block 数；
- phantom repair 不得持续增长；
- reporter queue 不得长期处于高水位；
- target eviction rate 不应因迁移显著持续升高；
- 推理错误率和 P99 不得因 meta RPC 上升。

### 8.3 回滚策略

按安全性从高到低：

1. `enable_v2_transfer=false`：立即停止主动复制，保留 V2 metadata 观测；LMCache 本地 cache 和 Dynamo 路由继续工作。
2. CEDFS `protocol_mode=dual_shadow`：V2 退回影子，不对外提供位置事实。
3. LMCache `globalkv_protocol=dual/v1`：在仍保留 V1 双报的窗口回退旧控制面。
4. 完全关闭 GlobalKV plugin：只保留 LMCache 本地 cache；这是控制面故障时的最终降级。

禁止把 V2 group/epoch/version 状态降级写入 V1 radix；回滚通过开关选择整套状态，不做有损格式转换。

## 9. 工作量与人员建议

以下是基于当前代码结构的工程量区间，不含外部基础设施排队时间：

| 阶段 | 预计工程量 |
| --- | ---: |
| A：协议骨架 | 3～5 人日 |
| B：权威 metadata 与影子 index | 8～12 人日 |
| C：逐块迁移与版本提交 | 8～12 人日 |
| D：生命周期、异步上报、full sync | 12～18 人日 |
| E：容量感知策略 | 8～12 人日 |
| F：观测、安全、清理 | 6～10 人日 |
| 集成、故障注入和灰度支持 | 8～12 人日 |
| **合计** | **53～81 人日** |

建议配置：

- 1 名 CEDFS/Rust owner；
- 1 名 LMCache/Python + transfer backend owner；
- 1 名集成/测试 owner，可在 A～C 阶段兼职、D 以后全程参与。

三个 workstream 可以部分并行，但关键路径是：

```text
A -> B -> C -> D -> canary
         \-> D 的 registry/reporter 可与 C 后半段并行
D -> E
D -> F
```

两名核心开发加一名集成人员，合理日历周期约 7～10 周。若先只交付“正确性 MVP”，完成 A+B+C 并保持 V2 transfer 仅 canary，约 3～4 周。

## 10. 里程碑与退出标准

### M0：V2 可部署

- V1 无行为变化；
- V2 capability 握手成功；
- Rust/Python proto 无漂移；
- 所有 V2 功能开关默认关闭。

### M1：Metadata Correctness MVP

- suffix store hash 与 LMCache 实际 key 一致；
- group 隔离生效；
- shadow inventory 与 worker inventory 100% 一致；
- mutation 可按 seq 定位。

### M2：Transfer Correctness MVP

- 部分成功逐块提交；
- target remove/transfer response 乱序不会复活副本；
- 长请求分页；
- canary group 无幽灵副本。

### M3：Recovery Ready

- lease/epoch 生效；
- meta 不可用不阻断推理；
- meta 重启后实例自动 full sync；
- sync 前副本不可见；
- sequence gap 自动恢复。

### M4：Policy Ready

- 空实例可预热；
- 热度衰减；
- 容量不足不迁移；
- 并发和 byte budget 生效；
- 24 小时无迁移—淘汰振荡。

### M5：Production Ready

- health/readiness/metrics/告警齐全；
- 安全和 RPC 限制启用；
- 24 小时 shadow + 24 小时 transfer soak 通过；
- 回滚演练通过；
- 运维手册和兼容矩阵完成。

## 11. 第一批可立即创建的开发任务

建议先创建以下 10 个任务，形成第一个可验证闭环：

1. 确认 `lmcache_instance_id + worker_id` 与实际 transfer rank 的映射，输出兼容矩阵。
2. 设计并评审 V2 proto，冻结 message/status/sequence 语义。
3. 建立 Rust/Python 共享 proto fixture 与 descriptor checksum 检查。
4. 在 LMCache 定义 structured `KvBlockMetadata`，复用 KV event parent-chain helper。
5. 修复 suffix store 上报，增加 A-prefix/B-suffix 单元测试。
6. 在 GlobalKV plugin 建立 metadata ledger 和 V2 mutation client。
7. 在 CEDFS 建立 instance registry、group manager 和 V2 shadow mutation handler。
8. target backend 返回逐块 transfer status，先覆盖 existing/copied/no-capacity/source-missing。
9. CEDFS 实现逐块 transfer commit，不启用自动 rebalance。
10. 建立两 fake instances 的无 GPU 控制面集成测试。

这 10 项完成后即可进入 dual-shadow，并能验证两个最高优先级问题是否真正被修复；不要在此之前开始 capacity-aware pressure 调优。

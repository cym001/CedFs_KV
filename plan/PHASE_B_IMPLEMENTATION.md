# 阶段 B 实施记录：权威 block metadata 与 shadow index

## 数据链路

LMCache 在 store 完成后，以 LocalCPUBackend 当前实际存在的 `CacheEngineKey`
为集合，从完整 token 序列复用 `build_full_sequence_chunk_infos()` 构造
`KvBlockMetadata`。因此已存在前缀 A、只写入后缀 B 时，B 的 `seq_hash`、
`parent_hash`、position 和 token slice 都来自真实完整 parent chain，而不是从
suffix tokens 重新猜测。

GlobalKV reporter 维护内存 ledger：

```text
seq_hash -> (parent_hash, position, offset, token_ids)
```

普通 store 和迁移 target 成功写入都会登记 descriptor；LocalCPUBackend 的
remove/eviction callback 会先删除 ledger，再上报 REMOVE mutation。

## CEDFS shadow 状态

`V2State` 只在 `protocol_mode != v1` 时创建，包含：

- `InstanceKey -> InstanceRecord` 注册表；
- 服务端根据 compatibility fingerprint 固定编码计算的 group ID；
- 每个 group 独立的 block map；
- 每个 block 的 descriptor、replica instance handle 和 mutation version；
- 每实例独立的 `committed_event_seq`。

mutation batch 在 group 锁内先应用到临时副本。只有所有新 event 的 sequence、
group、epoch、hash 长度、parent chain、offset 和 token 长度全部合法时，才替换
live shadow map 并推进 committed sequence。sequence gap 或坏 descriptor 不会
留下部分写入。

阶段 B 的 shadow index 仅观察，不参与 V1 查询、pressure migration 或位置返回。
lease、heartbeat、full inventory sync 和异步重试属于阶段 D。

## Binding 门禁

CEDFS Rust V2 binding 已由 `cedfs-proto` 构建生成。LMCache V2 mutation 需要从
同一份 canonical `kvcache_v2.proto` 生成并提交：

- `kvcache_v2_pb2.py`
- `kvcache_v2_pb2_grpc.py`

Python V2 binding 已从 canonical proto 生成，包内 import 已修正。生成结果使用
protobuf 7.35.1 / grpc 1.83.0，plugin dependency 与生成代码的运行时门禁保持一致。
capability 握手会解析 CEDFS 返回值，并将 Rust 嵌入的完整两文件 descriptor set
SHA-256 与 Python binding 重建的 descriptor set SHA-256 比较；不一致时 `dual`
禁用 V2 shadow、继续 V1，`v2` 则 fail-fast。

在 binding 缺失或运行时版本不兼容时同样不会假装 shadow mutation 已启用。

## 静态测试覆盖

- prefix 已存在时 suffix descriptor 使用完整 parent chain；
- 只上报实际写入的 key；
- sequence gap 不污染 shadow map；
- 相同 hash 在不同 compatibility group 中隔离；
- 非 root descriptor 缺少本实例 parent 时拒绝。

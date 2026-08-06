# CEDFS KV 单 Meta 运维与告警

本文适用于集群内多实例 KV cache、单个 CEDFS meta server 的部署。多个 meta server 之间的状态同步不在当前范围内。

## 健康与状态

CEDFS 在 `status_port` 提供独立 HTTP endpoint：

- `GET /live` 或 `GET /health`：进程和事件循环可响应时返回 HTTP 200；
- `GET /ready`：V1 模式返回 HTTP 200；dual/V2 模式仅在至少存在一个 compatibility group，且每个已注册实例都完成 inventory 恢复时返回 HTTP 200，否则返回 503；
- `GET /status`：返回协议模式、迁移开关、累计 metrics、meta generation、各 group 的实例/READY/块/副本数量及恢复计数。

设置 `enable_metrics=true` 后，聚合 metrics 还会按 `metrics_interval_ms` 持续写入结构化日志；关闭时 `/status` 中的通用 transfer/selection counter 为零，但 V2 state 的 lease、sequence、inventory 和 reconcile counter 仍持续维护。

探活只使用 `/live`，流量接入和主动迁移门禁使用 `/ready`。不要用 `/live` 代替 readiness。

## 核心告警

`/status` 中的 counter 为进程生命周期内单调递增值。采集端应保存前一采样值并对增量告警，不要把 compatibility group 或实例之外的动态值扩展为 label，更不能使用 block hash 作为 label。

| 告警 | 建议条件 | 严重级别 | 处置 |
| --- | --- | --- | --- |
| CEDFSNotReady | `/ready` 连续 3 个采样周期为 503 | critical | 检查未 READY group、LMCache heartbeat 与 inventory sync |
| LeaseExpired | `lease_expired_total` 5 分钟增量大于 0 | warning；持续增长为 critical | 检查网络、heartbeat 周期和进程重启 |
| MutationSequenceGap | `mutation_sequence_gap_total` 增量大于 0 | critical | 保持迁移关闭，确认 reporter queue drop，并等待 full inventory sync |
| InventorySyncFailed | `inventory_sync_failure_total` 10 分钟增量大于 3，或 success 长期不增长 | warning/critical | 检查 page checksum、页大小、epoch 和 meta generation |
| TransferFailure | `v2_transfer_failed_blocks_total` 或 `v2_rebalance_failure_total` 持续增长 | warning | 检查 source/target endpoint、容量和 epoch；必要时关闭主动迁移 |
| ReporterQueueDrop | LMCache 日志出现 `metadata queue full` 或 `sequence gap` | critical | 扩大有界 queue 前先确认 meta 延迟，随后触发 full sync |
| ReplicaMismatch | `reconcile_mismatch_total` 增量大于 0 | critical | 隔离实例，检查 descriptor conflict/checksum mismatch，重新注册并 full sync |

告警阈值应根据实例数与请求率基线调整，但 sequence gap、descriptor conflict 和 checksum mismatch 不应设置为可忽略事件。

## 安全降级

首选运行时降级是设置 `enable_v2_transfer=false` 并重启 CEDFS：这会停止主动复制，但保留 LMCache 本地 cache、metadata heartbeat、inventory 恢复和查询观测。不要通过关闭 heartbeat 或丢弃 mutation 来停止迁移，这会使 metadata 失真。

协议升级顺序为 `v1 -> dual_shadow -> v2`（LMCache 对应 `v1 -> dual -> v2`）。V1 在当前兼容发布周期仍保留但已 deprecated；确认 dual parity、所有 group READY、告警稳定后再切换 V2。回滚时按相反顺序操作，不跨过 dual shadow 验证。

## 关键配置门禁

- `status_port` 必须非零且不能与 gRPC port 相同；
- `metrics_interval_ms`、TTL、超时、分页和并发预算必须大于零；
- `hash_algorithm` 必须是明确支持的值，`sha256_cbor` 必须配置 `python_hash_seed`；
- `block_size` 必须能表示为 V2 descriptor 的 `uint32`；
- V2 transfer byte budget 必须至少容纳一个估算 token；
- LMCache 的 `globalkv_advertised_host` 必须是其他实例可路由的地址，不能使用 wildcard bind address；
- 注册的 scheme、host、port、path 必须与 search response 完全一致。

F-06（RPC interceptor 身份绑定与限流）按本轮实施要求明确不包含在本阶段交付中。

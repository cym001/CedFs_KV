# 阶段 A 实施记录：协议基线与可回滚骨架

## 1. 交付边界

阶段 A 只建立可部署的 V2 协议入口，不创建 instance registry、shadow
index、mutation queue 或后台任务，也不启用 V2 主动迁移。默认配置仍运行
V1，V1 service、message 字段和上报/迁移路径保持不变。

## 2. InstanceKey 与 rank 映射

V2 使用：

```text
InstanceKey = (lmcache_instance_id, worker_id)
```

`worker_id` 是 LMCache `CacheEngineKey` 中的全局分布式 rank。`local_worker_id`
只表示本机设备序号，多机部署时会重复，因此不能作为实例身份的一部分。

| 场景 | 是否同一 InstanceKey | 是否允许作为同 rank 迁移对 |
| --- | --- | --- |
| 同一推理副本、同一 worker 进程 | 是 | 是 |
| 两个推理副本、相同全局 rank | 否，instance id 不同 | fingerprint 一致时允许 |
| 同一推理副本、不同全局 rank | 否，worker id 不同 | 不允许 |
| 不同主机、相同 local worker id | 否，全局 worker id/instance id 判定 | 不能据此允许 |

实例 epoch、lease 和 compatibility group 的状态管理在阶段 B/D 接入；阶段 A
只固定协议字段和客户端身份绑定时机。LMCache plugin 在 engine metadata 可用后，
从 `engine.metadata.worker_id` 绑定 V2 身份，避免启动早期猜测 rank。

## 3. 协议与生成规则

- V1：`kvcache.proto`、`kvserver.proto`，内容未修改；
- V2：`kvcache_v2.proto` 覆盖 capability、注册、心跳、mutation、inventory
  sync 和 request lifecycle；
- V2：`kvserver_v2.proto` 定义逐 block 的 `TransferKvV2` 结果；
- `cedfs-proto/proto` 是唯一 proto 来源，构建输入顺序固定；
- Rust V2 binding 和 descriptor set 只生成到 `OUT_DIR`，禁止手工维护；
- capability 返回 Rust 服务实际嵌入 descriptor set 的 SHA-256，为后续 Python
  binding 启用前的跨语言一致性门禁。

受仓库开发规则限制，本阶段提交不运行 proto generator；Python V2 binding
必须在允许生成的 CI/构建环境从上述唯一来源生成，不能从 LMCache 内复制的
proto 生成。

## 4. 配置和运行模式

CEDFS：

```text
protocol_mode = v1 | dual_shadow | v2   # 默认 v1
enable_v2_transfer = false              # 默认 false
```

LMCache：

```text
globalkv_protocol = v1 | dual | v2      # 默认 v1
```

行为矩阵：

| CEDFS 模式 | V1 service | V2 service | V2 状态/后台任务 |
| --- | --- | --- | --- |
| `v1` | 开启 | 不注册 | 不创建 |
| `dual_shadow` | 开启 | 仅 capability/空骨架 | 不创建 |
| `v2` | 关闭 data V1，保留 meta 查询 V1 | 仅 capability/空骨架 | 不创建 |

| LMCache 模式 | V1 注册/上报 | V2 capability |
| --- | --- | --- |
| `v1` | 保持现状 | 不调用 |
| `dual` | 保持现状 | 失败只告警，不影响 V1 |
| `v2` | 不调用 | 失败则启动失败 |

`enable_v2_transfer=true` 与 CEDFS `protocol_mode=v1` 的组合会 fail-fast。
阶段 A 即使打开该开关也只有 capability 会公布状态，尚无 transfer worker。

## 5. 阶段门禁

- 默认值不改变 V1 网络服务和 LMCache V1 上报路径；
- `dual_shadow`/`dual` 只增加 capability 握手；
- V2 未启用时不创建 shadow index 或额外后台任务；
- 所有未实现的 V2 状态 RPC 明确返回 `UNIMPLEMENTED`，不会误报成功；
- V2 binding 生成和编译验证必须由允许执行项目工具链的 CI 完成。

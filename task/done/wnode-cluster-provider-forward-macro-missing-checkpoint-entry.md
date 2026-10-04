终态注记（2026-09-28 主控）：已合入 dev（合入 commit aed49c5）。
收口形态：
1. wedb/wnode/src/cluster_provider.rs：在 forward_cluster_provider! 转发宏补齐 add_new_checkpoint_entry，确保 Arc<dyn ClusterProviderFace> 句柄分发至具体实现并返回 Some(SlowFuture)。
2. wedb/wedb/tests/checkpoint_wiring.rs：primary_checkpoint_flow_wires_cluster_callbacks 改经 ClusterProviderHandle 句柄调用并断言 Some；新增专用测试 cluster_provider_handle_add_new_checkpoint_entry_forwards_and_truncates 锁定 Arc<dyn> 转发与截断推进。

# wnode forward_cluster_provider! 宏漏列 add_new_checkpoint_entry：Arc<dyn> 层恒短路 None，集群检查点登记链旁路

- 状态：ing（r8 波 c7 席发现，rustc 最小编译实验复现）
- 优先级：P1（行为缺陷：集群形态主库检查点后条目历史不登记、AOF 安全截断不执行）

## 缺陷本体

`wnode/src/cluster_provider.rs:215-254` `forward_cluster_provider!` 转发清单 25 项，对 trait 26 项（`:188-196` 声明的 `add_new_checkpoint_entry` 漏列）。

- `impl<T: ClusterProvider + ?Sized> ClusterProvider for Arc<T>` 因缺行继承 trait 默认体 `None`
- 唯一产线调用点 `wnode/src/database/database_manager_base.rs:429` 接收器是 `OnceLock<ClusterProviderHandle>`（`Arc<dyn ClusterProviderFace>`，boot.rs:253 注入）
- 方法解析命中 `Arc<dyn>` 层即止，永不 deref 到 wedb 真实现（`wedb/src/server/cluster_provider/traits.rs:634` 登记了 CheckpointEntry + safe_truncate_aof）
- 同函数 else 本地 `truncate_until` 臂因句柄在位不走

**结果**：集群形态主库检查点后，条目历史不登记、AOF 安全截断不执行、本地截断也不执行。

## 逃逸原因

`tests/checkpoint_wiring.rs:77` 走 `CheckpointCallbackFace` 具体类型直调，恰好绕开产线接收器类型，测试未拦。

## 修法

1. 宏清单补 `fn add_new_checkpoint_entry(full: bool, covered: AofAddress, store_checkpoint_token: u128, object_store_checkpoint_token: u128) -> Option<SlowFuture>;`
2. 防回归：经 Handle（Arc<dyn>）路径断言 `Some` 的单测，钉死宏清单与 trait 声明完整性
3. 顺手排查同族其余转发宏是否有同类漏列（逐项对 trait 声明清单）

## 复现实验（c7 席）

rustc 独立最小复现：`omitted via Arc<dyn> = None`（具体实现 `Some(7)` 被吞）。

# wcpr : CPR 检查点与恢复

## 项目介绍

wcpr 提供检查点持久化与崩溃恢复（CPR）：协同快照状态机、哈希索引快照二进制 I/O、检查点元数据与完整性格式。文件布局：`checkpoint_{token}.meta`、`index_{token}.ckpt`、`.tmp` 临时后缀。

## 模块组成

- `manager`：`CheckpointManager` 与 `CprStore` / `CprRecover` 宿主契约
- `meta`：元数据结构与文件名（`CheckpointMeta`、`CheckpointType`、`StoreMeta`、`IndexMeta`、`HlogMeta`）
- `index_ckpt`：哈希索引快照二进制读写
- `error`：错误类型

## 核心 API

- `CheckpointManager`：create_checkpoint(\_with_token)、recover_checkpoint_components / recover / recover_latest、take_index_checkpoint、list_checkpoints、find_latest_checkpoint、purge_checkpoint / purge_all / purge_outdated
- `CprStore` / `CprRecover`：宿主引擎端口；`RecoveredCheckpoint{meta, index, hlog, epoch}`
- `CheckpointMeta`：token / cp_type / index_meta / hlog_meta / store_meta / created_at / format_version / integrity_crc32；`CheckpointType`（FoldOver / Snapshot）
- `FORMAT_VERSION = 2`、`INTEGRITY_FROM_VERSION = 2`；`next_token`
- index_ckpt：write_index_checkpoint、read_index_checkpoint_truncated（内部 64B 头 `WEDB_IDX` magic、512 桶 32KB 批量硬件 CRC32）

## 设计要点

- 状态机相位：生成 token（以目录现存最大 token 为签发下界，防墙钟回拨倒序）→ PREPARE 捕获 tail → 封印只读（先封印后刷盘）→ epoch 排空屏障 → WAIT_FLUSH（flush_all + RangeIndex/BfTree CPR 快照 + token 子目录树递归 fsync：文件数据全平台刷盘、Windows 经写权限句柄；目录项 fsync 仅 Unix，Windows 无 fsync(dirfd) 原语而跳过）→ 原子写 index（tmp + fsync + rename + 父目录 fsync，仅 Unix 生效）→ 最后写 meta
- meta 最后落盘：失败 / 半截检查点绝不进恢复视图；失败清场回收本 token 全部文件；进程级闸门串行化并发检查点
- 调用方契约：检查点必须在纪元保护区外发起，保护区内调用以 CheckpointWhileEpochProtected 类型化错误 fail-fast 拒绝（先于任何状态变更，零副作用）
- 恢复语义：FoldOver 将 ReadOnlyAddress 对齐 TailAddress；Snapshot 按 mutable_fraction 重建可变区
- 完整性封签 integrity_crc32 自版本 2 起发布前回填、恢复强制校验；version < 2 遗留格式放行；元数据统一采用高效二进制 bitcode 编码

## 测试覆盖

manager.rs 内联测试仅覆盖：token 签发闸门单调性（墙钟回拨续发、目录下界钳制、叠加路径）与 sync_dir_tree（嵌套树 / 空目录 / 空文件、幂等、缺目录报错）。检查点创建 / 恢复往返、FoldOver / Snapshot、失败清场、purge 系列、索引快照读写与截断容错、完整性强校验与遗留格式放行由 wkv/tests/checkpoint 集成套件（recovery / edge / checkpoint_manager / index_checkpoint / fault_defense）覆盖。

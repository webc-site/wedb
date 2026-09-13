[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wcpr : CPR Checkpoint and Recovery

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wcpr provides checkpoint persistence and crash recovery (CPR): the coordinated snapshot state machine, binary I/O for hash-index snapshots, and checkpoint metadata with integrity formats. File layout: `checkpoint_{token}.meta`, `index_{token}.ckpt`, and a `.tmp` temporary suffix.

## Module Layout

- `manager`: `CheckpointManager` with the `CprStore` / `CprRecover` host contracts
- `meta`: metadata structures and file names (`CheckpointMeta`, `CheckpointType`, `StoreMeta`, `IndexMeta`, `HlogMeta`)
- `index_ckpt`: binary read/write for hash-index snapshots
- `error`: error types

## Core API

- `CheckpointManager`: create_checkpoint(\_with_token), recover_checkpoint_components / recover / recover_latest, take_index_checkpoint, list_checkpoints, find_latest_checkpoint, purge_checkpoint / purge_all / purge_outdated
- `CprStore` / `CprRecover`: host engine ports; `RecoveredCheckpoint{meta, index, hlog, epoch}`
- `CheckpointMeta`: token / cp_type / index_meta / hlog_meta / store_meta / created_at / format_version / integrity_crc32; `CheckpointType` (FoldOver / Snapshot)
- `FORMAT_VERSION = 2`, `INTEGRITY_FROM_VERSION = 2`; `next_token`
- index_ckpt: write_index_checkpoint, read_index_checkpoint_truncated (internally a 64B header with `WEDB_IDX` magic, 512-bucket 32KB batches with hardware CRC32)

## Design Notes

- State machine phases: issue token (current max token on disk is the issuance lower bound, guarding against wall-clock rollback) → PREPARE captures tail → seal read-only (seal before flush) → epoch drain barrier → WAIT_FLUSH (flush_all + RangeIndex/BfTree CPR snapshots + recursive fsync of the token directory tree: file data fsynced on all platforms, on Windows via a write-permission handle; directory-entry fsync is Unix-only, Windows has no fsync(dirfd) primitive and skips it) → atomically write index (tmp + fsync + rename + parent-dir fsync, effective on Unix only) → write meta last
- Meta lands last: failed / partial checkpoints never enter the recovery view; failures clean up all files of the token; a process-level gate serializes concurrent checkpoints
- Caller contract: checkpoints must be initiated outside epoch protection; calls inside a protected region fail fast with the typed CheckpointWhileEpochProtected error (before any state mutation, zero side effects)
- Recovery semantics: FoldOver aligns ReadOnlyAddress to TailAddress; Snapshot rebuilds the mutable region by mutable_fraction
- The integrity_crc32 seal is backfilled before publication and enforced on recovery from version 2; legacy formats below version 2 pass; metadata uniformly uses compact binary bitcode encoding

## Test Coverage

Inline tests in manager.rs cover only: token issuance gate monotonicity (rollback continuation, directory floor clamping, combined paths) and sync_dir_tree (nested trees / empty dirs / empty files, idempotency, missing-dir errors). Checkpoint create/recovery roundtrips, FoldOver / Snapshot, failure cleanup, the purge family, index snapshot read/write with truncation tolerance, and strict integrity enforcement with legacy pass-through are covered by the wkv/tests/checkpoint integration suites (recovery / edge / checkpoint_manager / index_checkpoint / fault_defense).


---

<a name="zh"></a>

# wcpr : CPR 检查点与恢复

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

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


[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wepoch : LightEpoch Epoch Protection

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Introduction

wepoch provides Garnet Tsavorite-style lock-free epoch protection: it manages epoch lifecycle for concurrent participants, decides when memory can be safely reclaimed (SMR), and offers epoch-protected scratch user words.

Both the participant entry and the manager are 64-byte cacheline aligned to eliminate false sharing; layouts are pinned by compile-time const asserts.

## Module Layout

- `entry`: `EpochEntry`, a 64B cacheline-aligned epoch entry (8B epoch + 8B thread_id + 4B reentrant + 1B reserved + 3B padding + 40B user words = 64B)
- `epoch`: `LightEpoch` manager, `Participant`, RAII guards, per-thread TLS state
- `error`: error types

## Core API

- `LightEpoch`: `register` / `resume` / `suspend` / `protect_and_drain` / `bump_epoch` / `safe_to_reclaim_epoch` / `drain` / `bump_and_wait` / `allocate_user_word`; `DEFAULT_MAX_THREADS = 128`
- `Participant`: `enter` / `refresh` / `exit` / `user_word`
- `current_thread_id()`: globally unique nonzero per-thread id (re-exported from `wbase::thread`, basis of TLS slot binding); user-word lifecycle: `allocate_user_word` allocation, `release_user_word` reclamation, `this_thread_user_word` / `set_this_thread_user_word` per-thread access
- `EpochGuard`: RAII protection entry, exits automatically on Drop
- `ProtectedScope`: RAII protection suspension (`!Send + !Sync`, must not cross threads)
- `EpochEntry` / `MAX_USER_WORDS = 5`: scratch user words (5 × 8B slots per participant)
- `DRAIN_LIST_SIZE = 16`: drain action list capacity

## Design Notes

- False-sharing elimination: `EpochEntry` owns a full 64B cacheline; `try_reserve` / `try_claim` form a Dekker-style mutual exclusion across reserved / epoch variables and require SeqCst
- Backoff: unified 3-stage adaptive state machine from `wbase::backoff` (32 spin rounds → 1024 yields → 50μs sleep)
- TLS management: 4 inline slots + overflow capped at 16; thread exit releases slots via Drop with a trailing SeqCst fence
- TLS-scope slots are thread-bound with transfer rejected by the type system (`ProtectedScope` is `!Send + !Sync`); a `Participant` handle may migrate across threads and binds to the entering thread at enter

## Test Coverage

Covers: cacheline alignment, participant capacity and slot reuse, refresh mechanics, drain-list full-load no-livelock under long-lived guards, TLS fallback on thread exit, zero leaks for transient instances; protection (suspend / resume / nested scopes / safe-epoch monotonicity), drain (exactly-once actions, epoch order, cascading), concurrency races and multi-instance isolation, user_word lifecycle and concurrent allocation.

---

<a name="zh"></a>

# wepoch : LightEpoch 纪元保护

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

wepoch 提供 Garnet Tsavorite 风格的无锁纪元保护：管理并发参与者的纪元生命周期，判定"何时可以安全回收内存"（SMR），并提供受纪元保护的暂存用户字。

参与者的 Entry 与管理器本体均为 64 字节缓存行对齐，杜绝伪共享；布局由编译期 const assert 钉死。

## 模块组成

- `entry`：`EpochEntry`，64B 缓存行对齐的纪元条目（8B epoch + 8B thread_id + 4B reentrant + 1B reserved + 3B 填充 + 40B 用户字 = 64B）
- `epoch`：`LightEpoch` 管理器、`Participant`、RAII 守卫、TLS 本线程状态
- `error`：错误类型

## 核心 API

- `LightEpoch`：`register` / `resume` / `suspend` / `protect_and_drain` / `bump_epoch` / `safe_to_reclaim_epoch` / `drain` / `bump_and_wait` / `allocate_user_word`；`DEFAULT_MAX_THREADS = 128`
- `Participant`：`enter` / `refresh` / `exit` / `user_word`
- `current_thread_id()`：线程全局唯一非零 ID（由 `wbase::thread` 统一导出，TLS 槽位绑定依据）；用户字生命周期：`allocate_user_word` 分配、`release_user_word` 回收、`this_thread_user_word` / `set_this_thread_user_word` 本线程读写
- `EpochGuard`：RAII 进入保护，Drop 自动 exit
- `ProtectedScope`：RAII 挂起保护（`!Send + !Sync`，严禁跨线程转移）
- `EpochEntry` / `MAX_USER_WORDS = 5`：暂存用户字（每参与者 5 个 8B 槽位）
- `DRAIN_LIST_SIZE = 16`：drain 动作列表容量

## 设计要点

- 伪共享消除：`EpochEntry` 独占 64B 缓存行；`try_reserve` / `try_claim` 跨 reserved / epoch 两变量 Dekker 式互斥，必须 SeqCst
- 退避策略：统一复用 `wbase::backoff` 三阶状态机（自旋 32 轮 → yield 1024 轮 → 50μs 睡眠）
- TLS 管理：内联 4 槽 + overflow 16 上限；线程退出 Drop 兜底释放槽位并补 SeqCst fence
- TLS 作用域槽位与线程绑定，跨线程转移受类型系统拒绝（`ProtectedScope` 为 `!Send + !Sync`）；`Participant` 句柄可跨线程转移，enter 时绑定当时线程

## 测试覆盖

覆盖：缓存行对齐、参与者容量上限与槽位复用、refresh 机制、长期守卫下 drain list 满载不活锁、线程退出 TLS 兜底回收、瞬态实例零泄漏；protection（suspend / resume / 嵌套作用域 / safe epoch 单调）、drain（动作恰一次、按纪元序、级联触发）、并发竞争与多实例隔离、user_word 生命周期与并发分配。

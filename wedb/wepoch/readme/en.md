# wepoch : LightEpoch Epoch Protection

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
- `current_thread_id()`: globally unique nonzero per-thread id (basis of TLS slot binding); user-word lifecycle: `allocate_user_word` allocation, `release_user_word` reclamation, `this_thread_user_word` / `set_this_thread_user_word` per-thread access
- `EpochGuard`: RAII protection entry, exits automatically on Drop
- `ProtectedScope`: RAII protection suspension (`!Send + !Sync`, must not cross threads)
- `EpochEntry` / `MAX_USER_WORDS = 5`: scratch user words (5 × 8B slots per participant)
- `DRAIN_LIST_SIZE = 16`: drain action list capacity

## Design Notes

- False-sharing elimination: `EpochEntry` owns a full 64B cacheline; `try_reserve` / `try_claim` form a Dekker-style mutual exclusion across reserved / epoch variables and require SeqCst
- Backoff: 32 spin rounds → 1024 yields → 50μs sleep
- TLS management: 4 inline slots + overflow capped at 16; thread exit releases slots via Drop with a trailing SeqCst fence
- TLS-scope slots are thread-bound with transfer rejected by the type system (`ProtectedScope` is `!Send + !Sync`); a `Participant` handle may migrate across threads and binds to the entering thread at enter

## Test Coverage

Covers: cacheline alignment, participant capacity and slot reuse, refresh mechanics, drain-list full-load no-livelock under long-lived guards, TLS fallback on thread exit, zero leaks for transient instances; protection (suspend / resume / nested scopes / safe-epoch monotonicity), drain (exactly-once actions, epoch order, cascading), concurrency races and multi-instance isolation, user_word lifecycle and concurrent allocation.

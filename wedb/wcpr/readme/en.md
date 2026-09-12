# wcpr : CPR Checkpoint and Recovery

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

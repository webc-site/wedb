# wedb_raft

[![Crates.io](https://img.shields.io/crates/v/wedb_raft.svg)](https://crates.io/crates/wedb_raft)
[![Documentation](https://docs.rs/wedb_raft/badge.svg)](https://docs.rs/wedb_raft)
[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)

`wedb_raft` provides a robust, high-throughput distributed consensus engine for WeDb built on OpenRaft and backed by a specialized Fjall LSM-Tree storage engine for write-ahead log (WAL) storage, state machine application, and snapshot management.

---

## 🚀 Features

- **OpenRaft Integration**: Full implementation of the Raft consensus protocol, including leader election, log replication, commit index progression, and cluster membership reconfiguration.
- **LSM-Tree Log Storage**: High-write-throughput append-only Raft log store implemented on Fjall partitions.
- **Zero-Copy Streaming Snapshots**: Snapshot generation and restoration with incremental streaming over gRPC/Protobuf.
- **High-Throughput State Machine**: Atomic batch execution of multi-key transactions and write cmds.
- **Dynamic Membership Changes**: Runtime node join, leave, and leader step-down without stopping the cluster.
- **Fast Crash Recovery**: Restores last applied index, membership configs, and committed states reliably on startup.

---

## 📦 Installation

Add `wedb_raft` to your `Cargo.toml`:

```bash
cargo add wedb_raft
```

---

## 🛠️ Usage Example

```rust
use std::sync::Arc;
use wedb_raft::conf::FjallConf;
use wedb_raft::store::FjallStorage;
use wedb_raft::types::{NodeId, RaftTypeConfig};

#[compio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = FjallConf {
        data_path: "./data/raft_node_1".to_string(),
        cache_size: Some(64 * 1024 * 1024),
        compression: None,
    };

    let node_id: NodeId = 1;
    let (log_store, state_machine) = FjallStorage::new(node_id, conf).await?;

    println!("Raft storage initialized for Node {node_id}");
    Ok(())
}
```

---

## 📜 License

Licensed under either of Apache License, Version 2.0 or MIT License at your option.

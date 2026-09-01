# wedb_cluster

[![Crates.io](https://img.shields.io/crates/v/wedb_cluster.svg)](https://crates.io/crates/wedb_cluster)
[![Documentation](https://docs.rs/wedb_cluster/badge.svg)](https://docs.rs/wedb_cluster)
[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)

`wedb_cluster` is the distributed clustering, networking, and Redis protocol coordination engine for WeDb. It ties together Raft consensus (`wedb_raft`), low-level multi-model storage (`wedb_embed`), zero-copy Redis cmd parsing (`webc_cmd`), and high-concurrency Compio networking.

---

## 🚀 Features

- **RESP2 & RESP3 Redis Server**: High-concurrency async TCP listener serving thousands of simultaneous connections.
- **Raft Cluster Orchestrator**: Manages cluster topology, heartbeat intervals, leader elections, and log sync.
- **Distributed Request Dispatcher**: Automatically routes read/write requests to Raft consensus or local read-only replicas.
- **Cross-Node Pub/Sub Broadcaster**: Cluster-wide channel and pattern message broadcasting.
- **Multi-Database & Multi-Namespace Isolation**: Multi-tenant database partitioning.
- **Comprehensive Command Handlers**: Full execution handlers for String, Hash, List, Set, ZSet, Stream, Bitfield, Geo, JSON, TimeSeries, Bloom, Cuckoo, TDigest, and Admin cmds.

---

## 📦 Installation

Add `wedb_cluster` to your `Cargo.toml`:

```bash
cargo add wedb_cluster
```

---

## 🛠️ Usage Example

```rust
use std::time::Duration;
use wedb_cluster::conf::{Conf, Endpoint, FjallConf, RaftConf, RedisConf};
use wedb_cluster::node::RaftNodeBuilder;
use wedb_cluster::redis::RedisServer;

#[compio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Conf {
        node_id: 1,
        raft: RaftConf {
            endpoint: Endpoint::new("127.0.0.1", 9001),
            advertise_endpoint: Endpoint::new("127.0.0.1", 9001),
            join: vec![],
            heartbeat_interval: Some(50),
            election_timeout_min: Some(150),
            election_timeout_max: Some(300),
        },
        fjall: FjallConf {
            data_path: "./data/cluster_node_1".to_string(),
            cache_size: None,
            compression: None,
        },
        redis: RedisConf {
            addr: "127.0.0.1:6379".to_string(),
            enabled: true,
        },
    };

    let node = RaftNodeBuilder::from_conf(&conf).await?;
    let redis_server = RedisServer::start(node.clone(), conf.redis.addr.clone()).await?;

    println!("WeDb Cluster Node 1 listening on Redis port 6379");
    compio::signal::ctrl_c().await?;
    redis_server.shutdown().await?;
    node.shutdown().await?;
    Ok(())
}
```

---

## 📜 License

Licensed under either of Apache License, Version 2.0 or MIT License at your option.

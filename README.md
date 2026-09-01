# WeDb (Redis-Compatible Multi-Model Distributed Database)

[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![CI Tests](https://img.shields.io/badge/tests-161%20passed-brightgreen.svg)]()

WeDb is an ultra-fast, distributed, persistent NoSQL database and multi-model embedded storage engine designed for mission-critical modern workloads. It is fully compatible with standard Redis (RESP2 and RESP3) protocols, powered by the Raft consensus algorithm and a high-performance LSM-Tree storage engine (Fjall).

---

## 🌟 Highlights & Key Features

- **🚀 Redis Protocol Compatibility**: Native support for RESP2 and RESP3 protocols. Works out-of-the-box with standard Redis clients across all languages (`redis-py`, `ioredis`, `go-redis`, `redis-rs`, `jedis`, etc.).
- **🧩 Multi-Model Rich Data Structures**:
  - **Strings & Numbers**: High-precision decimal, binary offsets, atomic increments (`INCREX`, `INCRBYFLOAT`, `CAD`, `CAS`).
  - **Bitmaps & Bitfields**: Segmented compressed bitmaps, arbitrary width bit manipulation (`BITFIELD`, `BITFIELD_RO`, `BITPOS`, `BITOP`).
  - **Hashes**: Subkey field encoding, individual subkey expiration (`HEXPIRE`, `HGETDEL`, `HRANDFIELD`).
  - **Lists**: Double-ended queues, atomic multiple element pop/move (`LMOVEM`, `BLMOVEM`, `LMPOP`, `BLMPOP`).
  - **Sets & Sorted Sets (ZSet)**: Set algebraic operations (`SDIFFCARD`, `SUNIONCARD`), lexicographical & score range queries, zset store/pop.
  - **Streams**: Consumer groups, PEL management, auto-claim, atomic acknowledgment deletion (`XACKDEL`, `XNACK`, `XDELEX`).
  - **RedisJSON**: Full JSONPath support, in-place document mutations, zero-copy querying.
  - **Probabilistic Data Structures**: Multi-chain Bloom Filters (`BF.*`), Cuckoo Filters (`CF.*`), T-Digest for accurate percentiles (`TDIGEST.*`), HyperLogLog (`PFADD`, `PFCOUNT`, `PFMERGE`).
  - **Geospatial & TimeSeries**: Geo-indexing with Geohash spherical distance queries (`GEOSEARCH`, `GEOSEARCHSTORE`), downsampled time-series chunk storage (`TS.*`).
- **🛡️ Distributed Strong Consistency**: Powered by OpenRaft consensus with asynchronous snapshotting, dynamic membership changes (Raft Join/Leave), and fault tolerance.
- **⚡ Dual Deployment Architecture**:
  - **Distributed Cluster Mode**: Distributed Raft consensus with automatic leader election and log replication.
  - **Embedded / Standalone Mode**: Ultra-low latency single-process engine running directly inside application memory or single daemon.
- **🔒 ACID Transactions**: Full support for `MULTI`, `EXEC`, `DISCARD`, and `WATCH` multi-key serializable transactions.
- **🌐 Multi-Tenant & Multi-DB Isolation**: Native namespace partitioning and logical DB switching (`SELECT`).

---

## 🏗️ Workspace Crates Architecture

WeDb is designed with modularity, zero-copy abstraction, and strict performance boundaries:

| Crate | Description |
|---|---|
| [`wedb`](./wedb) | Main server CLI binary and cluster orchestration entry point. |
| [`wedb_cluster`](./cluster) | Distributed cluster node, Redis TCP networking, pub/sub broadcaster, and Raft consensus runner. |
| [`wedb_embed`](https://github.com/webc-site/wedb_embed) | Core multi-model storage engine, LSM-Tree key composition, and low-level data structures. |
| [`wedb_raft`](./raft) | OpenRaft-based consensus state machine, log storage, and snapshot engine. |
| [`webc_cmd`](./cmd) | High-throughput zero-copy Redis cmd parser and strongly typed AST. |
| [`wedb_standalone`](./standalone) | Lightweight standalone server directly routing to embedded engine without Raft overhead. |
| [`wedb_proto`](./proto) | Protocol Buffer message definitions and Prost-generated serialization codecs. |

---

## ⚡ Quick Start

### 1. Build and Run

```bash
# Build the workspace
cargo build --release

# Run standalone server on port 6379
cargo run --release -p wedb -- standalone --port 6379

# Run a Raft cluster node
cargo run --release -p wedb -- cluster --node-id 1 --redis-port 6379 --raft-port 9001
```

### 2. Connect with Redis CLI

```bash
redis-cli -p 6379

127.0.0.1:6379> SET user:1001 "Alice"
OK
127.0.0.1:6379> GET user:1001
"Alice"
127.0.0.1:6379> HSET user:profile name "Alice" score 95.5
(integer) 2
127.0.0.1:6379> HGETDEL user:profile score
"95.5"
127.0.0.1:6379> JSON.SET doc $ '{"name":"Alice","skills":["Rust","Raft"]}'
OK
127.0.0.1:6379> JSON.GET doc $.skills[0]
"[\"Rust\"]"
```

### 3. Use as an Embedded Rust Engine

Add `wedb_embed` to your `Cargo.toml`:

```toml
[dependencies]
wedb_embed = "0.1"
```

```rust
use wedb_embed::{WeDb, Conf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Conf::new("/tmp/wedb_data");
    let db = WeDb::open(conf)?;

    db.set(b"key1", b"value1", &[])?;
    if let Some(val) = db.get(b"key1")? {
        println!("Value: {}", String::from_utf8_lossy(&val));
    }

    Ok(())
}
```

---

## 🧪 Testing and Verification

Run the full integration test suite with nextest:

```bash
./test.sh
```

---

## 📜 License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))
at your option.

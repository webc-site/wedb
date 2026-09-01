# wedb (CLI & Server Binary)

[![Crates.io](https://img.shields.io/crates/v/wedb.svg)](https://crates.io/crates/wedb)
[![Documentation](https://docs.rs/wedb/badge.svg)](https://docs.rs/wedb)
[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)

`wedb` is the official cmd-line interface (CLI) and server executable for the WeDb distributed database. It allows starting single-node standalone instances, launching Raft cluster nodes, performing cluster health checks, and managing database state.

---

## 📦 Installation

```bash
cargo install wedb
```

Or build from source:

```bash
git clone https://github.com/webc-site/wedb.git
cd wedb
cargo build --release -p wedb
```

---

## 🛠️ Command-Line Usage

### 1. Start a Single-Node Standalone Server

```bash
wedb standalone --port 6379 --data-dir ./data/standalone
```

### 2. Start a Raft Cluster Node

```bash
# Node 1 (Bootstrap leader)
wedb cluster --node-id 1 --redis-port 6379 --raft-port 9001 --data-dir ./data/node1

# Node 2 (Join Node 1)
wedb cluster --node-id 2 --redis-port 6380 --raft-port 9002 --join 127.0.0.1:9001 --data-dir ./data/node2

# Node 3 (Join cluster with multiple peers for high availability)
wedb cluster --node-id 3 --redis-port 6381 --raft-port 9003 --join 127.0.0.1:9001 --join 127.0.0.1:9002 --data-dir ./data/node3
# Or using comma-separated peer list:
# wedb cluster --node-id 3 --redis-port 6381 --raft-port 9003 --join 127.0.0.1:9001,127.0.0.1:9002 --data-dir ./data/node3
```

### 3. Check Cluster Status

```bash
redis-cli -p 6379 RAFT.MEMBERS
```

---

## 📜 License

Licensed under either of Apache License, Version 2.0 or MIT License at your option.

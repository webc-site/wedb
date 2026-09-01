# wedb_standalone

[![Crates.io](https://img.shields.io/crates/v/wedb_standalone.svg)](https://crates.io/crates/wedb_standalone)
[![Documentation](https://docs.rs/wedb_standalone/badge.svg)](https://docs.rs/wedb_standalone)
[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)

`wedb_standalone` provides a lightweight, single-node Redis server implementation that execs Redis cmds directly on the embedded LSM-Tree engine (`wedb_embed`) without the consensus overhead of Raft.

---

## 🚀 Features

- **Direct Storage Dispatching**: Bypasses network RPC and Raft log replication for ultra-low microsecond latency.
- **Full RESP2 / RESP3 Support**: Compatible with all standard Redis clients.
- **Embedded & Daemon Modes**: Can be run as a standalone background process or embedded inside another Rust server.
- **Rich Data Structures**: Full support for Strings, Hashes, Lists, Sets, ZSets, Bitmaps, Bitfields, JSON, Streams, and more.

---

## 📦 Installation

Add `wedb_standalone` to your `Cargo.toml`:

```bash
cargo add wedb_standalone
```

---

## 🛠️ Usage Example

```rust
use std::sync::Arc;
use wedb_embed::{WeDb, Conf};
use wedb_standalone::handler::handle_cmd;
use webc_cmd::parse_cmd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Conf::new("./data/standalone_demo");
    let db = Arc::new(WeDb::open(conf)?);

    let raw_cmd = vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()];
    let cmd = parse_cmd(raw_cmd)?;

    let reply = handle_cmd(&db, cmd)?;
    println!("Response: {reply:?}");

    Ok(())
}
```

---

## 📜 License

Licensed under either of Apache License, Version 2.0 or MIT License at your option.

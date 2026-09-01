# webc_cmd

[![Crates.io](https://img.shields.io/crates/v/webc_cmd.svg)](https://crates.io/crates/webc_cmd)
[![Documentation](https://docs.rs/webc_cmd/badge.svg)](https://docs.rs/webc_cmd)
[![Rust Edition 2024](https://img.shields.io/badge/Rust-Edition%202024-orange.svg)](https://www.rust-lang.org/)

`webc_cmd` is an ultra-fast, zero-copy Redis cmd parser and strongly typed AST library for Rust. It parses raw RESP cmd vectors into strongly typed `Cmd` and `RedisCommand` enums and extracts routing metadata, keys, and read/write flags.

---

## 🚀 Features

- **Comprehensive Command Coverage**: Parses hundreds of Redis cmds across all data structures:
  - Strings, Bitmaps, Bitfields, Hashes, Lists, Sets, Sorted Sets, Streams.
  - RedisJSON, Bloom/Cuckoo Filters, T-Digest, TimeSeries, Geo.
  - Connection management, Pub/Sub, Multi/Exec transactions, Admin & Cluster control.
- **Zero-Copy & Low Allocation**: Parses raw byte slices (`&[u8]`) and byte vectors efficiently.
- **Metadata Extraction**:
  - `is_write()` / `is_readonly()` / `is_admin()` classification.
  - `extract_keys()` for Raft multi-key transactions and slot hash routing.
- **Modern Rust 2024**: Designed with strict safety, no panics, and optimized match state transitions.

---

## 📦 Installation

Add `webc_cmd` to your `Cargo.toml`:

```bash
cargo add webc_cmd
```

---

## 🛠️ Usage Example

```rust
use webc_cmd::{parse_cmd, Cmd};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_args = vec![
        b"HSET".to_vec(),
        b"user:100".to_vec(),
        b"name".to_vec(),
        b"Alice".to_vec(),
    ];

    let cmd = parse_cmd(raw_args)?;
    match cmd {
        Cmd::HSet(key, pairs) => {
            println!("Key: {key}, Fields count: {}", pairs.len());
        }
        _ => println!("Parsed other cmd: {cmd:?}"),
    }

    Ok(())
}
```

---

## 📜 License

Licensed under either of Apache License, Version 2.0 or MIT License at your option.

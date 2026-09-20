# wconn : RESP Client Connection Library

## Positioning

wconn is the standalone RESP client-side library of the workspace, mirroring the independent client assembly `garnet/libs/client/Garnet.client.csproj`. Like its C# counterpart it is not an internal chunk of the server: the server itself consumes it on the runtime path — `Garnet.server.csproj` references the client project, and replication / migration reuse `GarnetClientSession` on the primary and replica sides. In this workspace `wedb` consumes `wconn` the same way: `src/client.rs` drives `GarnetClient`, `src/server/replication/replica_wire.rs` drives `GarnetClientSession` plus the wire frame encoders, and `src/error.rs` forwards `wconn::Error`. The crate stays a separate workspace member with an independent version, exactly as `libs/client` is a separate assembly.

## Module Layout

- `client`: `GarnetClient`, the end-user async client facade (mirrors `GarnetClient.cs`)
- `session`: `GarnetClientSession` and the replication wire frame encoders `encode_append_log_frame` / `encode_append_log_init_frame` / `encode_advance_time_frame` (mirrors `ClientSession/`)
- `network`: TCP plumbing, the network pump and reply dispatch (mirrors `ClientTcpNetworkSender.cs` / `GarnetClientTcpNetworkHandler.cs` / `NetworkWriter.cs`)
- `parser`: `RespReadResponseUtils`, RESP reply reading (mirrors `RespReadResponseUtils.cs`)
- `api`: typed helpers `InfoMetricsType` / `SortedSetPairCollection` (mirrors `GarnetClientAPI/`)
- `types`: channel items and roundtrip slots shared by the pump
- `error`: `Error` / `Result`

## Core API

- `GarnetClient`: connect / auth / string, hash, set, sorted set, list, key, server commands; fire-and-forget and awaitable roundtrips
- `GarnetClientSession`: raw session with command piping and batched append-log frame submission for the replication path
- `RespReadResponseUtils`: slice-based RESP reply readers

## Design Notes

- Built on compio (thread-per-core); the client pump is a spawned task fed by crossfire channels, mirroring the C# TCS-based completion pipeline with no blocking waits
- The session frame encoders exist because C# `GarnetClientSession` is shared by the server replication stack; keep them here rather than inlining protocol knowledge into the server crate

## Test Coverage

`tests/main.rs` hosts integration fixtures over a live loopback server; replication end-to-end coverage continues in `wedb/tests/replication_stream_e2e.rs` and `wedb/tests/replication_assembly_e2e.rs`.

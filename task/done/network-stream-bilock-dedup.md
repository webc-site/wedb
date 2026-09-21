# 网络流 BiLock 重复抽象 deduplication

## 背景与问题

服务端 wnode（net/stream.rs）与客户端 wconn（network/stream.rs、pump.rs）目前均手写了极高度相似的 TLS 包装层（基于 futures_util 的 BiLock 拆分、`poll_read`/`poll_write` 代理、追加读目标内存初始化等）。这违反了 DRY 原则，增加了后续维护和漏洞修复的成本。

## 改进方案

由于两者都需要基于 `compio` 和 `futures_util` 拆分并进行读写包装：
1. 我们将在 `wedb` 体系内提取一个公用的流抽象或独立模块（如 `wconn/src/network/tls_stream.rs` 或直接放入 `wbase` 中引入新的 feature 依赖）。考虑两者都需要，更合适放在 `wconn` 作为底座给 `wnode` 复用，或抽取到单独的 `wstream` / 放入 `wbase` 的 `tls` 或 `future` 模块。考虑到依赖隔离，最好是创建一个新模块 `wconn::network::tls_ext` 供本包与 `wnode` 使用，或者引入到一个共同的轻量级 crate 中。
2. 将 `tls_append_read`, `tls_write_flush`, `tls_shutdown` 等函数去重，使用泛型 `B: IoBufMut` 等接口使其能够包容服务端的泛型和客户端的 `Vec<u8>` 需求。
3. 替换掉 `wconn/network/stream.rs` 和 `wnode/net/stream.rs` 内部的重复实现，均指向抽离后的模块。

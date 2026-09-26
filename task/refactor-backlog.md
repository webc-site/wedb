# 重构 backlog

## 第 1 轮（refactor-r1）遗留候选

### 死代码 / 可见性降级（需先定 C# 兼容面取舍）
- wresp/src/read.rs：`try_read_u64`/`try_read_string`/`try_read_integer_as_span` 仅同文件内部消费，零跨 crate 调用；建议整族降 `pub(crate)` 或私有（保持协议解析面家族对称性再定）。
- wresp/src/cmd_strings.rs：`write_map_len_resp2` 仅 `write_map_len` 分派体与本文件测试消费，可降私有。
- wresp/src/resp_memory_writer.rs：`write_prefixed_len` 仅 `write_array_len` 消费，可降私有。
- whasher/src/lib.rs：`splitmix64` 仅同文件 `mix_thread_id` 消费；降私有需同步把模块头文档 `[`splitmix64`]` 链接改纯文本，避免 rustdoc private_intra_doc_links。
- wrecord/src/header.rs：`is_closed_word` 仅同文件消费，可降私有。
- wbase/src/varint.rs：`VARINT_2B_MAX`…`VARINT_4B_PAYLOAD_MASK` 等约 12 个 pub 常量全部只在文件内使用；`MAX_VARINT_LEN` 进入公开签名（`encode_u64_to_array` 返回数组维数）可保留 pub，其余可整批降私有，收益是 API 面收窄。
- wbase/src/pool/mod.rs：`PoolStats`/`stats()` 仅 wbase 自己的 tests/suite/pool_budget.rs 消费；若认定外部可观测面无需求，可整体删除该统计口。

### src 内联测试模块搬迁 tests/（需先提权或改公 API）
- wnode/src/resp/config_commands.rs：约 530 行 `mod tests`，依赖 `use super::*` 与 `crate::cluster_provider::ClusterProvider`（crate 私有），搬迁需先设计公开 host 口或保留为单元测试。
- wcol/src/itembroker/collection_item_broker.rs：约 1170 行 `mod tests`，重度依赖私有项，短期保留。
- whyperlog/src/lib.rs：约 450 行 `mod tests` 直接访问私有字段 `mcnt` 与私有方法 `init_sparse`；若要搬迁需把 DEBUG 导出口（dump_* 一族）统一 pub 化，对照 C# 导出面决策。
- wedb/src/server/cluster_config/mod.rs：测试仅 `use super::*`，未见明显私有耦合，下一轮优先试迁（同 wconf node_options 手法）。

### 编译期 / 写法
- wresp/src/key_spec.rs：`from_wire_name_single` 对 ALL_FLAGS（11 项）线性扫 + 逐字节滤下划线；项少收益有限，若扩表可换 phf 或编译期构建忽略大小写表。
- wnode/src/resp/objects/tiered_collection_ops/list.rs:152-175：`slot(j)` 闭包 + 两段 `for j in 0..args.len()`；左推逆序消费导致不能用单一迭代器免 clone 表达（iter rev/fwd 类型不同），现形态近最优，若改写可用 `args.iter().enumerate()` 双分支展开但会复制两臂体，暂不动，留待有第三处消费同机制时抽泛型 helper。
- 对照 ./garnet C# 测试面（test/standalone/Garnet.test/*）逐 crate 补关键断言：本轮仅做了恒真/无断言粗筛（全仓 `#[test]` 无断言者仅 wnode double_turnstile_barrier::single_participant_never_blocks，属活性性质测试，保留）。

### 工具链环境
- 仓根 unused.sh 依赖 `rust-analyzer scip`，在本机 mise shim 下报 "infinite recursion detected" 全目录计红；需固定可用 rust-analyzer 二进制（rustup component add rust-analyzer 或 mise 直链真实 bin）后重跑，补人工 pub 孤儿扫描的盲区（trait 方法、宏路径调用检不出来）。

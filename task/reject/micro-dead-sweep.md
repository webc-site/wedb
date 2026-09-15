# micro-dead-sweep 拒绝项

## 一 wcpr IndexMeta/HlogMeta bitcode derive 删除（glm 条 68 原方向）

原任务：删 wcpr/src/meta.rs 的 IndexMeta/HlogMeta bitcode derive，理由「持久化走手写定长」。

驳回：前提倒置。CheckpointMeta 内嵌 IndexMeta/HlogMeta 字段且自身 derive Encode/Decode，
其 encode/decode 是检查点元数据文件（checkpoint_*.meta）的生产持久化路径
（manager.rs:556/619、index_ckpt.rs:544/737、tests/cpr/meta_tamper.rs），derive 传递必需，删则编译失败。

修正执行：真正的死挂是手写定长编解码——IndexMeta/HlogMeta 的
to_bytes/from_bytes/decode_opt + META_SIZE 仅被 meta.rs 内两个 roundtrip 测试消费，
生产零调用，与 bitcode 构成双格式，违反 SKILL「数据格式别搞多格式」。
已删手写编解码与两测试，bitcode 收敛为唯一格式；wcpr bitcode 依赖保留。

## 二 AofAddress bitcode derive 删除（glm 条 56 原方向）

原任务：删 waof/src/address.rs:18 bitcode derive，理由「仅 :402 测试用」。

驳回：判定漏计传递消费面。wedb/src/server/replication/replication_history.rs:28 与
checkpoint_entry.rs:18 的复制历史 / 检查点条目结构体内嵌 AofAddress 字段并 derive
bitcode Encode/Decode（落盘持久化），删 waof 侧 derive 后 wedb 编译失败
（E0277 AofAddress: bitcode::Encode/Decode not satisfied，clippy 实测暴露）。

修正执行：恢复 derive 与 waof bitcode 依赖（cargo add）；serialize/deserialize/span_len
删除维持（assembly.rs aof_span 直写 8B LE 消灭 serialize 最后生产消费点）；
模块文档登记编码面契约。

## 三 aof/mod.rs 通配转发列名化（glm 条 41 原方向）

原任务：pub use waof_sublog::* 改逐项列名导出。

驳回列名方案：核实全仓消费点（wnode/service.rs、wnode/aof/sublog.rs、
wedb_standalone/tests/storage_api.rs、aof_domain.rs）全部走 aof::waof_sublog:: 直模块
路径，再导出面零消费，列名导出是给零消费者发通行证。整行删除（rust_review：禁止二次导出）。

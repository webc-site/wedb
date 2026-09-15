# micro-dead-sweep 微清理批

来源：next/glm.md 条 31、40、41、56、61、68 与条 15 残留核查（主代理已预清理原文，本批为核实后执行）。
判定口径沿用 task/done/zero-ref-pubs-cleanup.md：全仓 src+tests grep 出现次数 = 定义处即零引用；删 rust 符号若曾是 C# 函数对应实现，在 js/check/ignore 登记；测试孤儿删并适配测试。

基线：bun ./js/check.js 零输出零缺失；分支 dev @ 842fcd1。
拒绝项与方向修正见 task/reject/micro-dead-sweep.md。

## 清理清单（逐项处置与理由）

一 whlog iterate_version_chain 删
- wedb/whlog/src/hlog/io.rs:509 与 whlog/tests/hlog/inplace_lifecycle.rs test_iterate_version_chain（测试孤儿，随删）
- 对标 AllocatorScan.cs:IterateHashChain：C# 唯一上游 IterateKeyVersions 仅 Tsavorite 测试（ReadAddressTests）经 LogAccessor 调用，服务端零生产调用；rust 消费面为零，集成测试不能挂 cfg(test)，整删
- ignore 登记 storage.yml：AllocatorScan.cs 追加 IterateHashChain/IterateKeyVersions，连带 IterateKeyVersions 五文件全族（ObjectAllocatorImpl / SpanByteAllocatorImpl / TsavoriteLogAllocatorImpl / LogAccessor，删除该 rust 符号后 check.js 实际暴露的映射面）

二 wedb_standalone 空壳 lib 删
- wedb/wedb_standalone/src/lib.rs 仅 1 行 crate 注释，全仓无 wedb_standalone:: 引用（main.rs 直用 wnode/wlua/wconf，tests 直用依赖 crate）
- Cargo.toml 无 [lib] 段（自动探测目标），删文件即可，bin src/main.rs 不受影响；对标 garnet main 纯 bin 工程

三 wnode/src/aof/mod.rs:23 pub use waof_sublog::* 删
- 全仓消费点（wnode/service.rs、wnode/aof/sublog.rs、wedb_standalone/tests/storage_api.rs、aof_domain.rs）全部走 aof::waof_sublog:: 直模块路径，再导出面零消费，整行删除而非列名（rust_review：禁止二次导出）

四 AofAddress 编码面收敛（wedb/waof/src/address.rs）
- serialize（:108）/deserialize（:119）/span_len 删：仅 address.rs 测试调用；assembly.rs aof_span 原为「serialize 后去长度头」绕路，改直写 8B LE（address.to_le_bytes().to_vec()）消灭最后生产消费点；生产编码面为 from_span（cluster_session.rs 复制参数）+ from_string/to_aof_string（RESP 文本面）
- bitcode derive：初删后 wedb 编译失败暴露传递消费——replication_history.rs:28 / checkpoint_entry.rs:18 复制历史与检查点条目结构体内嵌 AofAddress 持久化（判死有误，方向驳回见 reject 文档），恢复 derive 与 waof bitcode 依赖
- 测试适配：serialize_roundtrip 改 span_roundtrip（手拼 8B LE + from_span 往返，保留 C# FromSpan 测试意图）；test_bitcode_roundtrip 删
- ignore 登记 server.yml 新增 AofAddress.cs：Serialize/Deserialize（带长度字节双形态不落地，FAILOVER 字节参数由 from_span 承接）
- PartialEq/Eq 及其余算子面保留（测试与生产在用）

五 wrecord/src/chunk.rs 死模块删
- ChunkCodec/ChunkIter/CHUNK_LEN_PREFIX_SIZE 全仓仅 chunk.rs 与 lib.rs 导出两处出现，零消费
- 与 wdev/src/chunk.rs（设备分块，活）同名不同物，删前已核对路径
- 模块与符号无 C# 对标 doc 注释（check.js 不追踪），无 ignore 负担

六 bitcode 死挂清理（方向修正后执行）
- whlog/src/flush.rs PageFlushRange：derive 仅被 flush_and_shift.rs 测试 15 使用，生产零 bitcode 消费；删 derive + use + 测试 15，cargo remove bitcode（whlog）
- wcpr/src/meta.rs：原定删 IndexMeta/HlogMeta derive 方向驳回（CheckpointMeta bitcode 持久化传递必需）；实际死挂为手写定长编解码——to_bytes/from_bytes/decode_opt + META_SIZE 仅测试消费，删之，bitcode 收敛为检查点元数据唯一格式
- wcpr bitcode 依赖保留（CheckpointMeta::encode/decode 生产持久化在用）

七 glm 条 15 残留核查
- network_quit / network_readonly：全仓零残留（主代理 wnode 清理已删），复核通过
- aof_replay_coordinator process_synchronized_operation：全仓零残留，复核通过
- wlua limited_allocator.rs / runner.rs / functions.rs 全部 pub 符号逐一清点：零全零引用；limited_allocator 十三符号在 managed_allocator 分配路径在用；functions.rs C 回调在注册表 / vtable 在用；runner.rs run_for_runner（C# LuaRunner.cs:1193 RunForRunner 公开 API）、with_options（C# LuaRunner(options) 构造重载对标）、RespObject（run_for_runner 返回类型）判活保留
- runner.rs DEFAULT_REDIS_VERSION：零生产引用，唯一消费 wlua/tests/rawset.rs；C# 为构造器内联默认值 "0.0.0.0" 非命名常量，删导出、测试内定义同名局部常量
- wmetric add_total_write_commands_processed：判活（resp_server_session.rs:957 生产调用），保留

## 执行结果

- 9 提交（wrecord / whlog / waof / wnode / wedb_standalone / wlua / wcpr / ignore 登记 / assembly 适配 + 文档修复），合并 dev（60e9830，无冲突）后合入主目录 dev（4773c33）
- 连带清理：assembly.rs AofAddress 导入收缩至测试模块；waof 模块文档重排（行首加号被 markdown 解析为列表标记触发 doc_lazy_continuation）
- 遇 E0460 wcustom/wext_roaring 共享缓存元数据冲突一次，重跑即消，非代码问题

## 验证结果

- bun ./js/check.js：退出 0 零输出（分支基线、分支 merge dev 后、主目录合并态三处复验）
- ./clippy.sh：3 任务全过，-D warnings 零警告（分支与主目录合并态各一轮）
- ./test.sh：wedb 2004 passed + 1 skipped，regress 2 passed（分支与合并态复验）

状态
- 完成

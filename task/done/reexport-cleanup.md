# reexport-cleanup 跨 crate 二次导出与 varint 转发面收敛

来源
- next/glm.md 原 条目 16：跨 crate 二次导出漏网 2 处
- next/design.md 原 条目 7：MIN_CHUNK_SIZE 二次导出 + ns_codec varint 转发面

引用面核实结论（全仓 grep src+tests）
- wbftree::MIN_CHUNK_SIZE 转手点：wnode/src/resp/rangeindex/range_index_chunked_serializer.rs:34 pub use
  - wnode 内部经转手导入：range_index_manager_replication.rs、range_index_migration_reader.rs
  - wnode 集成测试经转手导入：tests/range_index_chunked_stream.rs、tests/range_index_replication.rs
- wresp::SortedSetAggregateType as ZSetAggregate 转手点：wnode/src/storage/session/objectstore/sorted_set_ops.rs:13 pub use
  - wnode 内部：session_parse_state_extensions.rs、resp/objects/sorted_set_commands.rs
  - 跨 crate 下游：wedb_standalone/tests/storage_api.rs（wedb_standalone 已有 wresp 依赖）
- wval/src/ns_codec.rs NamespaceDbCodec varint 转发面
  - varint_len：外部零引用，wval 内部 session_prefix_len 经 Self 调用
  - encode_varint_to_array：外部零引用，wval 内部 encode_session_prefix_to_array 经 Self 调用
  - encode_varint：外部与 wval 内部均零引用（仅 tests/ns_codec.rs）
  - varint_len_fast：仅 tests/ns_codec.rs:525
  - varint_len_from_byte：wval 内部 session_prefix_len_from_slice 两处调用，外部仅测试
  - decode_varint：wval 内部多处调用（SessionPrefixBuf、decode_tagged_key、decode_sub_key），外部仅测试
  - varint 真值单点在 wbase/src/varint.rs，wbase/tests/main.rs test_varint_primitives 已全覆盖
    往返、边界、保序、截断、非规范防御

改动清单
1. wnode/src/resp/rangeindex/range_index_chunked_serializer.rs
   - 删 pub use wbftree::MIN_CHUNK_SIZE 及其文档；模块文档去掉「常量导出」表述
2. wnode/src/resp/rangeindex/range_index_manager_replication.rs
   - MIN_CHUNK_SIZE 改从 wbftree 直接 use
3. wnode/src/resp/rangeindex/range_index_migration_reader.rs
   - MIN_CHUNK_SIZE 改从 wbftree 直接 use（与既有 wbftree 导入合并）
4. wnode/tests/range_index_chunked_stream.rs、wnode/tests/range_index_replication.rs
   - MIN_CHUNK_SIZE 改从 wbftree 直接 use
5. wbftree/src/chunk.rs
   - MIN_CHUNK_SIZE 常量文档补 garnet 映射
     （libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:MinChunkSize），
     承接 wnode 转手删除后 check.js 的映射登记，映射落在唯一定义点
6. wnode/src/storage/session/objectstore/sorted_set_ops.rs
   - pub use 降为私有 use（ZSetAggregate 本地别名保留，签名不变）
7. wnode/src/session_parse_state_extensions.rs、wnode/src/resp/objects/sorted_set_commands.rs
   - ZSetAggregate 改从 wresp 直接 use 并本地别名
8. wnode/tests/session_parse_state_extensions.rs、wedb_standalone/tests/storage_api.rs
   - 同上改直连 wresp；ZSetRemoveRange 仍自 sorted_set_ops（本地产物）
9. wval/src/ns_codec.rs
   - 删 varint_len、encode_varint_to_array、encode_varint、varint_len_fast 四个纯转发
   - 内部调用点改直调 wbase::varint::{varint_len, encode_u64_to_array}
   - varint_len_from_byte、decode_varint 去掉 pub（内部辅助/错误面适配）
   - 清理不再使用的 MAX_VARINT_LEN 导入
10. wval/tests/ns_codec.rs
   - 删 test_oppv_varint_roundtrip_and_boundaries、test_oppv_monotonic_order_preserving、
     test_varint_len_lut_and_fast_dispatch（与 wbase test_varint_primitives 及
     wbase/src/varint.rs 编译期断言重复）
   - test_oppv_non_canonical_and_corrupted_defenses 改经公共 API
     （SessionPrefixBuf::from_slice / NamespaceDbCodec::decode_tagged_key）验证
     wval 错误映射面，不再触私有转发

执行与验证结果
- worktree 内 bun ./js/check.js：退出码 0，无缺失无重复输出，js/check/miss 空
- worktree 内 ./clippy.sh：moon 3 任务（bench/regress/wedb）全部完成，-D warnings 零警告
- worktree 内 ./test.sh：wedb 2036 过 1 跳过，regress 2 过
- cargo check --workspace --all-targets 通过
- rust_review 自审：无 allow、无占位、无私有死面；varint_len_from_byte 与
  decode_varint 私有化后仍被 wval 内部（session_prefix_len_from_slice、
  SessionPrefixBuf、decode_tagged_key、decode_sub_key）消费
- next 清理与本文档在并发代理重建 init 提交（4b74625）时已被收编入库；
  本次合并为 fast-forward（4b74625 -> 297a884），合并后 next 两锚点仍为 0 命中
- js/check/ignore 无需新增登记：删除的 varint 转发均系 rust 侧发明（无 .cs 映射）；
  RangeIndexChunkedSerializer.cs:MinChunkSize 映射随定义点迁至 wbftree/src/chunk.rs

遗留
- 无

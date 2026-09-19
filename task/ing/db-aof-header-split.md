优先级：低
来源：next/agy.db.md 条 10 立项。取证基线：主仓 dev 当下代码。

问题
waof aof/header.rs 735 行汇集 5 种协议头（基础头、分片头、单日志事务头、分片事务
头、分块大值头）+ 头类型枚举，全部手写位级解析打包同文件，单文件承载整个 AOF 线格式。

取证
- wedb/waof/src/aof/header.rs:28 pub enum AofHeaderType、:72 pub struct AofHeader、
  :283 AofShardedHeader、:330 AofSingleLogTransactionHeader、:390
  AofShardedLogTransactionHeader、:451 AofChunkHeader；文件共 735 行；lib.rs 全量
  re-export（wedb/waof/src/lib.rs pub use aof::header::{...}）。
- C# 对标：garnet/libs/server/AOF/AofHeader.cs——C# 单文件集中定义各类头
（ SALongHashSign / entryType 等位打包），但 rust 侧各头带独立 encode/decode/校验
实现，体量数倍于 C# 声明面。

修法建议
按基础头（AofHeader + AofHeaderType）、事务头（SingleLog/ShardedLog/Sharded 三
Transaction/Header 形态）、大值分块头（AofChunkHeader）拆三个子文件，
header/mod.rs 统一 pub use，对外路径零变化；编解码逻辑保持手写位运算不动
（bitcode 化不适用：线格式须对标 C# AofHeader 位布局语义）。纯搬运。

主代理补录（14:13，agy.db 晚波条 10 反证）：C# garnet/libs/server/AOF/AofHeader.cs 为 335 行单文件汇集全部 7 种头型，且存在跨型 IsChunked/SkipHeader/GetChunkedHeaderRef 契约要求同处。若你甄别认定拆分仍成立，请在落地记录里显式回应该反证（为何 rust 侧拆分不违背 1:1 契约）；若认定不成立，按规程转 task/reject 结案。

判词：成立，已落地（dev 9f496a7，FF 合入 656e2c4）。

步骤 0 复核（现刻 HEAD，认领前 1e7fae6）
- wedb/waof/src/aof/header.rs 实测 735 行，与票面一致。
- 五头族与枚举同文件承载，按符号重定位后行号与票面逐条吻合：
  :28 enum AofHeaderType、:72 struct AofHeader、:283 AofShardedHeader、
  :330 AofSingleLogTransactionHeader、:390 AofShardedLogTransactionHeader、
  :451 AofChunkHeader；票面 :283/:330/:390/:451 无误。
- C# 侧对位核实：garnet/libs/server/AOF/AofHeader.cs 335 行集中声明
  AofHeaderType/AofHeader/AofShardedHeader/两类事务头 + AofBasicChunkHeader/
  AofShardedChunkHeader（后两者 rust 侧只是枚举判别值，无独立结构体，不另立文件），
  AofChunkHeader.cs 45 行；rust 侧各头自带 const parse/to_bytes，体量数倍于声明面，
  票面主张成立。

拆分落位（按票面三子文件，未加第四层）
- header/mod.rs 168 行：模块 doc（原 1-2 行逐字）+ 跨头共用 const 写入原语
  write_at（原 9-21 行，全仓仍只一份）+ pub use 三子模块 + 跨头线格式锚点测试。
- header/basic.rs 297 行：AofHeaderType（含 ALL / total_size）+ AofHeader
  （含 flags 位段常量、header_type/set_header_type、is_chunked、unsafe_truncate_log、
  parse/to_bytes、skip_header、sequence_number_of、get_chunked_header_ref）+ Default。
- header/transaction.rs 240 行：AofShardedHeader + AofSingleLogTransactionHeader +
  AofShardedLogTransactionHeader。
- header/chunk.rs 79 行：AofChunkHeader。
- 行数对照：旧 735 行 → 新 784 行（+49），增量全部是四个模块 doc 与 use 头、
  测试骨架，无新逻辑、无新抽象层。

搬运中性取证
- 逐行多重集比对（旧 1e7fae6:wedb/waof/src/aof/header.rs vs 新四文件）：旧文件独有行
  仅 1 行，即原 tests 的 use super::{...} 续行（REPLAY_TASK_ACCESS_VECTOR_BYTES 改由
  wbase 直取）；其余 678 非空行逐字节原样在册。位级偏移/掩码/常量/字段顺序零改动。
- 消费点零改动：全仓 24 个引用文件一律经 waof 根 re-export（use waof::AofHeader 等），
  crate 内走 aof::header 路径，pub use 后两形态均不变；lib.rs re-export 列表未改一字；
  tests/ 无一站点需改口。旧 header.rs 已 git rm，无 shim、无重复定义。
- 单源自查：全仓 enum AofHeaderType 命中 1（basic.rs:16）、各头 struct 命中 1、
  fn write_at 命中 1（mod.rs:26）。
- 门禁锚点中性：11 枚 libs/….cs:Symbol 形态锚点改动前后逐枚相同
  （diff /tmp/anchors_{old,new}.txt 零差异）；bun js/check.js 退出码 0，
  ignore 语料与 js/check/ 零回写零删除（worktree git status 干净）。

验收实测（私有 CARGO_TARGET_DIR=/tmp/ct-aofheader，未跑 ./test.sh 与 ./sh/clippy.sh）
- cargo check --tests -p waof -p whlog -p wkv -p wnode：exit 0。
- cargo check --workspace --all-targets：exit 0。
- cargo nextest run -p waof：78 passed / 0 failed，头族 8 测试全绿并按新模块归位
  （aof::header::basic::tests::test_aof_header_roundtrip_and_flags、
  aof::header::transaction::tests::{test_aof_sharded_header_roundtrip,
  test_aof_transaction_headers_roundtrip}、aof::header::chunk::tests::
  test_aof_chunk_header_roundtrip、aof::header::tests::{test_header_disk_layout_anchors,
  test_skip_header_offsets, test_chunk_header_ref, test_header_parse_truncated_boundaries}）。
  其中 test_header_disk_layout_anchors 按 C# FieldOffset 逐字段断言绝对偏移
  （0/1/2/3/4/12、@16、@16/@18、@24/@26、chunk 0/4/8/12/20），即线格式字节面实测未动。
- rustfmt（wedb/rustfmt.toml：tab_spaces=2、imports_granularity=Crate、
  group_imports=StdExternalCrate）已跑，格式无残留。

遗留知会（非本票射程，未动）
- task/reject/agy.db.md:56 与 js/check/ignore/storage.yml:1905 的散文里仍写旧路径
  aof/header.rs / header::AofChunkHeader；后者是 storage.yml（禁改射程），
  两处均为理由说明文字、不参与 check.js 锚点判定，交各自域 owner 顺正。
- 本票基线 1e7fae6 上 wkv/src/session/consistent_read.rs:146 曾 E0308（双层 Result），
  与本票无关，dev 侧已修（合并后 workspace --all-targets 全绿）。

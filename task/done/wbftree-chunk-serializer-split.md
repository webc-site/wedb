优先级：中（拓扑对标：C# 三个独立件在 rust 挤成单文件三态机）
来源：next/agy.db.md 条 7。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
wbftree/src/chunk.rs 把序列化的写出、读入、跨节点迁移流读取三套互不依赖的状态机塞进一个 673
行文件，而 C# 侧是三个独立文件各承载一类；按 C# 文件名一一对位拆开即可，纯搬移零语义改动。

现状（主仓 HEAD 实测）
1. 三型同档：wbftree/src/chunk.rs:87 pub struct RangeIndexChunkedSerializer（impl :99 起，
   体到 :274）、:275 pub struct RangeIndexChunkedDeserializer（impl :288 起，Drop :561）、
   :570 pub struct RangeIndexMigrationReader<R: Read>（impl :578 起，Drop :669）。文件总 673 行。
2. 注册点：wbftree/src/lib.rs:58 mod chunk;、:65 pub use chunk::{…} 统一对外导出。
3. 该 crate 其余部分已是分域粒度（wbftree/src/{chunk,error,lib,stub,types}.rs +
   manager/ 目录模块 + service/ 目录模块），chunk.rs 是唯一的多态机混聚件。

C# 参考
1. libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs
2. libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs
3. libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs
三者均为独立类型文件，无继承、无共享内部状态（迁移读取器组合使用反序列化器，是消费关系）。

修法
1. 目录化：wbftree/src/chunk.rs → wbftree/src/chunk/{mod.rs, serializer.rs, deserializer.rs,
   migration_reader.rs}，mod.rs 只做子模块声明与对外 re-export，保持 lib.rs 现有
   pub use chunk::{…} 成员集合逐字不变。
2. 搬移前先核三型是否共用文件内私有常量或辅助 fn（分块上限、magic、变长整数编码 helper）：
   若有，按归属下沉到实际使用它的那个子模块，或上提到 chunk/mod.rs 作 pub(crate) 单点，
   禁复制成两份（本票的立论就是「一处定义」）。
3. 每文件的模块头文档注释随迁，保持 /// 在 garnet 中的相对路径:函数名 锚点原样
   （check.js 靠 File.cs:Fn 注释登记映射，锚点不得改口径也不得因拆分而丢挂）。
4. 不借机改写任何逻辑：本票 diff 应只呈现移动与 use 调整。

边界
与 next/tiered-collection-ops-file-split.md（wcol/wbftree 集合操作面拆文件）不同域：那条管
service/ 侧的集合命令操作，本票只管三个分块/迁移状态机件。

验收判据
1. wbftree/src/chunk/ 下三文件各自承载一个 pub 类型，符号锚点为
   RangeIndexChunkedSerializer::、RangeIndexChunkedDeserializer::、RangeIndexMigrationReader::，
   三者定义处各一处（grep 类型定义计数 = 1/1/1）。
2. wbftree/src/lib.rs 的 pub use chunk::{…} 成员集合与拆前逐字相同，wkv/wnode 消费方零改动
   （grep wkv/src wnode/src 内 use wbftree::…Chunked… 的行不变）。
3. 单文件行数 ≤300；无新增 pub(crate) 泄漏（除 mod.rs 汇聚点必要项）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh，由主代理合并后统一跑）。

双花登记
并发代理就条 7 另立同题薄票 next/db-bftree-chunk-split.md，并声明已并入 next/muse.db.md 条 10
（manager/service 边界纪律）。该增量落在 wbftree/src/manager 侧，与本票的 chunk.rs 拆件不同文件，
拆件棒不吞并；本票为 wbftree/src/chunk.rs 的正文载体，派发时以本票为准并删除该薄票，禁双花。
分拣补记（muse.db 条 10 增量，2026-09-19）：拆分时 manager/service 边界纪律一并落位（managers 与 serializers 分件归属）。

---

## 步骤 0 甄别判词（成立 → 落地；取证基线 = 认领时 dev 尖 893ede9，行号按符号现刻重取，不沿票面旧行号）

1. 三型同档：**成立**。`git show 893ede9:wedb/wbftree/src/chunk.rs` 实测总 673 行；
   `pub struct RangeIndexChunkedSerializer` :87（impl :99，体收 :255）、
   `pub struct RangeIndexChunkedDeserializer` :275（impl :288，`impl Drop` :561）、
   `pub struct RangeIndexMigrationReader<R: Read>` :570（impl :578，`impl Drop` :669）。
   票面 :274/:255 一处口径差（serializer impl 块收 :255，:257 起已是反序列化器状态枚举的
   文档行），不影响立论。
2. 注册点：**成立**。`wbftree/src/lib.rs` :58 `mod chunk;`、:65-68
   `pub use chunk::{DEFAULT_FILE_READ_BUFFER_SIZE, MIN_CHUNK_SIZE, RangeIndexChunkedDeserializer,
   RangeIndexChunkedSerializer, RangeIndexMigrationReader};`——拆后逐字未动。
3. 该 crate 其余部分已分域：**成立**。src 侧
   `{chunk,error,lib,stub,types}.rs` + `manager/{mod,checkpoint,flush,lifecycle,replication}.rs`
   + `service/{mod,bulk,lifecycle,ops,snapshot}.rs`，chunk.rs 是唯一多态机混聚件。
4. C# 三独立件：**成立**（`wc -l` 实测）——
   `libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs` 183 行、
   `RangeIndexChunkedDeserializer.cs` 314 行、`RangeIndexMigrationReader.cs` 189 行；
   三者无继承、无共享内部状态，`RangeIndexMigrationReader` 以字段 `serializer` 组合消费序列化器
   （rust 同形态），迁移读取器与反序列化器之间零引用。
5. 双花登记核销：票面所指薄票 `next/db-bftree-chunk-split.md` 现刻 next/ 与 task/ 全域
   grep 零命中（分拣时已按本票为正文载体删除），无第二花可删。

## 落地注记（分支 wbftree-chunk，代码提交 ea8660f，主仓 dev 纯 FF 落 c385112）

- 拆件：`wbftree/src/chunk.rs`（git rm，673 行）→ `wbftree/src/chunk/{mod.rs 43, serializer.rs 204,
  deserializer.rs 328, migration_reader.rs 120}`。验收 1 达标：三型 `pub struct` 定义处计数 1/1/1
  （serializer.rs:36 / deserializer.rs:38 / migration_reader.rs:17）。
- 常量归属（修法 2，无一份复制）：`KEY_LEN_BYTES`/`FILE_LEN_BYTES`/`CHECKSUM_BYTES`/`STUB_LEN_BYTES`
  写出与读入两侧共用 → 上提 `chunk/mod.rs` 单点；`MIN_CHUNK_SIZE` 按 C# 对位
  （`RangeIndexChunkedSerializer.cs:MinChunkSize` 在序列化器件内）落 serializer.rs，
  `DEFAULT_FILE_READ_BUFFER_SIZE` 按 `RangeIndexMigrationReader.cs:DefaultFileReadBufferSize`
  落 migration_reader.rs，`MAX_KEY_LEN_BYTES`/`MAX_FILE_LEN_BYTES` 唯一使用者是
  `process_chunk` → 落 deserializer.rs；`RANGE_INDEX_STUB_SIZE` 仍由 stub.rs 单点持有，
  跨件引用一律经 `crate::stub`。
- 文档随迁（修法 3）：模块头 1-26 行（流格式 + 分块规则三处单块原子性）留 mod.rs（其
  `[`MIN_CHUNK_SIZE`]`、`[`RangeIndexChunkedDeserializer::take_error`]`、
  `[`RangeIndexMigrationReader`]` 链经 mod.rs 的 pub use 汇聚点仍可解）；「与源的刻意差异」段
  （27-31，含 `[`StreamHasher`]` 链）随迁 serializer.rs——mod.rs 无 StreamHasher 真实使用者，
  doc-only `use whasher::StreamHasher;` 经 /tmp/anchorprobe 探针实测触发 `unused_imports`
  （rustc 1.100-nightly 不认 doc 链为使用），故按归属下沉到实际持有该字段与导入的序列化器件，
  零新增告警、零链接退化。
- 纯搬家在册（脚本 /tmp/split_chunk.py 按行区间切片，非空行逐枚核对）：拆前 598 非空行 →
  拆后四件 616 非空行；差集只有一枚旧 use 行 `io::{Read, Write},`（按件切为 `io::Write` 与
  `io::Read`），新增 19 枚行实例全为 use / mod 声明 / pub use 汇聚；serializer.rs 的
  `use std::io::Write;` 经编译核实为不需要（`StreamHasher::write` 是固有方法），已删，
  故 wbftree 编译零告警。
- 锚点中性（验收 2）：本件 `File.cs:Fn` 锚 23 → 23，键集合逐枚相同；全库 .rs 锚 4636 → 4636；
  `bun js/check.js` 在同一 dev 基（317fb1e）前后对跑 stdout 与 stderr 逐字节相同、
  ignore 语料回写态逐字节相同（判中性）。lib.rs 一字未改，wkv/wnode 消费方
  （wnode/src/rangeindex/{range_index_manager_migration,range_index_manager_replication,
  range_index_migration_receive_state}.rs 与 wbftree/tests、wnode/tests）零改动。
- 无泄漏壳（验收 3 后半）：无 `pub mod`、无 shim、无新增 `pub(crate)`（chunk/ 全域 grep 零命中），
  旧件 `git rm`。manager/service 边界纪律（分拣补记）：三型 state machine 全部收在 chunk/
  serializers 一户，manager 侧一行未迁；crate 内 manager/lifecycle.rs:517 与
  service/bulk.rs:5 对三型的引用只是注释。
- 偏差如实登记一条：验收 3 的「单文件行数 ≤300」在 deserializer.rs 上不可达——其本体
  （`DeserializerState` 枚 + 结构 + impl + `impl Drop`）纯搬家即 309 行，加头部 use 与两枚
  `MAX_*` 常量共 328 行；再降须拆散单一类型（背离验收 1）或把私有阶段枚举外提（背离 C# 对位件
  RangeIndexChunkedDeserializer.cs 314 行的嵌套形态），二者代价均高于此 28 行；其余三件
  43/204/120 达标。票面该数系 673/3 的均分估算，未按三型实际体量核。
- 门禁实测（私有 `CARGO_TARGET_DIR=/tmp/ct-wbchunk`，未跑主仓 ./test.sh 与 ./sh/clippy.sh，
  合并后终态 c385112 复跑）：`cargo check --workspace --all-targets` exit 0（0 error / 0 warning）；
  `cargo nextest run -p wbftree` 135 passed / 135；`cargo nextest run -p wkv --test main range_index`
  18 passed / 18（71 skipped）；`cargo fmt -p wbftree -- --check` 干净；
  `cargo clippy -p wbftree --all-targets` 零告警。
- 顺带项核销：R3 那条 clippy「this loop could be written as a `for` loop」
  （`while_let_on_iterator`）在 wbftree/src/manager/replication.rs:251 `FlushFiles::next`，
  dev 已由 dead-batch-six（fbd2859）就地改成 `for entry in entries.by_ref()`，本棒经 merge 继承，
  复跑 clippy 零告警，不重复改；链W 前两棒成果（flush_files 枚举单点 1db6e02、
  on_flush 裸面删除 e7d998e）在本件 diff 中一字未回退。

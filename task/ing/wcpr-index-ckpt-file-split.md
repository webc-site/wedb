优先级：低（747 行索引检查点件混三域）
来源：next/agy.db.md 条 13。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD（本票符号已按当下代码
二次重定位，原档引证的 PageAlignedBatch 已被并发重构拆件，见下）。

结论一句话
wcpr/src/index_ckpt.rs 把 64 字节头部编解码与槽位净化、对齐批缓冲 DirectIO 写出/读入状态机、
全量与截断恢复两入口三域塞进 747 行单文件；按域拆件、对外两入口签名不变。

现状（主仓 HEAD 实测，wcpr/src/index_ckpt.rs 共 747 行，2026-09-19 二次核位）
1. 头部格式域：常量 :27-:41（INDEX_MAGIC、HEADER_SIZE=64、BATCH_BUCKETS/BATCH_BYTES 与
   BUCKET_BYTES 混列一栈）、struct IndexCkptHeader impl IndexCkptHeader::encode :57、
   IndexCkptHeader::decode_opt :135。
2. 槽位净化域（与头部语义同属编解码）：:234 fn sanitize_data_slot、:257 fn resolve_read_cache、
   :274 fn sanitize_overflow_slot。
3. 对齐批缓冲域（原 PageAlignedBatch 已被拆为三型，本票按当下形态拆件）：
   struct AlignedBatch :174 及其 new_boxed :179 / Deref :196 / DerefMut :203 / IoBuf::as_init :210 /
   SetLen::set_len :216 / IoBufMut::as_uninit :223；
   struct BatchWriter :279 及 BatchWriter::new :292、write_bucket :304、resolve_slot :344、
   write_zero_bucket :357、flush_batch :371、finish :392；
   struct BatchReader :398 及 BatchReader::new :411、refill :429、read_bucket_into :456。
4. 流程入口域：:504 pub async fn write_index_checkpoint（:520 write_index_checkpoint_inner）、
   :599 pub async fn read_index_checkpoint_truncated；内联测试 :722 起（:728
   index_ckpt_header_codec_roundtrip 等）。
5. wcpr/src 现为平铺（error.rs、index_ckpt.rs、meta.rs、manager/），本件是该 crate 唯一多域混聚件。

C# 参考
1. 票内 cite 的 libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpoint.cs 不存在；
   真实对位是 libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs（249 行），
   同目录 Checkpointing/ 下是状态机件 IndexCheckpointSM.cs / IndexCheckpointSMTask.cs。
2. C# 的对齐内存/缓冲助手不写在 checkpoint 件里（Utilities/Utility.cs 一线），
   即「头部格式 / 对齐批缓冲 / 读写流程」在 C# 本就分家，rust 拆件是对标而非加架构。

修法
1. 目录化 wcpr/src/index_ckpt/{mod.rs, codec.rs, batch.rs, read.rs}：
   mod.rs 声明子模块并保持对外 re-export 成员集合逐字不变（write_index_checkpoint、
   read_index_checkpoint_truncated 及现有 pub 类型）；codec.rs 承接 IndexCkptHeader 的
   encode/decode_opt、:27-:41 头部常量与三净化函数；batch.rs 承接 AlignedBatch / BatchWriter /
   BatchReader 全部方法；read.rs 承接截断恢复读主体。
2. 对齐缓冲与 wbase::pool::AlignedBuf / wdev 对齐 IO 的关系只做搬移与可见性收敛，
   不在本票重写（若发现该批缓冲与 wdev 侧有重复实现，另立单条票，禁在本票顺手合并）。
   同理，本票不得反向合并并发的 PageAlignedBatch→AlignedBatch/BatchWriter/BatchReader 改名结果，
   拆件以当下三型形态为准。
3. 内联测试 :722 起按其形态处置：纯头部编解码单测留内联（随 codec.rs），
   需真实设备与 DirectIO 的写出/读回用例迁 wcpr/tests/（该目录已存在）。
4. 文档注释 C# 锚点随函数迁移，禁改锚点口径（check.js 按 File.cs:Fn 聚合）。

验收判据
1. 三域符号各一处定义：IndexCkptHeader::encode、IndexCkptHeader::decode_opt、sanitize_data_slot、
   sanitize_overflow_slot、BatchWriter::write_bucket、BatchReader::read_bucket_into、
   write_index_checkpoint、read_index_checkpoint_truncated 定义点各 1。
2. wcpr 对外导出集合（lib.rs pub use 面）前后逐字相同，wkv/wcompact 调用点零改动。
3. 单文件 ≤300 行；无新增 pub 泄漏（跨子件项一律 pub(crate)）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh）。

双花登记
并发代理就条 13 另立同题薄票 next/db-index-ckpt-split.md（同改 wcpr/src/index_ckpt.rs，
其分域口径与本票一致，且同样已把符号按当下代码修正为 AlignedBatch / BatchWriter / BatchReader），
两票同改一文件只取一棒：本票为正文载体，派发时以本票为准并删除该薄票，禁双花。

## 落地判词（2026-09-19，dev 纯 FF 至 1846b2a）

裁决：**判成立并落地**。开树时按主仓 dev HEAD（2d5b5cc）逐枚复核票面主张，全中无失实：

- 体量：`wc -l wcpr/src/index_ckpt.rs` = 747，与票面一致。
- 三域行号 13 枚锚点全中：常量 :27/:33/:37/:39/:41、`encode` :57、`decode_opt` :135、
  `sanitize_data_slot` :234、`resolve_read_cache` :257、`sanitize_overflow_slot` :274、
  `AlignedBatch` :174、`BatchWriter` :279、`BatchReader` :398、两入口 :504/:599、内联测试 :722/:728。
- 平铺现状 :24 主张复核为真（error.rs、index_ckpt.rs、lib.rs、meta.rs、manager/）。
- 未被并发拆分：`git log --oneline -- wedb/wcpr/src/index_ckpt.rs` 仅 `31c2388 init` 一枚。
- 无撞题：`git branch --list '*wcpr*' '*ckpt*'` 零命中；17 棵在途树逐一
  `git diff --name-only dev...HEAD | grep -c wcpr` 全部为 0，无异名同域在途树。
- 双花已消：`find . -name '*index-ckpt*'` 全库仅本票一份，next/db-index-ckpt-split.md 薄票
  在 dev 上已不存在，无可删。
- C# 参考复核为真：`Checkpointing/IndexCheckpoint.cs` 确不存在，真实对位
  `Index/Recovery/IndexCheckpoint.cs` 实测 249 行，同目录 `Checkpointing/` 下恰为
  IndexCheckpointSM.cs 与 IndexCheckpointSMTask.cs。

### 行数对照

| 载体 | 行数 | 承接（原 index_ckpt.rs 1-based 闭区间块） |
|---|---|---|
| 旧 wcpr/src/index_ckpt.rs | 747 | — |
| index_ckpt/mod.rs | 136 | :483-517 全量写入口 + :519-588 写主流程 + 子件声明与 read 再导出 |
| index_ckpt/codec.rs | 229 | :26-41 尺寸常量 + :43-52 头结构 + :54-170 encode/decode_opt + :230-243/:245-269/:271-276 三净化 + :722-747 内联单测 |
| index_ckpt/batch.rs | 299 | :172-228 AlignedBatch 及六 trait impl + :278-395 BatchWriter + :397-481 BatchReader |
| index_ckpt/read.rs | 158 | :590-720 截断恢复读入口 |
| 合计 | 822 | +75 = 四枚件 doc + 每件一份 import 头（旧 :1-24 由一件一份变四件各一份） |

单文件最大 299 行 ≤300 ✓（票面判据 3）。

### 搬家中性证据（脚本化，非目测）

搬运由 `/tmp/split_ckpt.py` 完成，13 个正文块一律按「原文件 1-based 闭区间行切片」落笔，
件体文本改动只允许来自两个受控入口：

1. **可见性前缀**：跨子件项按整行精确匹配加 `pub(crate) `，共 16 枚（尺寸常量 5：
   INDEX_VERSION/HEADER_SIZE/HEADER_CRC_OFFSET/BUCKET_BYTES/BATCH_BYTES；净化函数 3；
   BatchWriter/BatchReader 型名 2；被跨件调用的方法 6：`BatchWriter::{new, write_bucket,
   write_zero_bucket, finish}`、`BatchReader::{new, read_bucket_into}`）。脚本对每枚锚行
   断言「命中恰一次」，零命中或多命中即 `sys.exit`，杜绝漂移到别处。
   INDEX_MAGIC / INDEX_MAGIC_U64 / BATCH_BUCKETS 三枚仅件内自用，保持私有；
   `AlignedBatch` 及其 Deref/DerefMut/IoBuf/IoBufMut/SetLen 全 impl 仍 batch.rs 件内私有；
   `IndexCkptHeader` 保持原 pub(crate) 一字未动（票面「不对外导出」口径不变）。
2. **归一化校验**：四件件体（各自首块首行起至件尾）与对应原文切片，在「去空白 +
   去 `pub(crate)` + 去 rustfmt 折行尾逗号」后**逐字符相等**，四件全中 → 除换行折行与
   上述受控前缀外零差异。旧件除 :1-24 import 头外的 661 枚非空行，645 枚逐字节在册、
   16 枚仅多前缀、余下全部为 import 头重组件，无一行改逻辑。

锚点随块走位：`bun js/check.js` 前后 exit 0 且 **stdout 逐字节相同**（cmp 仅差自加 EXIT 行）；
另按 rustScan 同一 `File.cs:Fn` 正则全库取多重集，4263 → 4263，丢失 ∅ 新增 ∅。件内三枚
`.cs` 提及逐枚随块：`Tsavorite.cs:TakeIndexCheckpointAsync` → mod.rs（随写入口 doc）、
`ReadCache.cs:159 SkipReadCacheBucket` 与 `HashBucketEntry.cs:49 Address setter` → codec.rs
（随 resolve_read_cache），口径一字未改。

内联测试处置：:722-747 唯一单测为纯头部编解码往返（无设备、无 DirectIO），按票面留内联
随 codec.rs，其路径由 `index_ckpt::tests::…` 变为 `index_ckpt::codec::tests::…`，是随件走位
的必然结果；本件不含需真实设备的写出/读回用例——该类用例本就落在 `wcpr/tests/cpr/`
（roundtrip / rc_tag / meta_tamper）与 `wkv/tests/checkpoint/index_checkpoint.rs`，故本票零迁移。

### 门禁实测（私有 `CARGO_TARGET_DIR=/tmp/ct-wcpr`，未跑主仓 ./test.sh 与 ./sh/clippy.sh）

- `cargo check --workspace --all-targets`：exit 0，零 error 零 warning；merge 最新 dev 后
  两次复跑同判（带入面仅文档与 wnode 测试）。
- `cargo nextest run -p wcpr`：24/24 通过，exit 0。
- 附加取证 `cargo nextest run -p wkv`：220/220 通过——该包直接调用两入口做真实
  DirectIO 多批写出、CRC 校验、tail 截断读回与断链复核。
- `cargo fmt -p wcpr -- --check`：clean（工作树除本 payload 外零改动）。
- 判据 1：8 枚三域符号定义点各 1（`write_index_checkpoint` 与其 `_inner` 分属两枚，与票面同）。
- 判据 2：`wcpr/src/lib.rs` 零改动，`pub use index_ckpt::{read_index_checkpoint_truncated,
  write_index_checkpoint}` 面逐字未变；wkv/wcompact/manager::create/manager::recover
  调用点零改动（--all-targets 含全部集成测试编译通过为证）。
- 判据 3：无新增 `pub`、无 pub mod 泄漏壳（`mod index_ckpt` 仍私有，子件 `mod batch/codec/read`
  亦私有，跨子件项一律 pub(crate)）；无 shim、无再导出中转件，旧文件 `git rm`。

### 落位

分支 `wcpr-ckpt-split`：拆件代码提交 `a4a7500`，其后至 1846b2a 为逐棒回合 dev 的 merge
（dev 在本棒在树期间高频推进，主仓 FF 遇 index.lock 争用按 sleep 重试至干净窗口），
最终主仓 dev **纯 FF** 合入 = `1846b2a`。


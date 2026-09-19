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

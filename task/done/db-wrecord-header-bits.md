优先级：低
来源：next/agy.db.md 条 21 立项；next/muse.db.md 条 8 同面确认（muse 判「不拆定义、
纪律保持」，其守一无增量约束并入本票）。取证基线：主仓 dev 当下代码。

问题
wrecord header.rs 687 行：RecordHeader 16 字节双字位段编解码（RDH / RecordInfo /
FillerWords / KeyLen / ValLen 掩码组）全部内联单文件，位运算常量、打包函数、视图
访问器层次混聚，文件持续膨胀。

取证
- wedb/wrecord/src/header.rs:67 起位段常量群（RECORD_INFO_RESERVED_MASK :67、
  RECORD_INFO_FLAG_MASK :89、FILLER_WORDS_MASK :97、KEY_LEN_MASK :103、
  VAL_LEN_MASK :109、编译期断言 :122）、:145 pub struct RecordHeader、:664
  pack_rdh_word；RecordRef / RecordMut 经 Deref 复用（record_ref.rs / record_mut.rs，
  无代理层冗余）；文件共 687 行。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs 与
  garnet/libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs——C# 侧
  RecordInfo（元数据位段）与 RecordDataHeader（数据布局位段）本就两文件分层。

修法建议
保持 RecordHeader 唯一定义不分化、视图侧禁加代理（muse 条 8 纪律）；将位段常量群
（掩码、位移、编译期断言、pack_rdh_word 类纯位运算）提取为 header/bits.rs 子模块，
header.rs 留 RecordHeader 结构与字段访问器。后续新增位段一律进 bits.rs 掩码区 +
header.rs 访问器，禁在视图文件散落裸位运算。纯搬运。

主代理补录（14:13，agy.db 晚波条 21 反证）：C# RecordInfo.cs(364)+RecordDataHeader.cs(672) 两文件在 rust 已统一为 RecordHeader 一处定义（header.rs:145 附近），「再拆=倒退一处定义收益」为该拒件论据；但两文件形态又支持拆分。裁决口径：只要拆后全仓仍只有一套位段定义（不复制 RecordHeader），纯文件级职责拆分可接受；若发现拆分会迫使定义复制或双套编解码，判拒结案。

判词：成立，已落地（实现提交 3338344，入 dev 经合并 9273bf4）
步骤 0 甄别（认领时基线，逐条复核票面主张）
- header.rs 恰 687 行；票面行号全命中：RECORD_INFO_RESERVED_MASK :67、
  RECORD_INFO_FLAG_MASK :89、FILLER_WORDS_MASK :97、KEY_LEN_MASK :103、
  VAL_LEN_MASK :109、编译期断言 :117/:122、pub struct RecordHeader :145、
  pack_rdh_word :664；src/ 仅 codec/error/header/lib/record_mut/record_ref 六件，
  无 header/ 目录，本拆分此前未落地。
- 视图侧无复抄实至：record_ref.rs:104、record_mut.rs:406 各一处 impl Deref
  （Target = RecordHeader），两文件 MASK/_SHIFT/裸位运算零命中。
- C# 两文件分层在册：garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/
  RecordInfo.cs（364 行）、garnet/libs/storage/Tsavorite/cs/src/core/Allocator/
  RecordDataHeader.cs（672 行）。

拆分落位（按票面单文件 bits.rs，未加第二层）
- src/header/bits.rs 94 行：位段常量群整群——PAD_KEY_LEN :12、RecordInfo 字
  掩码组 :19-44（RESERVED/五标志位/FLAG_MASK）、RDH 三字段位段 :46-61
  （FillerWords/KeyLen/ValLen 的 SHIFT/BITS/VALUE_MASK/MASK）、MAX_FILLER_BYTES :65、
  编译期封闭性断言 1/2 :69/:74、纯位运算 align_record_size :78 / with_bit :84 /
  pack_rdh_word :90。件头文档登记「位段定义唯此一处 + 新增位段一律进掩码区 +
  header.rs 访问器 + 视图件禁裸位运算」纪律。
- src/header.rs 687 → 607 行：留 HEADER_SIZE :54、RECORD_ALIGNMENT :62、
  布局一致性断言（size_of/align_of）:66-67、RecordHeader :79 与其全部字段访问器
  （impl :89 起）与 Display :591。
- 两文件合计 701 行，净增 14 行，增量全为位段件头文档与 use 头，无新逻辑、
  无新抽象层、无第四层目录。

搬运中性取证
- 逐行多重集比对（旧 header.rs vs 新 header.rs + header/bits.rs 非空行）：
  旧侧仅 10 行不在新侧，其中 4 行是 wbase use 块换行形态、6 行是 KEY_LEN_SHIFT/
  KEY_LEN_MASK/VAL_LEN_SHIFT/VAL_LEN_VALUE_MASK/VAL_LEN_MASK/with_bit 的私有原形；
  新侧仅多出上述六项的 pub(super) 形态 + 三件 use 头 + 件头文档 6 行 +
  pub(crate) mod bits。位段字面量、掩码算式、注释与 C# 锚点原文一字未动，
  on-disk / wire 16 字节双字布局与 RDH 单字原子发布语义零改口。
- 守一取证：全仓 wedb/ 内 MODIFIED_BIT / FILLER_WORDS_MASK / KEY_LEN_MASK /
  VAL_LEN_MASK / pack_rdh_word 的定义命中各 1 处，且全部位于 header/bits.rs；
  RecordHeader 定义仍唯一（header.rs:79）。旧位置已删净，header.rs 无
  pub use bits::.. shim，crate 根导出面（lib.rs:27-35 十项）与拆分前逐项一致。
- 门禁锚点中性：header.rs（旧）与 header.rs+bits.rs（新）的 `路径.cs:符号`
  锚点集合逐枚 diff 为空，共 30 枚；codec/record_mut/lib 三件改动行全为 use 行。

消费点改口清单（旧位置删净、全仓一次性改口）
- crate 内三处：header.rs:13-19（新增 crate::header::bits::{..} 19 项）、
  codec.rs:8-13（KEY_LEN_BITS/align_record_size 改走 header::bits）、
  record_mut.rs:9-16（MAX_FILLER_BYTES/三标志位/align_record_size 同上）。
- crate 外零改口：wrecord 之外全部消费方走 crate 根 pub use（whlog/src/hlog/
  inplace.rs:4 取 MAX_FILLER_BYTES、whlog/src/hlog/{io,mod}.rs 与 whlog/src/scan.rs、
  wkv/src/{read_cache/window,read_cache/cleanse,read_cache/mod,store/flush}.rs 取
  HEADER_SIZE、wrecord/tests/record/header_and_bits.rs:9 取七枚位段常量），
  路径未经 header::，故 lib.rs 一处 pub use 分栏即收全；whlog/src/hlog/mod.rs 的
  SEALED_BIT/PAD_KEY_LEN 命中仅在散文注释里，无需改。

验收实测（私有 CARGO_TARGET_DIR=/tmp/ct-wrecord，未跑主仓 ./test.sh 与 clippy.sh）
- cargo check --tests -p wrecord -p waof -p whlog -p wkv：exit 0。
- cargo check --workspace --tests：exit 0（首轮基线上 wkv/src/session/
  consistent_read.rs:146 的 E0308 系他人 wip 快照所致，与本票无关，二次合并 dev 后全绿）。
- cargo nextest run -p wrecord --no-fail-fast：24 passed / 0 failed，其中
  header_and_bits 族 6 测（test_header_layout_and_field_offsets、
  test_header_const_codec_evaluation、test_record_info_atomic_bits_lifecycle、
  test_filler_words_field_setter、test_tombstone_lifecycle_and_address_preservation、
  test_pad_and_probes）全绿，即 16 字节排布、常量折叠编码与 Pad 魔数实测未动。
- cargo nextest run -p whlog --no-fail-fast：42 passed / 0 failed（位段常量主要消费方）。
- rustfmt（tab_spaces=2、imports_granularity=Crate、group_imports=StdExternalCrate）已跑，无残留。

遗留知会（非本票射程）
- bits 模块声明为 pub(crate) mod bits：AGENTS.md 禁的是 pub 模块，此处按 crate 内
  可见性放宽，否则同 crate 兄弟件（codec/record_mut）与 crate 根无法经 header::bits
  寻径（E0603），crate 根导出面仍是唯一公开入口。
- 本票按票面「header.rs 留 RecordHeader」保留文件形态（header.rs + header/bits.rs），
  与本仓既有目录件（waof/src/aof/header/mod.rs 族、wbftree/src/manager/ 族）风格并存，
  非本票裁决面。

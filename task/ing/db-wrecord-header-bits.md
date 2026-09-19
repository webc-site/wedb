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

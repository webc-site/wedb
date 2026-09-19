优先级：低
来源：next/agy.design.md 条 24 与 next/muse.design.md 条 16（两轮同题合并）。
取证基线：主仓 dev 当下代码。

问题
wval MetaValue 元布局常量半公开：TYPE_OFFSET/NEXT_EXPIRY_OFFSET/U64_LEN 私有，
SIZE_OFFSET/META_VALUE_SIZE 公开；其中 SIZE_OFFSET 全仓无外部消费者，属多余公开面；
半公开不一致使外部无法直读 type 偏移却能直读 size 偏移，布局纪律不闭合。

取证
- wedb/wval/src/meta.rs:34 const TYPE_OFFSET（私有）、:36 pub const SIZE_OFFSET、
  :38 const NEXT_EXPIRY_OFFSET（私有）、:40 const U64_LEN（私有）、:43 pub const META_VALUE_SIZE
- 外部消费实测：META_VALUE_SIZE 有真实跨 crate 消费（wedb/wkv/src/range_index/mod.rs:10、
  :266-:269 与 wedb/wkv/src/range_index/stub.rs:14、:100-:115 的
  [META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE] 信封拼接）；SIZE_OFFSET 外部零消费
  （全仓 grep 仅 meta.rs 内部 :186/:229 使用）
- C# 对标：garnet/libs/server/Objects/Types/GarnetObjectType.cs 相关元数据段，布局
  细节不外露，字段经类型方法读取

修法建议
SIZE_OFFSET 收回私有（无外部消费者，直接收口零影响）；META_VALUE_SIZE 保持 pub（有真实
跨 crate 信封拼接消费，且为 wval 对外布局契约的一部分）；type/expiry 偏移维持私有、经
MetaValue::read_collection_type 等关联方法读取。原则：布局常量默认私有，仅尺寸契约性
常量对外。禁为对称把 TYPE_OFFSET 也公开。

拒绝原因：取证不实——双副本不存在，SegmentChunks 已是唯一切片内核且被两侧消费

来源：next/agy.db.md 条 18（wdev chunk.rs 内存分块与 segmented_device 切片算法双副本）。

原主张：segmented_device.rs 内部另实现一套跨段与扇区对齐的读写切片划分逻辑，
应收敛到 chunk 模块。

取证（主仓 dev 当下代码）：
- 唯一切片内核：wedb/wdev/src/chunk.rs:86 pub(crate) struct SegmentChunks（跨段切片
  迭代器，文件头 doc 自注「消除 write_aligned 与 read_aligned 中的重复循环与算术
  逻辑」），配套共享换算 :65 segment_shift / :74 segment_mask / :15 validate_aligned_io。
- 读写路径全部复用内核：wedb/wdev/src/segmented_device.rs:1022（写）与 :1206（读）
  的跨段循环均 `for chunk in SegmentChunks::new(...)`；:32 导入列表即证。
- 被指认的「第二套」实为单点换算与快速路径判定，非切片算法副本：
  :373 get_segment_and_offset 仅用共享 segment_shift/mask 做单次段号换算（头部定位），
  :643 within_single_segment 是单段零切片快速判定（同用共享 mask），
  :730 容量收缩换算同源。无任何一处重复实现跨段边界划分。

结论：所指双副本不存在，无待办。segmented_device 的拆分问题由
next/db-wdev-segmented-device-split.md 另行承接（与本条不同题）。

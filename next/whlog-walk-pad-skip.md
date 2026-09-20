# whlog 页内走查 next_record 支持跳过 Pad 记录

来源：next/zcode.db.md 问题 1

## 问题

walk.rs 的 next_record 遇到 header.is_pad() 即返回 None 终止本页走查。
而在复活槽位或跨页写入时会打 Pad 标记，scan.rs 正确按 HEADER_SIZE + pad.val_len() 步进跳过 Pad，
walk.rs 提前截断会导致 flush_records_in_range 与 cleanse_page 漏扫 Pad 之后的存活记录。

## 涉及路径

- wedb/whlog/src/walk.rs
- wedb/whlog/src/scan.rs
- wedb/wkv/src/store/flush.rs
- wedb/wkv/src/read_cache/cleanse.rs

## 解决建议

1. 在 walk.rs 的 next_record 中补齐跳过 PadRecord 的逻辑，步进至下一有效记录。
2. 评估将 walk.rs 与 scan.rs 的单页步进内核合并为单一迭代器。
3. 增加包含 Pad 记录的单页完整走查测试。

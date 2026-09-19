拒绝原因：审查确认为现状良好——内核已单点收敛，无可执行待办（纪律性确认条）

来源：next/muse.db.md 条 9（存根治愈内核已收敛禁再开副本）。

原主张：patch_stub_record 与 encode_meta_stub_record 已是 wkv 唯一内核，compact/
flush/cpr_host 全部转调，现状良好，后续存根位变更只改内核。

取证（主仓 dev 当下代码）：
- 治愈内核单点在场：wedb/wkv/src/range_index/stub.rs:475 / :482 patch_stub_record
  消费点（doc 自注「治愈内核」），recreate_patch :718 / transfer_out_patch :732 为
  其补丁形态；编码单点 wedb/wkv/src/range_index/mod.rs 导出 encode_meta_stub_record
  （stub.rs:40 / :209 消费，wkv/src/lib.rs:26 统一 re-export）。
- 调用方全部转调无本地解码副本：wedb/wkv/src/compact.rs:17 导入 + :66 注释
  「治愈一律转调 wkv 唯一内核 crate::range_index::patch_stub_record」+ :84 实调；
  store/flush.rs 与 store/cpr_host.rs 同链（on_flush_pages 快照标记路径）。
- C# 对标：garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs
  PostCopyToTail / OnFlush 与 RangeIndexManager.cs SnapshotTreeForFlush 的存根
  位改写同样单点在 trigger 层。

结论：所述「已收敛」与实况相符，本条是对现状的正确确认而非待办；纪律（调用方
禁本地解码重写）随 next/db-range-index-stub-split.md 的文件拆分票传达（该票已注明
「内核不得复制第二份」），此处不再单独立项。

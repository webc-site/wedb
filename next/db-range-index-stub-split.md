优先级：中
来源：next/agy.db.md 条 6 立项。取证基线：主仓 dev 当下代码，行号为当下实测。

问题
wkv range_index/stub.rs 886 行混五职责：集合升阶、存根持久化编码、就地治愈修补
（recreate_patch/transfer_out_patch）、树锁与排空注销、回放重灌；且 :741-:886 在业务
源码内违规内联 145 行集成测试（cfg(test) mod tests）。

取证
- wedb/wkv/src/range_index/stub.rs:741 #[cfg(test)] :742 mod tests（约 145 行测试
  内联在生产源码文件尾部）。
- 职责地标：:40 / :209 encode_meta_stub_record 消费（存根编码）、:452 注释带
  patch_stub_record 治愈内核说明、:475 transfer_out_patch 治愈、:718 recreate_patch、
  :27 rebind_stub；acquire_tree_write :346（树锁守卫）。
- C# 对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs SnapshotTreeForFlush
  （快照刷盘）与 garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs
  PostCopyToTail（升阶触发）在 C# 侧本就分属 manager 层与 trigger 层两文件；
  治愈修补对应 GarnetRecordTriggers 的就地改写臂。

修法建议
拆子模块：promote.rs（升阶与重灌）、heal.rs（recreate_patch / transfer_out_patch /
patch_stub_record 消费面）、guard.rs（acquire_tree_write / acquire_tree_read 守卫与
排空），mod.rs 留编码与 re-export；:741 起测试迁至 wkv/tests/ 集成测试目录。
注意：patch_stub_record / encode_meta_stub_record 是全仓唯一治愈内核（见
task/reject/db-stub-heal-kernel-split-guard.md 的在场证据），拆文件只搬运消费面，
内核本身不得复制第二份。锁面改动与 next/design-range-index-locks-guard.md 协调
（该票先收敛 locks() 裸入口，本票再搬文件，避免同函数两票各改一次）。

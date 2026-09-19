优先级：低
来源：next/agy.db.md 条 20 与 next/muse.db.md 条 12 两轮同题合并。取证基线：主仓 dev 当下代码。

问题
wbftree 快照刷盘目录四度全量扫描：on_truncate、remove_addr_flush_files（两处）、
recover_all_trees_from_dir 各自 fs::read_dir 整目录枚举 + 文件名匹配解析；同目录
重复 IO 遍历，且对标 C# 的共享单点枚举缺失。

取证
- wedb/wbftree/src/manager/replication.rs:38 pub fn on_truncate（:43 read_dir）、
  :62 remove_addr_flush_files（:63 read_dir + :106 第二处 read_dir snapshot_dir）、
  :147 recover_all_trees_from_dir（:166 read_dir）；:26 parse_flush_file_name
  已是文件名解析单点。
- C# 对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:901
  EnumerateFlushFiles()（:909 Directory.EnumerateFiles 单点迭代器）被 :872
  OnTruncateImpl 与 :986 删除路径两处复用；snapshot 目录枚举 :1001 独立——C# 收敛
  flush 文件枚举为共享迭代器。

修法建议
对标 EnumerateFlushFiles 提供共享 flush 文件迭代器（walk_flush_files(dir) 产出
(path, name, addr)），on_truncate / remove_addr_flush_files / recover_all_trees_from_dir
改走共享迭代器；snapshot_dir 枚举保持独立（对标 C# :1001）。truncate 与恢复均为
低频路径，合并目标是消灭四份重复解析样板而非性能；与
next/db-bftree-on-flush-single-entry.md 同文件相邻认领时宜同分支处理。

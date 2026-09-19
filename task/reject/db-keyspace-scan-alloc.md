拒绝原因：取证不实——所指符号不存在，所指分配放大无证据

来源：next/agy.db.md 条 15（wkv store/keyspace.rs 528 行键空间扫描分配放大）。

原主张：keyspace.rs 做 KEYS / SCAN / 多库遍历匹配时逐条分配 Vec<u8>，应增零拷贝闭包
迭代器 scan_keys_with。

取证（主仓 dev 当下代码）：
- 符号消失：wedb/wkv/src/store/keyspace.rs（528 行）全文无 scan_keys、无
  collect_matched_keys（grep 零命中）。实际函数面：:63 expired_key_deletion_scan、
  :134 flush_database、:194 flush_virtual_database、:245 flush_namespace、
  :292 flush_virtual_namespace、:368 flush_all_databases、:421 keyspace_stats——
  职责是过期扫描与换号清库，不是 KEYS/SCAN 键遍历。
- 无逐条 Vec<u8> 分配：本文件 grep "Vec<u8>" 零命中；过期候选收集走共享内核
  collect_expired + ExpiredKeySet（:73-:86，键需所有权用于后续物理删除，
  分配属必要所有权转移非读放大）。
- KEYS/SCAN 链不在本文件：扫描迭代在 wedb/whlog/src/scan.rs ScanIterator（借页视图，
  无逐键堆分配证据）；C# 对标 ScanMethods.cs 的对应实现在 wnode 命令层。
- 「528 行」数字属实但内容定性错误。

结论：问题所指的代码路径不存在，无待办。若未来键空间扫描确需零拷贝化，
应按 ScanIterator 实际消费链另行取证立项。

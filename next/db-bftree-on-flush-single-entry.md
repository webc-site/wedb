优先级：低
来源：next/agy.db.md 条 19 与 next/muse.db.md 条 11 两轮同题合并。取证基线：主仓 dev 当下代码。

问题
wbftree 刷盘快照入口冗余：on_flush 与 on_flush_address 两公有入口仅差 Option<地址>
参数，且 None 分支走 bare_flush_path（无地址裸文件名），生产唯一消费走带地址版，
无参入口属自造变体（C# 只有一个带地址签名）。

取证
- wedb/wbftree/src/manager/flush.rs:13 pub fn on_flush（None 包装）、:18
  on_flush_address、:27 on_flush_internal（:41-:44 match logical_address 分出
  bare_flush_path / log_flush_path 两形态）。
- 消费面：生产唯一 wedb/wkv/src/store/flush.rs:84 on_flush_address；on_flush 裸入口
  仅 wedb/wbftree/tests/manager_and_stub/manager.rs:44、:234、:265 三处测试调用。
- C# 对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:681
  SnapshotTreeForFlush(ReadOnlySpan<byte> key, Span<byte> valueSpan, long
  logicalAddress)——唯一签名、必带 logicalAddress，无无地址重载；裸文件名形态在 C#
  不存在。

修法建议
删除 on_flush 裸入口与 on_flush_internal 的 None 分支（bare_flush_path 随之清理），
on_flush_address 更名回 on_flush 或保留现名（认领者定，倾向直接保留
on_flush_address 免动 wkv 调用点）；三处测试改调带地址版并按需调整断言文件名。
若 bare_flush_path 尚有其他消费（恢复扫描兼容旧裸文件），查 recover_all_trees_from_dir
的文件名解析后决定是否保留兼容读取——按 transpile「不需要向下兼容」口径，
旧裸文件格式支持一并删除。

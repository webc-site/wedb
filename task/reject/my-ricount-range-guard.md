拒件：RI.COUNT 区间计数防误用仅靠注释约束，主张加断言禁调

来源：next/muse.my.md 条 5。判定：不成立（票面自评单点正确；注释约束即 SKILL 钦定机制）。

拒绝原因
transpile SKILL O(1) 计数规约条款明文设计：「非全区间计数不设第二个计数命令，由 RI.SCAN / RI.RANGE 的 FIELDS KEY 纯键投影（ScanReturnField::Key 真实区间迭代）承担；区间计数严禁误用 MetaValue.size（会返回错值）」——不设第二命令 + 注释约束正是规范钦定的形态，现状即设计终态。票面自评「单点正确…RI.LEN 归一同一枚举正确」，无缺陷无缺口；加 debug_assert 或类型隔离属可选打磨，且区间入口分散在 SCAN/RANGE 多臂，断言面收益低于维护面。range_index_count 只读 size 不触树（wkv/src/range_index/ops.rs）本身是正确实现，非隐患。

引证
transpile SKILL O(1) 计数规约条款；wedb/wkv/src/range_index/ops.rs range_index_count；wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs network_ri_count；C# 区间侧对标 garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs NetworkRiScan/NetworkRiRange。

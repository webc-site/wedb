方案细化

BfTreeService 的 cpr_snapshot 移除对 catch_unwind 的依赖。
原先的 catch_unwind 无法在 panic=abort 时捕获错误。
改进方案是在 BfTreeService 创建时记录 enable_snapshots 标志位，由外层保证正确性。
在 cpr_snapshot 调用前做状态检查，如果未开启则返回 Result::Err，避免触发底层引擎 panic。
在 recover_from_cpr_snapshot 中使用 Result 直接匹配 BfTree::new_from_cpr_snapshot 的结果，去掉多余的 catch_unwind 包装。

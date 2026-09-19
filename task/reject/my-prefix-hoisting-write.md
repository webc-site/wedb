拒件：前缀外提只做到读批量，写面（service 入账 / 向量 registry_key / MSET）逐次拼前缀

来源：next/muse.my.md 条 10。判定：不成立（MSET 主张取证不实；其余为单记录路径无批量面）。

拒绝原因
1 MSET「每键一次 varint 重算」不实：wkv/src/session/mod.rs:905-906 try_upsert_batch_sync 首行 `let prefix = self.session.session_prefix(); let prefix_slice = prefix.as_slice();` 单次外提，循环内经 try_upsert_tag_sync_unprotected_with_prefix 透传——正是 SKILL「循环前缀外提」条款的落地形态。
2 service.rs enqueue_raw 与 vector_manager_locking.rs registry_key 是单记录路径（每个 StoreEvent / 每次向量登记一条记录），不存在循环批量面可外提；SessionPrefixBuf 栈上 19B 零堆（票面自认正确），单次 varint 编码为纳秒级栈开销，无折叠收益面。
3 「批量写面单次取 prefix_slice 透传」的可适用对象只剩 MSET（已做）与树内批量（tree_put_batch 由调用臂在锁内统一构造，同批同前缀），无剩余缺口。

引证
wkv/src/session/mod.rs:905-906/:919-923；wnode/src/service.rs enqueue_raw（单事件入队）；SKILL 性能优化条款「Prefix Hoisting」。

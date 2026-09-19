优先级：高（dev 测试基线红，计数轮前置）

单题：wkv 存储层（dbmeta 记录 / 磁盘 NS_MAP 还原 / ReadCache 脱钩即时失效）在当前 dev 上成片红。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，均确定性失败）
1 wkv::vdb::tests::test_dbmeta_record_roundtrip — wkv/src/vdb.rs:1359
  panic `copy_from_slice: source slice length (8) does not match destination slice length (9)`
  库内 panic，读写两侧长度不一致，信号最强，优先从这里查。
2 wkv::main::store::dbmeta_layout::cold_resolve_persist_survives_rebuild_and_probe —
  wkv/tests/store/dbmeta_layout.rs:54 「重建须还原磁盘 NS_MAP（persist 与 rebuild 同键）」
  left: None / right: Some(1)
3 wkv::main::store::flush_database::test_flush_database_batch_survives_rebuild —
  wkv/tests/store/flush_database.rs:855 「重建须还原磁盘 NS_MAP」None vs Some(1)
4 wkv::main::store::flush_database::test_flush_atomic_batch_survives_rebuild —
  wkv/tests/store/flush_database.rs:625 「库级墓碑须恢复」
5 wkv::main::store::swap_database::test_swap_pair_record_survives_rebuild —
  wkv/tests/store/swap_database.rs:299 「重建后两库指向 1:1 互换重现，绝无中间态」(1,2) vs (2,1)
6/7/8 wkv::main::store::read_cache::test_read_cache_{rmw,upsert,delete}_atomic_detach_and_invalidate —
  wkv/tests/store/read_cache.rs:83 / :139 / :186，三条同为「（脱钩后）旧 ReadCache 记录必须即时失效」，
  rmw 条另要求「携 prev 续链」

判读方向（须自行核实，不当结论）
2/3/4/5 同指库级映射与墓碑在 rebuild 后丢失或错指，与 1 的 dbmeta 记录长度不一致很可能是同一处改动
的前后表现（记录体增减字段而未同步读写两侧）；6/7/8 同指写路径脱钩旧 ReadCache 记录后未即时失效。
用 git log -p 追 wkv/src/vdb.rs、dbmeta/NS_MAP 读写面、read-cache 脱钩面的最近合入，判明是产线回归
还是用例期望未随既定改动更新——若判为用例过期，须给出 C# 对位证据（garnet 侧同名机制锚点），
不得仅凭「rust 现在这样」改断言。

改动域
wedb/wkv/**（含 wkv/tests/**、wkv/src/vdb.rs）。禁止触碰 wnode、waof、wresp、wedb/src/server
（各属另票，避让清单见下）。

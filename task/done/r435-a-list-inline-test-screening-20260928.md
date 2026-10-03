归档注记（2026-09-28 主控）：本预审档两张建议票均已消耗——R435-1（wcol list_object_impl.rs 内联块零扩面外迁）
由 task/done/wcol-lpos-lset-inline-tests-zero-widening-migration.md 落地；R435-2 纯登记票不入 todo（本仓「纯登记票
不可执行」口径），其六块 NOCHANGE 盖章与 3/8/10 出册判定转由台账重建席作回收载体消费，免后续重开。

r435 A 清单内联测试迁移预审（只读席，2026-09-28；行数以现树 dev 2ed9f226 后为准）

逐块判定：
1. whyperlog/src/tests.rs 458 行/18 测；私触面 30+（mcnt、with_pbit、HLL_HEADER_BYTES、
   ALPHA、init_sparse/update_sparse/sparse_to_dense/merge_grow/dump_*/
   compare_sparse_to_dense 全私），文件头已载「拆文件保留单元语义」终案
   → NOCHANGE（扩面成本 ≥30 项提 pub，含 debug-only 面）。
2. wkv/src/read_cache/mod.rs:194-763 实测 571 行/8 测（非旧账 386）；私触面：
   head_address、closed_until_address、page_inflight、turn_lock、buffer 五私字段直写 +
   INFLIGHT_CLOSED 私 const + pump_close_barrier 等 pub(crate)；r329 族 rc_epoch_drain.rs
   已按纯 pub 面迁出，留守块即当年判私块 → NOCHANGE（成本 ≈8 项，提 pub 破封装）。
3. wresp/src/catalog/simplified.rs 无 mod tests（0 测），已在
   wresp/tests/simplified_spec_folding.rs → 已迁收口，出册。
4. wcol/src/list/list_object_impl.rs:571-926 357 行/6 测；私触面仅
   read_list_position_input（私）+ list_position/list_set（pub(crate)），其余全 pub
   （ListObject.list/heap_memory_size、update_size、ListOperation、pub fn operate
   Lset=14/Lpos=17 臂直落、ObjectOutput::mount/result1/payload_view、
   pub read_list_position_params 同帧）→ MIGRATE 零扩面：解析用例走 pub 壳、
   命令用例走 operate 直驱；probe 计数器随迁 tests/ 独立二进制可行。
5. wnode/src/resp/objects/custom_object_commands.rs:548-873 327 行/5 测；rmw_guard 直调
   try_custom_object_rmw_sync、obj_load_custom_sync、CustomObjCtx、
   dispatch_custom_object_read/rmw 等 5+ 私执行臂；custom_object_recheck.rs 头已载
   「私有执行臂直调留守内联」既判案；他席在途勿碰 → NOCHANGE（成本 ≥5 项）。
6. wlua/src/lib.rs:53-237 实测 185 行/5 测（非旧账 322）；私触面：mod sys（私模块）、
   set/clear_callback_context、Error、LuaState::view 均 pub(crate)；lua_state_api.rs 头载
   r329「触私者留守」既判 → NOCHANGE（成本 4 项 + 私模块面）。
7. wbitmap/src/bitfield/execute.rs:458-764 308 行/6 测；私触面：
   check_bitfield_overflow/check_signed/check_unsigned/get_bitfield 四纯私原语 +
   length_from_type(pub(crate))，无生产外部消费方 → NOCHANGE（成本 5 项触红线）。
8. wacl/src/acl_parser.rs 无 cfg(test) 内联块（0 测），已在
   wacl/tests/acl_parser_rules.rs → 已迁收口，出册。
9. wnode/src/servers/consumer_registry.rs:698-844 实测 148 行/7 测（非旧账 268）；
   r329 已迁出 consumer_registry_counters.rs，留守块触 active_handler_count 私字段×2、
   is_terminating/wait_terminate pub(crate) → NOCHANGE（成本 3 项，r329 既判复核仍成立）。
10. wnode/src/node_options.rs 已不存：迁至 wconf/src/node_options.rs（1863 行，无
    cfg(test) 内联），测试已在 wconf/tests/node_options.rs(+max_databases)
    → 收口出册，「勿迁」旧判已被现实取代。

开票建议：
- R435-1（实票）：wcol LPOS/LSET 内联块零扩面外迁
  wcol/src/list/list_object_impl.rs:571-926 → wcol/tests/lpos_lpar_lset_frames_zero_alloc.rs；
  扩面 0；验证 cargo nextest run -p wcol 逐用例进程下 probe 分配基线不漂移
  （operate 仅增 from_repr+is_empty 判定）。
  排程约束：r3 checkjs 单点化票同触该文件注释面（read_list_position_params/
  read_list_position_input 锚），须待 r3 合入后开工。
- R435-2（台账票，纯登记不涉码）：本报告入册即消耗——3/8/10 出册、
  1/2/5/6/7/9 六块 NOCHANGE 盖章，免后续重开。

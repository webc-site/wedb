优先级：高

问题
分层态 SortedSet 范围与排名命令全缺失，穿透全量物化。树内仅按 member 字典序存 member -> 8B score，只承接 ZADD/ZSCORE/ZMSCORE/ZCARD/ZINCRBY/ZEXPIRE/ZTTL/ZPERSIST。ZRANGE/ZRANGEBYSCORE/ZREVRANGE/ZRANK/ZREVRANK/ZCOUNT/ZPOPMIN/ZPOPMAX 等在 tiered_zset_arm 末尾统一 Ok(false) 穿透：读族经 slow_load_eval 的 Degrade 臂全树物化求值（千万级集合一次 O(N) 内存物化）；写族经 run_async_rmw -> apply_rmw_post_operate，tiered 且不降阶时更触发 handle_bftree_drain_and_delete + promote_collection_to_bftree 整树销毁重灌（O(N) 物化 + O(N log N) 重建 + AOF 全量重发）。违背 SKILL「消除全量反序列化读放大」「对外 RESP 命令透明统一」承诺，大键一条 ZRANGE 即巨大延迟与内存抖动。

取证（dev 当下代码重取）
wedb/wnode/src/resp/objects/tiered_collection_ops.rs:597-607 zset_needs_write 仅列 Zadd/Zincrby/Zexpire/Zttl/Zpersist/Zcard；:1710 match 末尾 `_ => Ok(false)` 承接全部范围/排名/弹出操作；:1453 分值落树形态 `&score.to_be_bytes()` 以 member 为键（无分值序索引）。读路径 wedb/wnode/src/resp/objects/rmw_helpers.rs:468-485 ObjLoad::Degrade -> tiered_materialize_blob 一次性物化；写路径 rmw_helpers.rs:368-392 apply_rmw_post_operate 重灌臂。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRangeByScore、SortedSetRange（C# 内存 SortedSet 有序结构，范围 O(log N)，任意规模语义恒定）。

修法建议
在 wbftree 建以 score+member 复合编码的有序双向索引（第二棵树或组合键），实现原生页级范围扫描与排名；升降阶导出（export_entries）与该索引同源。若裁决认为代价过大，须在 doc/zh/collection.md 显式声明分层 zset 范围命令的性能折损并给限流口径，不得维持「透明统一」的失实承诺。落地须与 next/tiered-ttl-tombstone-residual-source.md（TTL 重灌收敛）同定写形，避免两次重建面打架。

收口判词（三棒收尾，2026-09-20，分支 tiered-zset-range）

结论：本票落地。分层态 zset 范围/排名族改树内流式窗口扫描原生执行，验收全绿。

一、对前两棒载荷的复核结论（自跑门禁，不重审设计）
1. 一棒载荷 9967c72 复验通过，设计口径原样承接：ZRANGE / ZREVRANGE / ZRANGEBYLEX /
   ZREVRANGEBYLEX / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZLEXCOUNT / ZRANK / ZREVRANK /
   ZCOUNT 经 arg12 通道入树内臂（zset_scan_select + ZSetWindow 窗口内核，内存只随
   结果窗口增长），消除读族经 slow_load_eval Degrade 臂的 O(N) 全量物化；写族
   （ZPOPMIN / ZPOPMAX / ZREMRANGEBY* / GEOADD / ZRANGESTORE）维持穿透，未越本票射程。
   原票「若裁决认为代价过大须在 doc/zh/collection.md 声明折损」一支亦由载荷落定
   （collection.md +28 行复杂度折损与限流口径），承诺失实问题随之消除。
2. wcol 抽出件（parse_range_options / write_sorted_set_result_payload / RangeArgError /
   ZRangeOptions 转 pub / try_parse_lex_parameter 转关联 pub fn）在合并后的树上确有
   分层侧消费方（tiered_collection_ops.rs 四臂共 8 处调用），单源成立，非虚挂导出。
3. 与载荷的两处必要偏离（均为回合 dev 所迫，非重设计）：
   - 载荷自带的 zrank_with_score 词元判定与 dev 的 parse_rank_with_score 同义并存，
     按「不留双路径兼容」删前者、分层表与内存臂一律改调 dev 单源；连带
     WITHSCORE 词元非法时的应答由载荷保旧的 ASYNC_REQUIRED 变为 dev 已定案的
     SYNTAX_ERROR，分层与内存两侧同函数同字节。
   - dev 已删的分层 ZEXPIRE / ZTTL / ZPERSIST 树内出账臂（成员 TTL 走整值重灌）不回滚，
     tiered_zset_arm 的 args12 由 dev 的弃绑改回绑定（本票范围/排名臂为消费方）。

二、二棒未竟半件清点（现场三处，本棒全部补齐）
1. wcol sorted_set_object_impl.rs 停在工作区未暂存态：dev 版为底 + 载荷抽出件重挂，
   try_parse_lex_parameter 已改关联 pub fn 而两处调用点仍 self.（编译不过）→ 改 Self::，
   与 9967c72 已提交态一致。
2. 树内正跑一次 merge dev（MERGE_HEAD=c0ef376，MERGE_MSG 自述三文件冲突）：二棒把
   slow.rs 与 tiered_collection_ops.rs 的冲突按 dev 侧定案并暂存，等于在本枚合并里
   静默清零载荷的 590 行树内范围臂与 arg12 接线（却保留其 769 行测件）。本棒按
   「dev 侧结构为底 + 载荷新增臂重挂」重做三处冲突（merge-file 三方合并 base=43fd56c、
   ours=9967c72、theirs=dev），共四段：TTL 出账臂取 dev 删除、range_opts_of 换算单点保留、
   WITHSCORE 取 dev 单源、穿透注记合流。
3. 合并目标 c0ef376 落后 dev 尖 58 枚（含 wcol count 双口径改名 purge_expired_len、
   promote_collection_to_bftree 增至四参）：先落该枚合并，再 merge dev 83e00fd（零冲突），
   对拍测件的手工升阶按 earliest_expiry 同源口径补成员到期水位入参。

三、验收实测（本棒门禁，私有 target CARGO_TARGET_DIR=/tmp/ct-tzs3）
- cargo check --workspace --all-targets：exit 0，零错零警告，无 #[allow( 。
- cargo nextest run -p wcol -p wnode --no-fail-fast：1144 跑 / 1144 通过 / 1 跳过，
  含载荷新增对拍 test_tiered_zset_range_rank_parity（10.5s PASS，分层树内臂与内存态
  对象层逐字节对照）、test_tiered_zadd_opts_zrange_geo、test_tiered_scan_full_iteration。
- bun js/check.js：exit 0。重复定义两枚（SortedSetObjectImpl.cs:SortedSetRange 三处、
  WriteSortedSetResult 两处）按 README 第 2 节收口为「锚点单归对象层实现函数、抽出件
  散文式对位」；实现缺失零；B 层 59 处非 libs 族提示为口径外存量，与本票无关，
  未动 js/check/ignore/** 任何语料。
- cargo fmt --check -p wcol -p wnode：干净。
- 未跑 ./test.sh、./sh/clippy.sh（按规程交主代理合并后统一跑）。
- 分支净改动 vs dev：5 files changed, 1611 insertions(+), 127 deletions(-)，
  全部落在本票射程（doc/zh/collection.md、wcol sorted_set_object_impl.rs、wnode
  sorted_set_commands/slow.rs、wnode tiered_collection_ops.rs、wnode tests/tiered_cmds_align.rs）。

四、残余与移交
- 范围族仍未树内化的面：ZRANGESTORE（写族，重灌通道）、ZRANDMEMBER / ZDIFF / ZUNION /
  ZINTER 多键与随机采样面维持物化通道，代价与限流口径已记 doc/zh/collection.md。
- 树内范围臂依赖 bf-tree 纯扫描入口（tree_member_score 走 scan_with_count_callback，规避
  点读与扫描交错触发 mini-page 合并缺陷），后续若动 wbftree 扫描器须连带复核本票四臂。


### 第 8 波（r8）确认轮占轴（2026-09-28，连胜口径：全席零真发现才累计，真发现清零）
- c1: r7 落地面复审（7c79fe4 十提交 95 文件 diff 等价性/漏删/测试弱化）
- c2: wtls 全量+wconn TLS 交互面（含 tls_flush 零消费新立案复核）
- c3: wresp 全量+wpubsub
- c4: wepoch+windex+wtxn+wreviv+wcompact 并发原语族
- c5: whlog+wbftree+whyperlog+wbitmap+wext_roaring+wext_json 存储外围
- c6: wvector 全量+wkv vdb
- c7: wedb 集群/复制/standalone+wnode cluster 双会话
- c8: r7 新迁测试与 G3 补测质量复审+bench/regress 余面

### 第 8 波（r8）确认轮战果（2026-09-28 晚，8 审查席 60 条→7 修复席全落地，8 提交 ff 并 dev @ d21a76b；8/8 非 CLEAN，连胜维持 0）
- **P1 真缺陷**：forward_cluster_provider! 宏漏列 add_new_checkpoint_entry（Arc<dyn> 层恒短路 None，集群检查点登记链旁路；rustc 最小复现）。本席与并发席 dev 侧修复撞题同闭环（取 dev 侧宏行），本席 26 方法全转发钉死测试在册防回归（handle_path_forwards_every_trait_method，摘行即红已反证）。
- D2 死码约 40 条：wresp fast_basic 死子系统（C# 同构零消费实证）+resp_writer+try_get+Vec 形 ACL+word()；wpubsub for_each_channel；wtxn 恒真 is_watched/stored-proc 三件/phase 恒 0/四死方法；windex retain/to_vec/as_slice/first+buckets 恒假 is_empty；wreviv stats 族；wepoch 六口 C# 对位面收 debug 门+Participant::try_suspend 删；wcompact compact_lazy 握手收口；wbitmap/wext_json 双 None 变体+Error::Regex+json_object new/dispose+wext_roaring Default；wvector extract_field/IndexConfig::new/OpCode TryFrom/namespace_bytes/导出孤儿族；wedb set_primary_replication_id/failed()/current_replication_offset 字段/local_node_hostname；whasher/wlua/wprobe 三 doc 残留。
- D1 收敛 6 宗：整数 bulk 四名单点（四 crate 调用点改名）、resolve_iid 委托、parse_value_token 七臂合一、inc_reentrant CAS 单源、set/set_addr 内核收口、TLS 臂 TlsHandles::new 单点。
- G1 迁移 15+ 文件（wresp 十文件/wpubsub 两/wtls client gate/wvector 两/wedb 两）；G3 补测 3 条（slowlog 畸形快照三例/封禁 merge/26 方法转发钉死）。
- 门禁：5195/5195 全绿 + clippy 0（收口 F1 漏网 current_replication_offset 构造点+并发席测试 MutexGuard 跨 await+doc 缩进三条）。
- 教训：①clippy.sh 带 --fix，跑后必查工作区残留单独提交；②删字段票射程必须含全部测试 crate（wnode_tls_test 漏网被 clippy 抓）；③确认轮非 CLEAN 是常态，60 条里仅 1 条 P1，余为重构债——债越清越薄但确认轮仍在产新发现（多为更深层的 C# 对位孤儿面）。

### 第 9 波（r9）确认轮战果（2026-09-28 深夜，d1-d6 六席：15 条→全落地，6 提交 ff 并 dev @ 7926413；CLEAN 1/6（d5 底座六 crate 首个 CLEAN），连胜 0/32）
- d1（r8 落地复审）：等价性七处全过+P1 防回归测试编译器实证有效（Arc<dyn> 漏列→默认体 -1 非 deref 兜底，哨兵反面必炸）；修 3 条（extractor 转义/null/嵌套数组判据测试补挂+两 doc 悬空指针）。
- d2（wnode resp）：2 条 P2=r8-F3 漏网门面薄壳（write_int32/int64_as_bulk_string）删。
- d3（wkv）：record_key_hash 导出删+entry_count 判据收敛 entry_valid 单点。
- d4（wcol+wresp）：10 口降 pub(crate)（expiration_queue 模块/geo 八口/object_payload/hash/zset/itembroker/format_member_ttl/RespCommandsTables）；4 口凭编译实证驳回（read_scan_input/read_list_position_params 有生产消费——审查席票面漏记、check_arg_count! 宏链即外部消费链、drain_events_for_test 夹具连坐）。
- d5（底座六 crate）：**CLEAN**。
- d6（并发原语+向量族）：store_types 纯写累加器+LockType::None 死变体+LogCompactor 死 Clone+LightEpoch 死 Default 删、测试握手口 doc(hidden) 批、RecordSizeReader 收内；3 Default（EpochEntry/HashBucket/OverflowPool）系 new_without_default 规范伴生物保留。
- 门禁：clippy 0；nextest 5197/5199（2 负载 flake 隔离绿：expire_persist_latch/vector_vadd_guard）。
- 结构判断：发现密度显著衰减（r7:78→r8:60→r9:15，且 r9 全为 P3 级小票+判据补挂），债趋薄；下一波若延续衰减有望进 CLEAN 连击段。

### 第 10 波（r10）战果（2026-09-30，6 审查席 13 条→亲修 11+R1/R2/R3 落地；A2 全 CLEAN，5/6 有发现，连胜 0/32）

占轴：A1 wnode/src/resp 命令实现层 / A2 wedb 主 crate+wpubsub / A3 wkv+windex+wbftree+whlog+wreviv+wcompact+wrecord / A4 wlua+wcol+wvector+wext_json+wext_roaring+wcustom+wval / A5 底座族+wnode storage/net/server 底层 / A6 测试质量专项（G1/G2/G3）。

审查席战果（13 条）：A1 五口死写出薄壳删+arity 门双套（收编被证伪）；A3 flush_pages_range 降级+on_flush_pages doc(hidden)+try_seal 死参数三层贯穿去参+walk 双胞互指+AddressSnapshot::new 删；A4 单消费 pub 口降级 4 处；A5 whyperlog 21 口收敛+GarnetClient::info 删+wconn RespReadResponseUtils 7 口降级+TESTONLY 普查挂账；A6 G1 可迁甄别/G2 垃圾 4 条/G3 缺口 2 组。

修复落地：
- D2 删除类：wnode resp session 五口写出薄壳（write_ascii_direct/write_direct/write_zero/write_one/write_resp3_bool）；GarnetClient::info 便捷壳（测试改 execute_for_string_result_async）；AddressSnapshot::new 七参构造（测试改字段直构）；wnode/src/server.rs.orig 合并残留；try_seal(invalidate) 三层死参数去参（C# kValidBitMask 在 rust 无对位，生产恒传 true，四 crate+三测试文件同步）。
- 可见性收敛：flush_pages_range pub(crate)、on_flush_pages doc(hidden)、wlua abort_suspended+wext_json is_static_path+wcol hash_collect+set_keys 降 pub(crate)、whyperlog 13 口 pub(crate)+8 口纯测试调试口收 #[cfg(test)] 门（含三处 import 随门）、wconn parser 7 口 pub(crate)。
- D1 登记：whlog walk 只读/可变双胞 doc 互指防第三胞（宏化按最小动作纪律不取）；custom.rs arity 门内联三行加注互指（收编不可行：dispatch 的 output 是 mem::take 缓冲、返回后回填覆盖 self.output 写入，wrapper 直写会丢错误帧——审查席「语义逐字等价」判据被主控 take/回填链亲验证伪）。
- 测试迁移：wext_roaring/src/roaring_bitmap_commands.rs 内联 6 用例迁 tests/roaring_command_dispatch.rs（派发注册/目录同源/空对象/初更拒参/not-found/reader 路径，try_parse_uint32 私有件留守）；wbftree stub 内联并入 tests/manager_and_stub/stub.rs（roundtrip/probes/const 求值断言全量保留）。
- 垃圾测试：wedb/tests/replication_manager.rs data_loss_check 弱重复删（synctrans 强版含消息断言已覆盖）。
- G3 补测：scatter-gather GET 低内存行为面+roaring 四语义（键隔离/大偏移块界/稠密晋升/删建）对照 garnet 测试落 wnode/tests、wext_roaring/tests（R3）。

留守甄别负结果票（防复扫，均编译实证）：wkv/read_cache/mod.rs（head_address/closed_until_address/page_inflight/turn_lock/buffer 私有状态字直断言）、wresp/command.rs（FIRST/LAST_READ_COMMAND+is_write_only 私有块界锚）、wresp/cmd_strings.rs（write_map_len_resp2 私有单口）、wpubsub/subscribe_broker.rs（for_each_pattern 私有）、wvector/fsm.rs（模块 pub(crate)+私有字段/常量直读）、wkv/vdb/gc_dead.rs（GcDeadLog::new pub(super) 一票否决+ticks 精确边界 secs 口不可复现）——G1 内联测试大面维持 r7「私依赖留守」先例，仅 wext_roaring+wbftree 两件可迁已迁。

挂账（下轮候选）：G2-4 wnode/tests 37 份 session_with 夹具收敛 support 模块；A5票4 TESTONLY 锚面普查（wacl24/wepoch16/wtxn12/wconf10/wcpr10/wbase11/wdev6）逐口 doc(hidden) 甄别；cmd_strings write_map_len_resp2+subscribe_broker for_each_pattern 显式授权 doc(hidden) 后迁；A4 geo 两口（geo_to_long_value/get_geo_hash_code）测试直呼按先例保留。

门禁：待填。

### r10 确认轮 c1/c2 批战果(2026-09-30,连胜 0/32 维持)

c1 批(6 席):c3(wkv 存储族)/c6(wedb+测试面)CLEAN;c1/c2/c4/c5 五条处置(a70365d):garnet 相对路径占位伪影 17 处订正(实错三处 GarnetUser.cs→ACL/User.cs、Resp/AofEntryType→AOF/、server/Resp/Metrics→common/Metrics/InfoMetricsType)、api.rs 头注漂移、resp_command.rs 二分注释漂移、GeoHash::spread 降 pub(crate)、SearchOutput::iter 降级票被编译实证驳回(tests/ 直呼两文件,回滚)。

c2 批(6 席):d6(测试第三批抽样 14 文件)CLEAN;d1/d2/d3/d4/d5 八条处置(3346448,并发席代收 -217 行):check_args 实述半句、StorageSession::delete_all_user_keys 死口删档(整文件仅余注释,连 mod 一起裁)+basic.rs:126 陈旧锚订正、IDatabaseManager::recover_checkpoint_async trait 口+唯一实现删、PubSubSessionCommands::write_null trait 默认+wnode 覆写双死臂删、list_remove 死赋值删(garnet 同形死代码)、LightEpoch/EpochEntry Display+join 助手删(rust 无隐式消费)、__simd_popc_x256 尾段收口 popc_u64_span(文档声明的单点收口兑现)、WalRecord/WalFrame 五薄壳访问器删(payload/header 测试消费保留)。

技法沉淀:①占位伪影"? "残尾是转写模板化石,rg『相对路径.*?』可全仓扫;②纯测试消费调试口的 dead_code 用 #[cfg(test)] 门而非 doc(hidden)(pub(crate) 口 tests/ 不可见);③C# Display/ToString 在 rust 无隐式消费面,照抄即死码;④并发席代收在途改动是常态,commit 后必须 git show 复核完整性。

### r10 确认轮 c3 批战果(2026-09-30,六专项新轴,76 条→64 落地+12 翻案留守,连胜 0/32;e040aa6+bba48e0)

占轴:e1 枚举死变体/e2 宏内层/e3 错误面/e4 类型面/e5 Default derive 面/e6 测试第四批。

- Default 死 derive 48 删+7 编译器裁决留守(字段推断/宏推断/级联三形态票面漏记);wvector VectorIdFormat 票面外备案
- 死变体 9 删+4 #[from] 翻案(wbase Layout/wedb Migration/wvector 两变体):**grep 构造点判活死对 #[from] 的 ? 隐式流入系统性漏检,死变体普查必须编译器驱动(删变体试编译)**——本轮最重要方法论修正
- 宏面 3 条落地;类型面 4 死字段删(approximate_count 活链证伪留守);e6 样板 1 条挂账
- **重大插曲:全量门禁抓到并发席 a537aca(repl-aof-pump wire 休眠门,已归档票)的测试面收口遗漏**——bisect 定位 08178f8 绿→9f5fd09 红→根因 a537aca;wire 门只拦 wire=None 钉线驱动(生产 dispose 与出册同发无留册形态,门无生产缺陷),七+一用例 attach_wire 接线适配+断链形态改 wire 在位+set_connected(false)。多会话教训:并发席落实现 merge 只跑定向回归,全量门禁是公共安全网,红时先 bisect 归属再动手
- stash 技法:多席并发+并发席代收环境下 stash pop 冲突,交叠文件比对后 checkout stash@{0} -- <非交叠> 定点恢复+drop

挂账累计:G2-4 夹具收敛 37 份;TESTONLY 普查(wacl24/wepoch16/wtxn12/wconf10/wcpr10/wbase11/wdev6);cmd_strings write_map_len_resp2+subscribe_broker for_each_pattern doc(hidden) 授权迁移;wlua script_cache_race_toss helper 收敛;VectorIdFormat #[default];Clone/Copy/Debug/PartialEq 死 derive 专项(e5 未扫面);恒 Some/Ok 死臂(e1 轴3 未扫)。

### r10 确认轮 c4/c5 批战果(2026-09-30,连胜 0/32;c4 44 文件+c5 168 文件)

c4 批(六席):Hash 死 derive 26 摘除(编译器 E0599 独立验证)、恒 Ok/恒 Some 收窄 4(purge_checkpoint unit 化/exceeds_max_argument_length bool 化/try_parse_value Option 化/try_get_database 删)、mod 死面收敛(wacl/wmetric 四 mod 收私+12 口双导收敛+GarnetServerMetrics::dispose 删)、TESTONLY 89 口三档甄别销账 A5票4(档② 7 处落码+候选 12 口列账)、json_assert.rs 死断言基建删(==自测恒真+hash 恒 0 假 mock)、lib.rs.orig 删;wnode/wkv 宏错误面余量席 CLEAN。f1 席方法学:Clone/Debug/PartialEq 死 derive 候选被 trait 求解级联抑制污染,**必须按 crate 拓扑自下游向上游分 trait 分轮清算**。

c5 批(六席):**Clone 死 derive 编译器驱动清算 161 型删+36 rescue**(g1 席,g1.py/g1_audit.json 工具留档;F1 候选名单污染面过半 125+ 查无 derive 未动);档② doc(hidden) 新落 10 口;wmetric slowlog 整树收 pub(crate)+SlowLogContext 提根;wlua race helper 收敛净缩 90 行;wnode aof 装配三连 helper 下沉 wnode_test 净减 266 行(52 测试绿);语义终审双席 g4(SET 过期双门 P3 注释互指,blame 归初版)/g5(CLEAN);测试第六批 CLEAN。门禁 5310 绿。

**未清算存量**:Debug/PartialEq/Eq/Copy 死 derive(g1 方法+工具可复用,按 trait 分轮防级联,每批一个清算席);高置信档②已清零。

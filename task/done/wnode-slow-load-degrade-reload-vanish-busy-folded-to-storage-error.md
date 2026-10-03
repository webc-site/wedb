甄别结论：通过（2026-09-29 主控甄别，定级 P2——load_stub rmw_helpers.rs:117-123 把 MigrationBusy 一律折 Err 违反 try_tiered_arm:169-183 在册验收「读面忙拒面不扩大」；slow_load_eval :1006/:1022 与 load_typed_sealed :1111-1115 None→Err；对照 common.rs:901-916 load_collection_stub_for_read 忙拒回退装载快照单点在位。修复：读臂走既有单点+None 重跑一次 obj_load_typed 键态已定为限；二次 Degrade 维持 Err；sealed None 映射 SealedLoad::Missing 交既有 Missing 臂）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。三处折叠位现码核实（:117-124/:1004-1023/:1111-1115）；try_tiered_arm :169-178 在码验收「读面忙拒不扩大」与 scan.rs:33-36/:162-165 契约文对照判据割裂坐实。执行席遵照（审核席修正）：读臂改走 load_collection_stub_for_read 既有单点+None 重跑一次 obj_load_typed（键态已定一次为限）；二次 Degrade 维持现行 Err 折算（勿折缺失帧——防把「零存储错误帧」偷换成「忙时报假缺失」），缺失帧仅限 Missing/WrongType/Present 三确态；sealed None 映射 SealedLoad::Missing 交既有 Missing 臂写回复验兜底。

原票面：
四族装载公共体 Degrade 臂二次装载把键消亡/懒降阶/迁移忙折成存储错误帧，纯读族（HMGET/LRANGE/ZRANGE 等）对并发键态迁移突发 -ERR slow path storage error，违背物化核 Ok(None) 既有路径契约与读面忙拒不扩大验收

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 对象常驻对象域单态装载：garnet/libs/server/Resp/Objects/HashCommands.cs、
   SetCommands.cs、ListCommands.cs、SortedSetCommands.cs 各 Network* 经
   storageApi.Read（libs/server/Storage/Session/ObjectStore/Common.cs:
   ReadObjectStoreOperation）装载即求值，应答恒 OK / NOTFOUND / WRONGTYPE 三态，
   无「二次装载竞态」通道：键在装载后被并发删除/过期，应答按装载时刻快照或
   NOTFOUND 形，绝不产生存储错误帧；读面亦无忙拒语义。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   装载公共体 Degrade 臂（wedb/wnode/src/resp/objects/rmw_helpers.rs）对二次装载
   结果一律折 Err(())，沿 slow.rs err_frame 单源出 -ERR slow path storage error：
   a) slow_load_eval :1004-1023（HMGET/HEXISTS/HSTRLEN/HRANDFIELD、SMEMBERS/
      SISMEMBER/SMISMEMBER/SRANDMEMBER、LRANGE/LINDEX/LPOS、ZRANGE 六变体/
      ZMSCORE 等纯读臂唯一物化通道）：obj_load_typed 得 Degrade 后重取
      load_stub（门禁 load_collection_stub）——键在两探针 await 窗内被并发
      DEL/TTL 到期判死/FLUSHDB 换号/前台写懒降阶摘 Meta 即得 None →
      :1006 return Err(())；并发升阶/RENAME 认领在册即 Err(MigrationBusy) →
      load_stub map_err(|_| ()) → Err(())（:118-123），读面忙拒面扩大——
      try_tiered_arm 头注（:169-178）在册验收「并发升阶/物化的自迁移开窗期间
      LRANGE/SCARD 等轮询读不得折存储错误帧」并为此专设读臂装载单点
      load_collection_stub_for_read（tiered_collection_ops/common.rs:901-916
      MigrationBusy 回退），本臂未走该单点；tiered_materialize_blob 契约文
      （tiered_collection_ops/scan.rs:33-36）「Ok(None) 键非分层态或类型不符
      （调用方维持既有路径）」，:1022 None → Err(()) 同违其唯一物化核契约。
   b) load_typed_sealed :1111-1115（SPOP/SMOVE、LMOVE/BLMOVE/LMPOP、BZPOPMIN/
      BZPOPMAX/BZMPOP/ZPOPMIN/ZPOPMAX、ZRANGESTORE/Z*STORE、GEOSEARCHSTORE
      等装载型写臂公共装载核）：tiered_materialize_blob_sealed 契约文
      （scan.rs:162-165）「Ok(None) …… 调用方落信封通道按 Missing 新建承接」，
      同文件唯一守约消费方 run_async_rmw 物化臂（:443-447 break 'materialize
      落信封通道按 Missing 新建）与之互证；本核却把 Ok(None) 折 Err(())——
      obj_load_typed 探 Meta 存活到 try_swap_in_window 之间并发 DEL/TTL/FLUSHDB
      落地即触发，装载体按存储忙拒绝而非契约承接。
   两处均与在册 wnode-object-scan-degrade 票同型不同位：该票钉
   shared_object_commands.rs slow::object_scan 折 exec_tiered_scan Ok(false)
   （HSCAN/SSCAN/ZSCAN 扫描族），本票钉 rmw_helpers.rs 装载公共体（四族点读
   与装载型写臂），函数、命令族、修复位均不重叠。
3. 逻辑危害确证
   纯读命令在分层大键上因无关并发写/删/清库突发存储错误帧，客户端读迭代中断；
   懒降阶场景本应回信封内存态正确应答，错帧丢失本可正确交付的数据面；装载体
   fail-closed 重试虽收敛，但违背 C# 三态契约、本仓「多路径行为同构」维度与
   三处在码契约文（契约方与消费方互相矛盾，后续席照注释施工即踩空）。零数据
   损坏、零 panic。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/rmw_helpers.rs:slow_load_eval（:1005-1007 load_stub None → Err、:1022 物化 None → Err、:118-123 load_stub MigrationBusy 折叠点）
wedb/wnode/src/resp/objects/rmw_helpers.rs:load_typed_sealed（:1112-1115 sealed Ok(None) → Err 折叠点）
wedb/wnode/src/resp/objects/rmw_helpers.rs:try_tiered_arm（:169-178 读面忙拒不扩大在码验收对照面）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:load_collection_stub_for_read（:901-916 读臂装载单点，本票复用既有机制）
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:tiered_materialize_blob / tiered_materialize_blob_sealed（:33-36/:162-165 Ok(None) 契约文）
对应 c# 文件与函数：
garnet/libs/server/Resp/Objects/HashCommands.cs:NetworkHGET 等（装载即求值，OK/NOTFOUND/WRONGTYPE 三态无错帧通道）
garnet/libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation（单态装载，无二次装载竞态面）

精炼执行方案：
1. slow_load_eval Degrade 臂收口：装载改走读臂单点 load_collection_stub_for_read
   （MigrationBusy 回退照常扫，读面忙拒面归零）；None 与物化 Ok(None) 按「维持
   既有路径」重跑一次 obj_load_typed（键态已定：Missing → on_missing 短路帧、
   WrongType → 既有帧、Present → 信封求值；再度 Degrade 理论不可达，为杜绝病态
   循环按一次为限兜底 on_missing 帧），复用既有装载决策核与帧出口，零新机制
2. load_typed_sealed Degrade 臂收口：sealed Ok(None) 映射 SealedLoad::Missing
   交调用方既有 Missing 臂（信封通道新建承接，与 run_async_rmw 消费形态收敛
   同一语义），写回复验 obj_writeback_rechecked_async 既有终态复验兜底并发交叠；
   同步订正 scan.rs 两处契约文与消费点一致，消除真值失真
3. 测试验证点：分层夹具（promote_collection_to_bftree）升阶后于 slow_load_eval
   两探针间注入 DEL/懒降阶写，断言 HMGET/LRANGE/ZRANGE 应答为既有缺失帧或
   信封内存态帧，绝不出现 -ERR slow path storage error；写族对装载体键注入
   并发 DEL，断言按 Missing 新建承接；既有 tiered_cmds_align、
   scan_family_dualstate_frames 回归不弱化

来源：甲轮29-B 交叉复审席（2026-09-29），错误传播链维度（task/review.md 4.2 多路径行为同构单面）。查重：wnode-object-scan-degrade 票射程为 HSCAN/SSCAN/ZSCAN 扫描族 slow::object_scan 臂（勿混）；wkv-bg-demote-claim-selflock 票已修面为摘除态自封窗懒恢复死锁（守卫透传），与本票 None 折叠位正交；err_frame 单源设计非缺陷（甲轮28-B 已认记）

终态注记（2026-09-29 执行席收口）：
合入 eec3dde（merge fix-slowload-degrade-fold → dev，内容提交 d1dc889）。
三处折叠位按审核席修正案收口：slow_load_eval Degrade 承接收口为 degrade_reload
单点——装载改走 load_collection_stub_for_read 读臂单点（MigrationBusy 回退装载
快照，读面忙拒面归零），装载 None / 物化 Ok(None) 重跑一次 obj_load_typed 出
Missing/WrongType/Present 三确态承接，再度 Degrade 维持 Err 折算（一次为限，
缺失帧仅限三确态）；load_typed_sealed sealed Ok(None) 映射 SealedLoad::Missing
交既有 Missing 臂（信封按缺新建 / on_missing 短路），头注同步订正。零新机制，
全部复用既有单点。
测试 wnode/tests/slow_load_degrade_reload_window.rs（真存储真命令）：读臂装载窗
内 DEL / 删后改型 / 信封重建三态承接（SMEMBERS/HMGET），写臂封窗物化窗内
FLUSHDB 换号 SPOP 按 Missing 承接答 nil（修复前红），静默对照零回归。配套
wkv STUB_WIN_* 停车钩子（debug_assertions 门内、仅 claim 在册的持窗者装载
消费，先例 STUB_LOAD_* 同形制）供写臂窗确定性注入。
射程说明：slow_load_eval 全部读命令（HMGET/SMEMBERS/LRANGE/ZRANGE 族等）现
均树内覆盖，Degrade 臂真实承接面为「路由门装载 None 穿透后 obj_load_typed
又见分层 Meta」的装载竞态形——该窗（路由门 read 与重装载 read 之间）无注入
点且不应为测试新增，读臂修复位由穿透三态承接 + 写臂同型 sealed 直击测试 +
代码审查三面锁定。cargo check --all-targets 绿。

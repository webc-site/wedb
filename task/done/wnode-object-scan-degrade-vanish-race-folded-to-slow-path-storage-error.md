终态注记（2026-09-29 执行席收口）：修复合入 aa30359（merge 57a9957 入 dev）。收口形态：slow::object_scan Degrade 承接臂改单装载点有界回环——exec_tiered_scan 返 Ok(false) 时按契约回环重装载一次（Missing → [0, 空数组] 逐字节 / Present 懒降阶落信封 → 对象层 operate 出等价内存态扫描帧 / WrongType → 装载核既有帧），不再折 Err 出 -ERR slow path storage error；承接重装载仍见 Degrade（病态忙偷换）维持既有 Err 折算勿兜 [0, 空]（审核钉的边界，与甲轮34-A 既判对齐）。同步订正 exec_tiered_scan 与 load_collection_stub_for_read 两处头注消除契约真值脱节。测试 wnode/tests/tiered_scan_degrade_reload_window.rs：经 wkv load_collection_stub 入口 debug-only 停车注入钩子（STUB_LOAD_PAUSE_INJECT 族，对标 GET_OR_OPEN_PAUSE_INJECT 停车-续跑握手）确定性停在两装载间让渡窗，窗内注入真实 DEL（三域 SCAN → [0, 空] 逐字节，修复前红）/ HDEL 懒降阶（内存态帧成对出件游标归零）/ 删后改型（既有 WRONGTYPE 帧），另含钩子一次性复位静默对照。cargo check --all-targets 全绿；test.sh / clippy.sh 由主代理门禁统一跑。

甄别结论：通过（2026-09-29 主控甄别，定级 P2——exec_tiered_scan 契约 scan.rs:311-314「Ok(false) 调用方维持既有路径」被唯一消费方 Degrade 臂折 Err（shared_object_commands.rs:505-520）出 -ERR slow path storage error；两装载间让渡点竞态键蒸发。C# ObjectScan 三态收口无存储错误帧通道。修复：二次 Degrade 维持 Err 勿兜 [0,空] 防病态忙偷换假缺失帧）

审核结论：通过（2026-09-29 甲轮35-B，P2 级）。契约方与唯一消费方矛盾坐实（scan.rs:311-315 Ok(false) 契约 vs shared_object_commands.rs:500-516 折 Err）；装载即 None 出 Ok(false) 先于落帧二次装载重写 output 安全；第一窗（两装载间 await 竞态）无承接确证；与甲轮34-A 装载族票同谱不同点非重复。执行席遵照：二次装载再度 Degrade 兜底与甲轮34-A 既判对齐——维持现行 Err 折算勿兜 [0, 空数组]（防病态忙偷换假缺失帧），[0, 空] 仅限 Missing 确态。

原票面：
对象扫描慢臂把 exec_tiered_scan 的 Ok(false)（键消亡/懒降阶竞态）折成存储错误帧，违背其「调用方维持既有路径」契约，纯只读扫描对客户端突发 -ERR slow path storage error

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 对象扫描单态无降级：garnet/libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan 经
   storageApi.ObjectScan（libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation）
   装载即扫描，返回 OK / NOTFOUND / WRONGTYPE 三态，对任意规模对象恒可用，
   绝无「扫描中途键态迁移」折叠成存储错误帧的通道。
   HSCAN/SSCAN/ZSCAN 是纯只读命令，C# 契约下键在扫描发起后被并发删除，应答是
   NOTFOUND 形（[0, 空数组]）或按装载时刻快照正常出件，不产生错误帧。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   分层扫描臂契约（wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan 头注）：
   「Ok(false) 装载即键不存在 / 非分层态（门禁探测与 claim 在册回退装载经读臂装载单点
   load_collection_stub_for_read 收口，调用方维持既有路径）」；装载单点
   （tiered_collection_ops/common.rs:load_collection_stub_for_read 头注）同口径：
   「Ok(None) = 键不存在 / 非分层态，折叠语义归调用方（与既有慢路径漏斗一致）」。
   但唯一消费方慢臂（wedb/wnode/src/resp/objects/shared_object_commands.rs:slow::object_scan
   的 ObjLoad::Degrade 臂）写的是
   「return if exec_tiered_scan(...).await? { Ok(()) } else { Err(()) }」——
   Ok(false) 被折成 Err(())，沿 garnet_api/slow.rs 的 err_frame! 单源出
   -ERR slow path storage error，并未「维持既有路径」。
   竞态窗具体形态：慢臂内两次装载之间（obj_load_typed 异步 Meta 探测命中返回
   Degrade 之后、exec_tiered_scan 内 load_collection_stub_for_read 之前隔 await 让渡点，
   他连接任务可插入）：并发 DEL/UNLINK 摘键、FLUSHALL 秒级换号清库、前台写触发
   懒降阶（Meta 摘除落信封）或删空自愈，任一落地即令第二次装载得 None →
   Ok(false) → 错误帧。exec_tiered_scan 内 refresh_tiered_meta 阶段的键消亡臂
   （Ok(false) → key_vanished → [0, 空]）只覆盖装载成功后的第二窗，
   第一窗（装载即 None）无承接。契约方与唯一消费方互相矛盾，属状态脱节。

3. 逻辑危害确证
   纯只读扫描在大键（已升阶分层键）上因无关并发写/删/清库突发存储错误帧，
   客户端侧扫描迭代意外中断；懒降阶场景本应回信封内存态扫描结果，错帧更丢了
   本可正确应答的数据面。零数据损坏、零 panic、重试自愈，但违背 C# 三态契约与
   本仓「多路径行为同构」维度；且 Ok(false) 契约注释宣称的折叠语义在唯一消费点
   不成立，后续席照注释施工即踩空（单源真值失真，同 wnode-tiered-zscan 票所指
   「注释宣称与实现矛盾」形态）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/shared_object_commands.rs:slow::object_scan（ObjLoad::Degrade 臂 Ok(false) → Err(()) 折叠点）
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan（头注 Ok(false) 契约与 load_collection_stub_for_read 装载点）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:load_collection_stub_for_read（头注 Ok(None) 折叠语义）
wedb/wnode/src/resp/garnet_api/slow.rs:err_frame（RESP_ERR_SLOW_PATH_STORAGE 出帧单源）

对应 c# 文件与函数：
garnet/libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan（OK/NOTFOUND/WRONGTYPE 三态收口，无存储错误帧通道）
garnet/libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation（装载即扫描单态）

精炼执行方案：
1. 慢臂 Degrade 臂收口：exec_tiered_scan 返 Ok(false) 时不折 Err，按契约「维持既有路径」
   重走一次 obj_load_typed 装载（此时键态已定：Missing → write_scan_not_found 出
   [0, 空数组]；Present → 正常 operate 出件；WrongType → 既有 WRONGTYPE 帧；
   再度 Degrade 理论不可达，为杜绝病态循环按一次为限兜底 [0, 空数组]），零新机制，
   复用既有装载决策核与既有帧出口。
2. 同步订正两处头注与消费点一致：exec_tiered_scan 头注 Ok(false) 语义补「慢臂承接为
   既有路径重装载」一句，消除契约与消费方脱节的真值失真。
3. 测试验证点：分层夹具（promote_collection_to_bftree）升阶后挂 pending_slow 形态，
   在慢臂两次装载间注入 DEL（或懒降阶写），断言 HSCAN/SSCAN/ZSCAN 应答为
   [0, 空数组]（DEL/清库）或等价内存态扫描帧（懒降阶），绝不出现
   -ERR slow path storage error；既有 scan_family_dualstate_frames 与
   tiered_zscan_corrupt_score_failfast 回归不得弱化。

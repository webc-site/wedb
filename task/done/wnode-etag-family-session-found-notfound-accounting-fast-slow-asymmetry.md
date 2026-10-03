终态注记: 已合入 main（commit: d19a3d6）。收口形态：读族快臂传入 session_metrics，条件写族快慢前置读均保持静默并在命令出帧处按 C# 真实计数表收口（SET 族存活计 found、缺席初写计 notfound；DELIFGREATER 删生效计 found、缺席与对象键计 notfound、失配零删零计）；补齐 user_read.rs 记账单源与 tests/etag_read_write_accounting.rs 回归测试。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

复核席异议注记（2026-09-30 独立复核席，2026-09-30）：残片真实（快臂四处 read_user_sync None 恒零欠计、快慢不对称，basic_etag_commands.rs:244/:313/:406/:481 实证；读族快臂补 session_metrics 对位 get.rs:52、写族计数点置命令出帧收口防降级重放双计均成立），但原票面 C# 契约两处主张经 Tsavorite 源码核实为反向误读，执行席严禁照原方案 2 与钉测 3 落地：
1 「SET 族缺席初写 C# 计 found」假。NeedInitialUpdate 恒真（RMWMethods.cs:36-43）只决定初写发生，不决定 status；Tsavorite 初写成功基码是 NOTFOUND|CreatedRecord（InternalRMW.cs:484-487 新建、:307 复活；StatusCode.cs remarks 钉死「RMW InitialUpdater: NotFound | CreatedRecord」），MainStoreOps.cs:337-341 据 status.NotFound 计 incr_session_notfound。应答面 InitialUpdater 照写 [newEtag,nil]（ExecuteETagSetCommand :297-309 无视 status），纯观测差异。rust 慢臂 Missing→notfound 与 C# 一致，非错位。
2 「DELIFGREATER 失配 C# 计 notfound」假。失配走 RMWMethods.Etags.cs:47-52 CancelOperation（SessionFunctionsWrapper.cs:193-195 → CANCELED）或 :205-214 no-Action → SUCCESS，DEL_Conditional（MainStoreOps.cs:307-318）IsExpired 假、NotFound 假，两臂均不触——C# 失配零计。
3 C# 真实计数表（执行口径唯一依据）：SET 族键存活（命中/失配零写）计 found；缺席/过期初写计 notfound；对象键 promote 后二次 SET_Conditional 初写计 notfound（BasicEtagCommands.cs:300-306）。DELIFGREATER 删生效计 found；缺席计 notfound；对象键（NOTFOUND|WrongType 落 NotFound 臂）计 notfound；失配零删零计。
4 写族降级重放双计实证成立：network_setwithetag :406 读 Hit（已计）后 :421 探针 Ok(None) bail 全重放、apply_etag_write 值腿败全重放，慢臂重读重计——写族计数点必须置命令出帧收口；读族 Hit/Missing 计后命令即闭环，直接补句柄即可无双计。
5 附带：user_read.rs:90-102 既有「RMW 前置读不入账」纪律注记系 GETDEL/INCR 族（C# 全链零计）口径，etag 族 C# 计数不在该谱，落笔时同步订正该注记适用边界。

审核结论：通过（2026-09-30 甲轮45-E，P3 级）。ETag 六命令快臂 read_user_sync 传 None 恒零入账、慢臂读漏斗折叠入账，快慢不对称且偏离 C# 条件算子计数语义事实确证。执行席遵照：在 user_read.rs 入账纪律单源补登 etag 族，读族快臂传入 session_metrics 统计，条件写族在命令层按算子语义收口（写生效恒计 found，DELIFGREATER 仅删生效计 found 其余计 notfound），快臂前置读保持静默防双计。
（复核席按上方异议注记修订前条执行口径：条件写族按「C# 真实计数表」收口，原注记「写生效恒计 found，DELIFGREATER 仅删生效计 found 其余计 notfound」中缺席初写与失配两形与 C# 反向，以复核席计数表为准。）

原票面：
ETag 六命令会话 found/notfound 入账快臂恒零、慢臂读漏斗折叠，快慢不对称且偏离 C# 条件算子计数语义

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：六命令在 C# 侧全部产生 incr_session_found/incr_session_notfound 会话入账。GETWITHETAG/GETIFNOTMATCH 经 storageApi.GET 走 MainStoreOps.cs GET 族既有计数（:30/:39 锚，键存缺即 found/notfound）；SET_ETagConditional → MainStoreOps.cs SET_Conditional(output 变体，:321 起) RMW 返回 OK 即 incr_session_found、NotFound 即 incr_session_notfound——SETIFMATCH/SETIFGREATER/SETWITHETAG 的 NeedInitialUpdate 恒真（RMWMethods.cs:37-38），无论命中、失配、缺席初写 RMW 恒 OK，C# 三写命令恒计 found；DEL_ETagConditional → DEL_Conditional（:290 起）以 status.IsExpired 为成功指示：真实删除计 found，失配/缺席/对象键一律计 notfound（语义为「有无产生写入」而非「键有无命中」）。
2. 工程现状确证：rust 快臂四处 read_user_sync(store, key, None, …) 传 None 句柄恒零入账（basic_etag_commands.rs 的 etag_read_fast、network_delifgreater、network_setwithetag、network_set_etag_conditional 四处前置读），而 GET 快臂同点位传 self.session_metrics 计数（basic_commands/get.rs:52 先例）；慢臂 read_value_and_etag_async → StorageSession::read_user → read_user_with_prefix 漏斗尾 record_outcome 恒入账。后果双重：(a) 快慢不对称——同一命令失闩/磁盘候选降级即从零计翻为计，路径决定计数，违背多路径行为同构；(b) 快臂整体欠计偏离 C#（C# 六命令全计）。慢臂折叠规则（Hit→found、Missing→notfound）亦与 C# 条件算子语义双向错位：set 族缺席初写 C# 计 found、rust 计 notfound；DELIFGREATER 失配（Hit 未删）C# 计 notfound、rust 计 found。user_read.rs 既有入账纪律单源未登记 etag 族口径，快臂 None 无注释支撑，非登记决策。
3. 逻辑危害确证：total_found/total_notfound 会话统计对 ETag 族系统性失真——热路径（快臂命中）恒不计、降级路径计数、同键两路结果不确定；INFO 观测面偏离 C# 且随负载降级率漂移。同谱先例 deviations §130（观测面超集/空表虚报立案族），本条为快臂欠计加双臂不一致的观测面失真，无数据面危害。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_etag_commands.rs:etag_read_fast、network_delifgreater、network_setwithetag、network_set_etag_conditional（四处 read_user_sync None 臂）
wedb/wnode/src/resp/basic_etag_commands.rs:read_value_and_etag_async（慢臂计数源，转调 StorageSession::read_user）
wedb/wnode/src/storage/session/common/user_read.rs:fold_outcome、read_user_sync（入账纪律单源，缺 etag 族条目）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:GET（:30/:39 计数锚）、SET_Conditional（:321 起 output 变体计数）、DEL_Conditional（:290 起 IsExpired 指示计数）
garnet/libs/server/Resp/BasicEtagCommands.cs:NetworkGETWITHETAG、NetworkGETIFNOTMATCH（storageApi.GET 路由）、NetworkDELIFGREATER（DEL_ETagConditional 路由）
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:NeedInitialUpdate（SETIFMATCH/SETIFGREATER/SETWITHETAG 恒真，RMW 恒 OK）

精炼执行方案：
1. 在 user_read.rs 入账纪律单源补登 etag 族条目：读族（GETWITHETAG/GETIFNOTMATCH）快臂传 session_metrics 对位 GET 件（键存缺即 found/notfound，慢臂既有折叠不动）。
2. 条件写族按 C# 条件算子语义在命令层收口，不沿用读漏斗折叠：SETIFMATCH/SETIFGREATER/SETWITHETAG 写生效（含缺席初写与失配零写）恒计 found；DELIFGREATER 删生效计 found、其余（失配/缺席/对象键）计 notfound；快臂的 RMW 前置读保持静默纪律，计数点移到命令出帧收口，杜绝降级重放双计。
3. 测试验证点：六命令快慢两路各执行一读，INFO 前后 total_found/total_notfound 差值与 C# 语义逐条恒等；降级臂与快臂计数一致；DELIFGREATER 失配计 notfound、SETIFMATCH 缺席初写计 found 两错位形钉测防回摆。

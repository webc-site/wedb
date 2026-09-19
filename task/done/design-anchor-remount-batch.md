优先级：低
来源：next/muse.design.md 条 2、5、8、11 与 next/agy.design.md 条 16 / next/muse.design.md
条 9 的锚点复挂增量（结构体分层本体已裁定不成立，见 task/reject/design-snapshot-triple-struct-unify.md）。
取证基线：主仓 dev 当下代码；check.js 判重口径实测（js/check.js:304 dupDefFind，
按函数文档注释内 CS_REF_REGEX 匹配「路径.cs:符号」聚合，>1 处即报）。

问题
五组 C# 锚点一符号多挂（均挂在函数文档注释内，check.js 会报重复定义），复挂形态为
「装配/委托/分支侧重复挂真实现锚点」；只改注释即可消除，不动代码。

取证（逐组：锚点 → 挂载点）
1 GetSslServerAuthenticationOptions 双挂（muse 条 2 报三处，实测 from_pem_files 无锚点，实为两处）：
  wedb/wnode/src/tls/config.rs:57 from_der 的 doc、:91 server_config 的 doc
  （server_config 是装配单点保留，from_der 去锚点改散文说明）
2 OnDispose 四挂：
  wedb/wnode/src/resp/array_commands.rs:160 network_del、
  wedb/wnode/src/storage/session/storage_session.rs:500 vector_registry_delete_hook、
  wedb/wnode/src/resp/garnet_api/mod.rs:331 with_vector_manager、
  wedb/wnode/src/resp/vector/vector_manager.rs:533 delete_vector_set
  （删除单点 delete_vector_set 保留，其余三处去锚点改散文引用）
3 VectorManager 构造锚点双挂（两函数皆非构造）：
  wedb/wnode/src/resp/vector/vector_manager_cleanup.rs ensure_cleanup_tasks_started、
  wedb/wnode/src/service.rs with_vector_set_preview，两处去锚点
4 ContextReadWithPrefetch 双挂：
  wedb/wkv/src/session/raw/batch.rs:24 read_batch_with 的 doc（「严格对照
  libs/storage/.../Tsavorite.cs:ContextReadWithPrefetch」）、
  wedb/windex/src/table.rs:608 prefetch_batch_probes 的 doc
  （read_batch_with 是 C# 公共 API 直接对位者保留；prefetch_batch_probes 是其内部预取探针
  段，去锚点改散文「协作段见 read_batch_with」——注意 muse 条 11 建议的保留方向反了）
5 GetDatabaseStoreStats / GetDatabasePersistenceStats 双挂：
  wedb/wnode/src/resp/garnet_api/mod.rs:203 project_db_snapshot、:182 project_aof_snapshot
  （投影函数）与 wedb/wmetric/src/info/garnet_info_metrics.rs:683 get_database_store_stats、
  :790 get_database_persistence_stats（统计真实现）同挂
  （wmetric 真实现保留，投影函数去锚点，doc 已自称「wmetric DbSnapshot 的全仓唯一组装点」）
C# 对标：garnet/libs/server/TLS/GarnetTlsOptions.cs:GetSslServerAuthenticationOptions、
garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose、
garnet/libs/server/Resp/Vector/VectorManager.cs:VectorManager、
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch、
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs 两统计函数。

修法建议
五组各保留一处真实现挂载（上文括号内判定），其余挂载点把「路径.cs:符号」改为散文式
说明（去冒号或改自然语言），一处一锚；落地后跑 ./js/check.js 确认重复组消除。
禁为去锚点删除任何语义说明文字。

分拣补记（next/agy.net.md 条 15 与 next/muse.net.md 条 7 同题并入本票组 1，net 域源档已分拣清空
删除；浅核 2026-09-19 主仓 dev）：GetSslServerAuthenticationOptions 双挂组在 net 与 design 两轮
重复上报，唯一载体以本票组 1 为准，net 域分拣不再另立（曾建的壳已剪）。增量约束：出站面
wedb/wconn/src/tls.rs ClientTlsConfig::new 对 GetSslClientAuthenticationOptions 现为单挂，
本票组 1 落地时勿动出站侧。

分拣补记（next/muse.db.md 条 3 与条 5 同题并入本票，db 域源档已分拣清空删除；浅核
2026-09-19 主仓 dev，bun js/check.js 实测）：条 3 即本票组 4（ContextReadWithPrefetch
双挂），保留方向以本票为准（read_batch_with 保留、prefetch_batch_probes 去锚，
muse 原建议方向相反已修正）。条 5 增量两组：其一 GetDatabasesSnapshot 三挂
（check.js 现仍报）——wedb/wkv/src/store/stats.rs:110 store_snapshot 为引擎真实现
保留，wedb/wnode/src/resp/garnet_api/mod.rs:116 store_snapshots 与 :565
StoreGarnetApi::store_snapshots 两处编排/转发去锚改散文（对标 C# 侧
StoreWrapper.cs:567 GetDatabasesSnapshot 单实现、InfoMetrics 为消费方不挂此键），
并入本票作第六组；其二即本票组 5（InfoMetrics 两键），同票处理不再重复。

分拣补记（muse.my 条 23 同题；浅核 2026-09-19 主仓 dev）：wlua/src/functions/redis.rs:401
try_fast_path_set 与 :478 try_fast_path_get 的文档注释各挂
LuaRunner.Functions.cs:ProcessCommandFromScripting（SET/GET 分支），与 :584
process_command_from_scripting 总入口三挂同符号（CS_REF_REGEX 捕获到符号名即计，分支
后缀不区分），check.js 判重同源；处置同本票口径——两快道去锚点改散文「见
process_command_from_scripting 总入口对应分支」，锚点只留总入口，作第七组。

分拣补记（muse.my 条 24 增量；浅核 2026-09-19 主仓 dev）：本票组 3（VectorManager
构造双挂）即该条前半；后半 HashSet 对未列——wedb/wnode/src/resp/objects/
tiered_collection_ops.rs:197-213 tree_put_batch 文档注释复挂 HashObjectImpl.cs:HashSet
与 SetObjectImpl.cs:Set。HashSet 真实现锚点在 wedb/wcol/src/hash/hash_object_impl.rs:247，
tree_put_batch 去锚改散文引用；SetObjectImpl.cs:Set 经查全仓仅 tree_put_batch 此一处
挂载，去锚会使该 C# 符号失锚进 miss 报表，处置二选一：保留该处并注明「判据引用非
实现锚」，或去锚后在 js/check/ignore 配套 ignore 说明，作第八组。

分拣并入（net 域锚点 6 组，源自 next/net-anchor-dup-collapse.md，2026-09-19）：GarnetClientSession 构造四挂、ConnectAsync 双挂、GarnetClient 构造双挂、NetworkIterativeSlotVerify 双挂、StartAsync 双挂、HandleNewConnection 双挂——去副保主只改注释；与 task/ing/net-checkjs-dup-anchor-trio.md 在途票同族，落地前先确认该票是否已收口，避免双改。

落地补记（2026-09-19，fix-anchor-remount，合并 commit cdf426b，只改注释 7 文件 17+/13-）：
组 1 已核销（net-checkjs-dup-anchor-trio 条二收口，tls/config.rs:93 单锚；出站面
wconn/src/tls.rs GetSslClientAuthenticationOptions 未动）；组 2 delete_vector_set 主锚
保留，network_del / vector_registry_delete_hook / with_vector_manager 三处去锚改散文；
组 3 两处皆非构造，ensure_cleanup_tasks_started / with_vector_set_preview 去锚改散文；
组 4 read_batch_with 主锚保留，prefetch_batch_probes 去锚改「协作段见 read_batch_with」；
组 5 wmetric 两统计真实现保留，project_db_snapshot / project_aof_snapshot 去锚；
组 6 wkv store_snapshot 保留，garnet_api 编排（:111）/转发（:563）两处去锚；
组 7 总入口 process_command_from_scripting 保留，SET/GET 两快道去锚；
组 8 已核销（tree_put_batch 现树为散文引用不匹配 CS_REF_REGEX，HashSet 真锚单挂
wcol hash_object_impl.rs:247）。验证：worktree 同环境 check.js 前后 diff 28 行均为
射程 6 组重复段删除、零新增（重复段不新增、缺失段不新增）；cargo check worktree 与
主仓合并后均通过（worktree 首次 cold check 曾报 9 个 E0599，系 fork 首建与并发推进
竞态假象，stash 基线与改动态复跑均通过，错误文件与本票 7 文件零交集）。
余量：重复定义段尚存 7 组均非本票射程——net 域 4 组（GarnetClientSession 构造、
ConnectAsync、GarnetClient 构造、NetworkIterativeSlotVerify，与已收口
net-checkjs-dup-anchor-trio 同族，票尾分拣并入清单注明避免双改，跳过）及
CopyFromImmutable / GetCollectionItemAsync / AcquireExclusiveForDelete 三组（他域，未动）。

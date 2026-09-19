来源：next/muse.design.md（review-design：数据链条死代码 重复机制 常量工具 模块拓扑，20 条待办）
独立复核档案。核销时间 2026-09-19，取证基线为主仓 /Users/z/git/db/wedb 分支 dev 工作树
HEAD f35ccee（行号按符号现刻重取）。

结案方式：本文件未由本人剪枝——并发代理已在 commit 3dccbd4（qw13 盘点结档分发）整档消费
（git show a7c9e88:next/muse.design.md 128 行 / 条 1-20 全数有去向），工作树与索引均已无
next/muse.design.md，故按 0 行结案转独立复核，不重建源档。头部综述段（check.js 现状）与
尾部「已确认单点无需动」段按规程不裁决。

裁决计数：已落地/不再成立 2（条 3、14）；不成立 8（条 4、6、9、10、15、17、19、20）；
成立并入既有载体 9（条 1、2、5、7、8、11、13、16、18）；改判 1（条 12，并发判「不成立」
不实，实为成立）。合计 20。

一 逐条复核（并发去向 → 本人实测）

条 1 PEM 解析两份同形 —— 成立，并入既有载体 task/ing/tls-pem-loader-single-source.md
（并发会话认领在跑，按规程禁重立；本人未建任何票）。实测重复仍在场：
/Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:226 load_certs、:242 load_private_key；
/Users/z/git/db/wedb/wedb/wconn/src/tls.rs:190 load_certs、:208 load_private_key，两 crate 各一份。
同题双档提示：next/design-pem-load-dedupe.md 与本载体同题（该 next 票源自 agy 条 1 + muse 条 1），
派发时只发 ing 侧。

条 2 服务端 TLS 三函数一锚点 —— 成立（锚点复挂），载体 next/design-anchor-remount-batch.md 组 1。
票面「三处」失准，实测两处：/Users/z/git/db/wedb/wedb/wnode/src/tls/config.rs:57（from_der doc）、
:91（server_config doc），from_pem_files 无锚点，与该票的实测更正一致。

条 3 WriteNull 三处版本分派 —— 已落地（task/reject/design-null-frame-already-sourced.md），复核一致。
单点在场：/Users/z/git/db/wedb/wedb/wresp/src/ext.rs:89/:94 声明、:132/:140 实现，模块头 :3-4 自述
null 一族仅此两入口。消费侧实测：
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/hash_commands.rs:751 write_null_array 仅
write_resp_array_len + 逐元素 write_resp_null_ver，无内联版本二选一；
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/list_commands/blocking.rs:447 起
write_collection_item_result 空值臂直转 write_resp_null_array_ver / write_resp_null_ver。

条 4 WriteSetLength 三层同义 —— 不成立（task/reject/design-anchor-dup-false-positives.md 条 4），
结论复核一致：/Users/z/git/db/wedb/wedb/wresp/src/resp_memory_writer.rs:508 write_set_length 的
doc 实测无 .cs 锚点（仅「写 set 头：根据 P 静态分派」）。但该档案内「cmd_strings 挂载为全仓唯一」
措辞失准：/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session_output.rs:171 亦挂
libs/server/Resp/RespServerSessionOutput.cs:WriteSetLength，与
/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:473 的短形态
RespServerSessionOutput.cs:WriteSetLength 并存；二者 key 因路径形态不同，
js/check.js:314 dupDefFind 不聚合，故不报重复的结论不变。

条 5 OnDispose 一挂四处 —— 成立，载体 next/design-anchor-remount-batch.md 组 2，挂载数实测一致：
/Users/z/git/db/wedb/wedb/wnode/src/resp/array_commands.rs:160、
/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:500、
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:331、
/Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_manager.rs:533 四处函数文档注释同挂
GarnetRecordTriggers.cs:OnDispose。/Users/z/git/db/wedb/wedb/wkv/src/store/mod.rs:120 的第五挂
是结构体字段 doc，js/check/rustScan.js:68 rsDocExtract 只收 function_item 前导 doc，不判重，
该票未列正确。

条 6 tree_put_batch 复挂 HashSet —— 不成立，复核一致：
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs:691 的引用是函数体内
行内 // 注释，不入判重；正式锚唯一在
/Users/z/git/db/wedb/wedb/wcol/src/hash/hash_object_impl.rs:247。

条 7 锁面两套入口 —— 成立，并入既有载体 task/ing/range-index-locks-acquire-api.md（并发认领在跑，
禁重立；next/design-range-index-locks-guard.md 为同题双档）。实测两入口在场：
/Users/z/git/db/wedb/wedb/wbftree/src/manager/mod.rs:378 pub fn locks 直裸 StripedRwLock、
/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:346 pub async fn acquire_tree_write 守卫封装。

条 8 VectorManager 构造锚点挂非构造 —— 成立，载体 next/design-anchor-remount-batch.md 组 3，
复核一致：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:1419（with_vector_set_preview，Builder 注入）、
/Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_manager_cleanup.rs:221
（ensure_cleanup_tasks_started，doc 自述「C# 在构造器内 spawn，Rust 构造发生在 compio 之外故惰性拉起」），
两函数皆非构造器，锚点误挂。

条 9 快照三级结构体加两级投影 —— 不成立（task/reject/design-snapshot-triple-struct-unify.md，
与 task/reject/agy.design.md 条 16 同题），复核一致：结构分层与 C#「存储层出数、指标层投影」同构
（/Users/z/git/db/wedb/wedb/wkv/src/store/stats.rs:45 StoreSnapshot →
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:195 project_db_snapshot、:181
project_aof_snapshot → /Users/z/git/db/wedb/wedb/wmetric/src/info/garnet_info_metrics.rs:681、:788）。
锚点复挂增量（投影函数与统计真实现同挂）并入 next/design-anchor-remount-batch.md 组 5。

条 10 闩锁双层同名同挂 —— 不成立（task/reject/design-txn-locktable-anchor-remount.md），复核一致，
票面「四组同挂」前提不实：/Users/z/git/db/wedb/wedb/wtxn/src/txn_lock_table.rs:108、:116、:124、:132
四函数各为一行 self.pin().bucket(bucket) 转发，锚点指向
libs/storage/.../Implementation/Locking/OverflowBucketLockTable.cs 四符号，与 windex 侧
HashBucket.cs 的闩是不同 C# 出处，不构成复挂也无算法复写（跨层薄包装不算重复）。

条 11 预取跨层撞名 —— 成立，载体 next/design-anchor-remount-batch.md 组 4，复核一致：
/Users/z/git/db/wedb/wedb/wkv/src/session/raw/batch.rs:24 与
/Users/z/git/db/wedb/wedb/windex/src/table.rs:608 两函数 doc 同挂 ContextReadWithPrefetch；
第三处 /Users/z/git/db/wedb/wedb/windex/src/prefetch.rs:2 挂在 pub const PREFETCH_WINDOW 上，
非函数 doc 不判重。该票对「保留哪一侧」的方向修正（保留 batch.rs 的 C# 公共 API 对位、
table.rs 去锚）本人认同。

条 13 错误文案四处散落常量 —— 成立，载体 next/cmd-strings-input-token-single-source.md 分拣补记，
实测六处文件级 const 仍在场且 wresp/src/cmd_strings.rs 无同名承接：
/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_commands.rs:34、:37，
/Users/z/git/db/wedb/wedb/wnode/src/resp/txn_resp_commands.rs:70，
/Users/z/git/db/wedb/wedb/wnode/src/resp/array_commands.rs:36，
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:205。

条 14 信封编解码六函数 —— 已落地（next/zero-consumer-dead-surfaces-batch-six.md:44 补记），复核一致：
/Users/z/git/db/wedb/wedb/wcol/src/object_payload.rs:51 obj_encode_into 为唯一真体，:57
obj_encode_custom_into 同族真体，:68/:74/:82/:88 四个入口分别为一行转调
（obj_encode→obj_encode_custom→obj_encode_custom_into、obj_decode→obj_decode_custom），
票面要求的「收敛为 encode_into 与 decode 两入口」在转调形态上已达成。

条 15 结果四枚举各说各话 —— 不成立（task/reject/design-status-enum-unify.md，与 agy 条 2 同题），
复核一致：/Users/z/git/db/wedb/wedb/wnode/src/types.rs:10 GarnetStatus、
/Users/z/git/db/wedb/wedb/wkv/src/session/raw/read.rs:22 StoreResult、
/Users/z/git/db/wedb/wedb/wcol/src/object_payload.rs:125 ObjLoad 三枚举分对 C# 三域
（garnet/libs/server/API/GarnetStatus.cs:9 四臂 / Tsavorite OperationStatus / 本项目分层 Degrade 自定义态），
在 wval 建跨层状态 trait 属自造架构，违 transpile SKILL 的 1:1 对标。

条 16 元布局常量半公开 —— 成立，载体 next/design-meta-layout-const-visibility.md，复核一致：
/Users/z/git/db/wedb/wedb/wval/src/meta.rs:36 pub const SIZE_OFFSET 全仓零外部消费者
（仅同文件 :186、:187、:229 内部使用），而 META_VALUE_SIZE 有真实跨 crate 信封拼接
（/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:14、:100-:115、:255-:271）。
收 SIZE_OFFSET 回私有、勿对称公开 TYPE_OFFSET 的修法正确。

条 17 集合阈值与哑值散两文件 —— 不成立（task/reject/design-tiered-constants-module.md），复核一致：
六个常量各一处定义、无第二定义无拼写漂移：/Users/z/git/db/wedb/wedb/wcol/src/lib.rs:29
SET_MEMBER_DUMMY_VALUE、:32/:35/:41/:46 四阈值，
/Users/z/git/db/wedb/wedb/wcol/src/types/garnet_object.rs:21 LIST_SEQ_BASE。
阈值组系 transpile SKILL:27-32 自定义架构（C# 无对应常量组），doc/zh/collection.md 以
wcol::TIERED_PROMOTE_THRESHOLD 路径钦定，搬家是造漂移不是去重。

条 18 巨文件待拆分 —— 成立，已按文件分票（体量实测：resp_server_session.rs 3172、
tiered_collection_ops.rs 2236、set_commands.rs 1431、garnet_api/slow.rs 1101、wkv/vdb.rs 1495、
wval/ns_codec.rs 779，票面数字至多偏 1 行）：
/Users/z/git/db/wedb/task/ing/resp-server-session-file-split.md（认领在跑；
next/design-resp-server-session-file-split.md 为同题双档）、
next/tiered-collection-ops-file-split.md、next/design-set-hash-commands-dir-split.md 与
task/ing/set-hash-commands-dir-split.md、next/design-vdb-file-split.md 与 task/ing/vdb-file-split.md。
slow.rs 与 ns_codec.rs 两文件余量判不成立（task/reject/design-garnet-api-nscodec-no-split.md），
本人复核该档：三执行域子模块已在场
（/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:15-17 mod objects/raw/slow），
拆分依据仅体量，不成立。该档「ns_codec.rs 56 个 fn」计数失准（实测 37 个 fn 声明、13 个 pub fn），
不影响其单职责结论。

条 19 wcol 反向依赖 wresp —— 不成立（task/reject/design-wcol-wresp-dep-inversion.md，与 agy 条 21
同题），复核一致：/Users/z/git/db/wedb/wedb/wresp/Cargo.toml 对 wcol/wval/wkv 零命中（本人用
Grep 工具复核，非 zsh glob），单向依赖无环即无倒置；C# 对象层自带 RESP 输出形态，
移 ObjectOutput 是零收益搬家且与 cmd-strings、resp-frame-literal 两票前提冲突。

条 20 garnet_api 四文件边界 —— 不成立（task/reject/design-garnet-api-nscodec-no-split.md），
复核一致：mod.rs 575 行承载 GarnetApiFace trait 加装配与投影，raw/slow/objects 三子模块即票面
建议的三执行域切分，现状同构；拆大 trait 无 C# 对标依据（C# 为 IGarnetApi 单接口）。

二 改判：条 12 lua 快路径复挂总入口 —— 成立（并发「不成立」判定不实）

并发档案 /Users/z/git/db/wedb/task/reject/design-anchor-dup-false-positives.md 第 24 行起的条 12
核销段，其事实前提经实测为假：该段称两快路径函数 doc 中「.cs 与符号之间是空格，不匹配
CS_REF_REGEX」。当前代码为冒号形态且三处同挂一 key：
/Users/z/git/db/wedb/wedb/wlua/src/functions/redis.rs:401（fn try_fast_path_set doc：
「SET 快路径（在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
:ProcessCommandFromScripting SET 分支）」）、同文件 :478（fn try_fast_path_get 同款）、
同文件 :584（fn process_command_from_scripting 正式锚「在 garnet 中的相对路径:
libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting」）。
三处均匹配 js/check/rustScan.js:34 CS_REF_REGEX，归一后 key 同为
libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting，且三者皆为 function_item
前导 doc（rustScan.js:68 rsDocExtract 收集面），故 js/check.js:304 dupDefFind 必报该组三挂载重复。
C# 事实：garnet/libs/server/Lua/LuaRunner.Functions.cs:3137 起 ProcessCommandFromScripting 是一体
函数，SET/GET 快路径是其内联分支，不是三个 C# 函数，锚点应一处一符号。

处置：本条为成立项，但活载体已在同域兄弟档在册
（/Users/z/git/db/wedb/next/muse.my.md 条 23「lua 快路径拆分致 check 重复」，动作同为
「快道去锚点留说明，锚点只留总入口」），该文件由另一代理持且正在裁决，按查重规程禁双立，
本人不建 task/ing 票。请主代理把本条并入 muse.my 条 23 的载体一并落地，并作废
design-anchor-dup-false-positives.md 的条 12 核销段（条 4、条 6 两段结论仍有效，勿连带删）。
落地时保留语义说明文字，只把两处快路径 doc 内的「路径.cs:符号」记号改为散文引用，
禁改函数体与分派语义。

三 复核派生（越界不立项，仅登记线索）

本人另测得：src 侧 doc 注释（/// 与 //!）中的 C# 锚点有 225 处为裸文件名形态
（连 tests 一并统计为 347 处，扫描 672 个 .rs 文件），
（例 /Users/z/git/db/wedb/wedb/waof/src/wal/commit.rs:2 TsavoriteLog.cs:TryEnqueueCommitRecord、
/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:473），而 csPathNormalize
（js/check/rustScan.js:36）只剥 garnet/ 前缀、不做 basename 回查，doc_file_fn_map
按归一路径为 key（rustScan.js:53-65），语料侧 key 是 libs/... 全相对路径，故这批裸形态锚点在
「已文档化」映射里登记为空，对应 C# 函数仍以缺失形式出现在 check.js 输出。此为工具登记纪律问题，
不属 next/muse.design.md 二十条任何一条，亦与在途票 task/ing/garnet-scan-cs-corpus-parse-gate.md
（语料解析门禁）不同面，故不立项，移交主代理决定是否单独开票（票量不小，宜按 crate 分批）。

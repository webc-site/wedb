MSETNX 慢路径存活判定漏探 Meta 域：升阶冷键被误判不存在后写出双域键

来源：next/glm.data.md 条 6（该文件已被并发分拣波消费删除，原文转抄存于
/Users/z/git/db/wedb/next/msetnx-slow-nx-meta-domain-probe.md，本档按当下主仓代码重新取证）。
取证基线：主仓 /Users/z/git/db/wedb，
分支 dev，HEAD a7402c4（bb06827、6311510 两轮复核：本档取证文件未变、锚点未位移），行号按符号在当下代码复核。

现状

快路径 /Users/z/git/db/wedb/wedb/wnode/src/resp/array_commands.rs:286 `network_msetnx`
的 NX 判定用三域存活探针：:308 `probe_alive_with_prefix(store, prefix_slice, chunk[0])`，
实现在 /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/ttl_sync.rs:425
`probe_alive_with_prefix` → :482 `probe_alive_domain_with_prefix`，
按 String → ObjectEnvelope → Meta 三域依序判活（:500 `Some(true) => KeyTag::Meta` 为
升阶键臂），任域命中即存活，任域磁盘候选即 `Ok(None)` 整体降级慢路径。

慢路径 /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:115 `C::Msetnx` 臂的
非续跑判定段只探两域：:128-134 `read_tag_with(key, KeyTag::String, ..)`、
:135-144 `read_tag_with(key, KeyTag::ObjectEnvelope, ..)`，:145-148 `if alive` 才回 `:0`。
缺第三探（`KeyTag::Meta`），而注释（:126 与 :112-114）自称「对标 C# unified 域 EXISTS」。

后果链：升阶（分层）键的元记录是它在存储里的唯一身份——
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1054-1056 的注释与
:1046-1049 的调用点写明 `promote_collection_to_bftree` 只做「upsert_raw 元记录 +
delete_raw 信封」（实现在 /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs），
信封域已被删除。于是一个大集合键的 Meta 元记录恰为磁盘候选时，快路径返回 `Ok(None)`
降级，慢路径两域皆空 → 判「键不存在」→ :150-157 逐键 `upsert_string` 写入 String 值并回
`:1`。此后 String 域与大集合 Meta 元记录并存，读侧 String 域优先命中并遮蔽原集合，
wbftree 树文件成为无主孤儿。降级触发条件（Meta 磁盘候选）与缺口耦合：MSETNX 一旦因
升阶冷键降级，慢路径必判其不存在，属近乎必现而非边角竞态。

C# 参考

/Users/z/git/db/wedb/garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:349
`MSET_Conditional`：:373-382 判定循环对每个键走
`var status = EXISTS(srcKey, ref unifiedContext);`（unified 域单记录视图，对象/元记录/
字符串一律可见），`status != GarnetStatus.NOTFOUND` 即 `count = 0; error = true`，
:384-390 写入循环遂零写入，回 `:0`。C# 无分层引擎、无「元记录 + 无信封」这一形态，
rust 要对齐的是「任意域任一记录非 NOTFOUND 即存在」这条判据，而非两域枚举。

修法

首选改调既有三域异步单点，把 slow.rs:126-148 的两域内联判定整体替换为
`StorageSession::exists`（/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:231-256，
已是「同步探针 → 三域 String/ObjectEnvelope/Meta await 兜底」的同语义实现，
命中 `GarnetStatus::Ok` 即 alive 回 `:0`）：一次消掉重复实现与漏域两件事，
禁止在慢路径再手写第三探凑出第三份三域判定。
若该单点在慢路径执行域（`StorageSession::new_readonly`，slow.rs:102-110）不可直接用，
则按 exists 的口径在本臂补齐第三探并注记转调阻塞点，不允许维持两域。
两形态都要把 :112-114、:126 的注释与真实判定域数对齐（现注释已自陈「双域」与「对标
unified 域 EXISTS」互相矛盾）。

优先级

功能缺口（写坏存储状态：双域键 + 孤儿树文件，且触发面与降级条件耦合）。
与 task/ing/slow-path-string-key-admin-arms.md 同文件同函数（slow.rs exec_slow_impl），
建议同棒处理以免顶行号；本单只改 Msetnx 臂判定段，不扩体。

交叉引用

1. /Users/z/git/db/wedb/task/ing/slow-path-string-key-admin-arms.md（同函数补臂，宜同批）。
2. task/ing/garnet-api-slow-path-command-split.md（认领前在
   /Users/z/git/db/wedb/next/garnet-api-slow-path-command-split.md，
   其「MSETNX 内联段约 80 行可先拆」与本单同段落；本单落地后其拆分段更短）。
3. 分层迁移臂的键级 TTL 保留（keep_ttl 分流，原
   /Users/z/git/db/wedb/next/tiered-promote-demote-key-ttl.md）只管迁移时的 TTL 旁路，
   与本单的存活判定域数无关，勿混改；判其是否已落地只认代码事实
   （/Users/z/git/db/wedb/wedb/wnode/tests/tiered_promote_demote_ttl.rs 与
   object_store_utils.rs:1023-1032 的 keep_ttl 传参已在树中，视为已合入，本单不动该面）。
4. 并发分拣波把本条原文另投为
   /Users/z/git/db/wedb/next/msetnx-slow-nx-meta-domain-probe.md（无 HEAD 复核的六行转抄），
   主题与本单同一；派单以本单为载体，勿双花。同题相邻但不同靶的是
   task/ing/tiered-drain-envelope-tombstone.md（转抄 stub
   /Users/z/git/db/wedb/next/promote-dual-domain-write-atomic.md），它修的是排空/删空链
   漏清信封域导致的双态残留键，本单只修 MSETNX 慢路径的存活判定域数：两单共用
   「信封域不可当唯一身份」这一事实但改动位点不重叠，可并行，谁后落地谁重核行号。

验收

1. 新增/扩展 e2e：大集合键触发升阶（可参照
   /Users/z/git/db/wedb/wedb/wnode/tests/collection_adaptive_tiering.rs、
   tiered_cmds_align.rs 的建键方式），`DEBUG FLUSHANDEVICT` 使 Meta 元记录落盘候选后
   `MSETNX k v [k2 v2]` 回 `:0` 且零写入，`TYPE k`、`EXISTS k`、`OBJECT ENCODING k`
   仍按原集合应答，存储里无 String 域记录、无新增孤儿树文件。
2. /Users/z/git/db/wedb/wedb/wnode/tests/msetnx_atomic.rs 既有半提交断言全绿；
   慢路径两形态（resume 标记 `b"1"` / `b"0"`）行为与 C# 一致。
3. cargo check 零告警（禁写 allow），不新增第二套存活探针。

盘点补记（qw13.invA+invB msetnx-slow-path-meta-domain-probe）：dev e75716e 复核原样：slow.rs C::Msetnx 臂仍只 read_tag_with(KeyTag::String) + read_tag_with(KeyTag::ObjectEnvelope) 两探，臂内 KeyTag::Meta/storage.exists 零命中。票面修法（改调 storage_session.rs 的 StorageSession::exists 三域单点）仍为首选。

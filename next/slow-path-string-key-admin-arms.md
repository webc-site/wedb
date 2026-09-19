字符串族与键管理族慢路径分派臂缺席：冷键一降级就把内部哨兵错误吐给客户端

来源：next/glm.data.md 条 5（该文件已被并发分拣波消费删除，原文转抄存于
/Users/z/git/db/wedb/next/slow-dispatch-basic-key-admin-arms.md，本档按当下主仓代码重新取证）。
取证基线：主仓 /Users/z/git/db/wedb，
分支 dev，HEAD a7402c4（bb06827、6311510 两轮复核：本档取证文件未变、锚点未位移），全部行号按符号在当下代码复核（原报行号有位移，本档为准）。

结论

命令层约定 `Ok(false)` = 本命令须异步闭环、快路径不残留输出，该信号一律被
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:509
`raw::dispatch(...) == Ok(false)` 接住并挂 `pending_slow`（:561-566
`SlowWait::for_command`），随后由执行域的 `exec_slow_impl` 应答。
但 /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:64
`exec_slow_impl` 的分派表只接了
Msetnx(:120)、Get(:170)、Pf 族(:183)、Dbsize(:197)、Keys(:214)、Scan(:243)、Info(:308)、
MemoryUsage(:408)、Hlen/Scard/Zcard/Llen(:432)、Hcollect/Zcollect(:458)、Customobjcmd(:525)、
RI 族(:558)、Expdelscan(:580)、Del/Unlink(:603)、Mget(:611)、Mset(:624)、哈希族(:656)、
集合族(:680)、列表族(:710)、zset 族(:762)、Geoadd 族(:781)、对象扫描族(:791)，
字符串族与键管理族的其余命令全部落 :831 的 `_ => write_error_raw(&mut output,
RESP_ERR_ASYNC_REQUIRED)` 兜底，客户端遂看到
`-ERR command requires asynchronous completion`。该文案是 rust 自造的内部哨兵
（grep /Users/z/git/db/wedb/garnet/libs 零命中），不是 C# 的任何应答，属未接线标记外泄。
C# 同命令在同步存储上下文里由 CompletePending 系列就地闭环
（如 /Users/z/git/db/wedb/garnet/libs/server/AOF/AofProcessor.cs:625、:674、:756 与
libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:61-63 的
`CompletePendingWithOutputs(wait: true)`），磁盘 pending 不改变应答形态，
客户端任何路径都不该看到这个错误。

缺臂清单与降级出口取证

字符串域（快表 /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/raw.rs:188-206）：
SET 的环形页翻转与 RI 门降级
/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:197-205，
SETNX 的存在性探测 `Ok(None)` 与写回 `Ok(Err(_))` :373-381，
GETSET/SETRANGE/GETRANGE/SUBSTR/STRLEN/APPEND/GETEX/SETEX/PSETEX/SETEXNX 同域共 26 处
`Ok(false)`；INCR/DECR/INCRBY/DECRBY 与 INCRBYFLOAT 的
`UserRead::Deferred`（/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/incr.rs:109）
与 `try_rmw_sync` 的 `Ok(Err(_))`（:124，另 :192 浮点臂）。GET 已有臂，
GET 域另有 9 处降级出口。

键管理域（快表 raw.rs:215-233）：EXISTS 三域探针 + 向量登记表
/Users/z/git/db/wedb/wedb/wnode/src/resp/key_admin_commands/types.rs:205 `Ok(None) => return
Ok(false)`；TTL/PTTL 同文件所在 :278 `network_ttl` 的 `Ok(None)` 出口（keys.rs:309）；
EXPIRE 族 keys.rs:245、PERSIST :269、EXPIRETIME/PEXPIRETIME :351；
GETDEL/RENAME/RENAMENX 读写混合域 keys.rs:147、:157；DUMP types.rs:177；
RESTORE types.rs:94、:107、:119。该文件 `Ok(false)` 合计 22 处。
OBJECT 四子命令（快表 raw.rs:488-496）降级出口
/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/mod.rs:688-710（`degrade` 旗标，
`ttl_of_sync` 已到期即回 nil，否则 `return Ok(false)`）。

现成内核（缺的只是分派臂，不是实现）

取证更新（aof-store-rmw-dead-replay-arms 落地 81c144c0/8e20def3 之后，原报的
`mainstore/advanced_ops.rs:34 rmw_main_store`（StringRMWOp 四臂）与
`main_store_ops::{getdel,append,setrange}` 已整批删除，不再是可用内核；AOF 重放
侧注释见 aof_processor.rs:1039、:1066，不设第二套手写 RMW 臂）。当下字符串域写侧
异步单源只剩存储会话两处：
/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:374
`rmw_string`（HLL 慢路径已在用，hyper_log_log_commands.rs:218/:228/:342）与
同文件 `setex`（会话面消费点
/Users/z/git/db/wedb/wedb/wnode/src/storage/session/txn_proc_view.rs:92-93）；
`mainstore/main_store_ops.rs` 现仅存 :25 `setex` 与 LCS 族。
数值/字符串命令语义的实现在命令层，同步形态一份：
basic_commands/incr.rs:134 `network_increment_by_float`、
set.rs:205 `network_getset`、:289 `network_setex`（:312 `network_setex_impl`）、
:359 `network_setnx`、:389 `network_setexnx`、:604 `network_append`、
key_admin_commands/keys.rs:130 `network_getdel`、get.rs:242 `network_strlen`。
慢路径补臂只允许转调上述单源（同步核 + 异步存储口组合），不得复活 StringRMWOp、
不得在 slow.rs 里另写第二套值处理。
读侧三域存活与 TTL 裁决的异步单点亦已在：
/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:258 `exists`
（String→ObjectEnvelope→Meta 三域 + 磁盘候选 await）、
/Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/ttl_sync.rs:484
`probe_alive_domain_with_prefix` 的异步等价面，TTL/EXISTS/EXPIRE/PERSIST/RENAME 各族臂
按此口径做异步裁决即可，不新起第二套探针。

修法

在 slow.rs:831 的 `_` 兜底之前补两域分派臂，按既有对象族的形态收口
（同文件 :656 哈希族即「多命令共用一臂 + 转调该域 slow 函数」的范式）：

1. 写侧数值/字符串臂：SET 族（Set/Setex/Psetex/Setnx/Setexnx/Getset）与
   SETRANGE/APPEND/INCR 族转调命令层 `network_*` 同步核 + `rmw_string` /
   `StorageSession::setex` 异步口，应答形态与会话快路径逐字节一致
   （含 `+OK`、`:1/:0`、整数值、bulk 串）；INCRBYFLOAT 转调
   `network_increment_by_float`。写入后 TTL 语义保持与快路径同口径
   （`try_upsert_sync` 清新 TTL 的等价异步口）。
2. 读侧裁决臂：EXISTS/TTL/PTTL/EXPIRETIME/PEXPIRETIME/DUMP/OBJECT 四子命令按
   `StorageSession::exists` 与 TTL 异步单点做三域 + 登记表裁决，缺键/过期回
   与快路径相同的 `-2`/`:-1`/`:0`/nil 形态。
3. 键变更臂：EXPIRE/PERSIST/GETDEL/RENAME/RENAMENX/RESTORE 转调存储会话既有异步口
   （RENAME 若仅同步形态存在则补齐异步体，沿用同域两域迁移语义，勿另起第三套迁移）。
4. 兜底 `_` 保留，但收窄为真正不支持慢路径的命令，并在注释写明「本臂出现的
   RESP_ERR_ASYNC_REQUIRED 即分派表漏接线的缺陷信号」，禁止再让常规命令落到这里。
   文案不对外可见是目标态，不接受「改成别的错误文案」作为修法。

分批与规模：建议三棒（读侧 TTL 族 + EXISTS / SET 与 INCR 写侧 / RENAME·GETDEL·
DUMP·RESTORE·OBJECT），每棒独立可验，单棒只动 slow.rs 与必要内核接线，
不重写快路径命令体。

优先级

功能缺口，且是本档三张慢路径票里最高的一档：客户端在冷键/环形页翻转下拿到内部哨兵
错误、命令完全不生效（非仅文案分叉）。按「死代码 > 重复/多套架构 > 污染扩散 >
功能缺口」序，本单与 task/ing/msetnx-slow-path-meta-domain-probe.md 同域，
后者是本单 Msetnx 臂内的正确性缺陷，两单可同棒合并（同文件 slow.rs，避免串行顶行号）。

交叉引用

1. /Users/z/git/db/wedb/task/ing/msetnx-slow-path-meta-domain-probe.md（同函数 Msetnx 臂的
   三域判定缺陷，建议与本单读侧臂同批）。
2. task/ing/garnet-api-slow-path-command-split.md（认领前在
   /Users/z/git/db/wedb/next/garnet-api-slow-path-command-split.md，纯移动拆分
   exec_slow_impl）。次序建议本单先行：给未接线命令补臂会改函数体量与行号，
   拆分后置更稳；两单同文件，勿并行。
3. /Users/z/git/db/wedb/task/ing/resp-null-protocol-single-source.md 覆盖 nil 帧版本感知，
   本单新臂的 nil 应答直接调该单收敛后的版本感知单点，勿再写 `$-1\r\n` 字面量。
4. lua 面同类缺口（脚本内命令拿不到慢路径应答）见
   /Users/z/git/db/wedb/task/ing/lua-call-pending-suspend-handoff.md，两单修法不同层，
   但本单补臂后 lua 面的 GET 等命令只需拿到应答即可闭环，宜先落本单。
5. 并发分拣波把本条原文另投为
   /Users/z/git/db/wedb/next/slow-dispatch-basic-key-admin-arms.md（无 HEAD 复核的原文转抄），
   主题与本单同一；派单以本单为载体，勿双花。

验收

1. `SET k v` → `DEBUG FLUSHANDEVICT`（现成用例见
   /Users/z/git/db/wedb/wedb/wnode/tests/debug_flushandevict.rs、
   /Users/z/git/db/wedb/wedb/wnode/tests/resp_slow_path.rs 同形态）后，
   SETNX/TTL/APPEND/EXISTS/OBJECT ENCODING/RENAME/GETDEL/DUMP/RESTORE 逐命令回真实应答，
   全链路 grep 不到对客户端写出的 RESP_ERR_ASYNC_REQUIRED。
2. 与 RESP2/RESP3 两版本、事务与 AOF 回放复用同内核的结果逐字节一致；
   cargo check 零告警（禁写 allow）。

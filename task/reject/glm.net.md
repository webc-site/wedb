来源：/Users/z/git/db/wedb/next/glm.net.md 条 3（客户端握手 SETNAME 帧与 C# 副本分叉）。裁决：观点不成立，整条删除。

原文要点

rust wconn 握手第二条发 CLIENT SETNAME <name>（/Users/z/git/db/wedb/wedb/wconn/src/network/mod.rs:61-80，
:77 发 ["CLIENT","SETNAME",client_name]）；声称 C# 副本同位发的是 CLIENT <clientName> 而没有 SETNAME
子命令 token，并据此归因为上游回归 commit d9dd1a2b9「cleanup clientname setup」把字段 SETNAME 重命名为
clientName 时将调用点从 ExecuteForStringResultAsync(CLIENT, SETNAME) 错改为
ExecuteForStringResultAsync(CLIENT, clientName)，结论是 C# 副本侧所有传非空 clientName 的 GarnetClient
建连必失败（含 gossip 节点客户端 client_name = Gossip-{endpoint}），并要求在 rust handshake 注释或
js/check/ignore 里声明「C# 现行帧为回归 bug、rust 保留 SETNAME 属有意偏离」。

拒绝原因

一、事实错误：C# 的 clientName 不是字符串名，而是子命令数组。
/Users/z/git/db/wedb/garnet/libs/client/GarnetClient.cs:98 声明 `readonly Memory<byte>[] clientName`，
:165 构造赋值 `this.clientName = clientName != null ? ["SETNAME"u8.ToArray(),
Encoding.ASCII.GetBytes(clientName)] : null` —— 数组首元素就是 "SETNAME" 字面量。调用点
:249（同步 Connect）与 :295/:309（ConnectAsync）走的是重载
`ExecuteForStringResultAsync(Memory<byte> respOp, ICollection<Memory<byte>> args)`
（/Users/z/git/db/wedb/garnet/libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:67），
respOp = CLIENT（:44 `"$6\r\nCLIENT\r\n"`）、args = 该数组，落网帧即 CLIENT SETNAME <name>。
同一文件 :45 的 SETINFO 也是同形态数组常量，二者调用写法一致，不存在「一个带子命令 token、
一个不带」的差别。

二、git 考证方向错：d9dd1a2b9 的 diff（/Users/z/git/db/wedb/garnet 内 `git show d9dd1a2b9 -- libs/client/GarnetClient.cs`）
显示改动只是把字段名 SETNAME 改成 clientName（含 :93-98 声明、:158-165 赋值、:226-229 与 :272-275 两处
if 判空与调用），赋的值 `["SETNAME"u8, name]` 一字节未动。即改名前后线上帧完全相同，无回归、无建连必败。
本仓服务端子命令表也一致接受该帧（/Users/z/git/db/wedb/garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs:380
`("SETNAME", RespCommand.CLIENT_SETNAME)`）。

三、即便按原文假定成立，其唯一动作是「往 rust 注释与 ignore 里写入一条对 C# 代码的错误描述」。
两侧行为本就一致（都发 CLIENT SETNAME <name>），无需声明任何偏离；按此立案反而会把一个不存在的
分叉固化成文档污染：ignore 与注释的理由正文同样是待核实的主张，核实不到的描述不许转写进代码文档。

附带核实的真问题（不在本条射程，另行处理）：/Users/z/git/db/wedb/garnet/libs/client/GarnetClient.cs:291-317
ConnectAsync 里 SETINFO + SETNAME 段重复出现两次，是上游副本的冗余，与帧名正确性无关，
rust 侧 handshake 只发一次是对的，不构成立项理由。

第二路分拣独立复核（2026-09-19，只认代码事实）：结论同上，维持拒绝。独立取证：
/Users/z/git/db/wedb/garnet/libs/client/GarnetClient.cs:98 字段声明为 `readonly Memory<byte>[] clientName`
（不是名称字符串），:165 赋值 `["SETNAME"u8.ToArray(), Encoding.ASCII.GetBytes(clientName)]`，
与 :45 的 SETINFO 数组常量同形态；对位 rust 发帧点 /Users/z/git/db/wedb/wedb/wconn/src/network/mod.rs:77
`exec(tx, &["CLIENT", "SETNAME", client_name], latency)`。两侧线上帧同为 CLIENT SETNAME <name>，
无分叉、无需任何「有意偏离」注释。

待主代理清理的残留载体（本代理无写权，未动）：
/Users/z/git/db/wedb/next/client-handshake-setname-divergence-note.md（2323B，正文为本条原文照抄
加「优先级：低」头）与本档案结论直接矛盾，属先立票后拒绝的漏剪，应删除，勿派 dev
（按它开工只会在 wconn handshake 注释与 js/check/ignore 里写入一条对 C# 的虚假描述）。
本代理独立复核同判：该壳文件当下仍在 next/（2323B，mtime 13:23），与 task/ing 下另六份
本文件分拣产物的「原文照抄壳」并存 —— 那六条成立待做的条目也被并发拆条复制成了 next/ 单问题壳
（pending-lat-zero-record-point、connection-exit-shutdown-close-notify、
initiate-replica-sync-typo-spread、replication-send-buffer-byte-cap、
replication-history-flush-mutex、replica-attach-recovery-lock-window），
六份壳均无取证订正，权威载体是 task/ing 对应票（各票头部已写明载体唯一性）；
其中 replica-attach-recovery-lock-window 一支壳认领后的目标路径与 task/ing 同名，git mv 会覆盖，
派单时须先剪壳。SETNAME 一支没有对应的 task/ing 票（本条已拒），壳件应直接删。

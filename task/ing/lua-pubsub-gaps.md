# Lua/PubSub 域四条甄别与实现规划

来源 next/glm.md 条 6、10、11、18。逐条对照 C# 核实结论如下。

## 一 no-script 位图未接线（成立）

对标 LuaRunner.cs:242（runner 构造即挂 noScriptStart/noScriptBitmap）与
AdminCommands.cs:95-115 CheckScriptPermissions、RespServerSession.cs:653
（CheckACLPermissions && CheckScriptPermissions）、:710（拒绝回
RESP_ERR_NOSCRIPT "ERR This Redis command is not allowed from script"）。

C# 语义：位图挂上后不摘除（session 级常驻）；脚本内 redis.call 经
ScratchBufferNetworkSender 重入 ProcessMessages 主循环同门拦截；命中位图
回 NOSCRIPT 且 commandStats.IncrementRejected。ACL 失败短路（&&）不再查
no-script；no-script 失败不回 NOPERM。

实现：
1. wresp/cmd_strings.rs 增 RESP_ERR_NOSCRIPT 常量（对标 CmdStrings.cs:207）。
2. RespServerSession 增 no_script_start/no_script_bitmap 字段（对标
   AdminCommands.cs:23-24），位图源经 OnceLock 静态缓存（C# NoScriptDetails
   为 static readonly），复用既有 no_script_details() 构建。
3. wlua ScriptingApi 增 attach_no_script_bitmap 默认空方法（对标
   LuaRunner.cs:242 挂载动作，wlua 不感知会话类型）；commands.rs 三个装载点
   （EVALSHA 上升装载/EVAL/SCRIPT LOAD）try_load_runner 成功后挂载——精确
   对标 TryGetFromDigest 命中不构建 runner 不重复挂、EVALSHA miss 不挂；
   编译失败分支 C# 已挂位图，rust 不挂（runner 构建失败路径），注释记录差异。
4. RespScriptingApi 实现挂载（写会话字段，Arc<[u64]> 共享静态源）。
5. process_messages 命令门拆三态：ACL 失败回 NOPERM/NOAUTH（现状）；
   no-script 失败回 NOSCRIPT。rust 侧无 commandStats 拒绝计数面，不造第二套。

测试：no_script_details 位图含 SUBSCRIBE/EVAL 等 NoScript 命令；脚本内
redis.call SUBSCRIBE 被 NOSCRIPT 拦截；门不误伤普通命令。

## 二 集群态 PUBLISH/SPUBLISH 缺跨节点广播钩子（成立）

对标 PubSubCommands.cs:108-112（SPUBLISH && clusterSession == null →
CLUSTER_DISABLED）、:140-147（EnableCluster 时 BlockingWait
clusterProvider.ClusterPublishAsync 后才应答 numClients）。现状 rust shard
恒回 CLUSTER_DISABLED，且无转发钩子。

实现（复用现有注入模式，不造第二套回调机制）：
1. wnode ClusterSessionFace 增 cluster_publish(cmd, channel, message) 默认
   false（无集群实现），ClusterSessionVtable 增函数指针；对标 C#
   IClusterProvider.ClusterPublishAsync 的会话侧切面投影（C# 直连 provider，
   rust 会话→集群域唯一通道是 ClusterSession 切面，注释说明）。
2. wedb ClusterSession 实现：cluster_manager 在场时内联 block_on 驱动
   try_cluster_publish_async（compio 单线程执行域等价 C# 网络线程
   BlockingWait；参照 wnode vector_store_callbacks 的 block_on 先例）。
3. wpubsub PubSubSessionCommands 增 has_cluster_session（默认 false）与
   cluster_publish（默认空）；network_publish 对标 C# 顺序改造：
   SPUBLISH 无集群会话 → CLUSTER_DISABLED（广播前）；本地广播 + drain 推送
   后，集群会话在场则转发；单机路径（默认实现）行为不变。

测试：mock 会话单机行为不变（SPUBLISH 回 CLUSTER_DISABLED、PUBLISH 本地
广播）；集群面在场时回调被触发且应答序正确。

## 三 Lua 装配缺口（部分成立）

3a status_reply：成立。C# LuaRunner.Loader.cs:136-138 直接
`return text`（裸 string → RESP simple string，返回值转换层已支持）；rust
loader.rs 返回 `{ ok = text or "" }` 不符，改回对齐。

3b set_user_handle 传播：不成立，按死代码删除处理。C# 传播实质是喂给
SessionScriptCache 内嵌 processor（独立 RespServerSession）的 ACL 门；rust
无内嵌 processor，脚本内 ACL 检查经 RespScriptingApi 直达外层会话
acl_user_handle（认证后天然最新），缓存侧 user_handle: Option<u64>（自造
类型，C# 为 UserHandle 引用）零生产消费，属架构等效下的死数据。删除
SessionScriptCache 的 user_handle 字段/set_user_handle/user_handle getter，
js/check/ignore/libs/server/Lua/SessionScriptCache.yml 登记 SetUserHandle。

范围外记录（不改）：C# RespServerSession.SetUserHandle(:368-371) 还传播
clusterSession，rust set_user_handle 无集群传播、wnode ClusterSessionFace
无 SetUserHandle 切面方法，属集群装配面缺口，另案处理。

## 四 HCOLLECT/ZCOLLECT `*` 缺 already-in-progress 互斥（成立）

对标 Common.cs:810（collectLock.TryWriteLock 失败回 NOTFOUND）→
AdminCommands.cs:666-678/699-711（default 分支回 RESP_ERR_HCOLLECT/ZCOLLECT_
ALREADY_IN_PROGRESS）；锁源 HashOps.cs:15 _hcollectTaskLock、
SortedSetOps.cs:17 _zcollectTaskLock（per StorageSession 各一把）；Dispose
时自旋等锁（StorageSession.cs:154-160）。现状 rust 全库收集臂
（garnet_api.rs exec_slow `*` 臂）无互斥，wresp 两常量零引用。

实现：
1. StoreGarnetApi 增 hcollect_in_progress/zcollect_in_progress 两个
   AtomicBool（对标 SingleWriterMultiReaderLock 单写位，粒度对齐 C# per
   storageSession）。
2. exec_slow `*` 臂 CAS 抢占：失败回对应 already-in-progress 文案；成功则
   扫描收集后释放。HCOLLECT/ZCOLLECT 各自独立锁位（C# 同）。
3. C# Dispose 自旋等锁由 Arc 所有权天然承担（exec_slow future 持 Arc 克隆，
   扫描完成才释放），无需对应物，注释说明。

测试：置位后第二次 HCOLLECT `*` 回 already-in-progress 文案；ZCOLLECT 同；
释放后恢复正常。

## 执行序

1. wresp 常量 + wlua loader.rs（一行对齐）
2. wlua cache.rs 死字段删除 + ignore 登记
3. wnode no-script 位图接线（字段/静态源/trait 挂载/门改造）
4. 集群发布钩子（wnode 切面 → wpubsub → wedb 实现）
5. HCOLLECT/ZCOLLECT 互斥
6. ./clippy.sh 零警告、./test.sh 全过、bun ./js/check.js 无新增缺失
7. 合并 dev 后并回主目录

# ACL LIST 与 USERS 转异步慢路径 await 全扫

来源：next/zcode.my.md 问题 3

## 裁决

成立，票面链路逐条核实无误。本票不是照抄 C#（C# 的 ACL 用户表是进程内
`AccessControlList` 字典，`NetworkAclList` 读内存句柄表，同步执行天然安全），而是让
本仓「ACL 零全局内存 + 存储为唯一真源」（doc/zh/db.md §3，transpile SKILL 明文自定义
设计）在自己的链路上正确闭环：本仓 LIST/USERS 的真源是混合日志，扫描必然是异步 I/O。

## 根因与证据

1. wedb/wnode/src/resp/acl_store.rs:143 `for_each_user` 为 `async fn`，内核是
   `store.hlog().scan(begin, tail, ..)`，跨 await 的逐页读（冷段落盘回读）。
2. 同文件 :179 `for_each_user_blocking` 用 `blocking_wait(self.for_each_user(..))`
   把上述异步扫描压回同步收割。
3. wedb/wnode/src/resp/acl_commands.rs:122 与 :184 —— `network_acl_list` /
   `network_acl_users` 各调一次 `for_each_user_blocking`。
4. 二者只在同步分派主路径被调用：wedb/wnode/src/resp/garnet_api/mod.rs:450 ACL 臂在
   `enter_batch` 之前直接 `session.process_acl_commands(cmd, &store)` 并 `return`，
   全程处于 compio 网络泵所在线程；compio 为 thread-per-core，该线程 `block_on` 收割
   磁盘 I/O 期间，同核其余连接的读写出全部停滞。
5. `for_each_user_blocking` 全仓消费者仅上述两处，无其他调用方。

## 单点机制核实（禁止新造挂起-续做）

只读全扫命令已有唯一的异步慢路径通道，本票复用之，不新增第二套：

1. 挂起体 `SlowWait` / `SlowFuture`：resp/slow_path.rs，会话字段 `pending_slow`，
   网络泵 resp/net/handler/drive.rs:202 `take_slow_wait` 后 await `resolve` 回写应答。
2. 存储域转挂骨架 `RespServerSession::route_slow_command`
   （resp/admin_commands.rs:166），SAVE/BGSAVE/LASTSAVE/COMMITAOF/EXPDELSCAN/DEBUG
   共用同一构造点 `SlowWait::for_command` → `GarnetApiFace::exec_slow`。
3. 慢路径执行表 `StoreGarnetApi::exec_slow_impl`（resp/garnet_api/slow.rs），
   KEYS / DBSIZE / SCAN / INFO(KEYSPACE·HLOGSCAN) 等全扫命令均在此 await 扫描。
4. 会话本地事实流入慢路径的既有口径是「快照尾参」：exec 在降级快照后
   `snapshot.push(...)`（INFO 的 8 字节 LE 库数上限、MSETNX 的续跑标记、HSCAN 的
   COUNT 上限、CUSTOMOBJCMD 的命令名），`exec_slow_impl` 从 `args.last()` 解码。
   ACL 的命名空间 / 认证器档位 / 引导态 default 用户同样只驻会话侧（慢路径无会话
   可达面），故走同一尾参口径，不新建并行通道。

## 修法

1. acl_commands.rs：`network_acl_list` / `network_acl_users` 改为 `async fn`，内核直接
   `store.for_each_user(..).await`；引入慢路径快照结构承载 `caller_namespace`、
   ACL 认证器在场位、引导态 default 用户的应答正文，并在同步侧提供
   `route_acl_scan`（arity 与快照采集后即转挂），会话侧门禁与渲染实现仍一处定义。
2. garnet_api/mod.rs：ACL 臂对 `AclList | AclUsers` 调 `session.route_acl_scan` 后
   return，不进 `process_acl_commands`；`process_acl_commands` 的 ACL 子命令臂收敛为
   余下同步命令（点查/写不属本票）。
3. admin_commands.rs：`route_slow_command` 增 `extra` 快照尾参入参（既有六处传空），
   仍是一个转挂骨架，不复制机制。
4. garnet_api/slow.rs：`exec_slow_impl` 增 `C::AclList | C::AclUsers` 臂，解快照后
   await 扫描渲染。
5. acl_store.rs：删除 `for_each_user_blocking`（改后零消费者），模块头注同步去掉
   「流式扫描经 blocking_wait 收割」的表述；点查冷记录回读的 `blocking_wait` 属
   doc/zh/db.md §3 既有设计的点查降级路径，不在本票范围。

## 同域漏网核实

ACL 族其余命令在同步臂只做点查或纯内存：CAT（静态分类表）、WHOAMI（会话句柄）、
GENPASS（随机数）、LOAD/SAVE（即时持久化空操作）、SETUSER/DELUSER/GETUSER/AUTH
（`AclStore` 点读写，键定位恒 `[ns][db0][KeyTag::Acl][user]`，非全扫）。全仓
`for_each_user` 消费者仅 LIST/USERS 两处，无同源漏网点。

拒绝结论：判净（核对 C# AccessControlList.cs 与 RespServerSession.cs，ACL 规则变更经 AclStore 互斥与代数递增广播闭环，会话执行前预门无锁即时收敛，读前采样消除孤儿窗口，DELUSER 吊销在途连接优于原型，无缺陷无分叉）

ACL 控制面规则更新与会话代数广播审查报告

一、审查视角与背景说明
审查视角：ACL 控制面规则更新与会话代数广播 (ACL SETUSER/DELUSER 规则热更、会话鉴权代数广播同步、跨命令执行期权限即时收敛与单向闭环)
核查目标与范围：
1. 核查 wedb/wacl/ 或 wnode/src/resp/acl/ 中 ACL 控制面管理逻辑。
2. 核查 ACL SETUSER, ACL DELUSER 规则变更后，如何向所有并发工作线程及已建立连接的 ClientSession 广播。
3. 核查全局代数（generation / epoch）递增与会话本地缓存的同步时机，是否能在下一次命令执行时无锁即时生效，是否存在孤儿权限窗口。
4. 对标 garnet/libs/server/ACL/AccessControlList.cs 与 RespServerSession.cs 鉴权逻辑。
5. 核验 doc/zh/deviations.md 既有在册条款，严禁将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/ACL/AccessControlList.cs:AccessControlList
garnet/libs/server/ACL/UserHandle.cs:UserHandle.TrySetUser
garnet/libs/server/Resp/ACLCommands.cs:NetworkAclSetUser
garnet/libs/server/Resp/ACLCommands.cs:NetworkAclDelUser
garnet/libs/server/Resp/RespServerSession.cs:RespServerSession.CheckACLPermissions
garnet/libs/server/Resp/AdminCommands.cs:CheckACLPermissions

核查确证事实：
1) 共享用户句柄与 CAS 原位换新：
在 garnet/libs/server/ACL/AccessControlList.cs 中，ACL 规则维护在全局并发字典 ConcurrentDictionary<string, UserHandle> _userHandles。每个客户端连接 RespServerSession 直接持有全局共享的 UserHandle 引用。当执行 ACL SETUSER 时，NetworkAclSetUser 构造新 User 实例，通过 userHandle.TrySetUser 执行 Interlocked.CompareExchange 原位替换引用指针。所有已建立连接在后续命令执行校验 CheckACLPermissions 时直接解引用 _userHandle.User，即时观察到权限变更。
2) DELUSER 句柄孤儿滞留缺陷：
在 garnet/libs/server/Resp/ACLCommands.cs:NetworkAclDelUser 中，删除用户仅调用 AccessControlList.DeleteUserHandle 从全局字典中移除键值映射。已建立连接仍然持有原有 UserHandle，Garnet 并未对在途会话执行任何撤销或断开操作，已认证客户端可以无限期沿用原有权限执行后续命令，直至物理连接断开。此为上游原型既有的权限滞留缺陷。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与控制面管理逻辑
rust 文件与函数：
wedb/wacl/src/access_control_list.rs:AccessControlList
wedb/wacl/src/user_handle.rs:UserHandle
wedb/wnode/src/resp/acl_commands.rs:RespServerSession::network_acl_set_user
wedb/wnode/src/resp/acl_commands.rs:RespServerSession::network_acl_del_user
wedb/wnode/src/resp/acl_store.rs:AclStore::write
wedb/wnode/src/resp/acl_store.rs:AclStore::delete
wedb/wkv/src/store/mod.rs:WedbStore::bump_acl_generation
wedb/wkv/src/store/mod.rs:WedbStore::acl_generation
wedb/wnode/src/resp/resp_server_session/auth.rs:RespServerSession::set_user_handle
wedb/wnode/src/resp/resp_server_session/auth.rs:RespServerSession::acl_refresh_park_needed
wedb/wnode/src/resp/resp_server_session/auth.rs:RespServerSession::refresh_acl_mount_if_stale
wedb/wnode/src/resp/admin_commands.rs:RespServerSession::check_acl_permissions
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession::process_messages
wedb/wnode/src/net/handler/drive.rs:ConnectionHandler::drive
wedb/wnode/src/aof/aof_processor_store_ops.rs:store_upsert
wedb/wnode/src/aof/aof_processor_store_ops.rs:store_delete

核查确证事实：
1) 存储为唯一真源与不可变连接私有句柄：
wedb 彻底杜绝全局可变用户字典。ACL 用户规则持久化于底层存储 KeyTag::Acl 记录，AccessControlList 仅承接引导期 default 用户单例。UserHandle 为只读快照引用，由各连接私有持有（acl_user_handle: Option<Arc<UserHandle>>），热路径位图判定无锁、无 CAS 争用。
2) 全局变更代数与写删收敛出口：
WedbStore 维护原子标量 acl_generation: AtomicU64。控制面写删操作（AclStore::write 与 AclStore::delete）受串行互斥锁 lock_acl 保护，在底层存储记录成功写入或墓碑删除生效后，调用 bump_acl_generation() 以 fetch_add(1, Ordering::Release) 推进全局代数。AOF 及主从复制回放 KeyTag::Acl 条目的 store_upsert 和 store_delete 同样对位推进代数，保证节点间与从机视图对齐。
3) 会话本地挂载与跨命令执行期即时生效：
会话本地记录 acl_mount: Option<AclMount>，保存挂载句柄时的代数快照 generation: Option<u64>。在每条命令进入执行循环（process_messages）前，同步门 check_acl_permissions 首先调用 acl_refresh_park_needed()，以 Ordering::Acquire 比较会话本地代数与引擎全局代数：
若代数相等，零存储点查、零锁开销，直通内存位图判定 acl_permits；
若代数落后，返回 AclGateVerdict::Parked，将命令读取游标原样回退，退出同步消费段；网络驱动层 drive 在异步上下文中驱动 pending_acl_refresh_fut，调用 refresh_acl_mount_if_stale 点查底层存储。
4) 读前采样消除孤儿权限窗口：
在认证臂 authenticate_user_via_store 与自改生效臂中，采取代数读前采样纪律：必须在点查存储记录之前采得引擎代数。若在点查与挂载窗口之间发生并发 SETUSER/DELUSER 推进代数，配对代数必然落后于引擎最新代数，下一条命令的鉴权预门立即感知落后并触发二次刷新收敛，从根本上杜绝了旧句柄搭配新代数导致孤儿权限长期滞留会话的竞态漏洞。测试用例 acl_auth_store_generation_presample_no_stale_privilege 针对该窗口进行了严格闭环断言。
5) DELUSER 即时收敛与安全闭环：
当用户被 ACL DELUSER 删除后，代数推进。已连接会话在下一命令预门触发 refresh_acl_mount_if_stale，存储点查返回 None，会话执行 revoke_acl_mount 清除本地句柄与挂载态，原命令重评被拦截并返回 -NOAUTH Authentication required.。修复了 Garnet 允许已删用户会话继续无期执行命令的安全漏洞。

四、核查视角规约确证与偏离对齐
1. 架构改良在册性核验：
核验 doc/zh/deviations.md 及 doc/zh/db.md，wedb 将 ACL 用户规则统一下沉至底层存储（KeyTag::Acl）并配合全局代数无锁广播收敛，属于法定的零全局可变字典与单物理存储多租户隔离架构改良。严禁将该自研改良误报为对 Garnet 原型 CAS 句柄字典的偏离缺陷。
2. 特值语法偏离在册确证：
doc/zh/deviations.md 第 134 条已明确登记：ACL 口令哈希携空白在 C# 经 byte.Parse HexNumber 容忍收受，而 Rust 侧经 hex_decode 精确拒收，属于在册有意偏差，符合系统安全性约束。

五、结论总结
本席对 C# Garnet 原型与 Rust wedb 仓内全链路进行了深度逐项核查。确证 wedb 的 ACL 控制面管理逻辑完备闭环：
1. ACL SETUSER 与 DELUSER 规则热更通过底层存储落盘与串行互斥锁确保写操作原子性；
2. 全局代数 bump_acl_generation (Release) 与会话预门 acl_refresh_park_needed (Acquire) 形成高效无锁的单向广播机制；
3. 代数读前采样严格消除了并发挂载窗口内的权限滞留风险；
4. 会话端在下一次命令分派前异步无锁收敛，DELUSER 后连接私有句柄即时撤销，彻底解决上游原型的孤儿会话权限泄漏缺陷。
全链路代码逻辑自洽、架构纯洁、测试锁面齐全，无任何漏项与并发漏洞。

视角结论:已穷尽

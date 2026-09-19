优先级：中
分拣注记（qw.my 第 11 轮条 3 拆出；浅核 2026-09-19：wacl/src/user.rs :88/:90 AtomicBool、:92 ArcSwap<CommandPermissionSet>、can_access_command :265 在场（行号小漂移）；与 ing/acl-setuser-live-connection-propagation.md 边界成立——那票管跨连接撤权传播语义缺口，本票管连接本地对象内部并发原语形态，修法须与那票排执行序避免互相踩形态）

连接本地 User 仍按 C# 全局共享句柄形态持并发原语：内层原子位图换代零竞争者
问题：wacl/src/user.rs:84-95 的 User 同时挂着四套并发机制——:88/:90 AtomicBool、
:92 ArcSwap<CommandPermissionSet>、:94 ConcurrentSet<AclPassword>。而鉴权热路径每命令一次原子
换代读（:266 can_access_command 与 :273-278 can_access_custom_command 的 enabled_commands.load()，
ArcSwap Guard 带 Arc 引用计数往返）；变更面 :298-334 add_category 及 :347/:394/:442/:484/:523 同型
各臂写成 `&self` + compare_and_swap 重试环，每应用一条规则先 cur.copy()（:310/:354 整体位图 +
描述串克隆）再 Arc::new 换代。
共享性实测：全仓 User 的改权入口只有 wacl/src/access_control_list.rs:31-32（启动期 default user）
与 wacl/src/user.rs:188/:208（from_rule_bytes 就地新建后 store），生产侧构造点
wnode/src/resp/acl_commands.rs:311/:562/:637/:741 全部是「点查存储 → from_rule_bytes 新建」，
每个 Arc<UserHandle> 只被一个会话持有（同结论见 task/ing/acl-setuser-live-connection-propagation.md
现状 1/5），即 CAS 环在本架构下没有第二写者，永不旋转。
C#：garnet/libs/server/ACL/User.cs:174/:229/:303/:358/:400/:441 的 Interlocked.CompareExchange 环
成立前提是全连接共享同一 User 实例，:669 `readonly HashSet<ACLPassword> _passwordHashes = []`
配 :42/:450/:462 的 lock 承接口令集——共享 + 可变才有原子换代与锁。我方按 SKILL:34 删掉共享后，
:92/:94 两层机制的对象已不存在，只剩每命令的原子间接与每连接一份并发哈希表的构造成本。
修法：User 改为构造后不可变（普通 bool 字段 + CommandPermissionSet 直接持有；口令集退化为
Vec<[u8; NUM_HASH_BYTES]> 小包），add_*/remove_* 由 AclParser 以 &mut self（或局部累积量）一次性
落定、消掉重试环与逐规则 copy；can_access_command 变纯位图读；外层 wacl/src/user_handle.rs:12
的 ArcSwap<User> 保留（自改刷新臂 :741 是唯一真实写者）。
条款：SKILL.md:34（权限句柄连接本地持有、内存与用户总量脱钩）、SKILL.md:17（并发字典/set 的适用
前提是跨线程共享）、SKILL.md:67（不搞多套机制）。

归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 b77e563（P4），收口形态：UserHandle 删 load() 保 user() 单名，三消费点随改，净-6 零行为变更。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r16-aclhandle，2026-09-26）定级 P4
核验记录（现码逐锚复跑）：
1. wedb/wacl/src/user_handle.rs 现树亲验：user() :25-29 与 load() :31-35 签名
   （pub fn(&self)->&Arc<User>）、#[inline]、函数体 &self.user 逐字符同体，成立
2. load() 全仓消费点恰 3 处成立：auth.rs:524、admin_commands.rs:177、:180（全仓
   grep .load() 其余命中均系 wkv/wtls/wbftree 等 ArcSwap 无关同名，wacl 内除定义外零消费）
3. user() 消费点超票面：acl_commands.rs:196/:346/:348/:599/:600/:777、core.rs:719、
   garnet_acl_authenticator.rs:34、user.rs:712、tests 12/:28/:46 抽验均成立
4. C# 对位成立：garnet/libs/server/ACL/UserHandle.cs:39 User 属性唯一读面、
   TrySetUser :48 起为写面 CAS；消费侧 AdminCommands.cs:139/:174-175 与
   RespServerSession.cs:306 均经 .User 解引用；deviations.md 无本条登记（§109 别案）
5. 非重复：task/ing 空、reject/issue/done 无同轴票（zcode-r165c-aclbroadcast 系
   TrySetUser 不移植别案）；方案合 fix.md 单套机制最小改动，验证锚（clippy.sh、
   test.sh、三测试文件）在位齐备

UserHandle 双名同体访问器 user()/load() 违反单机制与字段直取纪律

审核结论：通过（审核席 zcode-r17-review-dualname，2026-09-26，P4 清理级）

审核亲验记录（全部属实）：
1. 双名同体：wedb/wacl/src/user_handle.rs:26-29 user() 与 :32-35 load() 签名
   （pub fn (&self) -> &Arc<User>）、#[inline]、函数体 &self.user 逐字符相同，
   仅函数名与文档注释不同，两条注释各自对标同一个 C# User 属性
2. load() 全仓消费点恰 3 处（已过滤无关同名方法，无 UserHandle::load 函数指针
   或 use 引入形式）：auth.rs:524、admin_commands.rs:177、:180
3. user() 消费点不止票面所举 2 处，另有 acl_commands.rs:196/:348/:600/:777、
   resp_server_session/core.rs:719、wacl/src/auth/garnet_acl_authenticator.rs:34、
   wacl/src/user.rs:712、wacl/tests/access_control_list_tests.rs:12/:28/:46
   （本方案不动 user()，仅列举证全）
4. C# 对位：garnet/libs/server/ACL/UserHandle.cs:39 User 属性为唯一读面，
   TrySetUser(:48-56) 为写面 CAS；user_handle.rs:9-12 注释在案（存储单点模型
   下不移植）。deviations.md 无本条登记（§109 系 SETUSER 残留条，别案），无重复
5. 分流注记：票面文件 untracked，git mv 不可用，已用普通 mv 落 task/todo/

整理优化执行方案（供 task/fix.md 直接消费）：
1. 删 wedb/wacl/src/user_handle.rs:31-35（load() 含其文档注释与 #[inline]），
   保留 user() 单名对齐 C# User 属性唯一读面
2. 三处调用点改名 handle.load() -> handle.user()（返回类型同为 &Arc<User>，
   零行为变更）：wedb/wnode/src/resp/resp_server_session/auth.rs:524、
   wedb/wnode/src/resp/admin_commands.rs:177、:180
3. 验证：./sh/clippy.sh 零警告（wacl/wnode 域）+ ./test.sh；
   ACL 域既有用例（wnode/tests/acl_tests.rs、wacl_default_record_fallback_test.rs、
   wacl/tests/acl_command_whitespace_no_trim_locks.rs）保持绿即证无行为漂移

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# UserHandle 的读访问面唯一：User 属性（garnet/libs/server/ACL/UserHandle.cs:UserHandle.User），
全部消费点（RespServerSession.CheckACLPermissions、AdminCommands 权限判定等）均经该属性解引用
取用户；写面 TrySetUser 为共享句柄 CAS 换新（rust 存储单点模型下不移植，user_handle.rs:9-12
注释在案）。即 C# 侧「取当前用户」只有一个名字、一个语义。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust UserHandle（wedb/wacl/src/user_handle.rs）对同一私有字段 user: Arc<User> 暴露两个完全
同体的只读访问器：user()（:26-29）与 load()（:32-35），函数体逐字符相同（均返回
&self.user），仅文档注释措辞不同（「取当前用户（最新版本）」对「零克隆瞬态读取」），且两条
注释各自对标同一个 C# User 属性。仓内双名并存、各有消费点：user() 见
wedb/wnode/src/resp/acl_commands.rs:346（in_memory_default_user 的 handle.user().name 比对）
与 :599（network_acl_get_user 兜底）；load() 见
wedb/wnode/src/resp/resp_server_session/auth.rs:524（acl_permits 位图判定）与
wedb/wnode/src/resp/admin_commands.rs:177/:180（check_acl_permissions 两臂）。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
无运行时危害（纯只读转发），属接口面单机制违例：同一值两个名字构成无业务价值的第二访问面，
命中审查标准板块1「接口最小暴露与字段直取：模块内部专属结构体直接公开内部字段，严禁编写无
业务价值的多余包装函数」与「全链路唯一机制」条款（rust_review 纪律「pub 接口要设计，低耦合
高内聚」同向）。双名漂移风险：后续任一侧演化（如 load() 若被改为重读存储的语义）另一名不随
动即静默分叉；每轮审查与对账都要重复甄别两名是否同体，纯认知税。

涉及代码：
rust 文件与函数：
wedb/wacl/src/user_handle.rs:UserHandle::user
wedb/wacl/src/user_handle.rs:UserHandle::load
（消费点：wedb/wnode/src/resp/resp_server_session/auth.rs:524、
wedb/wnode/src/resp/admin_commands.rs:177、wedb/wnode/src/resp/admin_commands.rs:180）

对应 c# 文件与函数：
garnet/libs/server/ACL/UserHandle.cs:UserHandle.User

精炼执行方案：
1. 删除 UserHandle::load（wedb/wacl/src/user_handle.rs:32-35），保留 user() 单名对齐 C#
   User 属性唯一读面
2. 三处调用点改名：auth.rs:524 与 admin_commands.rs:177/:180 的 handle.load() 统一改
   handle.user()（返回类型同为 &Arc<User>，零行为变更）
3. 测试验证点：wacl/wnode 全量编译零错零警告（clippy）；ACL 域既有用例
   （wnode/tests/acl_tests.rs、wacl_default_record_fallback_test.rs、
   wacl/tests/acl_command_whitespace_no_trim_locks.rs）保持绿即证无行为漂移

终态：合入 520e74c，acl_commands.rs network_acl_who_am_i 注释锚订正为中性机制锚（C# 经 per-connection 认证器实例字段 GetUserHandle 读、会话 _userHandle WHOAMI 未用、rust 读会话 acl_user_handle，宿主位置分叉非行为分叉），零代码行为改动，cargo check --all-targets 通过

审核结论：通过（注释锚失实核心成立，纯注释订正 P3 级；唯票面「跨会话串号/修复型改良」定性被亲验证伪，执行须按第 2/7 点修正，不得照抄原订正文本）

判定要点：
1. 真实性成立：rust 注释 wedb/wnode/src/resp/acl_commands.rs:467-468 称「对标 C# 会话直读 userHandle.Name」，C# 实况 ACLCommands.cs:307 强转 (GarnetACLAuthenticator)_authenticator、:312 读 aclAuthenticator.GetUserHandle().User.Name——认证器实例字段 _userHandle（GarnetACLAuthenticator.cs:26，GetUserHandle :89-92 返回之），非会话直读，锚失实坐实。
2. 定性修正（执行红线）：票面「认证器级共享字段，任意连接认证成功即覆盖，跨会话身份串号缺陷」证伪——生产路径每连接 GarnetProvider.cs:61 GetSession 传 authenticator:null，RespServerSession.cs:282（_authenticator 全库唯一赋值点）每连接 CreateAuthenticator 新建实例（AclAuthenticationPasswordSettings.cs:28 new），认证器不跨连接共享，无串号缺陷可修；C# 另有会话级 _userHandle（RespServerSession.cs:136，AuthenticateUser :436 自认证器同步、:444-445 向 clusterSession/sessionScriptCache 传播，BasicCommands.cs:1947 消费）但 WHOAMI 未用之。「rust 系修复型改良」不成立，实为宿主位置分叉（认证器字段 vs 会话字段）而行为同效（连接级已认证用户名），「行为无需改动」结论不变。
3. 甄别性驳回条件未触发：rust network_acl_who_am_i（acl_commands.rs:469）经 user_name()（resp_server_session/core.rs:820-825）读 self.acl_user_handle 会话本地字段，非共享态，与 C# 机制不同形，票面前提不崩。
4. 非重复：task 池与 doc/zh/deviations.md 全册 grep whoami|WhoAmI|GetUserHandle|acl_user_handle，仅本票命中，无同面在册。
5. 先例谱系成立：纯注释订正 P3 先例在册（task/done/wbase-group-commit-broken-broadcast-watermark-doc-fork.md 同谱）；deviations.md 册头第 4 条「判据加符号名锚、严禁钉行号」纪律引用属实。
6. 路径订正：GarnetACLAuthenticator.cs 实际位于 garnet/libs/server/Auth/，票面涉码清单误写 Acl/。
7. 执行要求：订正文本改为中性机制锚——「C# NetworkAclWhoAmI 经 per-connection 认证器实例字段 GetUserHandle 读（行为同效连接级）；C# 会话另有 _userHandle（AuthenticateUser 同步）但 WHOAMI 未用；rust 读会话挂载句柄 acl_user_handle，宿主位置分叉非行为分叉」，严禁写入「共享句柄/跨会话覆盖串号缺陷」等已证伪表述。

ACL WHOAMI 注释锚失实：C# 原型实读认证器共享字段而非会话句柄，订正注释防后续票误判

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# ACLCommands.cs:312 NetworkAclWhoAmI 实读 aclAuthenticator.GetUserHandle().User.Name——认证器级共享字段（GarnetACLAuthenticator.cs:26/:34，任意连接认证成功即覆盖，存在跨会话身份串号缺陷）；非会话私有 _userHandle。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wnode/src/resp/acl_commands.rs:467-469 network_acl_who_am_i 的注释称「对标 C# 会话直读 userHandle.Name」，与 C# 原型实况不符。rust 行为本体（读会话本地句柄）系修复型改良（消除 C# 跨会话身份串号），行为正确无需改动，仅注释锚失实。
3. 逻辑危害确证
   无行为危害。按 deviations.md 册头第 4 条「判据加符号名锚」纪律，失实锚会使后续票引用时误判 C# 语义（误以为 C# 也是会话本地读），登记册注释锚订正先例同谱（§120/§130/§162 文档分叉注释订正先例）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/acl_commands.rs:network_acl_who_am_i（:467-469 注释）

对应 c# 文件与函数：
garnet/libs/server/Resp/ACLCommands.cs:NetworkAclWhoAmI
garnet/libs/server/Acl/GarnetACLAuthenticator.cs:GetUserHandle

精炼执行方案：
1. 注释订正：改为「C# NetworkAclWhoAmI 实读认证器共享句柄（GarnetACLAuthenticator.GetUserHandle，跨会话覆盖串号缺陷）；rust 读会话本地句柄系修复型分叉」。
2. 验证：仅注释文本改动，cargo check 通过即可，无行为面测试。

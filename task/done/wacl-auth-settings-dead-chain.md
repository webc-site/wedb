wacl 认证设置分派族与三档认证器整链生产死面（配置面断链）

来源：task/ing/wacl-auth-settings-dead-chain.md（fixloop 波次二）。落地形态为票面
修法二「收口为单档形态」。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev。
合入 dev：ff247f5（父 dev f536d49 + 分支 wacl-deadchain-b2 2bcde5a）。

一棒成果采信
一棒代理（撞 150 轮上限死亡）留有完整分支 rm-wacl-auth-settings（tip 24f0fe4，
相对 dev 5b42193 净 25 文件 +165/−465，工作树干净，10 个 ahead 提交全为反复
merge dev），未跑归档、未合 dev、未回填票面（主仓 git diff -- 该 ing 文件为空、
该文件最后一次提交为票据搬运）。二棒开新 worktree /tmp/fork/wacl-deadchain-b2
以 git merge 搬运其内容，合并树与 git diff dev...rm-wacl-auth-settings 逐字节
相同（diff 比对无差异）；搬运前后与 dev 的三次增量 merge（844c6f6 / 6f912f4 /
f536d49）全部无冲突，且这些 dev 提交均未触碰本单 25 个 payload 文件（comm 交集
为空），故 wnode/src/service.rs 的 store 单访问口、resp/resp_server_session.rs
的 ASYNC 臂、resp/acl_commands.rs 的 CUSTOM_OBJECT_ENTRIES 清单单点三处近期收敛
原样保留（三者所在 dev 提交 83fc02b / c45567e / ca97c51 皆为 merge-base 5b42193
的祖先）。并发票 f11-wnode-vtable、f12-hlen-o1、f17-vec-preview、f18-pulse-assert、
dev2/wlua-hash-key-copy、f15-wconn-anchor 射程零交集，未代跑其回归。

结论（二棒逐条复核，全部成立，无回退）
票面「定义在、生产零消费」论断逐条经全仓 grep 复核（.rs 全域，判读侧与写侧分查）：

一、设置分派族五文件 + settings/mod.rs 删除成立。IAuthenticationSettings /
create_authenticator / AuthSetup / NoAuthSettings / PasswordAuthenticationSettings /
AclAuthenticationSettings / AclAuthenticationPasswordSettings 的命中面在 dev 上仅
为定义文件自身、auth/mod.rs 与 lib.rs 的再导出，无任何生产装配调用点；trait
侧（IAuthenticationSettings: Send + Sync 界）亦无 dyn 消费。

二、三档聚合认证器删除成立。garnet_authenticator.rs（enum GarnetAuthenticator
及其 IGarnetAuthenticator 转发）、garnet_no_auth_authenticator.rs、
garnet_password_authenticator.rs 三个类型的构造点全仓为零，仅 wacl 内部（settings
族自身）互引；GarnetAuthenticator 亦不作任何形参/返回类型出现在存活面。

三、wnode 侧 acl_settings 整链为「写侧存在、读侧零」的死链：
AclCtx.acl_settings（resp/acl_commands.rs:53）在 resp/acl_commands.rs 内从未被读
（该文件对 ctx 的读取只有 authenticator / caller_namespace /
is_custom_command_registered 三项），RespServerSession.acl_settings 与
SessionDependencies.acl_settings 的生产写侧恒为 None（service.rs:1461
acl_settings: None），attach_acl 的 settings 分量同理。删除四处字段与形参、
调用点随之收缩为单参，无行为变化；AclAuthenticationSettings 的唯一字段
default_password 全仓零读取。测试侧（tests/acl_tests.rs 的 acl_settings() 助手、
resp_commandstats_session.rs 的 Some(...) 构造）按规不算消费者，随签名更新。

四、garnet_acl_authenticator.rs 的 pub type AuthenticateInternal 别名删除成立：
GarnetAclAuthenticator::authenticate 形参为 impl FnMut，别名在 rust 侧零引用，
仅注释仍指向 C# 的抽象 AuthenticateInternal（对位文档，保留）。

五、一棒的一处扩面（越界但不回退，理由如下）：resp/resp_server_session.rs
原自带私有 acl_password_check 复抄（其注释自认「wacl
GarnetAclWithPasswordAuthenticator::authenticate_internal 的同语义承接」，因
ascii_sanitize 当时为 pub(crate)），一棒把它提为 wacl 的 pub fn acl_password_check
（auth/garnet_acl_with_password_authenticator.rs）并经 auth/mod.rs、lib.rs 聚合导出，
会话与口令档同消费这一单点。这是消除既有第二套实现、非引入新机制，符合票面
「禁两套并存」与一处定义原则，采信。规范化语义逐字等价：wacl ascii_sanitize 的
b.is_ascii() 判据即原私有实现的 b <= 0x7F 判据，>0x7F 折 '?' 后走
AclPassword::from_string 取 SHA-256 比对，消费点仅
resp_server_session.rs:921 一处（与旧调用位同）。

六、注释口径订正（票面修法二第三句）已覆盖：resp_server_session.rs 的
authenticator_can_authenticate / acl_authenticator / attach_acl /
authenticate_user / NetworkAUTH / acl_permits 六处与 auth/mod.rs、
session_dependencies.rs、service.rs 的「NoAuth 档 / Password 档」措辞统一改为
「免认证形态 / ACL 单档」，并指向 service.rs 的 with_requirepass 装配位；
未新增机制，仅改口径。

C# 参考
garnet/libs/server/Resp/RespServerSession.cs:282 的
authenticator ?? storeWrapper.serverOptions.AuthSettings?.CreateAuthenticator(storeWrapper)
?? new GarnetNoAuthAuthenticator() 取档次序在 rust 不落（认证源唯一），据此在
js/check/ignore/garnet/libs/server/Auth/Settings/authentication-tiers.yml 逐符号登记
不实现理由：AuthenticationSettings.cs:CreateAuthenticator（含
GarnetAuthenticationMode 无落点）、NoAuthSettings.cs:CreateAuthenticator
（免认证即会话未挂认证器）、PasswordAuthenticationSettings.cs:CreateAuthenticator
（requirepass 收敛进 ACL 单档，service.rs:with_requirepass 落成带口令 default 用户）、
AclAuthenticationSettings.cs 的 CreateAuthenticator/CreateAuthenticatorInternal
（ACL 认证器直接构造，ACL 以底层存储 KeyTag::Acl 为唯一真源、废弃配置文件）、
AclAuthenticationPasswordSettings.cs:CreateAuthenticatorInternal（由
GarnetAclAuthenticator + acl_password_check 承接），Aad 两档
（AadAuthenticationSettings.cs、AclAuthenticationAadSettings.cs）按 transpile
规范不实现。与该文件既有 js/check/ignore/server.yml 的 Auth/Settings 族 Dispose
条目互不重叠。js/check.js 实测本 yml 未被自动淘汰、未被回写改写（worktree
git status 干净），说明登记项全部为生效判定项。

改动
- wedb/wacl/src/auth/：删 settings/ 整目录（五文件 + mod.rs）、删
  garnet_authenticator.rs / garnet_no_auth_authenticator.rs /
  garnet_password_authenticator.rs，删 garnet_acl_authenticator.rs 的
  AuthenticateInternal 别名；auth/mod.rs 与 lib.rs 再导出同步收缩，导出
  acl_password_check；档位在 rust 只有 ACL 单档的口径写入模块头注释。
- wedb/wnode/src/resp/：resp_server_session.rs 删私有 acl_password_check 复抄
  与 acl_settings 字段、attach_acl 收单参、六处注释订正；
  session_dependencies.rs / resp_session_consumer.rs / acl_commands.rs（AclCtx）
  随链删 settings 分量；src/service.rs 删 acl_settings: None 装配并订正
  StorageSessionProvider.acl 字段注释。
- wedb/wnode/tests/：7 个测试文件仅随签名/构造收口更新，零用例删除、零断言削弱
  （diff 逐项核对：无 #[test] 减项，acl_tests.rs 的 -69 行全为 settings 助手与
  attach_acl 双参形态收缩）。
- 新增 js/check/ignore/garnet/libs/server/Auth/Settings/authentication-tiers.yml。

验证
- cargo check --workspace --all-targets：EXIT=0，0 warning（在合入三次 dev 增量后
  各复跑一次，末次 34 crate 全量复查，覆盖 wnode/wtxn/wedb 面）。
- bun js/check.js：EXIT=0，无「语料失效」项；check/miss 与主仓基线逐文件相同
  （diff 空），Auth/Settings 族零缺失条目 —— 即票面验收「无新增缺失」成立。
  仅在 worktree 内跑（主仓跑会改写 ignore 语料），跑后 worktree
  git status --porcelain 除本单 payload 外为空。
- 未跑 ./test.sh 与 ./sh/clippy.sh（主代理集中回归）；requirepass_test.rs /
  acl_tests.rs 的端到端三态用例保持在场并随 --all-targets 编译通过。

边界与遗留
- IGarnetAuthenticator 与 GarnetAclWithPasswordAuthenticator 结构体本身仍为
  「定义在、生产零构造」（构造点仅 tests/acl_tests.rs），系票面修法二明文保留项
  （只留 IGarnetAuthenticator + 现用档）且为 C# 对位（IGarnetAuthenticator.cs、
  GarnetAclWithPasswordAuthenticator.cs），本单不收；其文件内的 acl_password_check
  已是生产消费点，故不构成新增死面。
- wconf / NodeArgs 的认证档位旋钮在修法二形态下仍不存在：requirepass-only 与
  ACL+requirepass 两形态在 rust 由同一 ACL 单档等价承接（requirepass 落带口令
  default 用户）。若产品面翻回修法一（真三档投影），须重开本票并撤本 ignore 条。
- AUTH/ACL 规则矩阵端到端用例归 next/resp-pubsub-acl-e2e-test-parity.md，
  本单按票面「勿重复铺测试」未加测试。
- 一棒 worktree /tmp/fork/rm-wacl-auth-settings 与分支 rm-wacl-auth-settings
  按分工保留未删，由主代理处置。

ACL SETUSER 只刷新发起连接自身句柄：目标用户的其它活跃连接无限期按旧权限放行

来源：next/glm.my.md 第 8 轮条。取证基线：主仓 /Users/z/git/db/wedb 分支 dev，
行号按符号在当下代码复核。

结论

rust 认证成功时按存储记录就地构造一个独立 User 实例并由该连接独占句柄，此后每条命令的鉴权
只读本地句柄位图、永不回读存储；ACL SETUSER 落存储后只在「SETUSER 目标恰为发起连接自己
已认证用户」时重读刷新发起者本地句柄。于是管理员对某用户执行 -@all、off、改密等撤权操作，
对该用户的其它在途会话完全不生效，延迟无上界（直到该连接断开重认证）。C# 同一操作是对
全局共享句柄做 CAS 换新，所有已认证会话下一条命令即见新权限。这是零全局内存的自定义优化
与 C# 即时生效语义的冲突点，代码只自证了自改半边，没有声明跨连接不生效是取舍。

现状

1. 每连接独立实例：/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_commands.rs:616
   authenticate_user_via_store（:637 User::from_rule_bytes 就地新建、:649
   AclAuthOutcome::Success(Arc::new(UserHandle::new(user)), target_ns)），
   挂载点 /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:902 set_user_handle。
2. 鉴权只读本地：resp_server_session.rs:2541 acl_permits（:2545-2549 取
   self.acl_user_handle 的 load().can_access_command(cmd)，无任何存储回读或版本比较），
   脚本面 :2556 acl_allows_command 同源。
3. SETUSER 只写存储：acl_commands.rs:295 apply_set_user（点查既有规则 → 复制改写 → 回写
   AclStore，见 /Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:122 write），
   全程不通知任何其它会话。
4. 刷新只覆盖自改半边：acl_commands.rs:661 process_acl_commands 内 :687 计算
   refresh_target（判据是 handle.user().name == name && self.namespace == ns，即目标恰为发起者自己），
   :741 命中才重读存储刷新发起者句柄；注释自述「句柄不共享，故显式刷新」，未提跨连接。
5. 句柄原语本身支持就地换新：/Users/z/git/db/wedb/wedb/wacl/src/user_handle.rs:12 user: ArcSwap<User>、
   :39 try_set_user CAS——只是 rust 没有任何一处让多个连接持有同一句柄的机制（这正是设计取舍）。

C# 参考

garnet/libs/server/Resp/ACLCommands.cs:139 NetworkAclSetUser：:164
aclAuthenticator.GetAccessControlList().GetUserHandle(username) 取全局共享句柄（不存在则
:167 new UserHandle(new User(username)) 并 AddUserHandle，:177 撞已存在时重取共享者），
末段 while (!userHandle.TrySetUser(newUser, currentUser)) CAS 换新——字典内共享句柄被就地替换
内部 User 指针，所有已认证会话下一条命令即读到新权限。规范源
SKILL.md:34（ACL 数据库持久化与零全局内存：句柄连接本地持有并随连接析构释放）、
doc/zh/db.md §3.3。

修法

不回收零全局内存这条自定义优化（它是 SKILL 明示的例外），只把「变更向活跃会话传播」补回来，
两处收口，禁并存两套失效判据：

一、引擎级 O(1) ACL 代数标量：在 WedbStore（或 wnode 侧 ACL 装配持有的引擎句柄）加一个
AtomicU64 acl_generation，AclStore::write 与 AclStore::delete 成功后 fetch_add(1)
（/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:122/:143 是 ACL 记录写删的唯一生产出口，
一处 bump 即覆盖 SETUSER/DELUSER/ACL SAVE 装载全部改权路径）。内存仍与注册用户总量脱钩，
只多一个标量。
二、会话侧缓存代数并在鉴权快路径比较：认证时随句柄一并记下当时的代数
（resp_server_session.rs:902 set_user_handle 处扩为一次带代数的挂载），
acl_permits 入口先比较本地缓存代数与引擎当前代数——相等即走现有位图快路径（一次原子读），
不等则按会话已绑定的 (ns, 用户名) 点查存储一次、重建句柄并更新缓存代数；点查失败或记录已删
按 C# 语义拒绝后续命令并留痕。快路径成本与现况同量级（一次 relaxed load 加一次比较）。
三、自改半边保留现逻辑（acl_commands.rs:741 的显式刷新与代数判据同源，不产生第二套口径），
但注释须改写为「跨连接经引擎代数收敛，自改臂是同连接的快路径捷径」。
四、若裁决认为「撤权延迟至重连」是可接受取舍，则必须反向收口：在 doc/zh/db.md §3.3 与本仓
注释里显式声明该取舍与运维缓解手段，并删掉 :687/:741 的自改刷新臂（该臂在取舍口径下也不该存在），
不得维持现状这种「一半即时一半不即时」的隐性双口径。

优先级

功能缺口（安全语义，且属对外承诺与实现不一致的一类）；本文件各票里排在
aof-replay-virtual-domain-context 之后、其余票之前。

边界

task/ing/wacl-auth-settings-dead-chain.md 管认证档位装配断链（哪一档认证器被构造），
与本单的「已认证会话权限失效传播」不同面，可并读不可并改；
next/resp-pubsub-acl-e2e-test-parity.md 管测试矩阵覆盖，本单新增用例并入该票的矩阵、
不另铺测试骨架；ns 0 超管门禁三处（acl_commands.rs:277/:385/:545）不在本单射程。

并发双花登记

同题在编排期内有两份细化文档，裁决与主修法（引擎级 AtomicU64 acl_generation 一处 bump
加会话侧缓存代数比较）一致，择一实施、另一份在合并时删除，勿两套失效判据并存：
本单 task/ing/acl-setuser-live-connection-propagation.md 与
task/ing/acl-setuser-active-session-propagation.md（同题，C# 侧多引
GarnetACLAuthenticator.cs:65-71 与 RespServerSession.cs:136 的共享句柄挂载证据，
并额外要求「记录已删按未认证处理」的行为口径，这两点值得并入落地方案）。
取单注意：他单行号已随树漂移（其标注 authenticate_user_via_store :573-607、
UserHandle::new :630、apply_set_user :252-311、refresh_target :683-697 与现树符号位置
:616/:637-649/:295/:687 不符），落地前须按符号重取；本单行号系当下 HEAD 复核。
他单未覆盖、本单独有的增量是修法第四项的反向收口分支（若裁定撤权延迟为可接受取舍，
则须显式声明取舍并删除 acl_commands.rs:687/:741 自改臂），以及 AclStore::write/:143 delete
为 ACL 写删唯一出口的一处 bump 收口证明。

验收

1. 端到端用例：连接 A 以用户 u 认证并持有某权限，管理员连接 SETUSER u -@all 后，
   A 的下一条受限命令即被拒（同一用例覆盖改密与 DELUSER 两个出口）。
2. 快路径无新增堆分配、无每命令存储读（探针或断言：代数相等路径零 read 调用）。
3. acl_permits 与 Lua 脚本面 acl_allows_command 同口径生效。
4. cargo check --workspace --all-targets 零告警，禁写 allow。

盘点补记（qw13.invA acl-setuser-live-connection-propagation）：dev e75716e 复核原样：acl_commands.rs:303 apply_set_user 仍点查→复制改写→回写 AclStore，无会话通知面；:753 set_user_handle 仍无条件直挂；resp_server_session.rs acl_user_handle（:266/:923/:953）仍本地快照判定，无版本比较/存储回读。与 zero-consumer-surfaces-batch-two 的 try_set_user CAS 环仍同病灶，宜并一棒。

落地判词（qw13 fixloop 二棒续棒 · 覆盖度 + 二棒改动 + 门禁）

一棒覆盖度（通读脏区 10 文件，逐项对照票面判据）
- 修法一 达成：wkv/src/store/mod.rs 引擎级单标量 acl_generation（Acquire 读、Release fetch_add、唯一 mutator
  bump_acl_generation）；wnode/src/resp/acl_store.rs write 与确有删除的 delete 经私有 bump 转口。一棒更指出本票
  「write/delete 为唯一出口」不完备——AOF 与复制回放的 KeyTag::Acl 条目臂绕过 AclStore 直写，故 aof/
  aof_processor.rs 两臂亦 bump 同一标量，改权全出口一处收敛。
- 修法二 达成：resp_server_session.rs AclMount{generation,from_store} 随 set_user_handle 快照挂载代数；鉴权预门
  admin_commands.rs check_acl_permissions 每命令比较代数，相等走 acl_permits 位图快路径（零存储读、零新增堆
  分配，代数相等即在 clone 之前早退），落后按 (ns, 用户名) 点查重建；记录删 / 不可解析按未认证撤销挂载
  （会话句柄与认证器镜像同撤）并 log 留痕；存储读失败维持现挂载交下一命令重判；引导期 default
  （from_store=false）点查无记录不误撤。位点自票面字面「acl_permits 入口」上移到其调用方预门——acl_permits
  为 &self 且 Lua 窗内亦调，点查须批处理纪元外、下沉即自锁；二棒认可此为唯一判据的正确落点，脚本面经外层
  命令预门刷新后的同一收敛句柄判定，无第二套。
- 修法三 达成：自改臂保留，acl_commands.rs 重读记录后调 adopt_acl_user，与跨连接臂共用同一出口、同一判据，
  注释改为同连接快路径捷径。
- 修法四 未采用（按票）。

二棒改动（方向以票面为准，尽量保全一棒劳动）
- 合并前基线先行单跑暴露两处测试数据 bug（生产逻辑一棒全对）：acl_tests.rs 重认证 newpw 帧长度 $6→$5
  （一棒误抄管理连接的 >newpw 六字节），错帧致 RESP 解析停摆、无应答（正是临终 DBG 所查的空输出）；
  lua_script_tests.rs redis.acl_check_cmd 断言 +N/+Y 改 $1 bulk 帧（Lua 串返回是 bulk，对齐同文件
  test_eval_acl_check_cmd_arms 既有断言）。
- 撞并发第二套判据：并入的 dev 已由 zero-consumer-surfaces-batch-two 删除 wacl UserHandle::try_set_user、句柄
  改构造即定格的只读快照（换代即重读存储后整体替换 Arc<UserHandle>）。一棒 adopt_acl_user 依赖就地 CAS，
  合并后编译即断。二棒按现 dev 语义重放：adopt_acl_user 改整体替换（会话句柄与认证器镜像同换新 Arc、
  挂载代数对齐调用方点查前采值，杜绝缓存到更新代数而漏并发改权），不复活 CAS、无第二套；自改臂同步。
- store/mod.rs 两处冲突按现 dev 语义重放：dbmeta_lock 保 dev 的 DbmetaLock 重构、本票 acl_generation 字段 /
  读方法 / bump / init 追加，未造第二套代数。

门禁（树内私有 CARGO_TARGET_DIR=/tmp/ct-acl2，未跑主仓 test.sh 与 sh/clippy.sh）
- cargo check --workspace --all-targets：三次回合 dev 后各复跑，0 warning、0 error、无 allow。
- cargo nextest run -p wacl -p wnode -p wkv --no-fail-fast：1358 passed / 1 skipped / 0 failed；本票 4 新例
  先行单跑全绿。
- bun js/check.js：exit 0；本票新例锚点归入既有 ACLCommands.cs:NetworkAclSetUser；唯一回写 storage.yml 的
  ResetRevivificationStats 属他票，git checkout 还原，本票零 ignore 净改动。
- rustfmt 仅施本票 5 个偏离文件（换行 / 参数折行，语义不动）。
- 分支 vs 并入的 dev 18b0789 净差仅 10 文件、539 增 / 44 删。

落地：代码加三度 dev 重放加 fmt 尖 dfc81be；归档票 commit 见分支 git log。

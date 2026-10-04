甄别结论：通过（2026-09-29 主控甄别，定级 P1——remount_truncation_survivors keyspace.rs:524 闭包外一次性 index.load()+:529 裸 find_tag 全程无 grow 协同；resize.rs:506-510 先切表后发布 InProgressGrow，截断幸存扫描采未迁完新表，ACL 用户键误剔不重挂→认证真源丢失 auth lockout。done/wnode-acl-user-enumeration 只修 for_each_user 面，:529 现码仍裸。修复：find_tag_cooperative 挂 StoreSession+new_session() 上提扫描段前，错误沿 Result 短路上抛严禁折成剔除）

审核结论：通过（2026-09-29 甲轮27-A 独立审核席，P1 级）。核心锚全复核：keyspace.rs:516 闭包外 load、:521 裸 find_tag 无协同；resize.rs:506 先切表后发布 InProgressGrow；后台扩容与 FLUSHALL 无共享互斥自由交错；漏检第二消费点属实（for_each_user 已收口票明写「接线点唯一」remount 未在列）。审核席执行面修正（执行席遵照）：
1. find_tag_cooperative 挂在 StoreSession，remount 须把 new_session() 上提至扫描段前再在闭包内调用，勿在 WedbStore 上新开第二套协同探针。
2. 锁测对齐 acl_tests.rs:2905 既有模板。

原票面：
FLUSHALL 恒根域住户重挂链首校验用裸 find_tag 无扩容协同，扩容迁移窗内 ACL 用户记录被误剔不重挂，截断后认证真源物理丢失（P1 票 auth-lockout 修复面被扩容窗重新打开）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ACL 用户句柄驻 AccessControlList._userHandles（garnet/libs/server/ACL/AccessControlList.cs:24）与 Tsavorite 存储彻底分离，FLUSHALL 绝不波及用户注册表；rust 单日志多库共享，ACL 记录驻存储即认证唯一真源，靠 remount 重挂保真——重挂自身必须对索引扩容窗免疫。会话触达索引必先协同后探针（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:70-73 入口铁律），rust 侧单点即 wkv find_tag_cooperative。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wkv/src/store/keyspace.rs:remount_truncation_survivors（:511-550）在 :516 闭包外一次性 let index = self.index.load()，:521 闭包内裸 index.find_tag(rec.key()) 做链首校验，全程无 ensure_split_by_hash 协同。grow_index（wedb/wkv/src/store/resize.rs:436 起）仅以 CAS 相位互斥、不持 dbmeta/acl 锁，wnode 后台 IndexAutoGrowTask（wedb/wnode/src/database/database_manager_base.rs:533 grow_indexes_if_needed）周期触发可与 FLUSHALL 自由并发；切表（resize.rs:506 index.store）后未迁分块的桶在新表恒空（done 票 wnode-acl-user-enumeration-bare-find-tag-grow-window-miss 双席亲证同款状态机）。index.load() 落在切表后即采到未迁完新表。头注 :491 自述「口径同 for_each_user 扫描面」，而 for_each_user 已改 find_tag_cooperative 协同单点（e76df226 收口）——本处为该票审核时误判「接线点唯一」漏检的第二消费点。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
ACL 键落未迁分块 → find_tag 采 None → None != Some(addr) 判链首不符 → 该用户记录剔除出 residents 不重挂。remount 返回后 flush_all_databases（keyspace.rs:472）shift_begin_address(tail) 物理截断不可逆，认证真源静默丢失：acl_commands NoRecord 臂 + auth.rs 非 0 租户禁回落 → 全租户 AUTH WRONGPASS 永锁；ns0 侧被改密 default 复活成认证绕过面——即来源 P1 票 task/done/wnode-flushall-destroys-acl-user-records-auth-lockout.md 的四害链经扩容窗回归。VectorRegistry::Metadata 单例同窗不重挂（向量上下文元数据丢失）。副本回放臂（aof_processor FlushAll 同调）同窗口。done 票仅瞬态漏报自愈（P3），本处物理丢失不可逆（P1）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/keyspace.rs:WedbStore::remount_truncation_survivors
wedb/wkv/src/session/mod.rs:StoreSession::find_tag_cooperative（修法对齐目标）

对应 c# 文件与函数：
garnet/libs/server/ACL/AccessControlList.cs:_userHandles（用户注册表与存储分离，FLUSHALL 无此窗）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead（先协同后探针入口铁律）

精炼执行方案：
1 remount 链首校验改经 find_tag_cooperative 协同单点（先分裂协同后现取活跃表探针），删除闭包外 index 句柄捕获；迁移内核错误沿 scan 回调 Result 短路上抛，严禁折成剔除（§35 折叠口径红线，for_each_user live_probe_err 同轨）
2 头注「口径同 for_each_user」声明随修复恢复真实
3 测试验证点：手工装配 InProgressGrow 部分分块滞留窗（对齐 acl_for_each_user_sees_all_users_during_grow_window 锁测形）内执行 FLUSHALL，断言全部 ACL 用户记录重挂后 AUTH 仍 +OK；回装裸 find_tag 即红

来源：甲轮11-B 审查席（2026-09-28），主控亲验三锚闭环（remount 现状/for_each_user 协同现形态/grow 并发窗与来源票害链）。

终态注记（2026-09-29 执行席收口）：已修复合入 dev（fix commit 3cc3fb1，merge 1b15980，分支 fix-remount-grow-coop）。收口形态：remount_truncation_survivors（keyspace.rs）new_session() 上提扫描段前，链首校验改 StoreSession::find_tag_cooperative 协同单点（先分裂协同后采样活跃表），删除闭包外一次性 index.load() 句柄捕获，未在 WedbStore 上开第二套协同探针；迁移内核错误经 keyspace.rs 模块级 live_probe_err（与 wnode array_key_iteration_functions::live_probe_err 同轨同形，wkv 层无法复用 wnode crate 私有项）沿扫描闭包 whlog Err 通道短路上抛，探测失败即 flush_all_databases 整体失败、shift_begin_address 截断绝不发生，零折叠剔除；头注「口径同 for_each_user」与回调禁写边界（日志面禁写不变、索引面协同迁移与 for_each_user 回调同位同形）随修复恢复真实。锁测：wkv/tests/store/flush_database.rs test_flush_all_remounts_acl_survivors_during_grow_window（对位 wnode acl_tests.rs:2905 acl_for_each_user_sees_all_users_during_grow_window 模板）：32768 桶两分块、2000 名 ACL 用户横跨两分块 + VectorRegistry Metadata 单例，手工装配 InProgressGrow 且先发制人迁移分块 1（部分迁移真实瞬态），窗内 FLUSHALL 后逐名断言重挂存活值原样、数据域全清语义不破、finish_resize_window 收窗后视图逐名一致；回装裸 find_tag 即红。worktree 全仓 cargo check --all-targets 通过（子代理不跑 test.sh/clippy.sh，集成验证归主代理门禁）。

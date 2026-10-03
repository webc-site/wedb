甄别结论：通过（甄别席 J7，2026-09-27，定级 P2——group commit 领导者交割窗取消致管线永久卡死，取消锚与争用窗双侧坐实）。enter :132-134 持锁置 leading 返 Lead、守卫 :175-178 迟构造，waof commit_to Lead 空臂 :180 与 commit_lock.lock().await :184 间为唯一可取消窗；取消锚 drive.rs:401-402 亲验；commit_lock 持锁方六处横跨磁盘 await——flush.rs:129/:184、log.rs:223/:277/:379、recover.rs:39（票面 :35 微漂），争用窗真实；leading 复位点仅排空 :224/错误 :240/Drop :81 全在 run_leader 路径内，无兜底复位，永久卡真成立；wkv 对照 :95-114 无中间 await，缺陷面收敛 waof 装配形。与 done 池 commit-wait 两票、inr 近亲票 4 均不同轴。派沙箱席 c01o。

审核结论：通过（P2 维持。三点链亲验坐实：group_commit.rs enter :132-134 持锁置位 leading 并返回 Lead，run_leader :175-178 入口才构造 LeaderGuard；waof commit_to :180 Lead 臂空落与 :184 commit_lock.lock().await 之间为唯一可取消窗——票面 :186 系行漂移，实质成立。取消锚亲读 compio-runtime-0.12.6 future/combinator/cancel.rs:112-127：WithCancelFailFast::poll 见令牌触发即返回 Ready(Err(Cancelled))，killable future 随 await 语句收场整体析构，内层 wait_for_commit_async future 是被 drop 而非仅不再 poll——LeaderGuard 未构造即随 drop 湮灭，leading 永久卡真。传导链四环（drive.rs:401 killable → service.rs:2178 async move 内联 → single_database_manager.rs:495 内联 await → commit.rs:210 join_all 内联驱动各子日志）全程零 spawn，drop 逐级级联至 commit_to future，链路成立。反证排查闭环：commit_lock 四类持锁方全部跨真实磁盘 await（truncate 删段 log.rs:234、副本回放对齐 log.rs:277 臂、recover 全程扫描 recover.rs:35、reset log.rs:379、副本 flush_only flush.rs:129），主端现实争用源为检查点尾截断（database_manager_base.rs:411/:570 truncate_until_async）；reset/truncate/recover 臂只复位位点原子量，全仓无任何 leading 兜底复位点；wkv 对照面 flush.rs:107-114 Lead 后直通 run_leader 无中间 await，缺陷面收敛 waof 装配形确证；副本臂走 commit_flush_only 不触 pipeline、AOF 未启用无路径，砖化面收敛主端提交流水线。查重成立：deviations.md 全册零在册；issue 池近亲票 wbase-group-commit-broken-broadcast-watermark-doc-fork 系 wait() 兜底臂文档-行为分叉，其审核席反向指认本票为「守卫构造前身份交割窗（另一缺陷窗）」，互不重复；done 池 commit-wait 两票系出网 armed 闩他轴。定级 P2：单连接取消毒化全局共享提交管线，永久性无自愈（重启前该子日志全部 commit_to 必入 Follow 臂、tx 永不 send、waiters Vec 无界积压），状态机悬挂死状态属板块 4.2 硬伤；缓释项记档不降级——--aof-commit-wait 默认 false（node_options.rs:578）的 opt-in 档、kill 须精准落于持锁争用窗故单事件低概率）

WAL 组提交 Leader 交割窗裸奔：enter(Lead) 置位 leading 与 run_leader 构造守卫之间隔着可取消 await（waof commit_to 的 commit_lock 等待），连接取消即 leading 永久卡真，该子日志提交管线砖化、等待者无界积压直至重启

问题分析：
1 Garnet 契约对齐：C# TsavoriteLog.CommitAsync（garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs）的提交等待是网络线程内 BlockingWait/epoch 轮询达标循环，无 rust 组提交 Leader 选举形态，不存在「Leader 身份交割后被丢弃」面；本票系自研组提交机制（wbase group_commit，对标 C# CommitAsync 语义）的契约自洽缺口——LeaderGuard 的存在本身（wbase/src/group_commit.rs:60-68 注释自陈「裸 leading 恒真 → 全部 enter 必入 Follow 臂 → 写平面永久砖化」是死亡形态）即承认收口守卫是必需肢。
2 工程现状确证：Leader 身份在 enter 持锁内交割（wedb/wbase/src/group_commit.rs:131-134 lock.leading = true 加 push 自身 waiter 后返回 Enter::Lead），而收口守卫 LeaderGuard 直到 run_leader 入口才构造（group_commit.rs:174-182）。waof 装配在两者之间插入可取消 await——wedb/waof/src/wal/flush.rs:165-188 commit_to：Enter::Lead => {} 之后 let _commit_guard = self.commit_lock.lock().await（:186）才 run_leader。取消锚已实证：drive.rs:401-404 killable(session_provider.wait_for_commit_async(), &kill_token) Cancelled 即 break 'drive——链路 drive.rs:401 → service.rs:2178 → single_database_manager.rs:495 → wnode/src/aof/garnet_log/commit.rs:203 join_all 驱动各子日志 wait_for_commit → commit_to。commit_lock 有四类在位持锁方（waof/src/wal/flush.rs:129 commit_flush_only 副本落盘入口、waof/src/wal/log.rs:223/:277/:379 与 recover.rs:35 控制面、前任 Leader 全程），Leader 当选后 parks 于 lock().await 并非罕见路径。时序：任务当选 Leader（leading=true 已置）→ parks 于 commit_lock → kill 触发 killable 取消，commit_to future 连同未入守卫的 Leader 身份整体丢弃 → 该子日志 leading 永久为真无复位路径 → 此后所有 commit_to/wait_for_commit 在 group_commit.rs:122-128 必入 Follow 臂，tx 永不 send 提交等待永挂，waiters 每次 push 永不 drain 无界增长。对照 wkv 装配面（wkv/src/store/flush.rs:95-113）Lead 后直通 run_leader 无中间 await，无此窗——缺陷面收敛于 waof commit_to 装配形与 wbase 契约未禁此形。
3 逻辑危害确证：一连接级取消（客户端断连触发 kill_token）毒化全局共享提交管线——主节点该分片 AOF 写确认面（出网 armed || wait_for_aof_blocking 热路径经 wait_for_commit_async）整体砖化直至进程重启；低概率单事件、永久性伤害，纯内存态无持久损伤（重启自愈）。

涉及代码：
rust 文件与函数：
wedb/wbase/src/group_commit.rs:enter（:118-135，:131-134 身份交割）、run_leader（:174-182 守卫迟到）、LeaderGuard（:60-68 死亡形态自陈）
wedb/waof/src/wal/flush.rs:commit_to（:165-188，:186 可取消 await 窗）
wedb/wnode/src/net/handler/drive.rs:出网 commit-wait 取消锚（:401-404）
wedb/wkv/src/store/flush.rs:无窗对照形态（:95-113）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitAsync（内联等待无 Leader 交割窗，自研机制契约对标）

精炼执行方案：
1 最小改法：waof commit_to 把 commit_lock.lock().await 提到 enter() 之前（持锁后协商，锁内 enter 重读水位仍正确；follower 在 Leader 持锁期间照常登记，批量折叠不受损）；或 wbase 侧让 Enter::Lead 携带守卫（身份交割即武装，契约面根除，wkv 面不受影响），二择一按单机制原则评审裁
2 回归锁测：killable 于 commit_lock 等待期取消 → 后续 commit_to 仍可当选 Leader、waiters 正常排空；wkv 面回归不回退

审核裁定执行方案（审核席 r28，2026-09-27，供 task/fix.md 直接消费，替代上文精炼执行方案）：
1 采方案二（Enter::Lead 携守卫，wbase 契约面根除）：group_commit.rs Enter 枚举 Lead 臂改携 LeaderGuard（enter 持锁交割身份同点武装，Drop 收口复用错误臂单点机制不变），run_leader 签名改收已武装守卫（排空 :224 / 错误 :240 两正常臂 disarm 语义原样保留），适配面封闭三处——waof commit_to（wal/flush.rs:165-188）、wkv flush_all（store/flush.rs:95-115）、wbase 回归套件 tests/suite/group_commit.rs。方案一否决，理由：锁前移后 Follower 于 Leader 持锁刷盘期间阻塞在 commit_lock 排队而非登记进 pipeline waiters，票面「follower 照常登记、批量折叠不受损」断言不成立（折叠退化为 has_new_tail 尾驱动级联的补偿形态）；且 Done 快速短路（wait_for_commit 已达标 0 I/O 路径，现零锁）被强加互斥开销，违数据面零开销与全链路单机制双判定
2 回归锁测三案：wbase 单测——enter 取 Lead(guard) 后立即 drop，断言 leading 复位且后续 enter 可再当选 Leader；waof 集成——commit_to 于 commit_lock 被占（truncate 持锁形态）时连接取消（drop future），后续 wait_for_commit 仍能当选并排空积压 waiters（ Brick 前形态须红：撤守卫同点断言复现 leading 卡真）；wkv flush_all 回归不回退
3 行号勘误随改（以现码为准，不影响判定）：commit_lock.lock().await 实际 waof wal/flush.rs:184（票面 :186）；killable 调用与 Cancelled 臂实际 drive.rs:401-402；wkv Lead 臂实际 :107、run_leader 实际 :111-114；守卫构造实际 group_commit.rs:175-178

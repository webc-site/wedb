甄别结论：通过（甄别席 J7，2026-09-27，定级 P3——注释订正零行为，方案 (a) 合理）。文档宣称 :65-66、Drop 广播 tx.send(Err(Broken)) :84、wait :149-158 第一臂 Ok(res) 原样上抛、Err(_) 水位兜底仅 rx 弃可达，亲验坐实文档-行为分叉。执行注意：与 waof-group-commit 票同文件同区域（彼票方案二改 LeaderGuard/run_leader），两票须排先后防注释冲突。派沙箱席 c01o。

审核结论：通过（P3 登记级。六项判定全过：一、真实性亲验坐实——LeaderGuard Drop 广播 tx.send(Err(Broken))（group_commit.rs:84）在 wait() 命中第一臂 Ok(res) => res（:149-150）原样上抛，Err(_) 水位兜底臂（:151-158）仅 tx 未 send 即弃（整管线 drop 等极端窗）可达，文档宣称句（:65-66）「经既有通道中断兜底臂按水位判定成功或回 Broken 上抛」确系未接线，非幻觉；C# 对照成立，TsavoriteLog.cs CommitAsync :1997/:2005 while (CommittedUntilAddress < tail ...) 达标循环恒以真实水位判定。二、反证闭环——消费面 waof commit_to（wal/flush.rs:171-177）与 wkv flush_all（store/flush.rs:99-104）对 wait 结果均直接 map_err 上抛，无上层按水位重试，伪失败直达调用方，仅靠更外层客户端重试兜住；伪失败窗口收敛于 Leader 步进中途 panic/取消且水位已推进未 retain 的窄窗（正常步进后 retain_mut 已摘走全部达标等待者），方向安全绝不假成功。三、查重成立——近亲票 waof-group-commit-leader-cancel-orphan-pipeline-brick 系守卫构造前身份交割窗砖化（另一缺陷窗），deviations.md 全册 grep 无本分叉在册，不重复。四、方向裁决采方案 (a) 注释订正零行为最小收口，拒 (b)，理由见文末裁定。五、格式合规双侧路径齐全。六、定级 P3 恰当：安全向文档-行为分叉，无数据丢失无挂起，实害为窄窗伪失败加后续按文档理解维护的埋错风险）

组提交 LeaderGuard Broken 广播不经水位判定兜底臂：文档宣称「经既有通道中断兜底臂按水位判定成功或回 Broken」未接线，target 已被实际水位覆盖的 Follower 收到伪失败（安全向文档-行为分叉）

问题分析：
1 Garnet 契约对齐：C# TsavoriteLog CommitAsync 等待达标循环按持久水位判定成功（garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs，未达标即 await 让出沿 NextTask 链重等）——等待者最终结果恒以真实水位为准，不因 Leader 身份消亡而伪失败。
2 工程现状确证：wedb/wbase/src/group_commit.rs:64-66 LeaderGuard 文档称 Drop 收口广播 Broken「经既有通道中断兜底臂按水位判定成功或回 Broken 上抛」；实际 tx.send(Err(Broken))（:84）在 wait()（:149-151）命中第一臂 Ok(res) => res 直接返回 Err(Broken)——Err(_) 兜底臂（:151-158 的水位判定）只在 tx 未 send 即弃（整管线 drop）才可达。后果：Leader panic/取消前部分刷盘已推进时，target ≤ 实际已提交水位的 Follower 收到伪失败。
3 逻辑危害确证：方向安全（绝不假成功，客户端重试覆盖）但与文档宣称机制不符且与 C# 水位判定语义弱化——低危文档-行为分叉；若后续按文档理解维护（如依赖兜底臂做水位收敛的新臂）会埋错。

涉及代码：
rust 文件与函数：
wedb/wbase/src/group_commit.rs:LeaderGuard 文档（:60-66）、Drop 广播（:84）、wait 双臂（:149-158）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:CommitAsync（水位判定语义对照）

精炼执行方案：
1 二择一：(a) 注释订正为实际行为（Broken 直达、水位兜底仅整管线 drop 可达），消文档错位；(b) wait() 把 Ok(Err(Broken)) 并入水位兜底臂（target ≤ 水位回成功）——若采纳 (b) 需确认与「真实错误由 Leader 侧返回」语义不冲突
2 验证点：按所裁方向补单测（Leader 半刷盘后 Broken、Follower target 已覆盖应得成功或文档化伪失败）

审核裁定执行方案：
1 采 (a) 注释订正（零行为最小收口）：group_commit.rs:65-66 括注改述为实态——Drop 广播 Err(Broken) 经通道直达 wait() 第一臂 Ok(res) 原样上抛，Err(_) 水位兜底臂仅 tx 未 send 即弃（整管线 drop 等极端窗）可达；补一句安全向说明「target ≤ 实际已提交水位的 Follower 得伪失败，重试经 enter Done 快路径（:121-123）收敛回成功」，沿 deviations 双向锚注纪律挂回指锚。:64 前文「通道中断兜底臂永不触发」句与 wait 文档（:139-140）「Leader 异常退出未广播」表述与实态相符，不动
2 deviations.md 顺延登记一条（登记级，纯注释加台账零行为，沿 §120/§130/§162 文档分叉注释订正先例）
3 拒 (b) 并臂：其一，伪失败窗口窄且安全向，消费面重试已覆盖，行为变更收益不抵成本；其二，并臂使广播送达的 Broken 部分静默转成功，削弱「广播即 Follower 见错」现行诊断不变量；其三，wait() 需按第一臂引水位闭包重判定，实为在共享契约错误路径上加分支，与「真实错误由 Leader 侧返回」哨兵单点注释（:15）的职责切分趋糊，且确定性锁测需构造步进中途水位推进未 retain 的时序窗，可落度差
4 票内行锚微订正：文档宣称句实为 :65-66（块 :60-68），第一臂实为 :149-150（票面 :149-151 跨两臂），内容所指无歧义
5 验证点：注释订正后 ./test.sh 全绿（零行为不新增锁测）；grep 确认「经既有通道中断兜底臂按水位判定」旧宣称字样全库零残留，新述与 :149-158 实态逐句对得上

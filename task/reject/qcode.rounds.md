qcode.rounds.md 分拣拒绝档案（本轮三裁定不成立；同档前六页为并发审计产物）

审计时间：2026-09-19。取证基点：主仓 dev 工作树实时 grep 与 garnet/ 实读，不采信台账原文、
不采信任何 sha。说明：本文件之前，task/reject/ 已有并发会话产出的六页同档清账
（qcode-rounds-first-wave-gate-landed.md、qcode-rounds-round8-second-wave-landed.md、
qcode-rounds-glm-round8-covered.md、qcode-rounds-round7-9-in-register.md、
qcode-rounds-round10-dispositions.md、qcode-rounds-foreign-inflight.md），它们覆盖台账
:54-217 的在册与落地判定；本页只收它们未承载、或我复核后改判不成立的三条，避免重复登记。

一、bf-tree ScanIter 深递归栈溢出主张（台账 :185-194「主代理裁决：ScanIter 深递归主张（不立案，留证据）」）

原文要点：第三方 crate bf-tree 的 ScanIter::next 在 Deleted 分支自递归
（range_scan.rs:144 move_to_next 后直接 self.next(out_buffer)），递归深度被认作同一叶内
墓碑连段长度；本仓消费点为 wbftree 的排空回调；一个死代理把它归因给
wnode::aof_shutdown_tests 的 dispose_flushes_uncommitted_frames_for_recovery，主代理判归因
不成立、留证据不立案，并称批量上限改造（scan_tree_in_batches 以 meta.size 为界）由基线
嫁接代理承接，「若复现出真栈溢出再单独立案」。并发审计（qcode-rounds-foreign-inflight.md
第一节）把该条留在「挂靠嫁接代理改造结果、未触发」的在途口径上。本轮改判为不成立、结案。

拒绝原因：其一，归因证据是虚构的——本仓从未存在 scan_tree_in_batches 与
drain_scan_iter_with_callback 两个符号（全仓 grep 零命中），台账据此叙述的「改造已嫁接」
无从复核，实际承接点是 wedb/wbftree/src/service/ops.rs:290 的 drain_scan_iter 与 :335 的
带 make_iter 驱动函数，其文档注释 1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:577
DrainScanIteratorWithCallback（该 C# 函数实存，定义行已核对）。其二，被指认的测试
wedb/wnode/tests/aof_shutdown_tests.rs 中的 dispose_flushes_uncommitted_frames_for_recovery
只写个位数十来字节的 AOF 帧，建树深度与墓碑连段都不可能撑起可观递归栈，栈溢出归因不成立。
其三，机制本体在第三方 crate 内部（~/.cargo/registry/.../bf-tree-0.5.6/src/range_scan.rs:157
的自递归调用），而 C# 侧消费同一枚引擎的方式是源生成 P/Invoke 到原生库 bftree_garnet
（garnet/libs/native/bftree-garnet/NativeBfTreeMethods.cs:13-14 的 LibName 常量与 :31 起的
[LibraryImport(LibName)] 声明族，BfTreeService.cs 只是其上的托管包装），根本没有可对标的
托管扫描实现：两侧同一递归，按 transpile 射程（对标 garnet C# 转写，不重写第三方引擎内部）
不属本仓待做项；真要收口也只能给上游 bf-tree 提 issue 或换依赖，不是一张能派给 dev 的文档。
后续若门禁或压力测试真复现出栈溢出，按当时代码事实另立案，不靠本页旧账。

二、「dyn 用法属正确形态、无需收口」主张（台账 :65 末句撤销项）

原文要点：第 8 轮 design 子代理自报本仓 dyn 24 处属正确形态，与主代理此前持有的「零 dyn
基线」冲突，主代理当场「按 dyn 收口方向定案不报」。

拒绝原因与去向：该主张不成立，维持不立案；但收口方向本身是有票的活，不在本页消失。本轮
复核工作树 dyn 出现 32 处（非测试路径），本仓既定方向是「删自造泛型与多余擦除层、收敛为
单一 dyn 后端」，与 C# 的非泛型单一可空句柄形态对齐；该方向上最大的一块（TransactionManager
自造的 L: TxnAofLog 泛型加 blanket 与空对象 impl）已立案并在认领队列中
（task/ing/wtxn-aof-log-dyn-backend.md，自述为 task/ing/txn-aof-marker-session-wiring.md 的
硬前置）。因此既不接受子代理「dyn 现状即正确形态、无需收口」的定案，也不接受主代理旧口径
「零 dyn」这条相反基线，判词一律以在册票的单一 dyn 后端收口方向为准。

三、阻塞命令推迟同批应答主张（台账 :35-36 第 7 轮 net 域撤销项）

原文要点：第 7 轮 net 审查曾疑阻塞命令（BLPOP 族）把应答推迟到同批之后处理是缺陷，
核 C# BlockingWait 同构后撤销。

拒绝原因：撤销判断成立，本页把它从「疑点」正式结为「不成立」，不再重登。C# 侧阻塞命令在
网络线程内以 AsyncUtils.BlockingWait 就地等待同批完成，证据
garnet/libs/server/ServerConfig.cs:191、:304 与 garnet/libs/server/AOF/AofProcessor.cs:307；
rust 侧同构落点为 wedb/wnode/src/cluster_session.rs:175-180（compio 单线程执行域内联驱动，
注释自述 C# BlockingWait 等价）、wedb/wnode/src/net/handler/drive.rs:197、
wedb/wnode/src/resp/admin_commands.rs:162-171，等待结果一律弃用的口径与 C# 一致。
阻塞族其余议题（分层大键挂起、null 帧的 RESP3 分派）不属本条，另有在册票承载，本页不代判。

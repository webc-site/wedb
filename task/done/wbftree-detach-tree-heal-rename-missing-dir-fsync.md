甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——heal 臂 rename 成功零 sync 与 publish 双屏障不对称，崩溃窗 fail-loud 冷路径）。亲验：heal 臂 cpr_snapshot+rename 成功臂零 sync（lifecycle.rs:566-577），publish 双屏障同文件 :742-757（wdev::sync_dir+理由自陈「rename 只改目录项，POSIX 下掉电后不保证可见」）不对称确凿；危害前提由本函数 doc（:546-555）自陈，引擎断言亲证（bf-tree 0.5.6 snapshot.rs:1200 assert 文件长度==file_size）；delete_file=false 唯一调用方 cold_tree.rs:123；get_or_open_tree file_has_cpr_magic→recover_from_cpr_snapshot（:347-354）；ing 池两张 fsync 票（接收面/新建目录形）与本 rename 形异点，双方票面已互证正交；deviations §78 系 wdev 新建段另一面。log::warn 不改签名合规。派沙箱席 c01l。

审核结论：通过（独立审核席 2026-09-27）

逐项亲核记录：
1 不对称亲证：detach_tree heal 臂（lifecycle.rs:568-575）rename 成功臂无任何 sync；publish_tree_from_snapshot_locked（:742-744）rename 成功臂 wdev::sync_dir(parent)?，:736-741 自陈同型理由。不对称成立。
2 引擎断言亲核：bf-tree 0.5.6 src/snapshot.rs:1200 assert_eq!(reader.metadata().unwrap().len(), bf_meta.file_size)，new_from_snapshot 读头后硬断言。
3 生产面 delete_file=false 唯一调用方：wedb/wkv/src/gc/cold_tree.rs:123 recycle_cold_bftrees（常驻冷树回收轮，dispose_tree_under_lock(id_key, false)）。原票「换号离线臂（delete_file=false）」措辞不准：换号 reclaim_bftree_keys（bftree_release.rs:163）走 detach_tree(id_key, true) 消亡臂不需 heal，危害臂实为冷树回收单臂，不影响缺陷成立。
4 危害链闭合：走 heal 臂的树系升阶/恢复出身（data.bftree 带 CPR 魔数且运行期 merge-to-disk 写长），掉电 rename 丢失即回退为「魔数真 + file_size 陈旧 + 长度写长」文件，get_or_open_tree（:347-354）file_has_cpr_magic 真走 recover_from_cpr_snapshot 撞上述断言。
5 反证排查（不翻案）：settle_detached_release 于 delete_file=false 臂 data_path=None 提前 return 无 sync；wkv 全 crate 零 fsync 调用点；wcpr sync_checkpoint_dir 系检查点目录非树数据目录。数据屏障已由引擎自闭：cpr_snapshot 收尾 vfs.flush()（bf-tree 0.5.6 snapshot.rs:1023）→ std_vfs.rs:69 sync_all()，快照文件内容持久，缺的恰是 rename 目录项屏障，上游无兜底。
6 崩溃后故障形态勘误：断言系 panic 非 Error::Recovery（封装层已移除 catch_unwind，Error::Recovery 只承接 ConfigError）——dev/test 构建下 panic 上抛，release（panic=abort）下进程终止；fail-loud 定性不变，可用性面实为单键不可读至进程终止。
7 查重：task/ 三区无同票；doc/zh/deviations.md 第 78 条 sync_dir 屏障系 wal 新建段面另一事项，反证本仓目录屏障为既定口径。
定级：P3（崩溃窗触发 + fail-loud + 控制面冷路径 + 单键可用性）。

精炼执行方案（审核整理版）：
1 detach_tree heal 臂 rename 成功后补 wdev::sync_dir(data_path.parent())：持条带写锁内（该臂本就执行 cpr_snapshot 重 I/O，同线同代价），仅成功臂需要；失败臂无换入不需屏障
2 sync_dir 失败语义对齐本臂 best-effort：detach_tree 返回 Option 无 Err 通道，与 rename 失败同型 log::warn，不得改签名上抛
3 测试：wdev::sync_dir 系自由函数无注入缝，行为断言（换入后重开成功）既有回归已覆盖；屏障调用本身以 wbftree 内 debug-only 计数钩子断言（与 PUBLISH_FAIL_INJECT 同文件同型，cfg(debug_assertions) 零生产开销），断言 heal 臂换入后计数递增；既有 lifecycle 回归全绿

wbftree detach_tree 离线收口快照换入 rename 后缺目录 fsync，与同 crate publish_tree_from_snapshot_locked 自订双屏障口径相悖（崩溃窗：冷树惰性恢复显式报错不可读）

问题分析：
1 Garnet 契约对齐：无逐函数对位（heal 系本封装层自加不变量收口，C# RangeIndexManager.Locking.cs:RestoreTree 直接信任 pre-stage）；本条系与 crate 自订发布口径的内部不一致，按 review.md 板块 2.2 真实落盘承诺维度收口。
2 工程现状确证：wedb/wbftree/src/manager/lifecycle.rs detach_tree（:566-575）tree.cpr_snapshot(&heal_path) 后 fs::rename(&heal_path, &data_path) 成功臂无任何 sync；同文件 publish_tree_from_snapshot_locked（:736-757）对同一形态换入明文执行数据 fsync + wdev::sync_dir(parent) 双屏障并自陈理由「rename 只改目录项，POSIX 下掉电后不保证可见——必须 fsync 换入目录，杜绝『上层已确认+数据文件目录项丢失→惰性恢复显式报错』的半途发布窗口」。危害前提由 detach_tree 本函数 doc（:546-555）亲证：树运行期基页落盘已把工作文件 data.bftree 写长，快照头 file_size 仅由 cpr_snapshot 收口，直接以工作文件恢复撞引擎自洽断言。
3 逻辑危害确证：冷树回收/换号离线臂（delete_file=false）heal rename 落页缓存未 fsync，掉电/崩溃后 rename 丢失、data.bftree 回退为写长的 pre-heal 工作文件，后续 get_or_open_tree → recover_from_cpr_snapshot 引擎文件长度==file_size 自洽断言失败回 Error::Recovery——该分层键冷数据显式不可读（fail-loud 非静默腐坏，崩溃窗加单键可用性面）；publish 路径已判定同一后果不可接受并修掉，两臂应同口径。

涉及代码：
rust 文件与函数：
wedb/wbftree/src/manager/lifecycle.rs:detach_tree（:546-555 doc、:566-575 rename 无 sync）、publish_tree_from_snapshot_locked 双屏障先例（:736-757）

对应 c# 文件与函数：
N.A.（封装层自加不变量，C# RestoreTree 信任 pre-stage 无对位）

精炼执行方案：
1 detach_tree heal rename 成功臂补 wdev::sync_dir(data_path.parent())（持条带写锁内，与 publish 同代价同口径）
2 锁测：模拟换入后目录项未持久（单测以注入 fsync 失败或以双屏障调用计数断言），恢复路径不再撞自洽断言；既有 lifecycle 回归全绿

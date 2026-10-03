甄别结论：通过（2026-09-29 主控甄别，定级 P3——删除形三函数（truncate.rs truncate_until_segment_impl:230-251、remove_segment:50-63）无 sync_dir 屏障，handle.rs:401 新建形在位对称缺口坐实；recover.rs:88-140 复活段前缀空隙吸收危害链对账成立。deviations §78 新建形明文四点异形不并案，本票第五形独立缺口。修复：三函数各补一次 sync_dir 复用 lib.rs:117 单点原语，erase_tail_after 经 remove_segment 随之收口）

审核结论：通过（2026-09-29 甲轮35-A，P3 级）。三路径零 sync_dir 复核成立（truncate.rs:209-251/:50-62/:74-101），新建形屏障 handle.rs:401 在位对称缺口成立；recover.rs 前缀空隙吸收臂坐实复活危害链；与 wcpr/wbftree/wdev-new-directory/§78 四形异面查重干净。与 wcpr 先例同形：先删后刷单点收口非第二机制。无修正意见。

原票面：
wdev 段删除 unlink 无父目录 fsync：掉电复活已截断段文件集，空间回收延迟且恢复水位回退

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧删段全程无目录屏障：LocalStorageDevice.cs:RemoveSegment（TryRemove + Dispose + File.Delete，尽力删除）与 StorageDeviceBase.cs:TruncateUntilSegmentAsync 编排的删段路径均依赖 NTFS 卷元数据自动持久语义。本票非对 C# 契约分叉，系 rust 自研「持久化发布双屏障口径」（wedb/wdev/src/lib.rs:sync_dir，全仓唯一定义处）在删除形的缺臂：task/done/wcpr-purge-checkpoint-unlink-dir-entry-missing-fsync-resurrection.md 已为「删除形」定案（先删后补 sync_dir，P3），但其修复射程仅覆盖 wcpr/checkpoint_store 的检查点目录；wdev 自身段文件删除路径未被该票、§78（wdev 段新建形）、wbftree-detach（rename 形）、wdev-new-directory（新建目录形）任一裁决覆盖，系第五种异形的独立缺口。五池与 deviations §40/§74（删段吞错面）/§78 查重：删段吞错与删段顺序（wdev-unordered-segment-purge 票）均已另案，删除持久性面零登记。

2 工程现状确证（Rust 现有实现路径与代码缺陷）
段 unlink 三路径均无父目录 fsync：
a) wedb/wdev/src/segmented_device/truncate.rs:truncate_until_segment_impl 第 2 步物理删段循环（remove_file 逐段）全量成功后仅推进 purged_segment，无 sync_dir(parent_dir())；
b) wedb/wdev/src/segmented_device/truncate.rs:remove_segment：broadcast_invalid + reconcile + debug_clear_segment 后 remove_file（NotFound 吞臂），无 sync_dir；
c) wedb/wdev/src/segmented_device/truncate.rs:erase_tail_after 第 2 步经 remove_segment 删提交段之后的孤儿段，unlink 持久性同缺（其第 1 步 set_len 收缩无文件 fsync，但残尾复活属幂等可再收面，非本票主危害）。
POSIX 语义下 unlink 的持久化同样须 fsync 父目录，掉电可致已 unlink 的段文件目录项复活；段新建形屏障（handle.rs:get_or_open_file 创建者 sync_dir）已闭合，删除形断裂。

3 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
删段与掉电竞速窗口内已截断段文件整集复活：recover（recover.rs:recover）前缀空隙臂把复活段吸收重建 start_segment，恢复水位回退至复活段起点，已截断段在设备层重新可寻址；引擎层 begin_address 栅栏挡住逻辑读，无数据正确性实害，实害收敛为：(a) 孤儿段文件集复活占盘，须等下一次 shift_begin_address / handle_capacity 重截断再收；(b) 设备级 start_segment 访问防御水位回退；(c) 容量有界设备的逐出腾空承诺在掉电窗失效。触发窗窄，定级 P3（同 wcpr 删除形票「复活占盘可再收」定级先例）。

涉及代码：
rust 文件与函数：
wedb/wdev/src/segmented_device/truncate.rs: truncate_until_segment_impl（第 2 步删段循环）、remove_segment、erase_tail_after（第 2 步孤儿段删除）
wedb/wdev/src/lib.rs: sync_dir（单点原语，删除形复用）
wedb/wdev/src/segmented_device/recover.rs: recover（前缀空隙吸收臂，危害链对账点）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs: RemoveSegment（删段吞错无目录屏障）
garnet/libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs: TruncateUntilSegmentAsync（删段编排无目录屏障）

精炼执行方案：
1 truncate_until_segment_impl 删段循环全量成功收口后（purged_segment 推进前）补一次 sync_dir(self.parent_dir())，一次调用覆盖本目录内全部 unlink；remove_segment 的 unlink 成功臂同补 sync_dir（先删后刷）；erase_tail_after 复用 remove_segment 即随之收口，不另起第二套机制。
2 与段新建形屏障（handle.rs 创建者 sync_dir）保持同点异形各自单点，不合并调用；非 Unix 平台由 sync_dir 平台臂恒 Ok 自然吸收。
3 测试验证点：truncate_until_segment / remove_segment 后父目录屏障调用链断言（掉电不可模拟按仓内惯例）；复活面幂等回归——复活段被下一轮 truncate 再收、SegmentGap 对真中部空洞 fail-fast 零变化；严禁为防复活引入第二套墓碑/标记机制。

终态注记（2026-09-29 执行席归档）：
合入 fix-wdev-unlink-fsync（156394a）→ dev merge a2a9ddd。收口形态与票面方案逐点一致：truncate_until_segment_impl 删段循环收口后、purged_segment 推进前一次 crate::sync_dir(parent_dir()) 覆盖本目录全部 unlink（屏障失败水位不动，相同段号重试幂等补完）；remove_segment unlink 成功臂先删后刷、失败透明上抛（NotFound 吞臂未变更目录项不刷）；erase_tail_after 经 remove_segment 随之收口零新增机制；与 handle.rs:401 段新建形屏障同原语（lib.rs sync_dir 单点）异形各自单点不并案，非 Unix 恒 Ok 吸收；lib.rs 双屏障口径文档同步扩至删除形。无第二套墓碑/标记机制。
测试 wedb/wdev/tests/device/unlink_persistence.rs 两枚：remove_segment_dir_barrier_failure_propagates_after_unlink——父目录 0o300 权限注入（unlink 需 w+x 照常成功，屏障 open(dir,O_RDONLY) 另需 r 即 EACCES）把「unlink 之后、返回之前」钉成确定性失败点，断言屏障调用链在位且失败上抛、撤除后 NotFound 幂等（修复前该注入下返回 Ok 必红；root 权限注入失效按实测分流断言）；resurrected_truncated_segment_is_reabsorbed_and_recollected——复活段（重建已删段文件模拟掉电目录项回魂）经 recover 前缀空隙吸收水位回退至复活段起点、下一轮 truncate 幂等再收、真中部空洞 SegmentGap fail-fast 零变化对照。
遗留：erase_tail_after 第 1 步 set_len 残尾收缩无文件 fsync 属幂等可再收面，票面已裁非主危害不另立案；worktree 内仅 cargo check --all-targets 零警告，test.sh/clippy 由主代理门禁统一跑。

甄别结论：通过（甄别席 J1，2026-09-27，定级 P2——删段枚举无序致失败残段非连续块，崩溃后 SegmentGap 拒启，复合窗触发）。双侧亲验成立——segment_entries（recover.rs:70-79）裸 read_dir 无排序，删段循环 truncate.rs:210-231 按枚举序逐段删，Windows 臂 :222-226 入队续删、Unix 臂 :227-229 首失败上抛，:132-134 自证注释对非连续残段为假；对照 recover.rs:117 sort_unstable + :123-136 中部空隙 SegmentGap 拒启无自愈，不对称反证成立；注入第 k 段失败后残段 {k, k+2..} 触发拒启的危害链自洽；C# RecoverFiles :180-213 静默重定 startSegment 永不拒启、RemoveSegment :354-359 尽力删除亲验属实；升序删+失败即停保证残段恒为连续块被前缀空隙臂吸收，SegmentGap 对外部真空洞 fail-fast 零变化，不触 §74b 裁决臂（:996-999 登记的是上抛-vs-吞错分叉，本票属裁决外删段顺序缺陷非重复）。行号无漂移。派沙箱席 c01d。

审核结论：通过（删段循读 read_dir 原生序未排序实证，与 recover.rs:117 sort_unstable 不对称反证；Windows 入队续删与 Unix 首败上抛崩溃窗两径皆实，erase_tail_after :91-95 注释自证同仓已知此偏序；C# LocalStorageDevice 静默重定/吞错对位属实，升序删+失败即停不回改 §74b 不触红线4；五池与 §40/§74/§78 查重归零。备注：CI 无 Windows 目标，主现实化为 Unix 崩溃窗，票未夸大）

wdev 删段扫描无序：延迟删除队列与 Unix 中途上抛的交错的残段崩溃后经 recover 空隙判定永久拒启

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
C# LocalStorageDevice.cs:RecoverFiles（:180-213）空隙状态机对任何残段形态静默重定 startSegment（segmentId != prevSegmentId + 1 即重置 startSegment = segmentId），永不报错拒启；RemoveSegment（:354-359）尽力删除吞错。rust 侧中部空隙 fail-fast（SegmentGap 拒启）系 §74 族数据完整性防御收紧裁决在册，本票不触碰该裁决臂；但本票病灶在于：由设备层自身截断产生的残段（逻辑上已废弃、语义上必然可吸收）也会触发该拒启臂，属自造残骸反噬可用性。

2 工程现状确证（Rust 现有实现路径与代码缺陷）
wdev/wedb/wdev/src/segmented_device/truncate.rs:truncate_until_segment_impl（:176-240）第 2 步物理删段按 segment_entries() 的 read_dir 文件系统枚举序逐段删除，非段号升序：
a) Windows 臂（:222-226）单段删除失败（读者持句柄 sharing violation）记入 pending_removes 后继续删除其余段，全部「成功或入队」即推进 purged_segment（:236）。若截断目标 T、段 k 删除失败入队而 k+1..T-1 中部分删除成功，磁盘残段为非连续形态（如 {k, k+2, ..., T-1} 与全部 >= T 段并存）。
b) Unix 臂（:227-229）首个删除失败即 return Err，purged_segment 不推进，但本次扫描中已先删除的段散布随机（同样非连续残段形态），且注释宣称的幂等补完依赖上层在崩溃前重试；进程在上层重试前崩溃即留下同样形态。
c) pending_removes 为纯内存队列，跨进程重启丢失（代码注释 :132-134 自认），重试点仅 handle_capacity 与 truncate 调用（:107-109、:178-179）。
崩溃后重启：wdev/wedb/wdev/src/segmented_device/recover.rs:recover（:121-136）升序扫描残段，首个残段被前缀空隙臂吸收（recovered_start = id），其后任一与残段不相邻的段号即命中 :125-128 中部空隙臂 Err(SegmentGap) 拒启，且该拒启无任何自愈路径（残段是已截断废弃段，逻辑上本应被吸收）。truncate.rs:132-134「重启后 recover 的段号空隙扫描重建 start_segment，残留段文件不参与有效日志语义」的自证注释对非连续残段形态为假。

3 逻辑危害确证
Windows 延迟删除队列 + 崩溃窗口（或 Unix 删段 I/O 故障 + 上层重试前崩溃）后实例永久无法启动：数据面完好（>= T 段连续完整）却被自造交错的废弃残段击穿空隙判定，可用性硬伤；同时该假自证注释（§74b 登记时引为「已自证」依据）误导后续对账席。§74b 裁决面（Unix 上抛 vs C# 吞错、Windows 队列机制）保持不变，本票仅修复残段形态不可控这一裁决外新缺陷。

涉及代码：
rust 文件与函数：
wedb/wdev/src/segmented_device/truncate.rs:truncate_until_segment_impl（删段扫描 :210-231）、retry_pending_removes（:136-159）、注释 :132-134
wedb/wdev/src/segmented_device/recover.rs:recover（空隙状态机 :121-136）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:RecoverFiles（:180-213，静默重定 startSegment 无拒启臂）、LocalStorageDevice.cs:RemoveSegment（:354-359，吞错尽力删除）、StorageDeviceBase.cs:HandleCapacity（:385-395）

精炼执行方案：
1 truncate_until_segment_impl 第 2 步改为：先收集本截断区间内 (id, path) 条目并按段号升序排序再逐段删除；Unix 臂保持首失败即上抛（§74b 裁决不动），Windows 臂首失败起将本轮其余段号全部计入 pending_removes 后即停扫。由此残段形态不变量成立：残段必为紧邻删除目标下方的连续段号块（或空），recover 前缀空隙臂恒可吸收，绝不产出中部空隙。
2 truncate.rs:132-134 注释订正为真实口径：队列丢失可被 recover 吸收的前提是删段按段号升序且失败即停，勿再以泛化「空隙扫描重建」自证。
3 测试验证点：将「排序 + 失败即停的删除计划」抽为纯函数，锁测单点：注入第 k 段删除失败，断言残段集合为连续块、recover 成功且 start_segment 重建正确；既有 wdev truncate/recover 套件全绿回归，SegmentGap 对真中部空洞（外部误删）的 fail-fast 行为零变化。

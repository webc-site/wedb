甄别结论：通过（甄别席 J1，2026-09-27，定级 P1——首部署断电整目录连段文件失踪，承诺虚标）。双侧亲验成立——lib.rs:49-79 双屏障唯一定义、mod.rs:206-211 with_params 裸建、handle.rs:245-250 let _ = create_dir_all 吞错、:401 sync_dir 只刷段文件所在目录内容不覆其自身目录项，POSIX 语义成立；宿主预建臂三锚 server.rs:187/:190/:194、service.rs:963、create.rs:222 全部落实，只补 wdev 两臂则生产首部署恒 no-op 的订正判断正确；C# LocalStorageDevice.cs:150-152 裸建无目录屏障、CreateHandle :431 起 OpenOrCreate 无屏障亲验，本票系 §78 收紧口径在「新建目录形」的缺臂，与 §78 已登记的段文件形不重复、与 wbftree-detach（rename 形）、wedb-repl-receive-checkpoint（接收面）两过审票异形异点不并案；修复为单点原语换调，最小可落。行号无漂移。派沙箱席 c01d。

审核结论：通过（需订正，已并入）（rust 锚点亲验：mod.rs:206-211/handle.rs:245-250 裸建+let _ = 吞错、:401 sync_dir 不覆目录自身链接项断链成立；lib.rs:49-79 双屏障唯一法源自陈相符；C# LocalStorageDevice.cs:152 亦裸建无目录 fsync，本票属自研口径缺臂非契约分叉、勿据 §78 回改判词准确；与 receive-checkpoint 票异层异点不重叠；吞错改上抛不波及装配容错。订正：宿主预建臂 server.rs:187-195/service.rs:963/create.rs:222 并入同单点（步骤 1b），否则 wdev 补臂对生产首部署恒 no-op，修复面不闭合危害面）

wdev 递归新建数据目录无父目录持久化屏障，首次部署整机断电可致整个数据目录连段文件一并失踪

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）
本仓已在 wdev/wedb/wdev/src/lib.rs:sync_dir（:49-79）立「持久化发布双屏障」全仓唯一定义：新建文件或 rename 换入后必须 fsync 父目录，否则断电后目录项不可见；该口径登记为 §78 新建段裁决的根据（段文件创建者承担 sync_dir 父目录屏障，wedb/wdev/src/segmented_device/handle.rs:401），lib.rs:19-22 与 sync.rs:51-55 据此对上层承诺「新段写入 + sync 即持久（含目录项）、调用方无需自行刷盘目录」。C# 原型（LocalStorageDevice.cs:GetOrAddHandle :503-518、CreateHandle :431-470）本无任何目录屏障（§78 已登记该差异为 rust 收紧方向），故本票非对 C# 偏差，而是 rust 自家收紧承诺在「新建目录」这一形上的缺臂——屏障链条在段文件层闭合、在目录层断裂。

2 工程现状确证（Rust 现有实现路径与代码缺陷）
段文件所在数据目录本身由两条新建臂产生，均无 sync_dir：
a) wedb/wdev/src/segmented_device/mod.rs:with_params（:206-211）构造期 create_dir_all(base_path 父目录)，返回值仅透传错误，新目录在其父目录中的目录项不 fsync；
b) wedb/wdev/src/segmented_device/handle.rs:open_file（:245-250）每次可写打开前 let _ = create_dir_all(parent)（错误还被静默吞掉），同样零目录屏障。
新建段文件的 :401 sync_dir(parent) 只 fsync 段文件所在目录的内容，POSIX 语义下该次 fsync 不覆盖此目录自身相对其父目录的链接项；即「wedb.db.<seg> / wal.log.<seg> 目录项持久」的前提「数据目录自身目录项持久」无人背书。消费侧同形对照：wnode/src/service.rs:open_wal（:963）create_dir_all(&wal_dir) 亦裸建，依赖设备层承诺的宿主自行补臂同样缺位（修复面须扩至宿主预建臂：审核席确证 wnode/src/server.rs:187-195 已以 let _ = create_dir_all 先行裸建 --dir/--wal-dir/--checkpoint-dir 三目录，若只补 wdev 内部两臂，生产首部署场景恒 no-op、修复面不闭合危害面——故本票修复面统一为「全仓新建目录一律换调单点持久原语」，杜绝修复面/危害面失配与「不扩层」自相矛盾）。

3 逻辑危害确证
首次部署（或运维新指 --dir / --wal-dir / --checkpoint-dir）后断电崩溃窗口内：段文件数据 fsync、段所在目录 fsync 全部做完，但数据目录自身的目录项未落盘，ext4 等日志文件系统延迟分配下重启后整个数据目录连同全部段文件从文件系统消失；主端已按「sync 即持久」回执给客户端与复制位点的写入成建制丢失，副本重启后空库目录不复存在、位点与盘面发散。属板块 2.2「真实落盘承诺不虚标」维度的承诺缺口（虚标臂），非性能问题。

涉及代码：
rust 文件与函数：
wedb/wdev/src/lib.rs:sync_dir（双屏障口径定义处 :49-79）
wedb/wdev/src/segmented_device/mod.rs:SegmentedDevice::with_params（:206-211）
wedb/wdev/src/segmented_device/handle.rs:SegmentedDevice::open_file（:245-250）
对应 c# 无对位臂（原型无目录屏障，锚 LocalStorageDevice.cs:CreateHandle :431-470、GetOrAddHandle :503-518，差异已在 §78 登记，勿据此回改）

精炼执行方案：
1 在 wdev 内以 sync_dir 为基座补「新建目录提交臂」单点原语（如 ensure_dir_persistent(path)：逐级 create_dir 实际创建成功的每一级对其父目录调用 sync_dir，Unix 生效、非 Unix 随 sync_dir 恒 Ok），mod.rs:with_params 与 handle.rs:open_file 两臂换调该单点，杜绝第三处裸 create_dir_all；handle.rs 处 let _ = 吞错改为错误上抛（目录建不成则后续 open 必败，早失败同口径）。
1b 同单点换调覆盖宿主预建臂：wnode/src/server.rs:187-195 三处 let _ = create_dir_all、service.rs:963 open_wal、wcpr/src/manager/create.rs:222 同形裸建一并换调 ensure_dir_persistent（预建臂吞错可保留 warn 容错形，但目录项持久屏障必须生效）。
2 lib.rs 契约文档与 §78 划界随行注记：双屏障口径覆盖「新建目录形」——新建文件 fsync 所在目录，新建目录 fsync 其父目录。
3 测试验证点：wdev 单测锁臂——临时目录下以不存在的多级父路径构造设备，断言 create 后段文件可读写、recover 可回扫；sync_dir 原语既有 Unix 真目录项 / 非 Unix 恒 Ok 两态回归；无新增运行期成本（仅目录首建路径触发）。

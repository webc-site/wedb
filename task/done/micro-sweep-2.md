# micro-sweep-2 微清理批 2

来源：主代理下发的八条清单（whyperlog readme / 浮点收敛 / hex 下沉 / waof 扫描单点 / waof 模块分层 / ascii 双死 / workspace 收口 / design.md 条 20 甄别）。
判定口径沿用 task/done/micro-dead-sweep.md 与 zero-ref-pubs-cleanup.md：纯移动选改动小路径、删除前全仓 grep、删 C# 对标符号登记 js/check/ignore。
基线：dev @ ee156ec。

## 逐条甄别与处置

一 whyperlog 补 readme
- 执行。whyperlog 无 readme/README.mdt/README.md，对标 whlog/waof 四件套（readme/en.md + readme/zh.md + README.mdt 模板 + README.md 产物）
- 内容定位：HyperLogLog 概率基数估计数据结构（稀疏 RLE / 稠密 6bit 寄存器，PFADD/PFCOUNT/PFMERGE 底层），显式注明与 whlog（HybridLog 日志分配器）命名区分：两域正交，whyperlog 对标 libs/server/Resp/HyperLogLog/HyperLogLog.cs（数据结构），whlog 对标 TsavoriteLog（日志）

二 浮点格式化三处收敛
- 甄别：主体已落地。wresp::resp_memory_writer::format_double 为权威单点（NaN/inf/zmij/去 .0）；wcol/src/resp/output.rs format_double_to 与 format_double 已转调 wresp::format_double；wnode 全部消费面（basic_commands/sorted_set_commands/list_commands/vector/advanced_ops/hash_ops）已走 wresp::format_double 或 ObjectOutput::format_double
- 残余收口：wcol/src/hash/hash_object_impl.rs 私有 format_double 经 ObjectOutput::format_double 的 String 中转再 into_bytes，多一次堆分配；改 zmij::Buffer 直取 as_bytes().to_vec()，注明单点在 wresp
- 不动 ObjectOutput::format_double 的 String 形态（对象域输出 API，多处生产在用）

三 hex 微工具下沉 wbase
- 甄别修正：两处并非同构重复。wacl hex_val 是 hex 字符折 4bit 值（二进制解码路径）；wlua ScriptHashKey::from_hex 是校验 + 小写化文本形态（std is_ascii_hexdigit 内联，不解码，非折值实现），无重复定义，保留不动
- 执行：wbase 新建 hex 模块（cfg feature "hex"，一处定义 hex_val 单字符折值 + hex_decode 成对解码）；wacl acl_password.rs 删本地 hex_val 转调 wbase::hex，from_hash 解码段改用 hex_decode 承接（长度/格式错误语义保持各自错误消息）
- wlua hash_key.rs from_hex 不改（语义为规范化 hex 文本，非折值，登记甄别结论）

四 waof 补内存窗口同步扫描 API
- 执行。wnode/src/aof/waof_sublog.rs scan 手写环形帧解析（read_header/read_vec/verify 逐帧推进）与 waof iterator.rs 内存分支同构，违一处定义
- waof WalLog 新增 scan_memory_with（同步零 I/O 内存窗口扫描，回调逐帧，起点不在窗口内返回 false，帧残迹/越界/校验失败平滑终止）；waof_sublog::scan 删手写段改转调
- C# 对标：TsavoriteLog 扫描单点（C# 无纯内存同步扫描 API，此为消除 wnode 手抄的 waof 自有工程面，注释注明）

五 waof 分 wal/（物理）与 aof/（语义）两模块
- 执行。header.rs 918 行里语义 AofHeader/AofShardedHeader/AofSingleLogTransactionHeader/AofShardedLogTransactionHeader/AofChunkHeader/AofHeaderType（+ 序列化原语 write_at）与 8B 物理帧头 RecordHeader（+ RECORD_HEADER_LEN/EMPTY_PAYLOAD_CRC/payload_crc）同居一文件
- 纯移动：wal/（header/config/disk_window/iterator/log/record/ring_buffer/sequence_number_generator）+ aof/（header/address/args），lib.rs pub use 保持 waof::AofHeader 等根路径兼容，全仓零改动；crate 边界不动
- 测试随文件走：RecordHeader 三测试入 wal/header.rs，Aof* 七测试入 aof/header.rs

六 wbase ascii 模块级双死删
- 甄别：wbase/src/ascii.rs 五函数（is_between/to_lower/to_upper/to_upper_in_place/to_lower_in_place）全仓生产零引用（唯一引用 wbase 自有测试）；C# AsciiUtils 的 rust 消费点已由 std make_ascii_* / eq_ignore_ascii_case 内联承接；wnode 与 wedb_standalone 的 wbase "ascii" feature 空挂
- 执行：删 wbase/src/ascii.rs + lib.rs 声明 + Cargo.toml feature + tests/main.rs test_ascii_primitives + wnode/Cargo.toml 与 wedb_standalone/Cargo.toml 的 "ascii" feature；js/check/ignore/common.yml 登记 AsciiUtils.cs 五函数（跑 check.js 按实际缺失修正）
- wnode dev-dep 逐项复核：aok/rcgen/tempfile/wvector/wedb_test/wnode_test 全在用；ctor 与 log_init 零引用（无 #[ctor] 属性、无 log_init::init），删除
- wedb_standalone dev-dep 复核：随 wbase "ascii" feature 清理同步收缩

七 workspace 依赖表收口
- 执行。根 [workspace.dependencies] 无 crossfire、clap：crossfire 9 crate 直写 "3.1.20"（waof/wbase/wcol/wconn/wcpr/wedb/wkv/wnode/wpubsub），clap 3 crate 直写 "4.6.6" + derive（wconf/wedb/wedb_standalone）
- 入根表统一版本（crossfire = "3.1.20"、clap = { version = "4.6.6", features = ["derive"] }），各 crate 改 workspace = true 形态；只收口声明不升级版本
- 收口为版本声明位置统一，非新增依赖，直接编辑 Cargo.toml

八 design.md 条 20 甄别（只产出归档文档与结论，next/ 由主代理清理）
- 甄别：纯决策记录文本（「三端口处理记录」），三项现状全部核实已落地：
  - VersionShiftFn 调用点组合消除：ClusterProvider::notify_version_shift_start/end（wedb/src/server/cluster_provider.rs:423/:434），检查点接线测试 wedb/tests/checkpoint_wiring.rs，VersionShiftFn 符号全仓零残留
  - ReplicationSinkFn 信号化拉取：ReplicationWakeTx = crossfire MAsyncTx<Array<()>>（waof/src/log.rs:36）+ set_replication_wake（:193）+ AofReplicationPump::attach_wake（wedb/src/server/replication/aof_replication_pump.rs:147），生产 replica_sync_session.rs:118 接线
  - StoreEventSink 保留 Arc<dyn Fn>：wkv/src/store/event.rs:86，含否决论证注释
- 处置：按 reject 归档（理由：记录性文本非待办，无行动项；条内「生产接线待 AOF 门控复制面完工补挂」属另一立项的既有待办，非本条新增），核实证据写入 task/reject/micro-sweep-2.md

## 验收口径

- ./clippy.sh 零警告（CARGO_TARGET_DIR=/tmp/fork/w5-micro2-target）
- ./test.sh 全过
- bun ./js/check.js 无新增缺失/重复
- 删除符号全仓 grep 零残留

## 冲突与并发提示

- 二、四、五涉及 wcol/waof：去重批已合并（基于最新 dev），动工前 fork 自最新 dev，遇冲突以先合并者为准

## 执行结果

- 41 文件 +700/-353，提交 19e8bf2，分支 merge dev（f5a2f47，cluster.yml 恢复工作区同内容改动后自动合并）后合入主目录 dev（7bedfc5）
- 一 whyperlog readme 四件套落地（readme/en.md + readme/zh.md + README.mdt + README.md），含与 whlog 两域正交的显式区分段
- 二 wcol hash_object_impl format_double 直取 wresp::format_double 字节视图，消除 String 中转堆分配
- 三 wbase 新增 hex 模块（feature "hex"：hex_val + hex_decode 定长解码）；wacl 删本地 hex_val 转调；wlua hash_key 甄别保留（判非重复，见 reject 文档）
- 四 waof WalLog::scan_memory_with 内存窗口同步扫描单点落地 + 集成测试；wnode waof_sublog::scan 手写环形帧解析段（37 行）删除改转调
- 五 waof 分 wal/（物理：header/config/disk_window/iterator/log/record/ring_buffer/sequence_number_generator）与 aof/（语义：header/address/args）；header.rs 按物理帧/语义头拆分；根 re-export 保持外部路径兼容，全仓零改动
- 六 wbase ascii 模块双死删（模块 + feature + 声明 + 测试 + wnode/wedb_standalone feature 引用）；wnode dev-dep 删 ctor/log_init（零引用）；js/check/ignore/common.yml 登记 AsciiUtils.cs 五函数
- 七 crossfire（9 crate）与 clap（3 crate）入根 [workspace.dependencies] 统一版本，各 crate 改 workspace = true，版本未动
- 八 design.md 条 20 甄别：三端口决策全部核实已落地（notify_version_shift_start/end、ReplicationWakeTx + attach_wake、StoreEventSink 保留），纯记录文本按 reject 归档（task/reject/micro-sweep-2.md，含逐项证据）；next/ 清理留主代理
- 并发提示：worktree /tmp/fork/w5-micro2 被兄弟代理复用写入三个文件（wconn/src/session.rs、wedb/tests/cluster_iterative_slot_verify.rs、js/check/ignore/cluster.yml 的 slot verify 重复项清理），非本批改动，未提交未合并，留待该批自行处置

## 验证结果

- bun ./js/check.js：无新增缺失/重复（残留 3 项 RespClusterIterativeSlotVerify 重复为基线遗留，主目录合并前即存在，归属 cluster-endpoint-iterverify 批；分支基线、分支 merge dev 后、主目录合并态三处复验一致）
- ./clippy.sh：3 任务全过，-D warnings 零警告（分支与主目录合并态各一轮，CARGO_TARGET_DIR=/tmp/fork/w5-micro2-target）
- ./test.sh：wedb 2064 passed + 1 skipped，regress 2 passed（分支 merge dev 后与主目录合并态复验）
- 合并：分支先 merge dev（无冲突）后合入主目录 dev，worktree 与分支已清理

状态
- 完成

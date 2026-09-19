AOF 三尺寸旋钮读侧接线与装配期互校验

来源：next/aof-size-knobs-read-side-wiring.md（第 8 轮 design 条 1，HIGH）。
落点分支 aof-size-knobs，合入 dev a75287ec。

甄别
- 立论成立：改前三字段（runtime_server_options.rs:67/:69/:71）在 wconf 之外零生产读者，
  装配期亦无任何组合校验，非法尺寸直落运行期；本棒补齐读侧单点与三条互校验。
- 事实修正一：单据把 single_log_aof（wnode/src/aof/waof_sublog.rs:33）当作物理尺寸决策点，
  实测该口只收已定容的 Arc<WalLog<SegmentedDevice>>；生产唯一的 AOF 物理日志构造在
  wnode/src/service.rs:790 open_wal（设备 :807 SegmentedDevice::single_file、日志 :808
  WalConfig::default() 即 16MB 窗口）。service.rs 属在飞 boot 装配棒射程，本棒不动，
  故投影的最后一跳（用投影值构造设备与物理日志）以形状交接，见末段。
  设备为单文件形态（segment_size() 为 None），即 aof-segment-size 在 rust 侧暂无设备落点。
- 事实修正二：「写侧齐」亦不成立。wconf/src/node_options.rs 全文 grep 三字段零命中，
  即无 CLI/配置文件参数源；CONFIG 面三条均为 ConfigMeta::read_only
  （runtime_server_config.rs:342/:349/:356），CONFIG SET 直接拒绝——与 C# 尺寸类
  启动期事实同口径，本棒不新增第二套可变性策略。三旋钮参数源补齐另列待办。
- 拒绝占位：不把配置值塞进 WaofSublog 字段冒充物理尺寸。那样 max_memory_size_bytes
  会对 INFO 与 AOF 体积限额读到配置值而真实窗口仍是 16MB，且 AOF 分块片上界
  （garnet_log/single_log_branch.rs:184 由 log_page_size_bits 决定）会超出环形窗口，
  把非法组合从「拒启」变成「运行期 RecordTooLarge」。故页位与窗口上限恒取物理
  WalConfig，只有窗口按 aof-memory 定容后页才随之生效（体检保证 memory ≥ 2×page）。

落地
- wedb/wnode/src/aof/aof_settings.rs（新，装配体检与投影单点，对标 C# 子日志设置装配口）：
  :48 from_options 一处读取（:49/:50/:51 为三字段全仓唯一生产读点）+ 三条互校验
  （:55 memory 位 ≤ page 位拒、:66 page 位 > segment 位拒、:76 page 位 < 主存页位 + 1 拒）
  + 一次性投影；:34 AofSettings 三元组；:98 wal_config 为物理日志设置唯一装载口；
  :114 knob_bits 收敛 C# 三份 AofXxxSizeBits 近亲方法（下取 2 的幂 + log2 一处，
  解析失败即点名配置项）；:29 MAIN_PAGE_BITS 取 wconf::DEFAULT_HLOG_PAGE_SIZE 单一真源
  （16m，与 C# defaults.conf 主存页同值）。错误文案点名 aof-memory / aof-page-size /
  aof-segment-size / --hlog-page-size 并给出可调下界；错误类型复用
  crate::Error::InvalidArgument（与 service.rs:668 的 hlog 内存/页体检同型，不另起枚举、
  不建第二套体检函数）。
- wedb/wnode/src/aof/garnet_log/mod.rs:72 GarnetLog::new 挂该体检——单物理与分片双拓扑
  共用的装配点（single_log_aof 亦经此口），故只此一处，无第二入口。
- wedb/waof/src/wal/config.rs:37 WalConfig 增 page_size（对标 C# 日志设置页位；:48 缺省与
  :61 WalConfig::new 均随 buffer_size 同值），既有窗口口径逐字节不变。
- wedb/wnode/src/aof/waof_sublog.rs:346 log_page_size_bits 改读 page_size，:351 记 memory
  投影口径，:35-38 记尺寸口径与装载口；mod.rs:21 出口 AofSettings。
- 用例（只写不跑，交主代理集中回归）：aof_settings.rs:159/:176/:194 三条非法组合各一条
  拒启断言（并断言文案点名配置项），:212 缺省组合通过与非 2 的幂就近下取，
  :234 合法组合下由 settings.wal_config() 构造物理日志后实测 log_page_size_bits == 25
  且 max_memory_size_bytes == 64MB，即投影值确为物理读数。

验收
- 前后 grep 读数：改前（基线 917754f5）三字段除 wconf 定义/格式化/测试外生产读者 0
  （wmetric 与 resp_info 命中的 aof_memory_size_bytes 是 INFO 观测字段，非本旋钮）；
  改后（dev a75287ec）生产读点 3 处（aof_settings.rs:49/:50/:51），全仓含用例命中 15 处。
- 缺省组合 128m/32m/1g 三条校验全过（页位 25 恰在主存 16m 页的两倍下界上），
  默认值下行为不变。
- 门禁：cargo check -p waof -p wnode --all-targets EXIT=0 且无告警。
  cargo check --workspace --all-targets 仍红于 wext_json（tests/json_commands_test.rs 以
  5 参调用 wext_json/src/json_object.rs:139 的 6 参 try_get，25 处）与 wext_roaring
  （lib test 21 处），为 dev 存量破口，两 crate 不依赖 waof/wnode/wconf，与本棒无关，
  集中回归前需先修。
- bun js/check.js 在 worktree 内 EXIT=0，本棒新增锚点（GetAofSettings 与三条
  AofXxxSizeBits、TsavoriteLog 页位读数）零重复定义、零虚构锚点；工具顺手改写
  js/check/ignore/server.yml（淘汰上述四条）与 common.yml，并夹带大量无关散文重排及
  他棒 TryParseAddressList 淘汰——js/check/ignore 属 gate-anchor 棒射程，已回退不入库，
  淘汰登记随该棒或主代理统一收口。

交接（本棒射程外，勿另起第二套）
- boot 装配棒：wnode/src/service.rs:790 open_wal 内在设备分配前调
  AofSettings::from_options(&options)?（与 C# 先体检后分配同序），并以
  settings.wal_config() 取代 WalConfig::default()；若要让 aof-segment-size 生效，
  设备需从 :807 的 single_file 换为 SegmentedDevice::segmented(path, segment_size_bytes)
  口径，段大小真值即 settings.segment_size_bytes。实配主存页容量接入
  aof_settings.rs:29 的 MAIN_PAGE_BITS 单点即可，校验本体无需改动。
- wait-for-commit 棒：提交频率与等待提交档位的组合校验在 AofSettings::from_options 体内
  接续（同一入口、同一 Error::InvalidArgument），勿再建参数体检函数或错误类型。
- 三旋钮 CLI/配置文件参数源（node_options.rs）另棒：补齐后本口与 CONFIG 回显同时生效，
  无需改动读侧。

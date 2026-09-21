# wkv 去除对 wconf 的反向依赖

## 判定

票面成立，开工核实如下。

1. 反向依赖属实：wedb/wkv/Cargo.toml 声明 wconf.workspace = true，代码消费仅三处：
   wkv/src/config.rs 的 GcConfig.compaction_type 字段用 wconf::LogCompactionType；
   wkv/src/gc/compact.rs 用 wconf::LogCompactionType 做 None 短路、Shift 分派，
   并在调用 store.compact 前把 LogCompactionType 逐档转换成 wcompact::CompactionType
   （Scan => Scan，其余 => Lookup），这就是票面指出的双写转换层；
   wkv/src/store/vdb_load.rs 用 wconf::MAX_DATABASES_MAX 界定回建路由的库号枚举空间。
2. 双写属实：wcompact::CompactionType（Lookup、Scan）与 wconf::LogCompactionType
   （None、Shift、Lookup、Scan）同族，Lookup/Scan 两名同义，且全仓唯一转换点就在
   wkv 内部，wkv 同时引用两个类型，构成底层依赖上层配置库的坏拓扑。
3. C# 对位：C# 里 Tsavorite 纯存储层（libs/storage/Tsavorite/cs/src/core/Compaction/
   CompactionType.cs）只有 Scan/Lookup 两档执行策略，四档政策枚举
   LogCompactionType（None/Shift/Lookup/Scan，libs/server/LogCompactionType.cs）
   由 libs/server 的 DatabaseManagerBase.DoCompactionAsync 消费并在 switch 各分支
   直接向 Tsavorite 注入两档枚举。本仓因内置 GC（GcManager 承担 DoCompactionAsync
   对位职责，覆盖无服务端进程的嵌入式场景，见 wkv/src/config.rs 注释）把该调度
   下沉进了 wkv，故四档枚举必须落在不高于 wkv 的层。wcompact 即压缩抽象 crate，
   是本仓该政策枚举的正确单源之家；wkv 已依赖 wcompact，wconf 反向依赖 wcompact
   无环（wcompact 依赖链不含 wconf）。

## 方案

1. 压缩类型单源：wcompact::CompactionType 收敛为四档
   None=0 / Shift=1 / Lookup=2 / Scan=3（repr u8，判别值沿用 C# 服务层
   LogCompactionType 序，CONFIG 槽位文本与数值口径不变），把 wconf::
   log_compaction_type.rs 上的解析与反查方法（MEMBERS、from_raw、as_name、
   try_parse）一并迁入；删除 wconf::LogCompactionType，wconf 的 CONFIG 表
   （EnumMeta、ConfigReconcile、RuntimeServerOptions）直接复用该类型。
   禁止转换层：wkv 调度器不再做跨类型 match，仅保留熔断 None 档归一为 Lookup
   档这一业务兜底；紧缩器 compact_with_filter 对非执行档（None/Shift）以类型化
   Err 拒绝，对标内核自保硬拒的既有风格，不做别名或壳。
2. MAX_DATABASES_MAX 归属：按 rust_review 规范「跨模块公共常量拆到 wbase 的
   cfg 模块按需启用特性」处理。该常量为单日志多库自定义架构的协议绝对上界，
   消费方横跨 wconf（NodeArgs::validate 启动定界）、wkv（vdb_load 回建枚举）、
   wnode（测试锚定），且 vdb_load 注释明确要求上界单点定义、不得另立第二套，
   也不应降格为构造参数注入（它是协议空间常量而非实例可调项）。故迁入
   wbase::cfg（新 feature cfg），wconf 与 wkv 改从 wbase 引用，
   MAX_DATABASES_MIN、DEFAULT_MAX_DATABASES 仅 wconf 自用，留在 wconf。
3. 依赖变更全部经 cargo：wkv cargo remove wconf；wconf cargo add wcompact 与
   wbase（features cfg）；wnode 的 wbase 增开 cfg 特性、增 wcompact（dev，测试
   命名字节类型用）；wkv 的 wbase 增开 cfg 特性。

## 验收

cargo check -p wkv -p wnode -p wconf -p wcompact -p wbase --tests 通过，
定向跑 wconf、wkv（gc/config/compact 测试）、wnode config_owner_bridge 等相关测试；
wkv 不再出现任何 wconf 引用；全仓仅一套压缩档位枚举。

终态注记：合入 2768979，主案落地——锁门谓词与写面谓词归一（dir/wal/checkpoint 任一非空即 acquire），dir 形参二选一取「有意保留空串即锁 cwd」语义（"" join wedb.lock 相对 cwd 解析恰覆盖 dir="" 全相对写面；补过滤反致「dir 空串+异置 wal-dir」锁集不相交漏面），全空由调用侧谓词跳过；另修前置 wdev::ensure_dir_persistent 裸相对路径父链误含 "" 致 create_dir("") ENOENT（不修则 dir="" 在锁门前第 1 步即炸、归一不可达）；测试段 8 三小节（dir="" 持锁 vs 显式同目录拒启、双 dir="" 同 cwd 闭环、常规持锁者反向拦截）全绿。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——server.rs:184-209 三目录各自判空、锁门仅判 dir 空整把跳过，而 wal_dir() 空派生 wal、checkpoint 空回落 parent 真实打开；datadir_lock.rs:82-83 过滤空串但 Some(dir) 无条件。C# LocalStorageDevice.cs FileShare.ReadWrite 多进程共开、bind 面拒第二实例。修复：主案任一非空即 acquire 内建过滤去重兜底，写明 dir 空串语义，二选一禁双机制）

审核结论：通过（2026-09-29 甲轮35-B，P3 级）。谓词分叉全链复核坐实（:185/:201-209 单判 dir vs wal_dir()/data_path() 空 dir 折出 "wal"/"wedb.db" 相对写面；validate 零 dir 空串拒收；DEFAULT_DIR "./data" 故触发面为显式配置）。执行席遵照（审核席订正）：acquire 的空串过滤只盖 wal/checkpoint 两参（datadir_lock.rs:82-83），dir 形参 ：81 无条件加锁——票面「三者全空 held 空集」表述与现码不符；落地时二选一写明：acquire 内补 dir 空串过滤使全空真零锁，或有意保留 dir="" 即锁 cwd 语义（写面恰在 cwd 语义上反而正确）。

原票面：
datadir_lock 空目录跳过谓词与实际写面不一致：dir 显式空串时数据段 WAL 检查点仍落相对路径真实文件，唯一防线整体缺席

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧无数据目录独占锁：Tsavorite LocalStorageDevice.CreateHandle 以 FileShare.ReadWrite 打开段文件（libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:434 fileShare 装配与 :455 CreateFileW），多进程可同时打开同一段文件族互不拦截；防第二实例仅靠 libs/server/Resps/GarnetServerTcp.cs:111-114 显式关闭端口复用在 bind 面拒第二实例。该缺席即 deviations §11 在册自研面前提（wedb/wnode/src/datadir_lock.rs 模块头注：SO_REUSEPORT 偏差下网络层拦不住，唯 flock 防线）。本票不挑战 §11 裁决，只钉自研守卫实现与其自身声明前提的脱节。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wnode/src/server.rs ServerBootstrap::run_async 第 1.5 步（:197-209）：锁门跳过判据仅看 node_args.dir 为空，dir 空即整把不取。但同一函数第 1 步（:184-195）对 wal_dir 与 checkpoint_base_dir 独立于 dir 各自判空建目录：wconf NodeArgs::wal_dir（wedb/wconf/src/node_options.rs:1322-1324）在 dir 为空且未显式配置 wal-dir 时返回 PathBuf::from("").join("wal") 即字面 "wal"（非空），run_async 照常 wdev::ensure_dir_persistent 真实建目录；锁门却因 dir 空整体跳过，两处谓词分叉。装配消费面同一组路径照常落盘：wedb/src/server/boot.rs:212 node.data_path()（node_options.rs:1340-1342，dir 空 join 得相对路径 "wedb.db"，SegmentedDevice O_RDWR 打开）；wedb/wnode/src/service.rs open_wal（:896-925）显式 wal-dir 缺席时按 data_path.parent() 回落 "./wal" 真实打开 WAL 设备；checkpoint_dir_of（:818-827）空基目录回落 data_path.parent()/Store/checkpoints。即 dir="" 配置下数据段、WAL 段、检查点目录全部是可写物理写面，datadir_lock.rs:60 注释「空目录形态由调用侧跳过，无目录即无互踩面」的前提在 bootstrap 生产路径不成立。wconf NodeArgs::validate 无 dir 空串拒收闸（wedb/wconf/src/node_options.rs validate 体零 dir 门），TOML `dir = ""` 或 `--dir ""` 即达此形态；缺省值恒 DEFAULT_DIR "./data"（node_options.rs:51），故触发面为显式配置。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
§11 威胁模型原话场景「同机同 UID 误启第二实例指向同一存储」在 dir="" 形态下失防：双实例同 cwd 启动，两进程分别打开同一 wedb.db 与 wal/wal.log.N 段文件族 O_RDWR 互踩，写入静默交错损坏存储；或 dir="" 实例与 --dir . 实例写同一物理文件族，后者持锁前者无锁照写，互斥失效。独占锁作为本仓防双写「唯一防线」存在成片豁免口，与 review.md 5.2「实例独占与安全边界：数据存储目录建立独占文件锁守护，防范多进程并发双写破坏存储」相抵。触发需显式空 dir 配置，非常规误启即可达，定级 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/server.rs:ServerBootstrap::run_async（:184-195 ensure 三目录与 :201-209 锁门跳过判据分叉）
wedb/wnode/src/datadir_lock.rs:DataDirLock::acquire（candidates 空串过滤与 same_path 去重已内建，缺调用侧正确送参判据）
wedb/wconf/src/node_options.rs:NodeArgs::wal_dir / NodeArgs::checkpoint_base_dir / NodeArgs::data_path / NodeArgs::validate（无空 dir 拒启闸）
wedb/wnode/src/service.rs:open_wal / checkpoint_dir_of（空 dir 下相对路径真实写面）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:LocalStorageDevice.CreateHandle（FileShare.ReadWrite 多进程共开，无目录锁）
libs/server/Resps/GarnetServerTcp.cs:GarnetServerTcp 构造（:111-114 bind 面拒第二实例，C# 唯一防线；本仓 SO_REUSEPORT 偏差下不适用，§11 在册）

精炼执行方案：
1. 锁面谓词与写面谓词归一：run_async 锁门去掉 dir 单独判空，改为 dir、wal_dir、checkpoint_base_dir 任一非空即调 DataDirLock::acquire（acquire 内建空串过滤与 same_path 去重，三者全空时 held 空集自然零锁，嵌入式无目录形态语义不变）；同步订正 datadir_lock.rs 头注「无目录即无互踩面」与 server.rs :197-201 注释措辞。
2. 备选：NodeArgs::validate 增 dir 空串拒启闸（生产 bootstrap 路径拒收，嵌入式直构不经 validate 不受影响）；两案取一，禁双机制并存。
3. 测试验证点：wedb/wnode/tests/datadir_flock_exclusive.rs 增 dir="" 互斥用例（实例 A dir="" 于临时 cwd 持 WAL 相对锁，实例 B 指向同一目录拒启；及 dir="" 与 dir=同目录 双实例闭环）；既有嵌入式空目录与 wal/checkpoint 去重用例回归绿。

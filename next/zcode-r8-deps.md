# zcode-r8-deps 依赖供应链×Cargo.lock/feature/重复依赖

视角:依赖与构建面(全仓横向)。方法:36 个 crate 的 Cargo.toml 全量清点 + Cargo.lock 336 包反查 + 全 src use 语句扫描 + feature 定义/cfg/消费三方矩阵。轮 1-7 未覆盖此面。

按优先级排:

1. wnode 死依赖 enum_dispatch

问题:[dependencies] 声明 enum_dispatch,src/tests 零引用,纯死重。
位置:wedb/wnode/Cargo.toml(第 70 行,紧随 whasher/windex 之后)
证据:grep -rn enum_dispatch wnode/src wnode/tests 零命中;真实消费仅 wlua/src/allocator.rs(use enum_dispatch::enum_dispatch)。wnode 自己的 cargo-machete ignored 也未豁免它,说明不是形态豁免,是残留。
建议:cargo remove enum_dispatch -p wnode。SKILL 允许 enum_dispatch 做静态分发,但 wnode 当前全走手写 match,无一处用宏;留声明只会误导后续开发以为 wnode 有 enum_dispatch 分发层。

2. SKILL 点名选型三项逃逸 workspace 单点管理

问题:fearless_simd、nested-text、luau0-src 是 transpile SKILL 点名选型(simd/配置文件/lua),但 [workspace.dependencies] 无条目,各 crate 直写版本。
位置:wedb/wbase/Cargo.toml(fearless_simd = { version = "0.7.0", optional = true })、wedb/wconf/Cargo.toml(nested-text = "0.1.0")、wedb/wlua/Cargo.toml([build-dependencies] luau0-src = "0.21.0")
证据:根 Cargo.toml [workspace.dependencies](41-80 行)只有 compio/crossfire/papaya/gxhash/bitcode/sonic-rs 等有条目;锁内三者均单版本(fearless_simd 0.7.0、nested-text 0.1.0、luau0-src 0.21.0+luau736),当前无漂移,但选型库与普通库管理分级不一致。
建议:三项提升入 [workspace.dependencies]。luau0-src 仅 wlua 一家 build 消费,可不升;fearless_simd 与 nested-text 至少要在 workspace 注释区登记版本锚点,防止新 crate 引入时各写各的。

3. 已有 workspace 条目、却被直写绕过(单点管理名存实亡点)

问题:根 [workspace.dependencies] 已有条目,同版本却在 crate 清单里直写,workspace 升级时这些点不会被 bump。
位置与证据(锁内均单版本、与 workspace 条目当前恰好一致,属纯管理漂移):
- wedb/windex/Cargo.toml:libc = "0.2.189"、log = "0.4.34" 直写(workspace 有 libc/log 条目)
- wedb/wpubsub/Cargo.toml:event-listener = "5" 直写(workspace 有 5.4.2)
- wedb/wnode/Cargo.toml:smallvec = "1.16.1" 直写、rustls-pki-types = { version = "1.15.1", optional = true } 直写(两者 workspace 均有条目;rustls-pki-types 直写是为与 compio-tls 内 rustls 对齐版本,已在 [package.metadata.cargo-machete] 注释登记豁免,但可用 workspace 条目达成同一目的)
- wedb/wbase/Cargo.toml [dev-dependencies]:aok = "0.1.18"、ctor = "1.0.13"、log = "0.4.34"、log_init = "0.1.39" 直写(同文件 gxhash 用了 .workspace = true,同清单两制)
建议:全部改回 .workspace = true。

4. 跨 crate 重复直写同版、无 workspace 条目

问题:同一第三方库在多个 crate 各写一份版本号,无单点。
位置与证据:
- num_enum 0.7.6:wcol/wmetric/wresp 三处直写(均真实消费,derive 转 enum)
- compio-tls 0.10.0:wconn/wnode 两处直写(均 optional + tls feature)
- enum_dispatch 0.3.13:wlua/wnode 两处直写(wnode 侧按第 1 条移除后剩单消费)
建议:num_enum 与 compio-tls 提升入 [workspace.dependencies](带 optional 语义由消费侧 dep: 引用表达)。enum_dispatch 待第 1 条落地后仅剩 wlua,可不动。

5. wvector 直引 rand,与全仓 fastrand 惯例分裂

问题:wvector 是全仓唯一直引 rand 的 crate,其余 17 个文件的随机源全是 fastrand(workspace 条目在)。
位置:wedb/wvector/Cargo.toml(rand = "0.9.5")、wvector/src/quantization.rs 105/224、wvector/src/provider/data_provider.rs 29/486/498
证据与判定:quantization.rs 两处是把 &mut rand::rng() 传给 diskann API(SphericalQuantizer::train / Transform::new,diskann 0.59 自身依赖 rand 0.9),属 API 边界硬约束,rand 不可避免;data_provider.rs 的 random_members 是自家采样逻辑(rand::seq::index::sample + rand::rng()),与 fastrand 侧无类型耦合,可换。
建议:保留 rand 但在 wvector/Cargo.toml 加注释登记 diskann 边界强制(wnode 的 machete 豁免注释是先例);random_members 侧可改 fastrand + 部分 Fisher-Yates,不改也应在注释里说明差异来源。锁内 rand 单版本,diskann 全家也用它,无重复版本代价。

6. webpki-roots 锁内双版本共存

问题:同一 crate 两版本共存:0.26.11(wconn 直写)与 1.0.9。
位置:wedb/wconn/Cargo.toml(webpki-roots = { version = "0.26", optional = true })
证据:Cargo.lock 中 webpki-roots@0.26.11 的 dependencies 就是 ["webpki-roots 1.0.9"],0.26.11 已是 1.x 数据的再导出包装层;证书数据本身单份,代价是双 crate 编译 + 依赖链多一跳。
建议:wconn 直依赖 1.x,摘掉包装层,锁内归一。此条非"版本旧要升级"(0.26.11 自身已完成数据迁移),是重复版本共存项。

7. 旁支 workspace 路径依赖版本下限滞后

问题:regress 与 bench 是独立 workspace,路径依赖 wedb 内部 crate 时 version 下限停在旧版。
位置与证据:regress/Cargo.toml(30-32 行)wbftree = { version = "0.1.4", path = "../wedb/wbftree" }(实际 0.1.6)、wdev = "0.1.3"(实际 0.1.7);bench/bench/Cargo.toml 同型(wbftree 0.1.4、wdev 0.1.3),且 sonic-rs = "0.5.8" 低于主仓 0.5.10(独立锁,不构成冲突)。
证据:caret 语义 ^0.1.4 覆盖 0.1.6,可构建,纯声明滞后,无行为影响。
建议:下次触碰旁支时顺手对齐下限即可,不值得专程一轮。

无增量确认(已核实无问题的面):

- 等价物绕过:tokio/serde_json/uuid/time/chrono/dashmap 在锁内均为第三方传递(diskann-providers/bf-tree/rcgen/rkyv/loom),无任何 crate 直接依赖,代码 use 零命中。
- std 锁绕过 parking_lot:std::sync::Mutex/RwLock 零命中。std HashMap/HashSet 绕过 gxhash:零命中。std::time::Instant 两处(waof/src/wal/sequence_number_generator.rs、wbase/src/time.rs)均有对标 Stopwatch/直方图高精度语义的注释登记,coarsetime 纪律保持。
- gxhash deterministic 全局 feature:wbase/src/map.rs 已在 papaya 域以进程级随机种子显式承接(GxBuildHasher::with_seed(SEED),fastrand 播种,OnceLock 单例),SCAN 确定性域走默认恒种子,两消费面种子层分离,workspace 注释与 qcode.my #4 判例一致,维持现状。
- SKILL default = ["roaring", ...]:wnode default = ["roaring", "json"],wext_roaring/wext_json 均 optional + dep: feature 挂载,无运行时动态注册残余。
- papaya + gxhash 收敛在 wbase map/set feature(即 SKILL 指定的 wbase/map.rs 单点),9 个消费 crate 全部经 feature 引用,无一旁路直依赖 papaya。
- feature 面无死角:wbase 30 个 feature(除 default)全有 cfg 消费;tls 链(wnode 46 处/wconn 28 处/wedb 24 处 cfg)完整;无任何 crate 出现 cfg 引用未定义 feature;wnode roaring/json 各 13 处 cfg 与 default 启用一致。门禁已双覆盖:test.sh = nextest --all-features,feature.check.sh = 逐 crate 单独构建抓漏声明(wbase-feature-decl-gate 判例)。
- dev/build 边界:全 workspace 唯一 build.rs 是 wlua(luau0-src + cc 编译 vendored Luau,sys.rs 手写 FFI,无 mlua 旁路);测试支撑 crate(wtest_base/wedb_test/wnode_test/wtxn_test)全部经 dev-dependencies 消费,无主代码依赖 dev-only 能力;「主依赖仅测试消费」全仓扫描零命中(wnode rustls-pki-types 为已登记的版本对齐豁免,见第 3 条)。
- 锁内其余重复版本(bitflags 1.3.2/2.13.2、getrandom ×3、io-uring ×2、syn 2/3、windows-sys ×3、toml_edit/winnow 双轨)全部为第三方传递图固有分裂,非本仓可控,不动。
- [workspace.dependencies] 无孤儿条目(每个条目至少一个消费 crate)。

视角结论:有增量

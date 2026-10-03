终态注记（2026-09-28 主控）：已合入 dev（合入 commit cc03c74）。
收口形态：
1. wedb/wnode_tls_test/Cargo.toml：wnode 依赖去 features = ["tls"]，改 wnode.workspace = true；新增 [features] tls = ["wnode/tls"]（默认关）；新增 [dev-dependencies] wedb = { workspace = true, features = ["tls"] }。
2. wedb/wnode/Cargo.toml 与 wedb/wedb/Cargo.toml：摘除 dev-dependencies 中的 wnode_tls_test 行，断除默认特性形态下测试无条件统一拉入 wnode/tls 的毒化闭包。
3. 测试迁居至 wedb/wnode_tls_test/tests/：wedb/tests/replication_egress_tls_gate.rs 迁入；wnode/tests 下 7 个纯 TLS 册迁入并加 #![cfg(feature = "tls")]；push.rs 拆分为明文基线（原地保留）与 TLS 订阅推送（迁入）。
4. wedb/wedb/tests/error_sink_tests.rs：订正册头注释，明确形态可见性（默认特性可见拒启端到端测，--all-features 下该册出局）。180s 超时长红根除。

# 无条件 dev-dep 把 wnode/tls 统一进测试形态：not(tls) 拒启门禁在测试二进制里整体编译出局

定级：P3（测试基础设施形制缺陷；生产零危害——前置票已证伪「静默明文起服」的生产判断，
但门禁的端到端拒启面在测试形态恒不可执行，且 error_sink_tests 在默认特性形态长红 180s）

## 领票复核与方案裁定（2026-09-28 主控二次现码复锚）

1. 现锚复跑（全部亲验，非票面转抄）
   - 门禁本体在位：`wedb/wnode/src/server.rs:181-182` `#[cfg(not(feature = "tls"))] guard_no_tls(node_args)?;`，
     定义 :912-921；「tls_cert 与 tls_key 必须同时提供」串确在 `#[cfg(feature = "tls")]` 臂 :906-908。
   - `cargo tree -p wedb -i wnode` 实测输出 `wnode v0.1.0 default,json,roaring,tls`，拉入路径
     `wedb [dev] → wnode_tls_test → wnode(tls)`；根因（无条件 dev-dep 特性统一）坐实。
   - manifest 现位：`wedb/wedb/Cargo.toml:104`、`wedb/wnode/Cargo.toml:85`、
     `wedb/wnode_tls_test/Cargo.toml:16`（票面 :17 漂移一行）。
   - `wedb/wedb/tests/error_sink_tests.rs:17` 整册 `#![cfg(not(feature = "tls"))]`，
     长红炸点为该册 :28 `boot_tls_gate_error_routed_through_node_variant`。
   - 全仓 `required-features` 零命中（Cargo 语义亦不能由 `[features]` 引用 dev-dependencies）。

2. 三案裁定：案 1 / 案 3 判不可行，采案 2 的结构分流落地形态
   - 案 1（消费方在自身 manifest 叠加 tls 特性）与案 3（`tls-test-support` 特性默认关）同撞
     Cargo 两条硬语义：`[features]` 不得指向 dev-dependencies，dev-dependencies 的 `features`
     只能无条件声明。故 wnode/wedb 自家测试形态里 tls 特性要么恒开（等于现状，门禁仍整体出局），
     要么根本无法条件开启 → 两案皆空转，予以否决。
   - 落地形态（案 2 精神，不新建包）：TLS 用例迁居支撑包自身 `tests/`，各包测试形态只链
     自己需要的 wnode 特性形态。
     a) `wnode_tls_test/Cargo.toml`：`wnode` 依赖去 `features = ["tls"]`，本包新增
        `[features] tls = ["wnode/tls"]`（默认关）；包内 tls helper 按 `cfg(feature = "tls")` 门控。
     b) `wnode/Cargo.toml:85`、`wedb/Cargo.toml:104` 摘除 `wnode_tls_test` dev-dep，
        使 wnode/wedb 默认特性测试形态链到非 tls 的 wnode，拒启门禁端到端测得起来。
     c) 以 tls 形态起服的消费册迁至 `wnode_tls_test/tests/`，册头 `#![cfg(feature = "tls")]`。
        候选消费册（须逐册现验后再迁，禁整册盲迁）：wnode/tests/{server_cert_reload,
        tls_shutdown_tail_timeout,server_mtls,cluster_outbound_cert_hotswap,server_lifecycle,
        push,handshake_timeout,client_laddr}、wedb/tests/replication_egress_tls_gate；
        混合册（tls 与非 tls 用例同册）按用例归属拆分，非 tls 部分原地留。
     d) 毒化闭包现验（主控实测）：默认特性形态把 wnode 拉进 tls 的只有 `wnode/Cargo.toml:85`
        与 `wedb/Cargo.toml:104` 这两条 dev-dep；`wnode_test`、`wtest_base` 均为 `wnode.workspace = true`
        不带特性，非拉入者。摘除这两行即断根，勿在他处找第二根因。
     e) 需 wedb 公共面（`run_cluster_server` 等）的 tls 册（`replication_egress_tls_gate`）
        优先落到 `wnode_tls_test/tests/`，由该包 `[dev-dependencies] wedb = { workspace = true,
        features = ["tls"] }` 承接（wedb 摘掉对 wnode_tls_test 的 dev-dep 后无环）；
        确需新建工作区包才成立的，先报主控裁定，席不自行加 workspace 成员。
   - 形态可见性终态（须在册头写清）：`--all-features` 门禁形态跑 tls 装配面（wnode_tls_test/tests），
     拒启面由 `error_sink_tests` 册头 `#![cfg(not(feature = "tls"))]` 在该形态出局；
     默认特性形态跑拒启端到端面（`error_sink_tests` 的 boot 门禁用例）；
     两面各守其一，禁「两面皆不可见」。
   - 现码复核补充（订正票面与册头注释）：`error_sink_tests.rs` 仅在册头注释提及 `wnode_tls_test`，
     零 API 依赖，摘除 dev-dep 后整册原地可跑；`replication_egress_tls_gate` 为唯一需搬离 wedb 的册。
     册头 :13-14 所称 wnode 侧 `tls_args_rejected_without_tls_feature` 集成册 **全仓零命中**
     （rg 亲验，仅存于该注释），属前置票 boot-tls-gate 的落空登记；拒启端到端面唯一真所在
     为 `error_sink_tests.rs:28 boot_tls_gate_error_routed_through_node_variant`。
     席须把该册头注释改写为现树实际形态（写清「哪一形态可见」），禁留指向不存在册的死引用。

3. 派席授权边界（本票涉 manifest，席禁手改 Cargo.toml 的通则由主控显式授权破例）
   允许改动的 manifest 面仅限下列：
   - `wnode_tls_test/Cargo.toml`：`wnode` 依赖去 `features = ["tls"]`；新增 `[features] tls` 段；
     新增 `[dev-dependencies] wedb`（携 `features = ["tls"]`，用 `workspace = true` 引用，
     不改 `wedb/Cargo.toml` 的 workspace 成员与 workspace.dependencies 清单）。
   - `wnode/Cargo.toml:85`、`wedb/Cargo.toml:104`：摘除 `wnode_tls_test` dev-dep 行。
   除上述外其余依赖一律 `cargo add` 形态；禁加 workspace 成员、禁动 `wedb/Cargo.toml` 主清单一行。
   禁触 `wedb/wedb/src/server/replication/**`、`cluster_provider/**`、`wedb/wtxn/**`、
   `wedb/wkv/src/read_cache/**`、`wedb/wreviv/**`、`wedb/wnode/src/aof/**`、`wext_json`
   （在途席域：aof 哨兵票、wext-json 票、wreviv 级联窗票同波并跑）。
   门禁语义（`guard_no_tls`、`has_tls`、四枚旗标投影）零改动，`tls_flags_node_projection` 勿重做。

## 甄别结论：通过（2026-09-28 主控据席 5fca3ec 实测复核，双侧现锚）

1. 事实链（现树实测）
   - 门禁本体在位且判据正确：`wedb/wnode/src/server.rs:181-182`
     `#[cfg(not(feature = "tls"))] guard_no_tls(node_args)?;`，定义 :913-921；
     `NodeArgs::has_tls()` `wedb/wconf/src/node_options.rs:1226-1233`，四字段无 cfg 门。
   - 输入面本已正确（前置票证伪记录）：四枚 `--tls-*` 旗标经 `ClusterArgs::from_args_iter`
     逐一落进 `NodeArgs` 且 `has_tls()` 为真，`--all-features` 与默认特性双形态各 2 passed
     （`wedb/wedb/tests/tls_flags_node_projection.rs`）。
   - 真根因：`wedb/wedb/Cargo.toml:104` 与 `wedb/wnode/Cargo.toml:85` 把 `wnode_tls_test`
     列为**无条件 dev-dependencies**，而 `wedb/wnode_tls_test/Cargo.toml:17`
     声明 `wnode = { workspace = true, features = ["tls"] }`。dev-dep 在 Cargo 语义里
     无法 `optional`（optional 只对普通依赖生效），故凡 `cargo test/check --all-targets`
     形态 wnode 一律以 tls 特性编译 → `not(tls)` 门禁体在测试二进制整体出局。
   - 可观测后果：`cargo test -p wedb --test error_sink_tests`（默认特性）实测
     3 run / 2 passed / 1 timed out（180.3s，炸点 :26:46 `必须拒绝启动: ()`）；
     `run_cluster_server_tls_gate` 实拿错误文本「tls_cert 与 tls_key 必须同时提供」，
     该串仅存在于 `#[cfg(feature = "tls")]` 臂 `server.rs:898-908`。门禁形态
     （`./test.sh` = `--all-features`）该册 0 tests 恒不执行，红不可见。

2. C# 契约
   `garnet/libs/host/GarnetServer.cs` 构造期校验先于起服（TLS 旗标已给而未启用即拒启），
   `garnet/libs/host/Configuration/Options.cs` 为校验真值源；C# 以单构建形态测试，
   不存在 rust 这种「测试二进制特性与生产二进制特性分叉」的承接面。故本票修的是**测试接线**，
   不动门禁语义。

## 涉及代码

rust 文件与函数：
- `wedb/wedb/Cargo.toml:104`、`wedb/wnode/Cargo.toml:82/:85`（无条件 dev-dep 声明面）
- `wedb/wnode_tls_test/Cargo.toml:17`（wnode features=["tls"] 拉入点）
- `wedb/wedb/tests/error_sink_tests.rs`（端到端拒启册，整册 `#![cfg(not(feature = "tls"))]`）
- `wedb/wedb/tests/tls_flags_node_projection.rs`（已由前置票落地的输入面锁测，勿重做）
- `wedb/wnode/src/server.rs:181-182/:913-921/:898-908`（门禁与 tls 臂，语义勿动）

对应 c# 文件与函数：
- `garnet/libs/host/GarnetServer.cs`（构造期校验先于起服时序）
- `garnet/libs/host/Configuration/Options.cs`（TLS 校验面）

## 候选正解（须主控裁定，三案互斥，勿并案）

1. 拆测试支撑 crate 的特性拉入：`wnode_tls_test` 不再声明 `wnode features=["tls"]`，
   改由消费方（`--features tls` 形态的测试）在其自身 Cargo.toml 里叠加 tls 特性，
   使 wnode 默认特性形态可被拒启门禁测试链接。
2. 端到端拒启面独立成包：新建仅测 not(tls) 门禁的测试 crate（或其 own package 的
   tests 册），该包 dev-dep 只依赖非 tls 形态的 wnode，彻底脱离 wedb/wnode 的 dev-dep 统一。
3. 特性互斥显式化：在 wedb/wnode 侧为 tls 测试支撑建独立 feature（如 `tls-test-support`），
   默认关；`--all-features` 门禁形态允许该册出局，但默认特性形态必须真正执行拒启测。

## 验收口径
- 默认特性形态：`cargo nextest run -p wedb --test error_sink_tests` 全绿且**贴即刻拒启**
  （现 180.3s 超时长红必须消失；若仍慢即门禁未触达，说明未修到根）。
- `--all-features` 门禁形态：`tls_flags_node_projection` 两测仍可见可执行，
  拒启端到端面在哪一形态可见须在票面与册头注释写清，禁「两面皆不可见」。
- `./sh/clippy.sh` 零告警、`./test.sh --no-fail-fast` 零红（归主控跑）。
- `bun js/check.js` 无新增缺失/重复簇。

## 边界与纪律
- 涉 Cargo.toml 特性接线：**席禁手改 Cargo.toml**（依赖一律 `cargo add` 形态），
  故本票由主控亲办或主控显式授权后派席；若派席，简报必须逐条列允许改动的 manifest 行。
- 禁触 `wedb/wedb/src/server/replication/**`、`cluster_provider/**`、`wedb/wtxn/**`、
  `wedb/wkv/src/read_cache/**`、`wedb/wreviv/**`、`wedb/wnode/src/aof/**`
  （在途席域：aof 哨兵票、wext-json 票、wreviv 级联窗票正跑；wreviv oversize 票已归档）。
- 禁 `#[allow]`/`#[expect]`；禁把 `error_sink_tests` 改 `#[ignore]` 或调超时掩盖长红；
  禁在 `guard_no_tls` 加旁路判据。
- 前验/后验命令面（沙箱私有 target，主控显式授权）：
  `cargo tree -p wedb -i wnode --format "{p} {f}"` 与 `-p wnode` 同取，断根前后各一次（tls 必消失）；
  默认特性形态 `cargo nextest run -p wedb --test error_sink_tests` 必须贴即刻全绿（180s 长红消失即根断）；
  `cargo check -p wedb -p wnode -p wnode_tls_test --all-targets` 与同三条加 `--all-features` 双形态绿；
  tls 面搬迁后 `cargo nextest run -p wnode_tls_test --features tls`（或 `--all-features` 形态）
  须实跑迁居册非 0 tests。
  仍禁 `./test.sh`、`./sh/clippy.sh`、`bun js/check.js`（归主控在主树跑）。
- 前置知识：本仓 doc/zh/deviations.md 台账文件在 2026-09-28 重置后已不在任何 ref，
  登记一律落票体（随 task/done 常驻可 grep）与代码注释，勿再引用不存在的 § 号。

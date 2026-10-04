# 非 tls 形态 TLS 旗标未落 NodeArgs：boot 拒启门禁静默放行（明文起服），锁测在门禁形态恒不执行
> 收口（2026-09-28 12:3x）：票面归因经席实测**证伪**——四枚 --tls-* 旗标在解析/投影面本已
> 正确落值（新册 tls_flags_node_projection.rs 两用例在默认特性与 --all-features 双形态
> 实测各 2 passed，字段值逐一对位、has_tls() 逐旗为真），非输入面失配，票 §1 无需执行。
> 真根因为构建形态失配：wedb/Cargo.toml:104 与 wedb/wnode/Cargo.toml:85 无条件把
> wnode_tls_test 列为 dev-dep，而该 crate Cargo.toml:17 声明 wnode features=["tls"]，
> feature 统一使 #[cfg(not(feature="tls"))] guard_no_tls（wnode/src/server.rs:181-182、
> 定义 :913-921）在测试二进制里整体编译出局；生产 cargo build 不含 dev-dep，门禁在位，
> 故原 P2「生产静默明文起服」不成立。席 5fca3ec（3 文件 +101/-0，纯新增测试与注记）
> 合入 dev abdb382（merge-tree rc=0）。
> 遗留（另票，主控裁定）：e2e 拒启面在默认特性形态仍红——error_sink_tests.rs 实测
> 3 run/2 passed/1 timed out（180.3s），且 run_cluster_server_tls_gate 拿到的错误文本为
> 「tls_cert 与 tls_key 必须同时提供」（仅存在于 #[cfg(feature="tls")] 臂 server.rs:898-908），
> 正解需把 wnode_tls_test 特性化/移出无条件 dev-dep（涉 Cargo.toml 特性接线，席禁改），
> 已立案 task/todo/tls-gate-test-form-feature-unification-dev-dep.md。
> 快照注记（2026-09-28 11:07）：本仓历史已被 squash 为单 `init` 提交，票面所引历史哈希
> （`c92de925`/`99d14ab0`/`eeae7e48`/`797b7b0b` 等）均不可 resolve；一切定位以**现树内容**为准
> （锚点行号请以 worktree 尖端复核，票内行号系 squash 前实测）。

定级：P2（静默接受不安全配置＝用户以为 TLS 已开实为明文；C# 侧为显式拒启。锁测在
`--all-features` 门禁形态整体编译出局，红不可见）

## 甄别结论：通过（2026-09-28 主控实测，两形态对照 + 链路逐点核）

1. 现象实测（三种形态，均已现跑）
   - 门禁形态：`cargo nextest run --all-features -p wedb --test error_sink_tests`
     → **`0 tests run`**（`wedb/wedb/tests/error_sink_tests.rs:9` 整册
     `#![cfg(not(feature = "tls"))]`，`--all-features` 使 wedb/tls 为真）；
     `cargo test -q -p wedb --features tls --test error_sink_tests …` 同 0 tests。
     → 该锁测在 `./test.sh`（= `cargo nextest run --all-features`，见 `wedb/test.sh`）下
     **恒不执行**，故本红不是门禁红，也永远不会被门禁抓到。
   - 默认特性形态（无 tls）在 dev `eeae7e48`：
     `cargo test -q -p wedb --test error_sink_tests` →
     `boot_tls_gate_error_routed_through_node_variant` **FAILED**，炸点
     `tests/error_sink_tests.rs:26:46` `expect_err("必须拒绝启动")` 拿到 **`Ok(())`**，
     全测耗时 **593.71s**（四旗标各 ~148s ⇒ 服务器真的按明文起服后又自行收场）。
   - 同测同形态在 `797b7b0b`：**0.01s PASS** ⇒ 红引入段 **(797b7b0b, eeae7e48]**
     （其间为我方第一/二批 P1 合并群 + 他席 `d03feac9`/`eeae7e48` r5-8）。
     `04169915` 亦已红（先前实测），与本波 r6 合并无涉。

2. 门禁链路点在位（逐点 grep 确证，排除「门禁没编进去」）
   - `wedb/wnode/src/server.rs:181-182` `run_async` 首部
     `#[cfg(not(feature = "tls"))] guard_no_tls(node_args)?;`，定义 :913-921；
   - 判据 `NodeArgs::has_tls()`：`wedb/wconf/src/node_options.rs:1226-1233`
     读 `tls_cert / tls_key / tls_issuer_cert / tls_client_target_host`
     （字段定义 :475/:505/:524，**无 cfg 门**）；
   - `run_cluster_server` 确经该路径：`wedb/wedb/src/server/boot.rs:117 → :133
     ServerBootstrap::new(args) → :144 bootstrap.run_async(..)`；
   - `wnode` 缺省特性 = `["roaring","json"]`（tls 关），`wedb` 缺省 = `[]`
     ⇒ `cargo test -p wedb`（不带 `--features tls`）时 `guard_no_tls` **函数体是被编译的**。

3. 归因收敛（待席 live 定死，唯一可容缺口）
   门禁体在、判据字段在、调用路径在 ⇒ 只可能是 **`ClusterArgs::from_args_iter`
   的旗标解析/向 `NodeArgs` 的投影面未把 `--tls-cert` 落值**（`has_tls()` 恒 false ⇒
   门禁视为「无 TLS 配置」直接放行）。席第一步即单点判定，勿绕：
   `assert!(ClusterArgs::from_args_iter(["wedb","--tls-cert","v"]).unwrap().node_args().has_tls())`
   ——一条纯同步断言即可把「解析/投影」与「门禁执行」两面切开。

4. C# 契约
   `garnet/libs/host/Configuration/Options.cs` 的 TLS 校验面在证书类旗标已给而 TLS 未启用
   时**拒启**（`GarnetServer` 构造期 `ValidateTlsOptions` 一类），不存在「带着 TLS 旗标
   静默明文起服」的形态；rust 侧该显式门禁已按契约写就（见上），本票修的是它的**输入面失配**。

## 涉及代码

rust 文件与函数：
- `wedb/wconf/src/node_options.rs`（`--tls-*` 旗标定义 :475/:505/:524、`has_tls()` :1226-1233、
  CLI 绑定臂与 NodeArgs 投影面）
- `wedb/wnode/src/server.rs:guard_no_tls`（:913-921）与 `run_async` 调用点（:181-182）
- `wedb/wedb/tests/error_sink_tests.rs:boot_tls_gate_error_routed_through_node_variant`
  （:14-31，现册 `#![cfg(not(feature = "tls"))]`）
- 端点：`wedb/wedb/src/server/boot.rs:run_cluster_server`（:117/:133/:144）

对应 c# 文件与函数：
- `garnet/libs/host/Configuration/Options.cs`（TLS 旗标校验/`ValidateTlsOptions` 面）
- `garnet/libs/host/GarnetServer.cs`（构造期校验先于起服时序）

## 精炼执行方案

1. live 定位并修复输入面失配：使四枚 `--tls-*` 旗标在**任何特性组合**下都落进 `NodeArgs`
   对应字段（不得因 `tls` 关而丢旗标或静默忽略未知参数）；修复落在解析/投影单点，
   禁在 `guard_no_tls` 里加旁路判据（门禁执行面本已正确）。
2. 补**门禁形态可见**的锁测（关键：不能只留 cfg 掉的册）：把「旗标落值 + 拒启」拆两面——
   a. 纯同步解析锁测（不受 `tls` 特性门，`--all-features` 门禁可执行）：四旗标逐一
      `ClusterArgs::from_args_iter(..).node_args().has_tls()` 为真，且
      `tls_cert`/`tls_key` 等字段值逐一对位；
   b. 端到端拒启面保留在既有 `#![cfg(not(feature = "tls"))]` 册内（该形态下 `guard_no_tls`
      才存在），并在册头注释写明「本册在 `--all-features` 门禁形态编译出局，拒启面由
      (a) 的解析锁测在门禁内承接」。
3. 收敛耗时面：修复后 (b) 面必须**贴即刻拒启**（现为 ~148s/旗标的实起服），
   若仍慢即为门禁未触达，说明第 1 步未修到根。
4. 若查明根因在他席在途票域（如该投影面正被其锁定改动覆盖），按
   [[project-merge-gate-protocol]] 只登记不修，交回主控开票。

## 边界与纪律
- 只在 worktree `/tmp/fork/boot-tls-gate-non-tls-form` 内改动，私有 target
  `/tmp/_rs/boot-tls-gate-non-tls-form`。
- 禁触：`wedb/wconn/**`、`wedb/wedb/src/server/replication/**`（r6 波刚合入，勿碰）、
  `wedb/wkv/**`、`task/refactor-backlog.md`。
- 禁 `#[allow]`／`#[expect]`、禁占位实现、禁改 Cargo.toml、禁新增 cfg 分支垫片
  （尤其禁「把锁测改成 `#[ignore]` 或调高/压低超时来掩盖 148s 起服」）。
- 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控）；**禁在主仓做 bisect**
  （主树有他席在途文件），如需二分只用 detach worktree + 私有 target。
- 自检：`cargo check -q --workspace --all-targets` 零告警 +
  `cargo nextest run -p wedb --test error_sink_tests`（默认特性）绿 +
  同一册 `--features tls` 形态 0 tests 且不报错 + 新解析锁测
  `--all-features` 形态**可执行且绿**（这是本票的门控点，务必贴出实跑计数）；
  `bun js/check.js` 自查锚单点化。
- 净行：以减行为主（删失配臂）；新增锁测另计并在提交信息写口径。
- 完工：单提交，消息以 `fix(wconf): ` 或 `fix(wnode): ` 起头（按实际落点），
  只 add 指派文件；不 merge 不推 dev。

---

## 席位登记（boot-tls-gate-non-tls-form 席，2026-09-28，worktree 实测）

1. **§3 归因证伪（第一步纯同步断言实测）**：票面唯一可容缺口「`ClusterArgs::from_args_iter`
   的解析/投影面未把 `--tls-*` 落值 ⇒ `has_tls()` 恒 false」**不成立**。
   `assert!(ClusterArgs::from_args_iter(["wedb","--tls-cert","v"]).unwrap().node_args().has_tls())`
   在 dev 尖端 `852ad2f` 现树**修复前即为真**（四旗标逐一落值对位，见新增册
   `wedb/wedb/tests/tls_flags_node_projection.rs`，默认与 `--all-features` 两形态各 2 passed）。
   静态链路同证：`wconf/src/node_options.rs` 旗标定义 :475/:479/:505/:524 无 cfg 门；
   `NodeArgs::override_explicit` 清单 :1638-1644 四旗标 + `tls_client_cert_required` /
   `tls_server_cert_required` / `tls_cert_refresh_freq` 全数在列；wconf 全 crate 零
   `cfg(feature="tls")` 分支。
2. **真实根因（实测铁证，§4 只登记不修，交主控开票）**：e2e 红不在解析/投影面，在
   **构建形态失配**——`wedb/Cargo.toml:104` 与 `wnode/Cargo.toml:85` 无条件把
   `wnode_tls_test` 列 dev-dep，而 `wnode_tls_test/Cargo.toml:17` 依赖
   `wnode features=["tls"]`，feature 统一使 `cargo nextest run -p wedb`（不带
   `--features tls`）链接的 wnode 为 **tls 构建**：`run_async:181` 的
   `#[cfg(not(feature="tls"))] guard_no_tls` 连同 `guard_no_tls`（server.rs:913-921）、
   本票 e2e 册所锁拒启臂、以及 wnode 自有锁册
   `tls_args_rejected_without_tls_feature.rs`（册头 `#![cfg(not(feature="tls"))]`）
   在全部测试形态编译出局；生产 `cargo build` 不含 dev-deps，门禁在位有效。
   实测证据：
   - `run_cluster_server_tls_gate`（默认特性）FAIL 1.6s，炸点 :22:9，实际错误文本
     `tls_cert 与 tls_key 必须同时提供`——该串仅存在于 `#[cfg(feature="tls")]` 的
     `tls_config_from_node`（server.rs:898-908），铁证测试链接 wnode/tls=on；
   - `error_sink_tests`（默认特性）TIMEOUT 180s（slow-timeout 90s×2 截杀），panic 点
     :26:46 `必须拒绝启动: ()`——`--tls-issuer-cert`/`--tls-client-target-host` 单旗
     走 tls 臂 (None,None)→Ok(None) 明文起服（与主控实测 ~148s/旗标自洽）；
   - `cargo tree -p wedb -e features -i wnode`：`wnode feature "tls"` 的拉入者唯一为
     `wnode_tls_test`。
3. **修复面归属**：正解需改 Cargo.toml（`wnode_tls_test` dev-dep 移册/特性化，或
   e2e 册改按 wnode 实际构建形态选形）——本票纪律禁改 Cargo.toml、禁新增 cfg 垫片、
   禁在 `guard_no_tls` 加旁路判据，且 wnode_tls_test/复制域系他席票活跃面，故按 §4
   只登记。本席交付 = §2a 门禁形态可见解析锁测 + §2b error_sink_tests 册头注记。
4. **§2 锁测落位自证**：`cargo nextest list --all-features -p wedb --test
   tls_flags_node_projection` → 2 用例在册（`tls_flags_land_in_node_args` /
   `no_tls_flags_leaves_has_tls_false`），实跑 2 passed（默认特性与 `--all-features`
   两形态皆尔）；`cargo nextest run -p wedb --features tls --test error_sink_tests`
   → 0 tests（原册门禁形态恒不执行不变）。

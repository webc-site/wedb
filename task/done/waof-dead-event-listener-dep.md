优先级：低
waof 清单死依赖：event-listener 声明于 [dependencies] 但 src 与 tests 全零引用
  来源：next/glm.design.md 第 12 轮条 2。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev。
  结论：一条依赖声明在生产与测试两侧都无引用，属清单面死代码，且与本仓既有的 machete 门禁口径冲突。判定成立且待做（改动面一行，风险为零）。
  现状：死声明 wedb/waof/Cargo.toml:22 event-listener = { workspace = true }；waof 全目录（src 与 tests）grep event_listener 只命中该 Cargo.toml 行自身；waof 无 [package.metadata.cargo-machete] ignored 豁免条目（对照 wedb/wnode/Cargo.toml:11-12 为 rustls-pki-types 显式登记的豁免形态）。真实消费方在 wbase：wedb/wbase/src/throttle.rs:13、wedb/wbase/src/pool/event_queue.rs:7、wedb/wbase/src/pool/work_set.rs:3 均 use event_listener::…，由 wbase 的 pool feature 以 dep:event-listener 承接（wedb/wbase/Cargo.toml:35 pool 段、:43 dep:event-listener、:54 optional 声明）。waof 经 wedb/waof/Cargo.toml:21 的 wbase features（含 pool）已传递可得该能力，直接声明是纯冗余，只让依赖图与审计工具误判。
  C# 参考：无对位（cargo 清单面，属一处定义与依赖图整洁）。
  修法：1) 删除 wedb/waof/Cargo.toml 该行，走同源工具路径 cargo remove event-listener（在 waof crate 目录内执行），依 SKILL:59「只能使用 cargo add 添加依赖，禁改 Cargo.toml」不手改清单文本；2) 复跑 machete 确认 waof 不再报 event-listener 且全仓无新增误报；3) 不做向下兼容处理、不保留注释说明，直接删（SKILL:12）。
  边界：不碰 wbase 的 feature 组合面；feature 声明口径与 dev-dependencies 双载归台账在册的 cargo 清单族。

甄别核实（实现代理，2026-09-19，分支 waof-dead-event-listener-dep，基点 dev 7b20cef）：
- 票面成立，非重复、非占位需求、非平台绑定：全仓 grep event[_-]listener 命中 61 处，其中 wedb/waof 侧仅 1 处，即 wedb/waof/Cargo.toml:22 声明行本身；waof/src（aof、wal、error.rs、lib.rs）与 waof/tests 零引用，waof 全目录连 README/AGENTS 文档面也无 event-listener 字样（grep -i 无命中）。
- 消费面核对：真实消费者确为 wbase（throttle.rs:13、pool/event_queue.rs:7、pool/work_set.rs:3），由 wbase pool feature 承接（wbase/Cargo.toml:35/:43/:54）；waof 的 wbase 依赖行已含 "pool"，删除直接声明后能力仍传递可得，不存在功能回归面。
- 豁免形态核对：waof/Cargo.toml 无 [package.metadata.cargo-machete] 段，wnode/Cargo.toml:11-12 有 ignored = ["rustls-pki-types"]，说明本仓确需保留死声明时会登记豁免，本行不是被有意豁免的项。
- 门禁口径核对：./sh/udeps.sh:9 即 cargo machete --fix，本仓清单面死项会被该门禁反复报出，留着只让门禁不可信。

改动与取舍：
1. 只删一行：cargo remove event-listener 在 /tmp/fork/waof-dead-event-listener-dep/wedb/waof 内执行，git diff 全仓仅 wedb/waof/Cargo.toml -1 行，工作区根 wedb/Cargo.toml 的 [workspace.dependencies] event-listener = "5.4.2"（:53）保留（whlog/wcpr/wcol/wnode/wbftree/wedb/wbase 等仍在直接消费），未被工具顺带改写。
2. 无 cfg/feature 痕迹需清理：waof/Cargo.toml 的 [features] 只有 default = []，waof/src 内 grep cfg(feature 零命中，本依赖不存在专属开关面。
3. 不登记 ignore：js/check.js 全文不读 Cargo.toml（grep "Cargo.toml" js/*.js 零命中），本改动不影响 C#↔rust 映射，js/check/ignore 语料无需增删。
4. 不加兼容注释、不留被注释掉的声明、不改 wbase feature 组合（票面边界）。

验收读数（worktree 内，CARGO_TARGET_DIR=/tmp/fork/waof-dead-event-listener-dep/target，未跑 test.sh / clippy.sh）：
- cargo check --workspace --all-targets：改动后首跑 exit 0（Finished in 42.37s，warning/error 计数 0）；并入 dev 最新内容后复跑 exit 0（13.03s，0 warning）。
- bun js/check.js：改前改后均 exit 0，输出逐字节相同（diff /tmp/checkjs-before.log /tmp/checkjs-after.log 无差异），js/check/ignore 递归 71 个 yml 的 sha256 前后全等（未被回写）。
- cargo machete（只读、不带 --fix，避免 --fix 在并发窗口改写他人清单）：改前 waof 报 event-listener + parking_lot；改后 waof 只报 parking_lot，其余 8 个 crate 报项完全一致，无新增误报，本票目标项已消。
- 合入：dev 由 36df578 快进到 8047cf3（git merge --ff-only 在主仓一次成功，工作树同步无残脏），git merge-base --is-ancestor 8047cf3 dev 为真，dev 侧 wedb/waof/Cargo.toml 已无 event-listener。

遗留与上报（不属本票改动面）：
- 票面「全仓唯一非误报死项」的主张不成立：machete 同轮还报 waof parking_lot（wedb/waof/Cargo.toml:27，waof 全目录 grep parking_lot 亦只命中声明行，疑为真死）、wcustom thiserror/parking_lot/whasher、wnode serde、wext_json itoa/whasher、wedb wlua/wmetric、wcompact whasher、wvector smallvec、wcustom/wedb_standalone 等，须逐条判真伪后另开同族票（按票面边界，本单为 waof 死项登记处，parking_lot 即记于此）。
- 并发提醒：主仓 HEAD 在我合入后又被 4d034cf、a828a78 推进，主仓工作树尚有他人 in-flight（next/qw.design.md、next/qw.my.md、wedb/wlua/src/hash_key.rs 等），本票一律未触碰。

crate 清单面死依赖第一批：waof parking_lot 与 wcustom/wnode/wext_json/wedb/wcompact/wvector 逐项甄别

落地：dev ff202ccd（分支 machete-dead-deps：834699d 改动 + c25b9b6 并 dev 6fc87518），
射程只到 8 个成员的 Cargo.toml，未动任何 .rs 运行期代码，未跑 ./sh/udeps.sh（含 --fix）、
./test.sh、./clippy.sh。

取证起点（worktree 内只读探针，cargo-machete 0.9.2）

原票写死 7 组 11 项，实跑报项为 8 crate / 13 项：多出的两项是 wedb_standalone 的
whasher、wlua（即原票「其余 8 个 crate 报项不变」的计数来源），同属清单面，一并甄别，
未再造第二份普查。waof 的 event-listener 已由前票删净（dev 8047cf3），本单未重复触碰。
基线全集：wnode serde、waof parking_lot、wedb_standalone whasher/wlua、wext_json
itoa/whasher、wcustom parking_lot/thiserror/whasher、wedb wlua/wmetric、wcompact
whasher、wvector smallvec。

逐条结论（判死 12，判活 1）

1. waof parking_lot —— 判死。30 个 rs（src+tests）零 `parking_lot` 字样；waof 的锁面
   实为 async_lock（waof/src/wal/log.rs:10 `use async_lock::Mutex as AsyncLockMutex`、
   :52 `pub commit_lock: async_lock::Mutex<()>`、:191/:544/:637 `.lock().await`），
   提交路径全异步，parking_lot 无处着力。
2. wcustom parking_lot —— 判死。6 个 rs 零命中，crate 内无任何 Mutex/RwLock/lock()，
   注册面走 const fn 指针表 + 静态枚举派发（src/custom_object_fns.rs 头部注释即此口径）。
3. wcustom thiserror —— 判死。crate 无 error.rs、无 `#[derive(Error)]`、无 `#[error(...)]`，
   全库 thiserror 零字样；错误面由 wbase/wtxn 承接。
4. wcustom whasher —— 判死。零命中；wcustom 不做哈希（派发按 txn_proc 槽位 id 单次比较）。
5. wnode serde —— 判活，转豁免登记。唯一消费点 wnode/src/resp/resp_command_docs.rs:285
   与 :357 的 `#[derive(Deserialize, Clone, Default)]`，其 Deserialize 由同文件 :10 的
   `use sonic_rs::Deserialize;` 再导出引入，另有 19 处 `#[serde(rename = ...)]` 辅助属性。
   实测 `cargo remove serde` 后 cargo check 报两处 error[E0463] can not find crate for
   `serde`（note 明确指向 Deserialize derive 宏），即 derive 展开体硬引用 `serde` 这个
   extern crate 名，machete 的正则形态看不见 → 属误报形态一/三混合。已并入该 crate 既有
   的 [package.metadata.cargo-machete]（先例同段的 rustls-pki-types）：
   `ignored = ["rustls-pki-types", "serde"]`，并在段前写五行理由注释。
   对照真消费面：wresp/wconf 有 `use serde::{Serialize, Deserialize}` 与
   `#[derive(serde::Deserialize)]` 直引，machete 不报，说明本项确是路径形态差异。
6. wext_json itoa —— 判死。15 个 rs 零 itoa；整数/浮点出串由 sonic-rs 序列化与 zmij
   承担（manifest 保留 zmij.workspace = true），无手写数字缓冲。
7. wext_json whasher —— 判死。零命中；json path 求值不做哈希，亦无落盘派生值。
8. wedb wlua —— 判死。124 个 rs 零 `wlua`，wedb 内也无 EVAL/RUNSHA/脚本缓存面；lua 的
   活消费方在 wnode（wnode/src/service.rs:40 `use wlua::LuaTimeoutManager`、
   wnode/src/resp/resp_server_session.rs:39 `use wlua::{...}`），删声明不影响 wlua 存活。
9. wedb wmetric —— 判死。零 `wmetric::`；wedb 的指标条目经 wresp 的指标面产出
   （wedb/src/server/gossip/gossip_stats.rs:3 `use wresp::metrics::MetricsItem`），
   wmetric 的活消费方同样在 wnode（session_parse_state_extensions.rs 等 10 余处）。
10. wcompact whasher —— 判死。12 个 rs 零命中；紧凑化路径的哈希/校验在 whlog 与 windex
    （二者各自保留 whasher 声明并被 machete 判为已用）。
11. wvector smallvec —— 判死。19 个 rs 零 `smallvec`/`SmallVec`；定长向量面由
    bytemuck + diskann-* 承接，未做栈上小向量优化。
12. wedb_standalone whasher —— 判死。整 crate 只有 src/main.rs 一个 rs 文件，零命中。
13. wedb_standalone wlua —— 判死。同上（main.rs 只做 wnode 装配 + clap 参数）。

误报形态逐条排除（不是照抄 machete 输出）

形态一（feature 承接）：8 个成员的 [features] 里没有任何 `dep:wlua`、`dep:wmetric`、
`dep:whasher`、`dep:smallvec`、`dep:itoa`、`dep:parking_lot`、`dep:thiserror`、
`dep:serde` 项，也无 `x = ["y/dep-x"]` 形态（wnode 四项特性只挂 compio-tls/
rustls-pki-types/rustls-pemfile/wext_roaring/wext_json），删后无悬空特性项需收口。
形态二（build.rs / proc-macro / cfg 门内）：8 个成员均无 build.rs、无 benches/、无
docs/；唯一的 cfg 门内消费是 wnode rustls-pki-types（前票已豁免，本单补注释理由：
wnode/src/tls/config.rs:12-19 在 `#[cfg(feature = "tls")]` 门内经
compio_tls::rustls::pki_types 取 CertificateDer）。wnode serde 是本形态的 proc-macro
变体，见上第 5 条。
形态三（workspace 声明、消费在他处）：12 项删除前均先在所属 crate 内以 rg 明写
pattern 复核（`\bparking_lot\b` 等，未用裸通配符，规避 zsh glob 假阴性），确认该
crate 自身零引用；同时确认 [workspace.dependencies] 的对应声明仍被别的成员引用
（parking_lot 8 处以上、thiserror/itoa/smallvec/whasher 各多处、serde 由 wresp/wconf
仍声明、wlua/wmetric 由 wnode 声明），故本单未越界删根清单，也不留悬空 workspace 项。
形态四（dev/build 依赖测试面在用）：射程全在 [dependencies]，[dev-dependencies] 一项
未动（如 wedb_standalone 的 itoa/parking_lot dev 声明保留）。

处置合规

12 判死项一律在对应 crate 目录内 `cargo remove <dep>`（.agents/skills/rust_review
SKILL.md 第 103 条与 transpile SKILL.md 第 59 行：依赖只用 cargo 子命令增删，禁直接
编辑 Cargo.toml），逐 crate diff 为纯逐行删除、无占位、无兼容注释；合计
8 files changed / 15 删 / 7 增（7 增全是 wnode 的豁免注释与新 ignored 行）。

门禁读数（全部在 worktree 内，独立 CARGO_TARGET_DIR=/tmp/fork/machete-dead-deps/target）

cargo check --workspace --all-targets：首轮（含误删 wnode serde）exit 101，仅两处
E0463，据此判活；豁免后 exit 0；并入最新 dev（6fc87518）后复跑 exit 0，
零 warning（该轮 20+ 个本地 crate 全量重查，因 wbase::convert 收口触发下游重建）。
cargo machete 复跑（只读）：`did not find any unused dependencies`，exit 0，
报项 13 → 0，无「同一项既没删也没豁免」。
rg 零命中复核：12 项在所属 crate 内命中面只剩其 Cargo.toml 声明行（删前留档）。
bun js/check.js（只在 worktree 内跑）：exit 0，js/check/ignore 语料 71 个文件前后
shasum 全等（零改写），新增文件只在 check/miss 与 js/check/miss（gitignore 内），
跑后 git status --porcelain 为空。

移交项

1. 无运行期死码外溢：12 项删除都是纯清单面装饰，对应 crate 本就零引用，没有出现
   「某 mod/函数只为该依赖存在」的情形，故不需要另立死码票。
2. wedb 层的 lua 与 metric 接线缺口（原票射程外）：wedb 作为集群接线层目前完全不引用
   wlua/wmetric，EVAL 族与 LATENCY/SLOWLOG 指标面全部由 wnode 承担。若后续要让 wedb
   侧接管集群级脚本缓存或延迟统计，需按需求重新 cargo add 并打通链路，属功能票。
3. [workspace.dependencies] 全仓悬空普查不在本单射程，本单只保证被删 12 项未把根清单
   声明变孤儿；若要一次性核清 71 个 workspace 键的活引用面，另立票。
4. cargo-machete 的 `--with-metadata` 读数更准但会改写 Cargo.lock（本仓 wedb/Cargo.lock
   未被 git 跟踪），本单只用默认只读形态；若要把「machete 零报项」固化进 CI 门禁，
   另立票决定口径。

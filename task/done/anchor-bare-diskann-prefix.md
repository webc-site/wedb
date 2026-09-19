# anchor-bare-diskann-prefix（wvector 裸 diskann-garnet 前缀锚补全路径）

来源: next/cs-corpus.inv1.md §3.4 票 1（slug 同名）+ §3.3 批 8（wedb 与 wcustom 的 wvector 前缀族 71 处）。
棒: fixloop 开发棒（纯注释批票，无功能变更）。认领提交 = 8e5db03（盘点档改名入 task/ing，未独占、未剪行）。

## 射程

`diskann-garnet/<X>.cs:<Sym>` → `libs/server/Resp/Vector/<X>.cs:<Sym>`，只改注释锚点前缀，禁触碰函数体。

现刻（HEAD dd9d15f）实测该形态 71 枚，分布在 wvector 的 6 个文件：票面点名的 service.rs（10）、
filter/runner.rs（12）、filter/expression.rs（11）三档共 33 枚，其余 38 枚在
filter/attribute_extractor.rs（21）、filter/compiler.rs（9）、element_data.rs（8）。
规格 §3.3 的「三档拆 30/25/16」与 §3.2 的「wvector 涉文件 1」与现树不符（实为 6 文件）。
本棒按票面判据「71 处同形态前缀缺段」全量收口 6 文件 71 枚——半套只清 33 枚会把同族留在原地，
且 6 文件在途无他棒（task/ing、next 全量 grep 无第二张票涉 wvector；tmp/fork 八条在跑分支
`git diff --name-only dev...<br>` 对 wvector 零命中），越界风险已核。批 8 未列的 wvector 文件已尽，
无需再派续棒。

## 判据

1. 形态判据：`js/check/rustScan.js` 的 `CS_REF_REGEX` + `csPathNormalize` 只剥 `garnet/` 前缀、
   不做 basename 回查，故 `diskann-garnet/X.cs:Sym` 归一后 key 为 `diskann-garnet/X.cs`，
   与语料侧 `libs/server/Resp/Vector/X.cs` 永不相交 → `doc_file_fn_map` 登记为空（映射不入册）；
   `symbolCheck.js` 侧因路径含 `/` 但文件不存在，落入 B 层「路径失真」129 处的主力。
2. 改后须被门禁采信：`libs/` 起始为 A 层硬断言族，故每枚改锚前逐枚验
   （a）目标 C# 文件在 `garnet/libs/server/Resp/Vector/` 实存，
   （b）`<Sym>` 以词边界命中该文件文本（与 symbolCheck 同判据）。核验映射不成立者保留散文或登
   `js/check/symbolignore.yml`，禁虚构路径锚。实测 71/71 通过、零 MISS、零豁免登记。
3. 读数判据：`bun js/check.js` 树内前后对跑——「# 实现缺失」段与 symbolCheck B 层读数净减、
   无新增缺失、A 层违规零新增、锚点全库多重集只增不减；ignore 语料回写以
   `git checkout -- js/check/ignore/` 还原，不入本票提交。
4. 不动清单：`diskann-garnet` 作上游仓库名/crate 名的散文与不带 `:Sym` 的路径叙述均不登记，不改写。

## 判词（完工）

载荷：d1d8233（6 文件 71 枚，71 增 71 删一一对位，非注释行改动 0——`git diff -U0` 过滤实证）；
两次回合 dev 后 FF 入 dev = 3e9d8bc（merge 树 655cf1e、3e9d8bc）。

- 改锚数：71 枚（service.rs 10、filter/runner.rs 12、filter/expression.rs 11、
  filter/attribute_extractor.rs 21、filter/compiler.rs 9、element_data.rs 8）。
  目标 C# 文件分布：DiskANNService.cs 10、ExprRunner.cs 12、VectorFilterExpression.cs 11、
  AttributeExtractor.cs 21、ExprCompiler.cs 9、VectorManager.ElementData.cs 8。
- check.js 前后对跑（同一 dev 基线对照树 /tmp/gate-adp，回合后复跑读数一致）：
  - symbolCheck：违规 129 → 58，净减 71；A 层 libs 族前后皆 0（加前缀后未有一枚转 A 层红）；
    wvector 命中明细 71 行 → 0 行；「锚点命中 4642（去重 4642）」「裸文件名跳过 463」「豁免 2」前后不变。
  - 「# 实现缺失」段：前后皆无该段（0 项），无新增缺失。
  - stdout「# 重复定义」段：前后逐字节相同（12 组，本批 71 枚与全库既有 `libs/server/Resp/Vector/*` 锚
    键集零重叠 → 71 枚全为纯新登记键，不制造重复组）。
  - 锚点全库多重集：71 枚由 `diskann-garnet/*` 无效键整体迁移到 `libs/...` 有效键，
    新键全部此前全库零枚（净新登记 71），无任一枚锚消失（总数 4642 不变，语义只增不减）。
  - ignore 语料回写：两树跑完 `git status -- js/check/ignore/` 皆空，零回写、零改动本票提交。
  - exit 码：前后 0。
- cargo check --workspace --all-targets（CARGO_TARGET_DIR=/tmp/ct-adp 私有）：exit 0，warning 0
  （首跑见 wacl 的 unused dependency `arc-swap` 一枚，核为 HEAD 存量、dev 已由 b6641a8 清理，非本票）；
  cargo fmt 以 rustfmt --config-path wedb/rustfmt.toml 只施本票 6 文件，改后零额外 diff。
- 禁跑项遵守：未跑主仓 ./test.sh 与 ./sh/clippy.sh。

### 未登记残锚清单与理由（本票射程内保留原样）

| 位置 | 形态 | 处置与理由 |
|---|---|---|
| service.rs:1、:8、:47、:50、:72、:231 | `diskann-garnet` 作上游 crate/仓库名（lib.rs、dyn_index.rs、ADAPTIVE_L_SAMPLES、IndexState、SearchResults、DynIndex） | 非 `.cs:Sym`，CS_REF_REGEX 不匹配；且所指为 diskann 上游 Rust 侧件，非 garnet C# 语料，改路径即虚构锚 |
| element_data.rs:1、filter/attribute_extractor.rs:1、filter/compiler.rs:1、filter/runner.rs:1、filter/expression.rs:1 | `//! 对标 diskann-garnet/<X>.cs` 文件头叙述 | 无 `:Sym` 不登记；语料同名文件实存，作人读出处叙述正确，留 |
| store.rs:1、:10、:26、quantization.rs:1、:34、provider/mod.rs:1、fsm.rs:1、:101 | 同族 crate 名/`VectorManager.Callbacks.cs`、`garnet.rs`、`fsm.rs` 散文（store.rs:26 无冒号） | 零 `.cs:Sym` 命中、不登记；store.rs 属他票射程（§1.4 票 vector_store_callbacks 族），本棒不越面 |
| 全 6 文件 | 「路径.cs:行号」数字形态锚 | CS_REF_REGEX 冒号后要求 `[A-Za-z_]`，数字形态不登记；wvector 实测该类 0 枚，无需处理 |

批 8 未点名的 wcustom/其他 crate 截断锚（§3.2 记 wcustom 截断src 1、wedb 截断src 11）不属本票形态族，未动。

## 进度

- [x] 认领本票（首提交 8e5db03）
- [x] 现刻取证：71 枚逐枚定位、符号核验 71/71、重复锚碰撞 0 组
- [x] 树内改锚 + 门禁（cargo check 零告警 / check.js 前后对跑 B 层 129→58 / fmt 零额外 diff）
- [x] 合并归档（dev FF 3e9d8bc）+ 本票尾判词

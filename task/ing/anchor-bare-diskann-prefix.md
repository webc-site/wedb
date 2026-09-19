# anchor-bare-diskann-prefix（wvector 裸 diskann-garnet 前缀锚补全路径）

来源: next/cs-corpus.inv1.md §3.4 票 1（slug 同名）+ §3.3 批 8（wedb 与 wcustom 的 wvector 前缀族 71 处）。
棒: fixloop 开发棒（纯注释批票，无功能变更）。

## 射程

`diskann-garnet/<X>.cs:<Sym>` → `libs/server/Resp/Vector/<X>.cs:<Sym>`，只改注释锚点前缀，禁触碰函数体。

现刻（HEAD dd9d15f）实测该形态 71 枚，分布在 wvector 的 6 个文件：票面点名的 service.rs（10）、
filter/runner.rs（12）、filter/expression.rs（11）三档共 33 枚，其余 38 枚在
filter/attribute_extractor.rs（21）、filter/compiler.rs（9）、element_data.rs（8）。
规格 §3.3 的「三档拆 30/25/16」与 §3.2 的「wvector 涉文件 1」与现树不符（实为 6 文件）。
本棒按票面判据「71 处同形态前缀缺段」全量收口 6 文件 71 枚——半套只清 33 枚会把同族留在原地，
且 6 文件在途无他棒（task/ing、next 全量 grep 无第二张票涉 wvector；tmp/fork 八条在跑分支
`git diff --name-only dev...<br>` 对 wvector 零命中），越界风险已核。

## 判据

1. 形态判据：`js/check/rustScan.js` 的 `CS_REF_REGEX` + `csPathNormalize` 只剥 `garnet/` 前缀、
   不做 basename 回查，故 `diskann-garnet/X.cs:Sym` 归一后 key 为 `diskann-garnet/X.cs`，
   与语料侧 `libs/server/Resp/Vector/X.cs` 永不相交 → `doc_file_fn_map` 登记为空（映射不入册）；
   `symbolCheck.js` 侧因路径含 `/` 但文件不存在，落入 B 层「路径失真」129 处的主力。
2. 改后须被门禁采信：`libs/` 起始为 A 层硬断言族，故每枚改锚前逐枚验
   （a）目标 C# 文件在 `garnet/libs/server/Resp/Vector/` 实存，
   （b）`<Sym>` 以词边界命中该文件文本（与 symbolCheck 同判据）。核验映射不成立者保留散文或登
   `js/check/symbolignore.yml`，禁虚构路径锚。实测 71/71 通过、零 MISS。
3. 读数判据：`bun js/check.js` 树内前后对跑——「# 实现缺失」段与 symbolCheck B 层读数净减、
   无新增缺失、A 层违规零新增、锚点全库多重集只增不减；ignore 语料回写以
   `git checkout -- js/check/ignore/` 还原，不入本票提交。
4. 不动清单：`diskann-garnet` 作上游仓库名/crate 名的散文（如 quantization.rs:1、fsm.rs:1、
   store.rs:10、service.rs:1/8/47/50/72/231）与「路径.cs:行号」形态数字锚均不登记，不改写。

## 进度

- [x] 认领本票（首提交）
- [x] 现刻取证：71 枚逐枚定位、符号核验 71/71、重复锚碰撞 0 组
- [ ] 树内改锚 + 门禁（cargo check / check.js 前后对跑 / fmt）
- [ ] 合并归档 + 票尾判词

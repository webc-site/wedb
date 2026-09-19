README 双语主文档与工作区根 README 的 crate 拓扑整体过时：幽灵 wram、虚构 16 分片、ext_* 假名、
crate 计数与 wkv 再导出清单失效，且存在未渲染的根 README 与手工拷贝副本

来源：next/glm.design.md 第 8 轮条 1、条 2 合并立项（同一失因：crate 边界重组后文档未跟；修法是同
一次清扫 + 同一次 mdt 再生成，拆两单必互踩 readme/{zh,en}.md）。取证基线：主仓绝对根
/Users/z/git/db/wedb，分支 dev，行号按当下文件实况重取。

结论
两套 README 家族仍在描述一个已经不存在的模块拓扑。判定成立且待做。

现状一 顶层 readme 家族（readme/zh.md、readme/en.md 与 readme/zh/README.md、readme/en/README.md）
- /Users/z/git/db/wedb/readme/zh/README.md:47 mermaid 画 `wram["wram: 内存分配器与内存水位追踪"]`
  节点、:91 对标矩阵 libs/common 行列 wram 并挂 GitHub 链接、:92 modules 行列
  ext_json / ext_roaring / ext_noop 三个 crate 名、:106 称 wnode 含「16 分片无争用网络缓冲池
  （LimitedFixedBufferPool）」。/Users/z/git/db/wedb/readme/en/README.md 逐条对应
  （:47、:91、:92、:106 "16-way sharded lock-free buffer pool"，另 :80 也写 "16-way sharded
  buffer pool"，票面未列此第五处）。
- 事实核对：workspace members 共 36 个（/Users/z/git/db/wedb/wedb/Cargo.toml:3-39）中无 wram；
  wram 的内容现居 /Users/z/git/db/wedb/wedb/windex/src/ram/（direct_vm.rs、tracker.rs、mod.rs），
  /Users/z/git/db/wedb/wedb/windex/src/ram 是 rust 侧 DirectVirtualMemory /
  NativeMemoryTracker 的唯一归属，windex/Cargo.toml 不存在 wram 依赖，故 "windex --> wram"、
  "wram --> wbase" 两条依赖边为虚构。
- 模块 crate 实名 wext_json、wext_roaring（/Users/z/git/db/wedb/wedb/wnode/Cargo.toml:15-18 的
  default = ["roaring","json"] 与 roaring/json 两个 feature 分别 optional 依赖它们）；ext_noop
  在 rust 无对应物——/Users/z/git/db/wedb/garnet/modules/NoOpModule 是示例插件，
  /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:14 已裁定动态模块改编译期静态特性，剔除
  属预期，但 README 的 modules 行未同步。
- 「16 分片」纯虚构：C# /Users/z/git/db/wedb/garnet/libs/common/Memory/LimitedFixedBufferPool.cs:63
  构造签名 `LimitedFixedBufferPool(int minAllocationSize, int maxEntriesPerLevel = 16, int
  numLevels = 4, ...)`，即每层条目上限 16、层级数 4，全类无分片概念（同文件 :5-9 类注释自述
  「array of concurrent queues，queue[i] 存 2^i * sectorSize」的分级形态）；rust 对位
  /Users/z/git/db/wedb/wedb/wbase/src/pool/limited.rs 同为层级池、无分片，其 :28
  `DEFAULT_BUFFER_SIZE = 1 << 16` 是 64KB 块大小，疑被误读为「16 分片」。且该池归 wbase/pool，
  不在 wnode 内，:106 把它记在 wnode 账上双重失真。
- 副本形态：`cmp` 实测 /Users/z/git/db/wedb/readme/zh.md 与 /Users/z/git/db/wedb/readme/zh/
  README.md byte 级相同，/Users/z/git/db/wedb/readme/en.md 与 /Users/z/git/db/wedb/readme/en/
  README.md 亦然；而 mdt 模板 /Users/z/git/db/wedb/README.mdt 只 include readme/{zh,en}.md 平铺
  两份。/Users/z/git/db/wedb/sh/skills/doc.md 规定的工作流是「readme/zh/ 分章源 + readme/zh.md
  聚合导航」，现退化为两份手工拷贝，无脚本同步即漂移温床。
- 另有一处票面未提的同族缺陷：/Users/z/git/db/wedb/README.md 与 /Users/z/git/db/wedb/README.mdt
  byte 级相同（diff 空），即根 README.md 仍是含 `<+ ./readme/en.md >` 指令的未渲染模板，对外展示
  的是原始 include 行而非正文。

现状二 工作区根 /Users/z/git/db/wedb/wedb/README.md（48KB 双语聚合件，源为
/Users/z/git/db/wedb/wedb/readme/en.md 与 /Users/z/git/db/wedb/wedb/readme/zh.md，经
/Users/z/git/db/wedb/wedb/README.mdt include，再生成入口 /Users/z/git/db/wedb/sh/dist.sh:24
`bun x mdt .`）
- :9 "delivered as fifteen focused Rust crates"、:416「十五个职责单一的 crate」，实际 members 36。
- wram 残留共 15 处：:20、:42 目录锚点，:57、:464 正文，:220、:627 mermaid 节点，:243、:247、
  :650、:654 依赖边（含虚构的 windex→wram、wram→wbase），:285、:692 目录树列 wram/ 条目，
  :336-340、:743 wram 专章。
- :342 wbase feature 清单列 `float`（order-preserving f64 bits），而
  /Users/z/git/db/wedb/wedb/wbase/Cargo.toml 的 [features] 无 float、src 下无 float.rs（该模块已
  删）；同段还漏列现存的 future / group-commit / convert / num / store_type 等。
- :330 声称 wkv "Re-Exports from member crates" 27 项，逐项核对 /Users/z/git/db/wedb/wedb/wkv/
  src/lib.rs:19-45 的 pub use 清单，下列项均不在：LogCompactor（现
  /Users/z/git/db/wedb/wedb/wcompact/src/compactor/mod.rs:122）、CheckpointType（现
  /Users/z/git/db/wedb/wedb/wcpr/src/meta.rs:83）、ScanRecord / StorageBackend /
  StorageBackendType / TreeTuning（现 /Users/z/git/db/wedb/wedb/wbftree/src/types.rs:159 等），
  TtlProbe 全仓 grep 无任何定义。整段 API Reference 按已重组的 crate 边界整体失效。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/common/Memory/LimitedFixedBufferPool.cs（层级池口径的来源）。
- /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs
  与同目录 NativeMemoryTracker.cs（rust 实现现居 windex/src/ram/，README 专章自述 mirroring 的对
  象）。
- /Users/z/git/db/wedb/garnet/modules/{GarnetJSON,NoOpModule,RoaringBitmap}（modules 行的对标源，
  其中 NoOpModule 按 SKILL 剔除）。

修法
1. 内容纠偏一次做全：删 wram 全部引用与依赖边，直接内存/内存追踪归 windex（并补 windex 小节），
   缓冲池归 wbase/pool；modules 行改实名 wext_json / wext_roaring 并删 ext_noop 行（如要保留示例
   插件叙述，改写成「C# 侧 NoOpModule 系示例插件，rust 按静态特性裁定不转写」）；「16 分片无争用
   网络缓冲池」改层级池真实口径（4 层、每层条目上限 16、块大小 64KB），并从 wnode 段移到
   wbase/pool 段；crate 计数按 36 成员改写；wbase feature 清单按 Cargo.toml 实况重取；wkv 再导出
   清单逐项重核，错的改指真实归属、无定义的（TtlProbe）删。
2. 单源化：readme/zh.md 与 readme/en.md 定为唯一 mdt 源（README.mdt 现即 include 这两份），
   readme/{zh,en}/README.md 两份手工拷贝删除，或改回 doc.md 规定的「分章源 + 聚合导航」形态并让
   聚合件只做导航； whichever，禁止继续双写。
3. 根 /Users/z/git/db/wedb/README.md 用 `bun x mdt`（/Users/z/git/db/wedb/sh/dist.sh:24）渲染生成，
   不再手抄模板；wedb 侧改完源文件后同样重生成 /Users/z/git/db/wedb/wedb/README.md。
4. 文档类改动不动代码，收口以「grep 全仓 readme*/README* 无 wram / ext_noop / fifteen /
   十五个 / float 残留」为准。

优先级
打磨（无功能影响），但排本批先做：幽灵 crate 与假 API 清单会被后续审查代理当成事实引用，属文档
侧污染源，越晚修越贵。

边界
task/ing/auth-ns-default-spec-drift.md（现仍在 next/ 分拣中）管 doc/zh 规格与代码口径漂移，与本单
不同文件不同事实面；本单只管 README 家族的目录拓扑与副本形态。

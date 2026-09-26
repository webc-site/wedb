甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P4
核验记录：rust 亲验——wedb/Cargo.toml grep license/workspace.package 零命中（确无 [workspace.package] 节）；wext_json/Cargo.toml [package] 仅 name/version/edition 三字段（license 与 description 唯一缺席者）现读亲见；license="Apache-2.0" 经 grep -l 确仅 wedb 与 wedb_standalone 两份、其余 35 crate MulanPSL-2.0；仓根与 wedb/ 均无 LICENSE 文件（ls 亲验）。C# 参照——garnet/LICENSE 单许可全仓覆盖形态属实。查重：deviations 全册 license 零命中；四池零同轴。架构：[workspace.package] 单源继承符合单机制；version/homepage 不入单源的限定（九种版本各自演进、homepage 逐 crate 子路径非同值）避免抹平，方案已承接审核席对票面 :503/:631 行号漂移与 workspace.dependencies 牵动的订正；license 单一化列为维护者定夺项并给多数决默认方向，验证点 cargo package/metadata 断言闭环。纯元数据面零运行时代码。格式：纯文本、双侧齐全。定级 P4：工程卫生/发布元数据（publish 必失败限 wext_json 单 crate、义务条款混发属法务卫生面），无运行期危害。

审核结论：通过（审核席 zcode-r22-review-cargo，2026-09-26）

事实亲验全部成立：
1. 三态分裂实证：38 份 Cargo.toml 清点，wedb/wedb/Cargo.toml:5 与 wedb/wedb_standalone/Cargo.toml:5 为 license = "Apache-2.0"，其余 35 crate 为 license = "MulanPSL-2.0"，wext_json/Cargo.toml [package] 仅 name/version/edition 三字段（license 与 description 唯一缺席者，37 crate 均有 repository/homepage/description）。
2. wedb/Cargo.toml（257 行）仅 [workspace]/[workspace.dependencies]/[workspace.lints] 三节，确无 [workspace.package]。
3. 仓根与 wedb/ 目录均无 LICENSE 文本文件，README 无许可声明；garnet/LICENSE 确为 MIT 单一许可全仓覆盖形态。
4. cargo package -p wext_json 实证警告 manifest has no description, license, license-file ...，crates.io 发布侧 description 与 license/license-file 为强制字段，当前元数据 publish 必失败；全 38 crate 无一设 publish = false。
5. 查重：doc/zh/deviations.md 零 license 相关登记（grep 计 0），task/issue、task/todo、task/reject 全池无 Cargo 元数据面票据，零撞面。

审核整理执行方案（修正票面两处与行号漂移）：
1. wedb/Cargo.toml 增设 [workspace.package] 节，收 license、repository、edition 三字段单源（edition 38/38 = "2024" 全一致、repository 37 crate 同值 https://github.com/webc-site/wedb.git，均可单源）；各 crate [package] 改 license.workspace = true、repository.workspace = true、edition.workspace = true 继承，删除逐 crate 重复声明。
2. 不入单源的字段：version 保持各 crate 独立声明（现存 0.1.0 至 0.1.9 共 9 种版本各自演进，强行单源会抹平版本差异并牵动 workspace.dependencies 内部版本引用 38 条）；homepage 保持逐 crate 声明（各 crate 指向各自子路径 tree/main/<crate>，非同值不可单源）。
3. license 单一化决断（需维护者定夺，倾向最小变更）：统一 MulanPSL-2.0（35 票多数，仅改 wedb 与 wedb_standalone 两处 Apache-2.0），或统一 Apache-2.0（改 35 处）；定夺后在 [workspace.package] 落单值。
4. wext_json/Cargo.toml [package] 补齐 description、keywords、categories、homepage 等字段（对齐其余 37 crate 形态），license 走 workspace 继承；仓根补 LICENSE 全文文本文件（与定夺后的 license 一致）。
5. 测试验证点：cargo package --list -p wext_json 无元数据缺失警告；cargo metadata 全仓 package.license 字段单值断言；cargo metadata --no-deps 正常解析；常规门禁（nextest --all-features）不回归。

license 三态分裂（Apache-2.0 双入口 / MulanPSL-2.0 卅五 / wext_json 缺席）且全仓无 workspace.package 单源继承

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# garnet 上游全仓单一 MIT License（garnet/LICENSE 于仓根统一覆盖全部子工程），发布元数据单源，无按子工程分裂 license 的形态。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
38 crate 呈三态：wedb（wedb/wedb/Cargo.toml:503）与 wedb_standalone（wedb/wedb_standalone/Cargo.toml:631）license = "Apache-2.0"；其余 35 crate license = "MulanPSL-2.0"；wext_json（wedb/wext_json/Cargo.toml）整个 [package] 仅 name/version/edition 三字段，license、description、repository、homepage、keywords、categories、docs.rs metadata 全缺，为 38 crate 中唯一无 license 与 description 者。全部 38 crate 均未设 publish = false（默认可发布形态），edition/license/repository 各自手工重复声明最多 38 份，顶层 wedb/Cargo.toml 无 [workspace.package] 继承节，分裂态被逐份复制掩盖。仓根与 wedb/ 目录均无 LICENSE 文本文件，README 无许可声明。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
三害：其一 wext_json 按当前元数据 cargo publish 必失败（crates.io 强制 description 与 license/license-file 字段），发布链路在唯一缺口 crate 上断裂；其二 MulanPSL-2.0 与 Apache-2.0 义务条款不同（MulanPSL 双语文本义务与 Apache 专利条款互异），同仓混发法律状态不清，且仓内无任何 LICENSE 全文佐证实际授权；其三 38 份手工重复声明使后续 license/edition 调整需逐文件改，分裂无法在编译期或发布期自动发现，纯靠人眼比对。

涉及代码：
rust 文件与函数：
wedb/Cargo.toml:1-257（[workspace] 与 [workspace.dependencies]，无 [workspace.package] 节）
wedb/wedb/Cargo.toml:503（license = "Apache-2.0"）
wedb/wedb_standalone/Cargo.toml:631（license = "Apache-2.0"）
wedb/wext_json/Cargo.toml:1-5（[package] 仅 name/version/edition，license 与 description 全缺）

对应 c# 文件与函数：
garnet/LICENSE（MIT 单一许可文件全仓统一形态参照）

精炼执行方案：
1. wedb/Cargo.toml 增设 [workspace.package]（version/license/repository/homepage/edition 单源），各 crate [package] 改 license.workspace = true 等字段继承，消除逐 crate 重复声明
2. license 单一化决断（MulanPSL-2.0 与 Apache-2.0 二选一，需维护者定夺后全仓统一），wext_json 补齐 description 等发布必填字段，仓根补 LICENSE 文本文件
3. 测试验证点：cargo package --list -p wext_json 校验元数据完整可发布；cargo metadata 全仓 package.license 字段单值断言；常规门禁不回归

合入哈希：f9e76840 收口形态：[workspace.package] 单源 license(MulanPSL-2.0)/repository/edition＋39 crate workspace 继承，wext_json 补齐发布元数据，仓根补 MulanPSL-2.0 LICENSE 全文；cargo package --list -p wext_json 零警告、metadata 单值断言、check --workspace 零告全绿

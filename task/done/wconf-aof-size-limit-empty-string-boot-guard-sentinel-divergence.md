甄别结论：通过（甄别席 J7，2026-09-27，定级 P3——出厂空串触发面真实，门侧补 filter 单机制合规）。门现位 service.rs:1914（票面 :1896-1901 行漂，门本体 is_some() && !node.aof 未滤空坐实）、validate :1314 与 aof_size_limit_bytes :1512 双 filter 豁免、toml 面 :1637 在列；C# Options.cs:867-871 IsNullOrEmpty 豁免亲验；defaults.conf:188 出厂 "AofSizeLimit":"" 触发面真实。deviations :359-361 限额任务无重拉系他案不撞。派沙箱席 c01o。

审核结论：通过（真案。rust 门实形 service.rs:1897 is_some() && !aof 未滤空串；同字段 validate:1314 与 aof_size_limit_bytes:1512 皆 .filter(!is_empty) 豁免——三门两宽一严属实；空串可达且装载层不归 None（无 clap default，override_explicit:1637 直拷 Some("")，toml 空值→Some("")），validate 先滤空放行→boot 门后炸路径闭合；C# :867-871 IsNullOrEmpty 豁免原文核实，defaults.conf:188 出厂携 "AofSizeLimit":"" 系真实触发面（措辞未夸称出厂即炸，准确）；五池无同题票、deviations 无豁免登记、reject node-timeout 异字段。方案裁决：取门侧加 !is_empty（合仓既定 raw Some("")+各消费点滤空单机制，index_max_size 同构；源端归一反破坏对称扩面））

整理执行方案（供 fix 消费）：
1 service.rs:1897 互斥门补 .filter(|s| !s.is_empty()) 与 validate:1314 同形
2 锁测：aof-size-limit="" 且 aof 关态启动通过；非空值携关态仍拒启不回退

aof-size-limit 空串哨兵在启动互斥门误判为已配置，AOF 关态携空值配置模板被错误拒启（C# IsNullOrEmpty 豁免缺失）

问题分析：
1 Garnet 契约对齐：C# Options.cs Validate（libs/host/Configuration/Options.cs:867-871）门为 `if (!EnableAOF) { if (!string.IsNullOrEmpty(AofSizeLimit)) throw }`，空串与未设同语义（缺省即空串 Options.cs:256），aof 关态携 aof-size-limit="" 配置正常起服；仅非空限额值配 aof 关才拒启。
2 工程现状确证：rust 启动互斥门（wedb/wnode/src/service.rs:wire_background_tasks :1896-1901）判据为 `node.aof_size_limit.is_some() && !node.aof`，未滤空串。同字段另两口径均豁免空值：validate 格式定界（wconf/src/node_options.rs:1314 `.filter(|s| !s.is_empty())`）与行为折算单点 aof_size_limit_bytes（node_options.rs:1511-1512 同 filter）。NodeArgs 的 aof_size_limit 在 toml 导入导出面在册（node_options.rs:1637），故 CLI 显式传 --aof-size-limit "" 或配置模板写 aof-size-limit = ""（仿 C# 缺省 conf 形态）叠加 --aof 缺省关，rust 拒启而 C# 续起，同字段三门内两宽一严，哨兵映射反向。
3 逻辑危害确证：可用性面误拒——部署自 C# 习惯配置迁移或模板带空值键的关 AOF 实例直接起服失败（fail-closed 无数据危害，板 5.1 默认值与哨兵值映射维度）；且与仓内自身「空即关闭」口径分叉，后续审查遇 aof-size-limit 空值形态易误读为已配置。

涉及代码：
rust 文件与函数：
wedb/wnode/src/service.rs:wire_background_tasks（:1896-1901 互斥门）
wedb/wconf/src/node_options.rs:aof_size_limit 字段（:961）、validate 格式定界（:1314）、aof_size_limit_bytes（:1511）

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:867-871（!EnableAOF 段的 !string.IsNullOrEmpty(AofSizeLimit) 门）

精炼执行方案：
1 service.rs:wire_background_tasks 互斥门判据改 `node.aof_size_limit.as_deref().is_some_and(|s| !s.is_empty())`，与 validate/aof_size_limit_bytes 同口径，精确对位 C# IsNullOrEmpty 语义；非空值 + aof 关维持拒启（既有面不动）。
2 测试验证点：aof 关态下 toml 携 aof-size-limit = "" 装配口起服成功；携 "64mb" 仍拒启（锁既有错误臂）；aof 开态空值不拉起限额任务（aof_size_limit_bytes 折算臂回归）。

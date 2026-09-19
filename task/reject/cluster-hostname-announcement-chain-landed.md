集群 hostname 宣告链断链：本地 worker 恒 None，无配置源亦无 OS 回退

结论：已失效（票据所述缺口在当前 dev 全部已实现），拒绝执行。

来源：qcode 第 9 轮 net 条 3（LOW）。取证基线 dev HEAD 0671aca5。
复核基线：dev HEAD a75287ec（2026-09-19），票据取证已过时，链路已被合入。

逐条核验（票据 → 现状）
- 「wconf/src 全目录 grep hostname 零命中」→ 已有配置项：
  wedb/wconf/src/node_options.rs:395-401 `cluster_announce_hostname`（clap
  `--cluster-announce-hostname` 默认空 + serde 派生自动纳入 nested_text 导入/导出面，
  即票据方案的配置侧原样落地）。
- 「cluster_manager.rs:180/:191 两臂 hostname: None，注释失真」→ 已收口单源：
  wedb/wedb/src/server/cluster_manager.rs:61 `resolve_announce_hostname`
  （配置非空直取，空则 :72 `os_hostname` 一次 gethostname，C#
  `string.IsNullOrEmpty(hostname) ? Format.GetHostName() : hostname` 的等价）；
  :211 两臂共用，:230/:241 均 `Some(hostname.as_str())`，失真注释已订正。
- 装配传递链已通：wedb/wedb/src/server/boot.rs:241 读
  `node.cluster_announce_hostname` → cluster_provider.rs:343 `init_local`。
- 「下游 :425 输出臂 / :458-467 反查臂死路」→ 本地 worker 恒有 hostname 后自动
  活化，无需改动（与票据方案一致，现状即此形态）。
- 「补一用例断言 preferred=hostname 下 CLUSTER SHARDS 与 -MOVED 吐主机名」→
  已存在：wedb/wedb/tests/cluster_announce_hostname.rs
  （cluster_shards_emit_announced_hostname + -MOVED 主机名重定向两用例）。

无残余工作，不开分支。

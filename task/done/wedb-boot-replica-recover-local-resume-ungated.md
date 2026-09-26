甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P4
核验记录：C# 亲验——ReplicationManager.cs:511-531 REPLICA 分支 ClusterReplicaResumeWithData 门控（:522 现读）、GarnetServerOptions.cs:646 默认 false、StoreWrapper.cs:377-399 集群分支委托均在树；rust 亲验——boot.rs:78 open_from_args 无条件前置（角色迟至 :261 initialize_cluster_config 后可知）、:281-283 双角色一律回填 set_current_replication_offset、wedb/src/server/replication/replication_manager.rs:1358-1359 注释自陈「副本重启后等待与 primary 重新同步」与实际行为相反——双失真现码仍存。查重：deviations 全册 grep ResumeWithData 零命中；四池无同轴（ing wconf-defaults-knobs 系本票第 3 点联动订正对象非并案）。架构：登记级零行为改动；护栏（按 C# 回改须前置角色可知、严禁仅加门）亲验 boot 装配序成立。格式：纯文本、双侧路径齐全。定级 P4：治理面（台账+注释订正），防发散由 §116 协商链在案承接，无运行期危害。

审核结论：通过（登记级+登记订正级，零行为改动；boot.rs:78 无角色门控前置恢复、:281-283 双角色回填、C# REPLICA 门控臂与默认 false、replication_manager.rs:1358-1359 注释自相矛盾、在案 wconf 票失真论断全部亲验属实；deviations 全册零命中无撞面；护栏注记得当——角色可知后置于 :260，仅加门不可行）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. deviations.md 顺编立目（§151 位）：钉 C# StoreWrapper.cs:377-399 / ReplicationManager.cs:511-531 / GarnetServerOptions.cs:646 三锚与 rust boot.rs:78/:281-283 现状锚，裁决「副本 --recover 恒等效 ResumeWithData=true 本地续用」为刻意改良（消空库读窗、免主端全量重同步压力，防发散由 sameHistory/trunc_floor 协商链承接），附护栏：按 C# 门控回改须同步前移 boot 装配序（角色可知须前置），严禁仅加门。
2. 订正 replication_manager.rs:1358-1359 注释为「数据面恢复由 wnode open_from_args 角色无关前置承接，副本重启即本地续用，等待 attach 增量/全量由协商链裁决」并回指新条目。
3. 在案 ing 票 wconf-defaults-knobs-absence-unregistered 第 2/3 点与 :39 改判「行为等效 C# 置 true」，其执行时该项从十项族摘出归新条，勿重复登记。
4. 测试验证点：双节点拓扑——副本写键后 --recover 重启，断言 attach 前本地键可读且 INFO replication 位点为本地重放尾；对照不带 --recover 重启为空库待全量；./sh/clippy.sh 零警告。

副本角色 --recover 重启本地全量恢复无 ClusterReplicaResumeWithData 门控（码内注释与在案票登记双失真）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。C# 集群模式恢复链按角色门控本地数据面恢复：StoreWrapper.cs:RecoverAsync（:377-399）集群分支委托 clusterProvider.RecoverAsync → ReplicationManager.cs:RecoverAsync（:511-531），REPLICA 分支仅在 serverOptions.ClusterReplicaResumeWithData（GarnetServerOptions.cs:646，默认 false）为真时才执行 RecoverCheckpointAndAOFAsync（本地检查点 + AOF 设备恢复 + InitializeIf + 全量重放 + replicationOffset.SetValue）；默认部署下副本带 --recover 重启为空库，等待 rm.Start() attach 后由主端全量重同步。单机分支（无角色概念）才无条件恢复。replicationOffset 回填同样只在 RecoverCheckpointAndAOFAsync 内发生——C# 默认副本重启位点为 0。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）。rust 启动链把数据面恢复整体前置于角色可知之前：wedb/wedb/src/server/boot.rs:run_cluster_server 第 :78 行无条件调用 StorageSessionProvider::open_from_args，其 (recover, aof) 四路分派（wnode/src/service.rs:open_from_args_with_config :1640-1677）仅凭命令行旗标——(true, true) 臂 open_recovered_with_config_and_aof（service.rs:1501）对副本角色同样执行检查点恢复 + 向量回建 + WAL 设备恢复 + initialize_if + replay_aof(u64::MAX) 全量重放；节点角色迟至 :260-269 initialize_cluster_config 载入集群配置后才可知。随后 :281-283 以 recovered_aof_tail 对主/副本两角色一律回填 rm.set_current_replication_offset；attach 臂 start_replication_attach（cluster_provider/replication.rs:290-356）再按协商链（negotiate_resync sameHistory / trunc_floor 预检）走增量或全量。净效果：rust 副本 --recover 重启恒为「本地续用」形态，等效 C# ClusterReplicaResumeWithData=true，而 C# 默认部署为 false。双失真确证：其一，wedb/wedb/src/server/replication/replication_manager.rs:recover_async 注释（:1358）自陈「REPLICA+ClusterReplicaResumeWithData 分支：该配置面未落地，副本重启后等待与 primary 重新同步（C# 未配置该项时同语义）」——与实际行为相反（本地数据面已在 boot.rs:78 恢复完毕，等待重同步的是增量追平而非空库待灌）；其二，在案 ing 票 wconf-defaults-knobs-absence-unregistered 第 2/3 点据此断言该项「行为恒同 C# 默认部署形态、零运行期危害」，若照案执行将把失真论断固化进 deviations 台账。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）。不丢数据（协商链 sameHistory / trunc_floor 已防历史发散，§116 在案），但三面可观测分叉无台账：a) 副本重启至 attach 完成（accept 已开、attach 在 cluster_provider.start() 之后发起）窗口内副本可读面为陈旧本地数据，C# 默认为空库——对拍「副本重启后即刻 GET 旧键」双侧发散；b) gossip 广播与 failover 判定基线 replicationOffset：rust 为本地重放尾、C# 默认为 0，副本升主资格评估输入不同；c) 主端负载形态分叉：C# 默认副本重启必全量重同步，rust 走增量。三者均系「未登记行为分叉 + 注释/在案票登记失真」类危害（治理面为主），非静默数据危害。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/boot.rs:run_cluster_server（:78 open_from_args 无角色门控且先于 :260 initialize_cluster_config；:281-283 双角色一律回填位点）
wedb/wnode/src/service.rs:StorageSessionProvider::open_from_args_with_config（:1640 四路分派仅凭 (recover, aof)）与 open_recovered_with_config_and_aof（:1501-1611 副本角色同走全量恢复+重放）
wedb/wedb/src/server/replication/replication_manager.rs:ReplicationManager::recover_async（:1357-1359 注释自陈失真）
wedb/wedb/src/server/cluster_provider/replication.rs:ClusterProvider::start_replication_attach（:298 attach 判据 recover()）

对应 c# 文件与函数：
garnet/libs/server/StoreWrapper.cs:RecoverAsync（:377-399 集群分支角色门控委托 vs 单机分支无门控）
garnet/libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync（:511-531 REPLICA 分支 ClusterReplicaResumeWithData 门控）
garnet/libs/server/Servers/GarnetServerOptions.cs:ClusterReplicaResumeWithData（:646 默认 false）

精炼执行方案：
1. 裁决收口（建议方向 1，登记级零行为改动）：确认「副本 --recover 恒本地续用」为刻意改良（消除副本重启空库读窗、免主端全量重同步压力；防发散由协商链承接），doc/zh/deviations.md 立目登记：锚 C# 上述三处，注明 rust 以启动链角色前置重构等效 ResumeWithData=true 恒开形态、严禁按 C# 门控回改时须同步重构 boot 装配序（角色可知须前移）而非仅加门。
2. 订正两处登记失真：replication_manager.rs:1357-1359 注释改为「数据面恢复由 wnode open_from_args 角色无关前置完成，副本重启即本地续用（等效 C# ResumeWithData=true），等待 attach 增量/全量由协商链裁决」并回指新条目；在案 ing 票 wconf-defaults-knobs-absence-unregistered 的 ClusterReplicaResumeWithData 项从「行为恒同 C# 默认」改判「行为等效 C# 置 true」，随票执行的 deviations 条目同步。
3. 测试验证点：集成测试双节点拓扑——副本写入后带 --recover 重启，断言 attach 发起前本地键可读且 INFO replication 位点为本地尾（锁现状形态）；对照不带 --recover 重启为空库待全量；跑 ./sh/clippy.sh 零警告。

合入哈希：609a207 收口形态：deviations §162 立目裁决「副本 --recover 恒本地续用（等效 C# ResumeWithData=true 恒开）」＋§160 族第 7 项失真摘出订正＋replication_manager.rs recover_async 注释双失真订正回指＋双节点真 TCP 现状锁测 replica_recover_local_resume_ungated.rs 全绿，零行为码改动（登记号因 dev 侧 wext_json 条先入库由 §161 让位顺编 §162）。

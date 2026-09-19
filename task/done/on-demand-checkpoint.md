on-demand-checkpoint 配置面接线 + allow_data_loss 回归派生式

来源：next/on-demand-checkpoint-flag-unwired.md 立项（qcode10.design 条 1）。取证基线：dev
HEAD de9f7d3a。

判定
成立且待做（走原单修法 1「开放该旋钮」）。C# 三源齐备：garnet/libs/host/Configuration/
Options.cs:453-454 声明 --on-demand-checkpoint、:992 落入服务选项、
garnet/libs/server/Servers/GarnetServerOptions.cs:405 字段默认 true、:653-654
AllowDataLoss 派生式，读者两处
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:190
与 :280。rust 侧字段与读者俱在而写侧断链：
wedb/wedb/src/server/cluster_provider.rs 构造期恒 true，set_on_demand_checkpoint 全仓零引用，
wconf 无该旋钮，唯一读者
wedb/wedb/src/server/replication/replica_sync_session.rs 的按需重拍判据恒走真支——死旋钮。

方案（一处机制，parse→store→consume 打通）
1. wconf/src/node_options.rs NodeArgs 增 on_demand_checkpoint（CLI --on-demand-checkpoint +
   nested_text 同字段，默认 true，形态照抄既有默认真值旋钮 protected_mode）与
   fast_aof_truncate（CLI --fast-aof-truncate + nested_text，默认 false，
   Options.cs:449-450；派生式的另一输入，此前无 CLI 面恒假）。二者入
   override_explicit 覆盖表与 Default，投影单点 runtime_server_options() 一处写值。
2. wconf/src/runtime_server_options.rs 增 on_demand_checkpoint 字段（默认 true，
   GarnetServerOptions.cs:405）。CONFIG 面不注册：C# RuntimeServerConfig.cs 只把
   fast-aof-truncate / use-aof-null-device 列为只读项，on-demand-checkpoint 无 CONFIG 名额，
   照此不设第二读径。
3. wedb/src/server/boot.rs 装配期一次注入 cluster.set_on_demand_checkpoint（与
   set_fast_aof_truncate 同位同型）。
4. wedb/src/server/cluster_provider.rs 删 allow_data_loss 槽与 set_allow_data_loss，
   allow_data_loss() 改编译期唯一算式 fast_aof_truncate && !on_demand_checkpoint，
   对齐 GarnetServerOptions.cs:653-654（C# 第三项 UseAofNullDevice 本仓未移植，无 null
   设备形态，注释据实点名）。
5. wedb/src/args.rs 删 ClusterArgs::allow_data_loss 直配旋钮与其单测（C# 无该 CLI 选项，
   AllowDataLoss 恒派生），boot.rs 的注入随槽位一并移除；
   replication/replica_sync_session.rs 的混尽放行单测改设两输入。

优先级
功能缺口 + 死代码。

交叉引用
- task/ing/boot-assembly-projection-single-source.md 治三字段样板双抄，本单只加
  on_demand_checkpoint 一条注入事实。
- next/runtime-options-read-only-path-fields-unprojected.md 的「不裁定」段把
  fast_aof_truncate 列为「C# 旋钮未开放」，本单按派生式需要把它开放并投影，该单不再涉及。
- 不触碰 take_on_demand_checkpoint 的拍摄与截断链。

验收
- CLI/配置文件改 on-demand-checkpoint 后，按需重拍判据与 allow_data_loss 派生随动；
  全仓 allow_data_loss 仅一处算式，无 setter、无直配 CLI。
- cargo check --workspace --all-targets 零 error 零 warning。

落地
已按上述五步完成（分支 dev5/on-demand-checkpoint，提交 ef8eb75e）。on_demand_checkpoint
的写侧现有生产注入点 boot.rs:161 与测试两处，不再是零引用 setter；allow_data_loss 槽与
setter 已删，ClusterProvider::allow_data_loss 为唯一算式；ClusterArgs::allow_data_loss
直配旋钮与其单测随删。wconf 单测
node_options.rs:test_truncate_odc_knobs_parse_store_project 钉死 CLI、nested_text、
三层合并、导出与投影五面；replica_sync_session 单测补
odc_disabled_skips_reshoot_entirely（关开关即跳过重拍落回 skip 直推），
odc_exhausted_proceeds_when_data_loss_allowed 改由两输入置出派生命中面。
js/check.js 对本单两旋钮无新增缺失报告（默认真值旋钮无 CONFIG 名额，与 C#
RuntimeServerConfig 一致，故无 ignore 需登记）。

# command-catalog-dedup：命令元数据双源归一

来源：next/design.md 条 1（P1 命令元数据双源：手写 ACL 目录表 vs 内嵌 JSON）。

## 背景与痛点

命令元数据存在两份真值：

1. wedb/wresp/src/catalog/data.rs：2494 行手写 CMD_ENTRIES 静态表（353 条，
   cs/name/cmd/cats/parent 五字段），消费方 wacl/src/acl_parser.rs、
   wacl/src/user.rs、wacl/src/command_permission_set.rs。
2. wedb/wnode/src/resp/RespCommandsInfo.json：include_str 内嵌的 garnet
   RespCommandsInfo.json（6969 行，353 条），wnode/src/resp/resp_commands_info.rs
   OnceLock 解析为 INFO 全量表；RespCommandsDocs.json 同样 wnode 内嵌一份。

命令增删需双改，漂移即 ACL 判定与 INFO 输出不一致。

## 甄别结论

问题成立。C# 对标：

- garnet/libs/resources/ 是独立程序集，EmbeddedResource 内嵌
  RespCommandsInfo.json 与 RespCommandsDocs.json，单一真值。
- garnet/libs/server/Resp/RespCommandsInfo.cs 运行时（静态初始化）反序列化
  该 JSON 构建全部索引，其中 AclCommandInfo 即 ACL 消费面；
  garnet/libs/server/ACL/User.cs 直接消费 TryGetCommandsforAclCategory。
- C# 侧不存在第二份手写 ACL 表，rust 侧 catalog 手写表为转写期自造物。

拓扑约束：wnode 依赖 wacl 依赖 wresp，ACL 目录必须留在 wresp 层。
JSON 未实现命令条目处置：对标 C# 全量保留，不裁剪。

漂移甄别：一次性对比探针（分支内运行后删除）证实归一前两份真值内容一致：
353 条扁平条目键集与 ACL 分类位集零差异；JSON 内 29 条 IsInternal 条目与两条
Command=SECONDARYOF 条目（Name=SECONDARYOF / SLAVEOF，历史别名）均被手写表
忠实收录。问题性质是架构性双源风险而非已发生漂移。

## 方案与实现

1. 新建 crate wresources（对齐 garnet/libs/resources 资源组织）：
   - wedb/wresources/：lib.rs 仅导出 RESP_COMMANDS_INFO_JSON 与
     RESP_COMMANDS_DOCS_JSON 两个 include_str 常量，零依赖。
   - 两份 JSON 自 wnode/src/resp/ git mv 迁入，wnode 侧副本删除。
2. wresp catalog 归一：
   - data.rs 手写表删除（-2494 行）；catalog/mod.rs 以 OnceLock 运行时解析
     wresources::RESP_COMMANDS_INFO_JSON 生成目录（sonic-rs + serde，
     cargo add 引入），对外 API 不变：CmdEntry、try_get_resp_command_info、
     try_get_by_cs_name、children_of、commands_for_category，wacl 零改动。
   - 解析规则：根 + 子命令先序扁平，全量收录含 IsInternal 与 Name=SLAVEOF
     历史别名条目（与 C# AclCommandInfo 全收一致；SLAVEOF 去重仅存在于
     INFO 表扁平枚举索引，因枚举键冲突而起，ACL 目录无此约束）；
     name 保持小写，ACL 描述行为不变。
   - 解析失败启动期 expect 显式报错（内嵌资源失败属工程错误，禁止静默降级）。
   - 解析时机选 OnceLock 运行时解析，与 wnode INFO 表及 C# 静态初始化一致，
     不用 build.rs，无两套并存。
   - CmdEntry.cs/name 由 &'static str 改 String；连带
     wedb/src/server/cluster_session.rs cluster_sub_name 一行 as_str()。
3. wnode 最小改动：
   - resp_commands_info.rs、resp_command_docs.rs 的 include_str 改引
     wresources 常量。
   - acl_categories_from_member_names 下沉 wresp 为
     RespAclCategories::from_member_names 单处定义，wnode 删本地版。
4. resp_commands_info_data.rs 的 strum 成果不动，仅修正注释。
5. 双源一致性探针随双源消失而删除，catalog 单元测试锚定计数（353 = 260 根
   + 93 子）、SECONDARYOF 双别名、JSON 声明序、分类位集与成员名解析。

## 验证结果

- 检查脚本：bun ./js/check.js 输出 0 缺失 0 重复（归一过程中曾报
  RespCommandsInfo.cs:TryInitialize 双映射，entries 注释去除映射声明后消除）。
- 静态检查：./clippy.sh 0 警告（未用 allow）。
- 自动化测试：./test.sh 干净轮 2018 项全过 + 回归门 2 项全过；满载下存在
  负载敏感 flaky（append_scan / concurrent_flush，失败点漂移、单跑均过，
  whlog/wkv 不依赖本次改动面，15 轮单跑无失败），与本次变更无关。
- 规范审查：按 rust_review/SKILL.md 自查（cats 解析链改 match、依赖统一
  cargo add 产出、无 allow、注释中文、C# 映射注释格式合规）。

# anchor-bare-wconf-runtime-options — wconf 裸锚入册

## 认领与来源

- 来源：next/cs-corpus.inv1.md §3.4 票 2（slug 同名），射程按 §3.3 批 1（1a + 1b）。
- 状态：在途（本票只做注释形态，不动函数体、不动语料、不动 cs-corpus.inv1.md）。
- 树：bash fork.sh anchor-wconf → /tmp/fork/anchor-wconf，CARGO_TARGET_DIR=/tmp/ct-awc 私有。

## 射程

- wedb/wconf/src/runtime_server_options.rs
- wedb/wconf/src/node_options.rs

## 判据

- 把裸名/数字行锚改写为「garnet 相对全路径.cs:符号」登记形态（口径 = .agents/skills/transpile/SKILL.md
  的映射注释规范与 js/check/rustScan.js 的 CS_REF_REGEX + csPathNormalize），使 js/check.js 的
  doc_file_fn_map 映射册采信。
- 挂锚前置条件：C# 对位文件在 garnet/ 软链内 find 实证实存，且被引符号在该文件文本中实存
  （A 层 symbolCheck 的 \b 断言口径）。逐枚实证见下方清单。
- 映射不成立（无独立具名对位口 / 册内已在别处持锚 / ignore 已在册且理由成立）者按口径改散文，
  点名 C# 成员但不写可登记冒号形，禁虚构路径、禁为增数硬挂。
- 数字行形态（File.cs:123）CS_REF_REGEX 不认、symbolCheck 亦不采，属佐证性叙述；
  仅当它是某 rust 项的唯一映射声明时才酌情改名锚，余者留。

## 现刻取证实数（本票自算，与盘点数对照）

同口径脚本（js/check/rustScan.js 的 CS_REF_REGEX + csPathNormalize，逐行匹配）：

- runtime_server_options.rs：裸名锚 35 枚（doc 面 35/35，全注释面亦 35）+ 数字行锚 3 枚 +
  全路径锚 1 枚（第 5 行 GarnetServerOptions.cs:GarnetServerOptions，在册）。
  盘点 §3.2 记 35 → 逐字吻合。
- node_options.rs：裸名锚 9 枚（doc 面 6 + 非 doc 行内 3）+ 数字行锚 123 枚 +
  全路径锚 5 枚。盘点 §3.2 记裸 src 9 → 吻合；行号 src 记 123 → 吻合。
- 合计裸名 44 枚 = 盘点 §3.2 wconf 裸src 47 中本两文件之和（余 3 枚在他文件，射程外）。

映射册基线（本票取数时 rustScan 全仓实测）：登记文件 697、条目 4262、函数前导 doc 重复组 13。

## 锚点清单表：现形态 → 目标登记形态 → C# 实证路径:行

### 1a runtime_server_options.rs（35 枚裸名 + 1 枚数字行主锚）

行 12 GarnetServerOptions.cs:ClusterTimeout → libs/server/Servers/GarnetServerOptions.cs:ClusterTimeout → 声明 garnet/libs/server/Servers/GarnetServerOptions.cs:251
行 14 GarnetServerOptions.cs:ReplicaSyncDelayMs → 同前缀 → 同文件:382
行 16 GarnetServerOptions.cs:AofReplayMaxLagBytes → 同前缀 → 同文件:387
行 18 GarnetServerOptions.cs:AofTailWitnessFreqMs → 同前缀 → 同文件:150
行 20 GarnetServerOptions.cs:AofSyncMaxLagBytes → 同前缀 → 同文件:395
行 22 GarnetServerOptions.cs:ReplicaDisklessSyncDelay → 同前缀 → 同文件:415
行 24 GarnetServerOptions.cs:ReplicaAttachTimeout → 同前缀 → 同文件:425
行 27 GarnetServerOptions.cs:ClusterReplicationReestablishmentTimeout → 同前缀 → 同文件:640
行 29 GarnetServerOptions.cs:CompactionMaxSegments → 同前缀 → 同文件:236
行 31 GarnetServerOptions.cs:CompactionType → 同前缀 → 同文件:225
行 35 GarnetServerOptions.cs:SlowLogThreshold → 同前缀 → 同文件:287
行 37 GarnetServerOptions.cs:SlowLogMaxEntries → 同前缀 → 同文件:292（同行 StoreWrapper.cs:243 为容量佐证，留数字形）
行 40 GarnetServerOptions.cs:ObjectScanCountLimit → 同前缀 → 同文件:513
行 42 GarnetServerOptions.cs:EnableScatterGatherGet → 同前缀 → 同文件:376
行 44 GarnetServerOptions.cs:AofSizeLimitEnforceFrequencySecs → 同前缀 → 同文件:206
行 46 GarnetServerOptions.cs:CommitFrequencyMs → 同前缀 → 同文件:157
行 48 GarnetServerOptions.cs:ExpiredObjectCollectionFrequencySecs → 同前缀 → 同文件:216
行 50 GarnetServerOptions.cs:ExpiredKeyDeletionScanFrequencySecs → 同前缀 → 同文件:162
行 54 GarnetServerOptions.cs:EnableAOF → 同前缀 → 同文件:86
行 56 GarnetServerOptions.cs:MaxDatabases → 同前缀 → 同文件:615
行 58 GarnetServerOptions.cs:CheckpointBaseDirectory → 同前缀 → 同文件:625
行 60 GarnetServerOptions.cs:LogDir → libs/server/Servers/ServerOptions.cs:LogDir → 声明 garnet/libs/server/Servers/ServerOptions.cs:92
  （LogDir 在 GarnetServerOptions.cs 仅被继承读用、无声明行，故改挂声明处，不冒充该文件成员）
行 62 GarnetServerOptions.cs:UnixSocketPath → libs/server/Servers/GarnetServerOptions.cs:UnixSocketPath → 同文件:605
行 64 GarnetServerOptions.cs:EnableCluster → 同前缀 → 同文件:54
行 66 GarnetServerOptions.cs:AofMemorySize → 同前缀 → 同文件:101
行 68 GarnetServerOptions.cs:AofPageSize → 同前缀 → 同文件:106
行 70 GarnetServerOptions.cs:AofSegmentSize → 同前缀 → 同文件:112
行 72 GarnetServerOptions.cs:AofPhysicalSublogCount → 同前缀 → 同文件:117
行 74 GarnetServerOptions.cs:AofReplayTaskCount → 同前缀 → 同文件:122
行 76 GarnetServerOptions.cs:WaitForCommit → 同前缀 → 同文件:196
行 79 GarnetServerOptions.cs:AofSizeLimit → 同前缀 → 同文件:201
行 81 GarnetServerOptions.cs:AofReplayDriftThreshold → 同前缀 → 同文件:128
行 83 GarnetServerOptions.cs:AofReplayDriftCheckFreq → 同前缀 → 同文件:139
  （观察：该字段 C# 初值为 1，CLI 面 Options.cs:237 无 Default 即 0，注释「默认 0」取自 CLI/defaults 通道；
  本票只改形态不改语义断言，留待默认值专项票复核）
行 85 GarnetServerOptions.cs:ReplicaSyncTimeout → 同前缀 → 同文件:420
行 89 GarnetServerOptions.cs:FastAofTruncate → 同前缀 → 同文件:400
行 91 GarnetServerOptions.cs:405 OnDemandCheckpoint（数字行主锚）→
  libs/server/Servers/GarnetServerOptions.cs:OnDemandCheckpoint → 同文件:405
  （该键已由 wedb/wedb/src/server/cluster_provider/flags.rs:128 函数 doc 持锚，本处为字段自身载体，
  字段 doc 不入 dupDefFind 采集面，故改形不新增条目、亦不新增重复组）

小计：登记新键 35（前 35 枚），行 91 改形不增数。

### 1b node_options.rs（9 枚裸名）

行 38 ServerOptions.cs:DEFAULT_RESP_VERSION → libs/server/Servers/ServerOptions.cs:DEFAULT_RESP_VERSION → 声明 garnet/libs/server/Servers/ServerOptions.cs:15 ⇒ 挂锚（新增 1 键）

行 30 Format.cs:defaultBindLoopBack ⇒ 改散文（C# Format.defaultBindLoopBack，声明 garnet/libs/common/Format.cs:39）
行 32 Format.cs:defaultBindAny ⇒ 改散文（同文件:36）
  理由：rust 侧仅 DEFAULT_BIND / DEFAULT_BIND_ANY 两常量就地承接，无同名函数口；
  js/check/ignore/common.yml 的 libs/common/Format.cs 块已逐名列举 defaultBindAny、
  defaultBindLoopBack 两枚（块理由第三类「BCL 数值与格式化工具 Format.*」），
  挂全路径锚会触发 ignoreLoadAndPrune 自动淘汰该两条既经甄别的登记，属越权改登不属改锚。
行 412 RespServerSession.cs:Send ⇒ 改散文（C# RespServerSession.Send，声明 garnet/libs/server/Resp/RespServerSession.cs:1440，
  WaitForCommit 读点在该文件 :1475）
  理由：libs/server/Resp/RespServerSession.cs:Send 已由 wedb/wnode/src/resp/resp_server_session.rs:2216
  （函数 take_output_into 前导 doc）持全路径锚，本处只是读点佐证，按「一处一锚、子处散文」不双挂。
行 477 / 1537 Format.cs:TryParseAddressList ⇒ 改散文（声明 garnet/libs/common/Format.cs:52）
  理由：同键已由本文件 :791（函数 endpoints 前导 doc）持全路径锚（在册），两处均属引用佐证。
行 904 / 1684 Options.cs:GetServerOptions ⇒ 改散文（声明 garnet/libs/host/Configuration/Options.cs:771）
  理由：C# GetServerOptions 是整副 GarnetServerOptions 装配口（含设备工厂、TLS、认证、检查点等段），
  rust 侧按 NodeArgs::runtime_server_options / server_config 等分段投影承接，无 1:1 具名实口；
  js/check/ignore/hosting.yml 的 libs/host/Configuration/Options.cs 块已登 GetServerOptions
  （理由自陈「已转写活项…（NodeArgs::runtime_server_options 投影 RuntimeServerOptions 播种）」），
  挂名锚会淘汰该既经甄别条目，判留登记、本处改散文。
行 1821 ServerOptions.cs:ValidatedPageSizeBits ⇒ 改散文（声明 garnet/libs/server/Servers/ServerOptions.cs:157）
  理由：同键已由 wedb/wconf/src/size.rs:39（函数 validated_page_size_bits 前导 doc）持锚（在册），
  本处为调用点佐证。

### 1b 行号形态（123 枚）判词：一律留

按 §3.2 两真判据逐条核：本文件数字行锚绝大多数是「File.cs:行号 + CLI 属性名」的默认值/校验位佐证
（Options.cs 系 30+ 枚指向 libs/host/Configuration/Options.cs 的 CLI 属性声明行），
其中 index-resize 系（行 241/246/628）、reviv 细化系（行 138/171/173）、certificate 吊销档（行 346）
等名目恰是 hosting.yml 不转写清单点名的项，改挂名锚即与既有登记自相矛盾；
另有行 821/882/857/866/881 的 Convert、IntRangeValidation 属 .NET BCL 与校验特性名，非对位函数；
行 271/1394 引 GarnetServer.cs:508 CreateAOF，实测 garnet/libs/server/GarnetServer.cs 不存在
（真身在 libs/host/GarnetServer.cs，本票射程外，另票处置），机械补前缀反成假锚。
故本批数字行形态零改写，只记判词。

## 门禁口径

1. cargo check --workspace --all-targets 零告警（树内，私有 target）。
2. bun js/check.js 树内前后对跑：映射册条目数净增 ≥36、「# 实现缺失」段与 stderr 逐字节不变、
   重复定义组数不变（基线 13）、ignore 语料零回写（若有非本票回写一律 git checkout -- 还原）。
3. cargo fmt 只施本票两文件。
4. 禁跑主仓 test.sh / sh/clippy.sh。

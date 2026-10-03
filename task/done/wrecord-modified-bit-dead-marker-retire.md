记录头 MODIFIED 原位更新位为「只写零读」死标记：C# 承责不变量（WATCH 版本推进确定性）已由 wkv 写面无条件 bump_watch_version 单点承接，六处位写点成热路径纯死工，建议整体清退并将 RecordInfo.cs:TryResetModifiedAtomic 锚改走 ignore 登记

审核结论：通过但收窄（2026-09-28 独立审核席 + 主控采纳，定档 P3 治理性死标记清退维持）

一、真实性确证（席一手，fix 直接引用，不必重跑）
- 「生产零读方」成立且按位形态全扫：MODIFIED|is_modified|set_modified|is_in_place_updated 全仓命中仅 wrecord 本体（bits.rs:25/:45、header.rs:14/:173-188/:607、record_mut.rs:14/:227/:239/:241/:353-354）+ whlog/src/hlog/inplace.rs:88/:113/:146/:181 + 自测面 + 文档注释面；裸移形态 `<< 59` 唯 bits.rs:25 与 header_and_bits.rs:18，全仓零 `>> 59`、零 bit59 十六进制字面量。
- 非破坏性偏差（判「通过」而非判拒的关键）：整字比较/掩码面逐一核过——RECORD_INFO_RESERVED_MASK/FLAG_MASK 唯消费在 bits.rs:72-73 编译期断言（零运行时校验）；record_mut.rs:224-237 CAS 比的是循环重载值不含 bit59 常量；header.rs:521-523 is_null 只判 prev_address==0；inplace.rs:380-394 revivify 整字 store 经 RecordHeader::new 构造（bits.rs:117-125 仅融 TOMBSTONE）天然不含 MODIFIED；wcpr 与 wreviv 两目录本位 grep 零命中；落盘为页字节原样写出（flush.rs / hlog/io.rs / hlog/shift.rs 零 Modified 引用），旧磁盘镜像 bit59=1 残留无判定面，恢复读头只经 ADDRESS_MASK 与具名位访问器。
- 不变量承接等价：C# 该位是「原位写去重门」（MainStore/RMWMethods.cs:426、MainStore/DeleteMethods.cs:31、ObjectStore/RMWMethods.cs:99/:124、DeleteMethods.cs:20/:29、UnifiedStore/RMWMethods.cs:165、DeleteMethods.cs:23/:35、SessionFunctionsUtils.cs:119/:139/:159 全为 `if (!logRecord.Info.Modified) IncrementVersion`）+ Watch 复位窗（TransactionManager.cs:402-412 → ClientSession.cs:362-367 → ModifiedBitOperation.cs:21/:52-53）。rust 侧全部写臂无条件 bump_watch_version（wkv/src/session/mod.rs:800 定义、StoreSession 转发 :1207；调用清单 write/mod.rs:228/:290/:348/:450/:466/:594/:655、write/rmw.rs:143/:186/:238、collection.rs:107/:146/:159、range_index/ops.rs:255/:347/:491/:495、migration.rs:416；wnode 投影 storage_session.rs:912/:945/:963/:1009/:1053、aof_processor_store_ops.rs:131/:408、ttl_sync.rs:147/:152/:184、tiered_demote.rs:338/:342、common.rs:702、scan.rs:240、rmw_helpers.rs:141/:675/:732、slow.rs:815/:821）收敛于 WatchVersionMap::increment_version 单机制（wtxn/src/watch_version_map.rs:61）。EXEC 判据为相等比较（txn_watched_keys_container.rs:77 冻结读版、:94 全等复验），版本数值幅度不在协议面可观测，故多推进不改变 0→非0 判定，无多余 abort、无重试风暴。
- 查重：deviations.md 全册本位零命中；五池唯 task/issue/checkjs-miss-post-r7（其 :70-72 明文把功能真伪裁决让给另立票）与 task/done/wconf-node-args-time-knobs-upper-gate-compio-panic.md:185（把 b 面收口判给本票）；header_and_bits.rs:349-350 注释证明委托层裁撤在前、本位本体生死无既往裁决，非重开。在途他席（bench/**、replica_sync_session、windex、wtls、wedb server 域）与本票触面零交叠。

二、收窄四条（必守，违者门禁回红）
1. js/check 收口面须扩为 RecordInfo.cs 双条目：除 TryResetModifiedAtomic 外，`SetModified`（RecordInfo.cs:234，garnetScan.js:81 只采 method_declaration，属性 Modified 不入册）的全仓唯一规范锚即 header.rs:179 该行；删 is_modified/set_modified 即灭此锚，`# 实现缺失` 极可能新挂 SetModified 而非归零。收口须把 SetModified 与 TryResetModifiedAtomic 一并入 js/check/ignore/storage.yml:1339 既有同文件条目组（理由同源同句），并把验证点改为「check.js 缺失段无任何 RecordInfo.cs 条目」，以主树实测裁决。
2. 文档与注释清扫面补全：票面只列 wedb/README.md:357/:764 与 readme/zh/en.md:330，漏 wrecord 自身 bit59 布局面四处（wrecord/README.md:37/:103、wrecord/readme/zh.md:23、en.md:23）与叙述性论证面（wrecord/src/header.rs:30/:89 头文档；wkv/src/session/raw/modify.rs:67「回 None 会跳过 MODIFIED 位落笔」——该论证因果本体即本位，清退后须改写为按「已提交态不撤回」独立成立；wkv/src/session/raw/write/inplace.rs:925「SetTombstone + SetModified 原位落笔」）。口径钉死：大写位引用（MODIFIED 位名/位图/布局）随面清扫，C# 对位锚名词叙述保留（record_mut.rs:398、record_ref.rs:97、ttl_sync.rs:164、watch_version_regression.rs:182 属后者，禁删）。
3. 双轨让路显性化：checkjs-miss-post-r7 票 :25-28 预断 b 面「属实现存活、应改挂存活公开位」与本票 ignore 方向相反，主控归档时已注记作废；b 面（RecordInfo.cs:TryResetModifiedAtomic）从该票登记批中排除，单一序列为本票先落。
4. 事实锚错订正与措辞降档：票面「C# RecordInfo.Modified（bit 59）」错误——C# 实为 bit 51（RecordInfo.cs:33-37 偏移链 + LogAddress.cs:14 kAddressBits=48：Tombstone48/Valid49/InNewVersion50/Modified51/Sealed52）；rust bit59 正确，两族布局本就非同拓扑（rust 无 Valid 位，header.rs:31 在册自陈），清退并段时禁再新增「同 C#」断言（bits.rs:18 既有虚假同拓扑陈述非本票义务、不阻塞）。「六处位写点成热路径纯死工」有约三成夸大，订正为「四处显式 set_modified + record_mut.rs:241 else 臂独立 fetch_or 为死工；revivify 合字臂 :227/:239 的 MODIFIED 系搭既有 CAS 顺风车的掩码，清退只化简掩码、省不了总线同步」。

三、形态裁决
择 d（缺陷面判空）：不变量已由 bump_watch_version 单源承接且有 wkv/tests/delete_miss_watch.rs 钉死，本票为治理性死标记清退 + 对账收口，非正确性修复。零新增机制：不镜像 API、不加开关、不改 bump 单点；js/check 沿用 checkjs-miss-respreadutils 先例与 storage.yml 既有条目组。不合票（与 checkjs-miss-post-r7 各为「功能裁决+源码清退」与「登记批」，仅 b 面按收窄三让路）。

四、执行方案（fix 直接消费，含票面原案 a-d 步）
1. bits.rs:22/:24-25/:44-45 位段并除（掩码段 0x7FF→0xFFF 一类，按现码符号定位）；header.rs:171-189/:607 删 is_modified/set_modified/is_in_place_updated 面与 Display 分量。
2. record_mut.rs:14/:221-242/:351-355 化简：CAS 臂保留、合字掩码去 MODIFIED、:241 else 臂独立 fetch_or 化零、:38-41/:202/:221/:258 注释同步。
3. whlog/src/hlog/inplace.rs:80/:88/:113/:128/:146/:181 删写点与相关注释论证。
4. 测试承接：wrecord/tests/record/header_and_bits.rs（:18/:93/:100-180/:376-383/:411-420）与 lifecycle_and_chains.rs（:114/:535）删 modified 常量与断言——混排用例（:148/:411-420）须去 mod 分量而非删整案，防弱化；whlog/tests/hlog/inplace_lifecycle.rs:619/:642/:669 删探针后，:652/:657 值复读与 :660-672 帧字节差分＋单调位计数断言保留即承判（该断言系改动集上界式，清退后仍过），票面「勿弱化为无断言」照办。
5. 文档面按收窄二全清单同步；js/check 按收窄一收口。
6. 回归夹具：cargo test -p wrecord -p whlog -p wkv（归主控跑）、delete_miss_watch、tests/compact 墓碑计数族不回退。
7. 未尽面（禁顺手扩）：check.js 对 property 不入名录的判定依 garnetScan.js:81，SetModified 归属须主树实测；wnode AOF 回放 bump 点与 wkv 写面「重放不双推进」互斥契约是否完整覆盖原位臂属既有承判域，本票零触。


问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# RecordInfo.Modified（bit 59，RecordInfo.cs:36/:54）在 garnet 是有承责读方的活标记，射程如下：
- 写方（原位/复制发布成功后置位）：SessionFunctionsWrapper.cs:60/:67/:75/:84/:93/:103/:143/:163/:176/:233
  （PostInitialWriter/InPlaceWriter/PostInitialUpdater/InPlaceUpdater 等包装族 `logRecord.InfoRef.SetModified()`）、
  InternalRMW.cs:171/:431/:530/:568、InternalDelete.cs:129。
- 读方（决策）：
  a) WATCH 版本去重门：每个 InPlace* 回调成功后 `if (!logRecord.Info.Modified)
     watchVersionMap.IncrementVersion(keyHash)`——复位窗口内首次原位写推进一次版本，
     连续原位写不重复推进（MainStore/RMWMethods.cs:426、MainStore/DeleteMethods.cs:31、
     ObjectStore/RMWMethods.cs:99/:124、ObjectStore/DeleteMethods.cs:20/:29、
     UnifiedStore/RMWMethods.cs:165、UnifiedStore/DeleteMethods.cs:23/:35、
     SessionFunctionsUtils.cs:119/:139/:159）。
  b) WATCH 复位：TransactionManager.cs:402-412 Watch(key) 调 ResetModified →
     ClientSession.cs:362-367 → InternalModifiedBitOperation(reset=true)
     （ModifiedBitOperation.cs:21）→ TryResetModifiedAtomic（RecordInfo.cs:160-176，
     CAS 自旋 + IsClosedWord 早退 + kMaxLockSpins 预算），复位失败即
     OperationStatus.RETRY_LATER（ModifiedBitOperation.cs:52-53）。复位令 WATCH 之后
     的首次原位写必推进版本（否则位残留置脏会漏 abort——此为该位的核心不变量）。
  c) IsModified 查询 API（UnsafeIsModified，ClientSession.cs:386-394；记录不在内存时
     按「地址 ≥ BeginAddress 即视脏」保守回脏，ModifiedBitOperation.cs:61）——
     全仓生产消费面零，仅 Tsavorite 自测 test.recordops/ModifiedBitTests.cs 使用。
- 不参与的职责（逐面核过）：检查点回写与紧缩/复制跳过不读该位（Allocator/Revivification/
  紧缩族全文零 Modified 引用）；并发读者可见性与快照冻结由 Sealed/InNewVersion 承载，
  与 Modified 无涉。Redis 官方标准侧：WATCH/MULTI 只要求「WATCH 后任何变更必致 EXEC
  abort」，「去重」是 C# 自有记账优化，非外部语义义务。

2. 工程现状确证（Rust 现有实现路径与代码现状）
- 位写方（全仓唯一）：显式四处 whlog/src/hlog/inplace.rs:88（置墓碑）/:113（GETDEL 读删）
  /:146（原位 RMW）/:181（恢复期存根自愈）的 rec_mut.set_modified(true)；
  隐式两处 wrecord/src/record_mut.rs:241 else 臂 info_fetch_or(MODIFIED_BIT)
  （update_value_with_slack，覆盖 try_update_in_place/try_grow_record_in_place）与
  :227/:239 revivify 合字 CAS 臂（try_revivify_in_chain 经此落位）——即全仓每一次
  原位发布都对该位做一次 AcqRel 原子 RMW（record_mut.rs:121）。
  访问器：header.rs:173/:181、record_mut.rs:353-354、别名 is_in_place_updated
  （header.rs:187-188）。
- 位读方：生产零。is_modified 仅三处消费——header.rs:188 别名（其自身亦零生产消费）、
  header.rs:607 Display 调试串（观测面，非决策）、wrecord/whlog 自测
  （wrecord/tests/record/header_and_bits.rs:100-180/:376-383/:411-420、
  lifecycle_and_chains.rs:114/:535、whlog/tests/hlog/inplace_lifecycle.rs:619-669）。
  旁路写法逐形态扫净：全仓 `1u64 << 59` 仅 bits.rs:25 定义与 wrecord 自测常量，
  无 `>> 59` 裸移读，无第二处 `& MODIFIED_BIT` 判定（record_mut.rs:227/:239 为 OR 掩码
  写方）；wtxn/wcpr/wreviv/wedb 服务层零 modified 引用；wtxn WATCH 面
  （transaction_manager.rs/txn_watched_keys_container.rs）无任何位复位。
- 同一不变量的承接机制（在册、有钉死测试）：rust 以写面单点无条件推进取代 C# 的门控推进——
  wkv/src/session/mod.rs:800 bump_watch_version（WatchHook 装配，对标 C#
  Post RMW 回调族无条件推进面自陈 :784-790），所有成功写臂各恰一触点：
  upsert 同步臂 write/mod.rs:228/:290/:348、删/取删臂 mod.rs:450/:466/:594/:655、
  rmw 臂 write/rmw.rs:143/:186/:238、集合与 TTL 面 collection.rs:107/:146/:159、
  range_index ops.rs:255/:347/:491/:495 与 migration.rs:416。判据口径为无条件
  （wkv/tests/delete_miss_watch.rs 头注自陈并钉死），比 C# 更强：每次成功写必推进，
  「WATCH 后变更必被检出」不变量无需位复位即成立；C# 的去重仅是省版本表记账，
  无外部可见语义。IsModified/ResetModified API 无 Redis 命令对位（Redis 无此命令），
  按「Redis 标准优先于 C# 上游实现细节」与本仓 r7 裁撤不镜像非协议面的既有口径无需转写。
- 定性结论：同一不变量已由他机制承接（b 排除），而该位本身在 rust 无任何生产读方
  （非 c：无既有 done/reject 票或 doc/zh/deviations.md 条目在册裁决——task 五池 grep
  仅 checkjs-miss-post-r7 票 b 面的锚簿记条目，且该票第 2 条明文把「真伪复核」留给
  另立功能票，即本票；deviations §115/§131/§48 均涉 WATCH 版本轨/锁轨，不涉本位）——
  故为 a 类死标记：六处位写点是热路径纯死工（每次原位发布多发一次 AcqRel 原子 RMW
  总线同步，update/grow/resident 路径的 :241 独立 fetch_or 为最净损失面），
  触「零死代码与假桩清退」红线。

3. 逻辑危害确证（实际危害）
非正确性缺陷（不变量有承接、无可达误判窗），不立 P1/P2。危害限于两面：
a) 热路径死写——原位发布全部多付一次 RecordInfo 共享缓存行原子 RMW，且 whlog
   inplace_lifecycle.rs:669 断言（「原位发布须置脏标记，否则页不会落盘」）名不副实：
   本仓页落盘经缓冲池页写锁与 AOF/水位承接，flush 路径不读该位（whlog flush/snapshot
   面零 Modified 引用），该测试只是自证性钉桩，反向固化死位；
b) 对账基座悬案——check.js `# 实现缺失` 的 RecordInfo.cs:TryResetModifiedAtomic
   与 `# 仅词元提及` 同源条目，其收口方向（改挂存活位 vs ignore）取决于本位存废裁决，
   不定案则锚面永挂。

涉及代码：
rust 文件与函数：
wedb/wrecord/src/header/bits.rs:25 MODIFIED_BIT、:44-45 RECORD_INFO_FLAG_MASK、
  :72-73 位域封闭断言
wedb/wrecord/src/header.rs:14（import）、:173-182 is_modified/set_modified、
  :187-188 is_in_place_updated、:607 Display mod= 字段
wedb/wrecord/src/record_mut.rs:14（import）、:38-41 头文档、:225-245
  update_value_with_slack 位写臂（:227/:239 revivify 合字、:241 else 臂 fetch_or）、
  :353-355 set_modified
wedb/whlog/src/hlog/inplace.rs:88/:113/:146/:181 四处 set_modified(true) 及 :80/:128 注释
自测：wedb/wrecord/tests/record/header_and_bits.rs（:18/:82/:93/:100-119/:138-152/
  :167-180/:349/:376-383/:411-420）、wedb/wrecord/tests/record/lifecycle_and_chains.rs:114/:535、
  wedb/whlog/tests/hlog/inplace_lifecycle.rs:619-669
文档面：wedb/README.md:357/:764、wedb/readme/zh.md:330、wedb/readme/en.md:330
对账面：js/check/miss/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.yml、
  js/check/ignore/storage.yml:1339（RecordInfo.cs 既有条目组）
承接面（不改，仅作判据引用）：wedb/wkv/src/session/mod.rs:784-806 bump_watch_version、
  wedb/wkv/src/session/raw/write/mod.rs、write/rmw.rs、write/inplace.rs、
  wedb/wkv/tests/delete_miss_watch.rs、wedb/wtxn/src/watch_version_map.rs

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:Modified(:215-223)、
  SetModified(:234)、TryResetModifiedAtomic(:160-176)
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ModifiedBitOperation.cs:21-65
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:171/:431/:530/:568、
  InternalDelete.cs:129
garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/SessionFunctionsWrapper.cs:60/:67/:75/:84/:93/:103/:143/:163/:176/:233
garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs:362-394
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:426、MainStore/DeleteMethods.cs:31、
  ObjectStore/RMWMethods.cs:99/:124、ObjectStore/DeleteMethods.cs:20/:29、
  UnifiedStore/RMWMethods.cs:165、UnifiedStore/DeleteMethods.cs:23/:35、
  SessionFunctionsUtils.cs:119/:139/:159
garnet/libs/server/Transaction/TransactionManager.cs:402-412（Watch→ResetModified）

精炼执行方案：
1. 位清退（纯删除，不新增任何机制——单机制纪律：WATCH 推进面维持
   bump_watch_version 单点不变）：
   a) bits.rs：删 MODIFIED_BIT，bit 59 并入保留段（RECORD_INFO_RESERVED_MASK
      `0x7FFu64 << ADDRESS_BITS` → `0xFFFu64 << ADDRESS_BITS`），
      RECORD_INFO_FLAG_MASK 同步去位，:73 封闭断言原式即过；
   b) header.rs：删 is_modified/set_modified/is_in_place_updated 与 Display 的 mod=
      字段，收 :14 import；
   c) record_mut.rs：删 :353 set_modified；update_value_with_slack 删 :241 else 臂
      （该臂化零，独立 AcqRel fetch_or 消失）；revivify 合字臂简化为
      `word & !(TOMBSTONE_BIT | SEALED_BIT)`（CAS 仍保留，墓碑/密封清除语义不动）；
      收 :14 import 与 :38-41 头文档；
   d) whlog/src/hlog/inplace.rs：删四处 set_modified(true) 及 :80/:128 注释中的
      MODIFIED 字样；
   e) 自测：wrecord header_and_bits.rs/lifecycle_and_chains.rs 删 modified 常量与断言
      面；whlog inplace_lifecycle.rs:619-669 删 is_modified 探针（原位发布与墓碑计数
      断言改按值复读与既有墓碑探针承接，勿弱化为无断言）；
   f) 文档面：README.md:357/:764 与 readme/zh.md、readme/en.md:330 常量清单去
      MODIFIED_BIT。
2. 锚收口（落 checkjs-miss-post-r7 票 b 面的功能裁决）：死标记清退后「改挂存活公开位」
   对象灭失，TryResetModifiedAtomic 自 js/check/miss/.../RecordInfo.yml 转
   js/check/ignore/storage.yml:1339 RecordInfo.cs 组，理由句写明：C# Modified 位为
   WATCH 版本推进去重门（MainStore/RMWMethods.cs:426 族）+ Watch 复位窗
   （TransactionManager.cs:408），本仓同一不变量由 wkv 写面无条件 bump_watch_version
   （wkv/src/session/mod.rs:800，wkv/tests/delete_miss_watch.rs 钉）承接，位本体无
   生产读方，随 r 波死标记清退不镜像；IsModified/ResetModified API 无 Redis 命令对位。
3. 纪律：本票触 wrecord/whlog 源码与 js/check 登记面，与在途他席脏文件
   （cluster_session/**、windex/src/entry_info.rs、wtls/src/stream.rs、wedb/Cargo.toml、
   wconn/src/network/stream.rs）零交叠；不改任何原位发布协议、RDH 单字发布与页锁序。

测试验证点：
- cargo test -p wrecord -p whlog -p wkv 全绿；wkv delete_miss_watch、
  whlog tests/compact 墓碑计数族、whlog inplace_lifecycle（去探针后）不得回退；
- 主树跑 bun js/check.js：`# 实现缺失` 与 `# 仅词元提及` 中 RecordInfo.cs:
  TryResetModifiedAtomic 归零，js/check/miss/.../RecordInfo.yml 产物消失，
  `# 重复定义` 段无新增；
- 全仓 grep MODIFIED_BIT / is_modified / is_in_place_updated / set_modified /
  `1u64 << 59` 零残留（含 doc/readme 四面）；
- 快照兼容复核：恢复面（wcpr/cpr_host）对 bit 59 零依赖已扫净，退役后页镜像该位恒零，
  本仓无跨格式读方，无需数据迁移动作。

## 终态注记（开发席，2026-09-28 落地完毕）

形态：择 d 治理性死标记清退，按审核结论四条收窄执行，零新增机制、零触 bump_watch_version 单点及其调用点。两枚主体 commit＋本注记票面一枚：111cec3（源码+自测清退，9 文件）、776d5fd（文档面+js/check 登记，7 文件）、本票面文件随注记提交（第三枚 chore，含本注记自身，其哈希即所在 commit）。以下行号除注明外均为领票基线（HEAD=717fa53）pre-edit 行号。

### 一、源码清退对位（111cec3）
- wrecord/src/header/bits.rs:24-25 删 `MODIFIED_BIT: u64 = 1u64 << 59` 及其文档；:22 `RECORD_INFO_RESERVED_MASK` 前值 `0x7FFu64 << ADDRESS_BITS`（bits 48..58，11 位）→ 后值 `0xFFFu64 << ADDRESS_BITS`（bits 48..59，12 位）；:44-45 `RECORD_INFO_FLAG_MASK` 去 MODIFIED 项（bits 59..63 五标志 → bits 60..63 四标志；即 0xF800_0000_0000_0000 → 0xF000_0000_0000_0000）；:70-73 编译期封闭性断言原式不改（48+12+4 恰铺满 u64::MAX，cargo check 实测编译期即证）；:75-77 RDH 断言零触。
- wrecord/src/header.rs:14 import 收；:171-189 删 is_modified/set_modified/is_in_place_updated 三访问器（:179 即 SetModified 全仓唯一规范锚，随删灭失→转 ignore）；:601/:607 Display 去 `mod={}` 字段与实参；:30/:89 头文档位段叙述按收窄二改 48..59 保留/60..63 四标志。
- wrecord/src/record_mut.rs:14 import 收；:41 删 `[Self::set_modified]` intra-doc 链接（:37-38 C# 锚名词 TryResetModifiedAtomic 族叙述保留）；:224-242 核心化简：revivify 合字臂 :227/:239 掩码 `(word | MODIFIED_BIT) & !(TOMBSTONE|SEALED)` → `word & !(TOMBSTONE|SEALED)`（CAS 自旋保留、墓碑/密封清除语义不动，只化简掩码）；:240-241 else 臂 `info_fetch_or(MODIFIED_BIT)` 整臂删除（update/grow 臂原位发布不再触 RecordInfo 字，独立 AcqRel 总线同步消失）；:351-355 删 set_modified；:201-202/:221-223/:258/:288 注释同步。info_fetch_or/fetch_and/info_set_bit 经 set_tombstone 存活，零死码告警。
- whlog/src/hlog/inplace.rs:80 注释去「置位 MODIFIED」；:88/:113/:146/:181 四处 `set_modified(true)` 写点删除（:146/:181 的 `if r.is_some(){…}` 壳随之化零，r 直通返回）；:128 零副作用硬契约措辞改「本内核不发布任何新状态」。
- wkv/src/session/raw/modify.rs:66-69：「回 None 会跳过 MODIFIED 位落笔并诱导调用方 RCU 追加」按收窄二改写为按「已提交态不撤回（新值字节已落笔且对无锁读者可见）+ 诱导 RCU 追加产生第二份效果」独立成立，非简单删句。wkv/src/session/raw/write/inplace.rs:925 「SetTombstone + SetModified 原位落笔」→「SetTombstone 原位落笔」。
- 事实锚订正入落地注释（bits.rs 保留段新文档块）：C# Modified 实为 bit 51——一手亲读 garnet RecordInfo.cs:33-37 偏移链（kIsReadCacheBitOffset=kAddressBits−1=47、Tombstone=48、Valid=49、InNewVersion=50、Modified=51、Sealed=52）+ garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs:14 `kAddressBits = 48`（票面原文路径省目录，实读补全）；与 rust 原 bit 59 非同拓扑（rust 无 Valid 位，header.rs:31 在册自陈），落地注释与 commit message 均禁「同 C#」断言；bits.rs:17-18 既有虚假同拓扑陈述按收窄四非本票义务，未触。措辞降档已照办：commit message 与注释均写「四处显式 set_modified + else 臂独立 fetch_or 为死工；revivify 合字臂系搭 CAS 顺风车掩码、清退只化简掩码」，未沿用「六处纯死工」。

### 二、测试臂增删对位（零弱化；去分量项均注明）
- wrecord/tests/record/header_and_bits.rs：:18 镜像常量删；:82 文档行删；:91-93 正交性 all_flags 五分→四分（去分量，:94-95 ADDRESS_MASK 与 0x07FF_FFFF_FFFF_FFFF 双断言保留）；:100/:106-108/:118-119 lifecycle 用例 modified 分量删（整案保留，seal/tombstone/rc/in_new_version 断言链完整）；:138/:143/:152 分量删；:148 混排字面量 `addr | MODIFIED_BIT | SEALED_BIT` → `addr | SEALED_BIT`（去分量非删案）；:164-183 纪元正交案 :167/:174/:180 分量删（整案保留）；:376-383 InPlaceUpdated 别名节整节删（该节为题位专测，属删臂；所在大案其余节保留，节号顺延）；:411 注释去 set_modified；:419-420 两行删、:421-422 sealed/valid 断言保留（去分量）；:349-350 既往裁决史注（现 :331-332）保留未动。
- wrecord/tests/record/lifecycle_and_chains.rs：:114/:535 modified 分量删（所在等长更新案与终态对拍案其余断言全保留，两臂整帧相等 `grow_buf == full_buf` 判据不动）；:464/:503/:598/:626 叙述性大写位引用随面清扫。票面未列此四处注释，依收窄二「大写位引用随面清扫」口径纳入，非扩面。
- whlog/tests/hlog/inplace_lifecycle.rs：:581-582 测试 23 文档头同步；:619-623 is_modified 探针闭包删；:642/:669 两枚探针断言删（:669「否则页不会落盘」系名不副实自证钉桩，随位清退）；:657-660 值复读（同槽位读回新全值）、:676-681 旧值前缀/新增段、:684-686 邻槽帧与尾部零推进全保留；:660-668 头字节窗口扫描上界式保留并**收紧**（删 `|| i == RDH_WORD_OFFSET - 1` 字节 7 豁免）；:670-675 行号漂移与票面误差订正——票面称「单调位计数断言系上界式清退后仍过」：一手实测 :670 `before.1 | after.1 == after.1`（只增不减，确为上界式，清退后过），但 :671-675 `assert_eq!((before.1 ^ after.1).count_ones(), 1)` 为恒翻一枚位的精确式（清退后翻位数归 0，原样保留必红），故改钉为更强精确断言 `assert_eq!(before.1, after.1)`（RecordInfo 字一字不动），并删 :654 注释「+ 脏标记位」分量。对照臂 `touched * 4 <= reemit` 上界不等式清退后仍过（实测改动集缩一枚字节，判据更宽）。无 #[allow]、无假桩。

### 三、文档面完成度（776d5fd；收窄二全清单 10/10，零漏项）
wedb/README.md:357/:764、wedb/readme/zh.md:330、en.md:330（常量清单去 MODIFIED_BIT）；wrecord/README.md:37/:103、wrecord/readme/zh.md:23、en.md:23（布局叙述 48+11+5→48+12+4，bit59 并入保留段注记）；wrecord/src/header.rs:30/:89、wkv raw/modify.rs:67、wkv write/inplace.rs:925（首 commit 内）。保留面零误删：record_mut.rs:398（现 :389）、record_ref.rs:97、wkv ttl_sync.rs:164、wnode/tests/watch_version_regression.rs:182 四处 C# 对位锚名词叙述原样在册（一手 grep 复核）。
全仓门禁 token 复扫：`MODIFIED_BIT / is_modified / set_modified / is_in_place_updated / 1u64 << 59` 于 wedb+js 零残留；`>> 59`、`<< 59`、0x0800_0000_0000_0000 裸形态零命中。

### 四、js/check 双条目登记原文（776d5fd，storage.yml RecordInfo.cs 既有条目组，现位 :1347-1348/:1360-1368）
名录新增两行（插在 SetInvalid 与 UnsealAndValidate 之间、UnsealAndValidate 与 WriteInfo 之间，随组内字母序）：`- SetModified`、`- TryResetModifiedAtomic`。理由句追加原文：
「SetModified/TryResetModifiedAtomic 属 C# Modified 位承责面（WATCH 版本推进去重门 MainStore/RMWMethods.cs:426 族 + Watch 复位窗 TransactionManager.cs:408→TryResetModifiedAtomic CAS 复位）；本仓同一不变量由 wkv 写面无条件 bump_watch_version（wkv/src/session/mod.rs:800，wkv/tests/delete_miss_watch.rs 钉死）单源承接，EXEC 判据为相等比较（wtxn txn_watched_keys_container.rs:77/:94），位本体全仓零生产读方，随 wrecord-modified-bit-dead-marker-retire 死标记清退（rust 原 bit 59 并入保留段）不镜像；C# Modified 实为 bit 51（RecordInfo.cs:33-37 偏移链 + LogAddress.cs:14 kAddressBits=48），与 rust 位段布局非同一拓扑；IsModified/ResetModified API 无 Redis 命令对位」。
双枚依据一手核验：garnetScan.js:81 实测只采 `method_declaration`/`local_function_statement`，C# 属性 `Modified`（RecordInfo.cs:215）不入册、方法 `SetModified`（:234）与 `TryResetModifiedAtomic`（:160）入册；SetModified 全仓唯一规范锚 header.rs:179 已随访问器灭失，不入 ignore 必新挂缺失。验证点交主树实测：check.js 缺失段与仅词元提及段应无任何 RecordInfo.cs 条目（含本二枚）、`# 重复定义` 无新增、js/check/miss/.../RecordInfo.yml 产物消失（该产物未入库，本票运行前目录本就不存在，一手 ls 核验）。b 面单一序列按收窄三：本票先落，checkjs-miss-post-r7 之 RecordInfo.cs 预断作废。

### 五、未尽面
1. wkv/tests/write_kernel_failpath.rs:255 注释「（MODIFIED 位正常置位）」过时化——wkv 唯二可触文件之外（禁触线口径），留主控；不在门禁 grep token 列，不回红。
2. SetModified 之属性/方法入册判定虽已一手核 garnetScan.js:81，最终归属仍以主树 bun js/check.js 实测裁决（票面第七步口径）。
3. bits.rs:17-18 既有「同拓扑」陈述（收窄四明示非本票义务）与 wnode AOF 回放 bump 互斥契约（既有承判域）均零触。

### 六、自查风险
- 磁盘/快照兼容（自验非破坏性）：wcpr/wreviv/whlog-src/wedb-src 四目录清退后 `modified|MODIFIED` 一手 grep 零命中；恢复读头仅经 ADDRESS_MASK 与具名位访问器，RESERVED/FLAG_MASK 掩码唯消费在 bits.rs 编译期断言（零运行时校验），故旧磁盘镜像 bit59=1 残留无任何判定面、亦无需迁移动作；flush/hlog-io/hlog-shift 落盘为页字节原样写出，零本位引用（复核票面断言属实）。
- 观测面：RecordHeader Display 调试串去 mod= 字段，纯观测，无决策消费。
- 承判迁移确认：清退后 whlog inplace 原位臂写面缩为「RDH 单字发布（+复活臂 CAS）」，原位值可见性/落盘由页写锁与缓冲池承接（本位从不参与），帧差分精确断言（before.1==after.1）已把「原位发布零触 RecordInfo 字」钉成新判据。
- 编译自查：cargo check --offline -p wrecord -p whlog -p wkv（含 --tests）零告警零错误；cargo fmt 只读 --check 三面干净（一处 import 重排已随之修正）。门禁全量（test/clippy/check.js/delete_miss_watch/tests:compact 墓碑计数族）归主控跑。

分支：wrecord-modified-bit-retire；commit：111cec3（源码+自测清退）、776d5fd（文档+js/check 登记）、本票面注记随第三枚 chore commit 提交（其哈希即该 commit 自身）。

---

## 主控验票注记（2026-09-28 r436 收票）

### 1 并回与净面
- merge `f6ee433`，三 commit（`111cec3` 源码+自测清退、`776d5fd` 文档+js/check 登记、`b80479b` 本票面注记），
  对 first-parent 净面 17 文件 **+111/−139（净减码 28 行）**，全部落在 wrecord/whlog/wkv 三域 + 文档/票面/js 登记，
  禁触线（wnode、wedb、wtxn 等）零越。

### 2 位段与写面复核（独立读码）
- `bits.rs`：`RECORD_INFO_RESERVED_MASK` 由 `0x7FF<<48` 改 `0xFFF<<48`（bit 59 并入保留段，共 12 位），
  `RECORD_INFO_FLAG_MASK` 去位成四标志；封闭断言「48 位地址 + 12 位保留 + 4 标志 == `u64::MAX`」算术亲验成立，位段无缺口。
- `record_mut.rs`：`publish_val_layout` 的 else 臂（原 `info_fetch_or(MODIFIED_BIT)`）整臂删除而非空壳保留；
  `revivify` CAS 合字改 `word & !(TOMBSTONE_BIT | SEALED_BIT)`，复核确无 modified 残留位；`RecordMut::set_modified` 删后全仓零调用点。
- 生产读方归零独立复扫（本席一手 grep）：`MODIFIED_BIT` / `is_modified` / `set_modified` / `is_in_place_updated` /
  `1u64 << 59` / `0x0800_0000_0000_0000` / `>> 59` 于 `wedb/` + `js/` **零命中**，与席报一致
  ⇒ 「只写零读死标记」定性坐实。
- 承责迁移确认：WATCH 版本推进不变量由 `wkv` 写面无条件 `bump_watch_version` 单源承接、EXEC 侧为相等比较，
  清退位本体不削该契约（`wkv/tests/delete_miss_watch.rs` 在册钉死）。

### 3 磁盘/快照兼容性判定：采纳
- `RESERVED`/`FLAG_MASK` 唯消费在编译期断言（零运行时校验），恢复读头仅经 `ADDRESS_MASK` 与具名位访问器
  ⇒ 旧镜像 bit59=1 残留无任何判定面，无需迁移动作；落盘为页字节原样写出，零本位引用。既不制造伪破坏，也不夹带兼容垫片。

### 4 测试面：收紧非弱化，采纳并订正票面
- **主控票面之误，席一手纠正并记录**：票面原称「`inplace_lifecycle.rs` 位计数断言系上界式，清退后仍过」不成立——
  `:671-675` 实为 `(before.1 ^ after.1).count_ones() == 1` 的「恒翻一枚位」精确式，清退后翻位归 0，原样保留必红。
  席改钉更强精确断言 `assert_eq!(before.1, after.1)`（原位发布一字不动）并删 `RDH_WORD_OFFSET - 1` 字节豁免，判据收紧。
- `header_and_bits.rs` / `lifecycle_and_chains.rs` 去分量项逐处核验：整案与其余断言链（seal/tombstone/rc/纪元正交、
  整帧相等对拍）全保留，无整臂偷删、无断言降档；`InPlaceUpdated` 别名节属题位专测整节删，随位清退正当。
- 全臂零 `#[allow]`/`#[expect]`、零假桩。

### 5 js/check 登记面：形制合规
- `js/check/ignore/storage.yml` RecordInfo.cs 既有条目组内**逐名两行**追加 `SetModified`、`TryResetModifiedAtomic`
  （不采双名合写锚——check.js 不识别），理由含「`bump_watch_version` 单源承接 + C# Modified 实为 bit 51、位段非同一拓扑」双据。
- 属性不入册的判定经一手核 `js/garnetScan.js:81` 只采 `method_declaration`/`local_function_statement`
  ⇒ C# `Modified` 属性（`RecordInfo.cs:215`）无册位，`SetModified`（:234）/`TryResetModifiedAtomic`（:160）有，登记枚数正确。

### 6 未尽面处置（主控亲办）
1. `wkv/tests/write_kernel_failpath.rs` 注释「（MODIFIED 位正常置位）」过时分量删除 → commit `f133e8e`。
2. check.js 实测已毕：EXIT=0，本票双条目（SetModified / TryResetModifiedAtomic）在册未复现，
   实现缺失名录无新增（详见 task/done/r437-r438-gate-record-20260928.md 第三节）。
3. `bits.rs` 既有「与 C# 同名位段同拓扑」陈述与本票席新注（bit 51 非同拓扑）自相矛盾 → 主控订正 commit `5eca964`，
   改为「仅职责对位、位号与位宽一律不同拓扑、禁作『位号同 C#』断言」。

### 7 教训入册
- 票面内任何「测试判据清退后仍过」类断言必须由施工方一手读码复核，主控不得以推测代验；本票为该形制第二次触发（前次见 boot 首席伪锚）。

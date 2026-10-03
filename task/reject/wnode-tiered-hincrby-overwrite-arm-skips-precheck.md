审核结论：驳回

拒绝理由：核心可达性判据不成立。「存量值经增量格式化后变长，编码记录长可合法越过存根契约被树内校验拒绝触达 tree_put_rejected」经亲码算术复核为假：

1. 分层集合存根契约是常量非变量：建树调参恒 TreeTuning::DEFAULT_RI_COLLECTION（wbftree/src/types.rs:99-102，min_record_size=2、max_record_size=1024、max_key_len=128；唯一集合建树点 promote.rs:117-124/254 同源；resolve_tuning 仅推导 leaf_page_size 不动长度契约，wbftree/src/manager/lifecycle.rs:79-83；迁移/副本侧 rebind 复用同调参）。且升阶契约闸（wkv/src/range_index/promote.rs:139-147）整批拒含超长字段的集合，含 >128B 字段的哈希根本进不了分层态 ⇒ 覆写臂 field ≤ 128B 先天成立。
2. HINCRBY 上界：存量值必须过 parse_hash_incr Stock 档解析为 i64（hash.rs:396/428-438），贴契约上限的 900+B 值在此即回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER 不写；可增量值格式化恒 ≤ 20B（"-9223372036854775808"，前导零增量不进格式化），记录 ≤ 1B 旗标 + 8B ticks + 20B = 29B，key.len()+record_len ≤ 157 < 1024。i64 进位变长（19 位 → 20 位、MAX 回绕加负号，恰 +1B）真实存在，但距契约顶 867B，一字节进位填不平。
3. HINCRBYFLOAT 上界：format_double 走 zmij 最短往返并剥 ".0" 后缀（wresp/src/resp_memory_writer.rs:12-39，"inf"/"-inf" 三字节特形），最劣全展开 ≤ 约 330B，记录 ≤ 339B，key.len()+record_len ≤ 约 467 < 1024；超长浮点字面量（如 800 位全数字形）解析后经最短往返反而收缩，越界不可达。

故存量覆写臂 tree_put_rejected 实际仅剩树句柄/引擎配置偏离态可达（wbftree/src/service/bulk.rs:100 with_tree None → InvalidArguments），common.rs:346-348「仅树引擎配置与存根契约偏离可达」文注属实，票面「对该路径失真」判词不成立；「合法 HINCRBY 恒回 Internal 误导帧」与「信封/分层双态分叉」两危害面均不可物化，错误归因定级失去承载。姿态不对称本身属实（zset.rs:396-400 Zincrby 新存量单流统一预检 vs hash.rs:461-469/547-553 无预检）但无后果：zset 分值记录定长 8+1(+8)B，其预检在现契约下同样恒过，存量臂补预检系死代码级防御位，不构成缺陷立项。

§178 划界结论：§178（doc/zh/deviations.md:458-461）判据面为「分层态键侧空/超长成员 InvalidKV 与内存态受理:N 的双态分叉，边界线=引擎受理面」，载荷侧空值明文不入分叉。本票系值侧覆写增长臂，字面上不在 §178 判据面内（既定形态甄别不构成驳回主因）；但本路径引擎受理面恒不被触碰（树内长度拒写不可达），无双态分叉可登记，票面危害前提整体落空。

代码锚点复核（锚位全部属实，缺陷定性与可达性不属实）：hash.rs:443-445 新字段臂预检、461-469 存量臂无预检直落 tree_put_ok/tree_put_rejected、523-531/547-553 Hincrbyfloat 同形；zset.rs:396-405；common.rs:311-323/330-344/346-355；wkv/src/range_index/ops.rs:844-858 validate_bftree_record 拒判面（key.len()+record_len > max_record_size 即拒）；bf-tree 0.5.6 tree.rs:1108-1147 引擎 InvalidKV 四门（键超长/空键/Record too large/Record too small）；C# garnet/libs/server/Objects/Hash/HashObjectImpl.cs:285-349 HashIncrement 存量覆写原位拷贝或数组重分配加 HeapMemorySize 增量记账恒成功、HashIncrementFloat 同面，票面对 C# 行为的描述无误。

HINCRBY/HINCRBYFLOAT 存量覆写臂跳过 tiered_precheck，与 ZINCRBY 预检姿态不对称

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# HashObjectImpl.cs:HashIncrement（:285-350）与 HashIncrementFloat 存量成员覆写恒成功（数组重分配加 HeapMemorySize 增量记账），无任何长度失败面；升阶前信封态 rust 亦无此拒。分层态树内写为自研面，契约闸以 tiered_precheck 单点承接（common.rs 文注：预校验先于任何写入，InvalidKV 应答，校验经 validate_bftree_record 单点）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   tiered_collection_ops/hash.rs Hincrby 新字段臂预检（:443-445）后落树，存量覆写臂（:463-469）无 tiered_precheck 直接 tree_put_ok，被拒走 tree_put_rejected；Hincrbyfloat 存量臂（:547-555 附近）同形；zset.rs:tiered_zset_arm Zincrby（:398）新存量单流统一先预检——同域两族姿态分叉。tree_put_rejected（common.rs:350）文注自称仅树引擎配置与存根契约偏离可达，对该路径失真：存量值经增量格式化后变长（i64 进位多一位、double 最短往返变长），编码记录长可合法越过存根契约被树内校验拒绝触达此臂。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   分层大哈希字段值逼近存根记录契约上限时，合法 HINCRBY 确定性回 Internal 分层树写被拒：树配置与存根长度契约偏离 错误帧（重试恒败），错误归因误导；同命令在未升阶信封态恒成功，双态分叉。fail-closed 无数据损坏，可达窗口窄。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:tiered_hash_arm
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:tiered_precheck
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:tree_put_ok
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:tree_put_rejected
wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs:tiered_zset_arm

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat

精炼执行方案：
1. Hincrby 与 Hincrbyfloat 存量覆写臂补 tiered_precheck（以格式化后载荷长度入参），与 Zincrby 单流同构，tree_put_rejected 恢复仅配置偏离可达的文注真实性。
2. 测试验证点：分层态 hash 存量字段值贴存根契约上限时 HINCRBY 进位变长，断言回 InvalidKV 预检错误帧（非 Internal 偏离帧）且树内零副作用。

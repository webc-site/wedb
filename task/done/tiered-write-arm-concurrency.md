分层写臂删除结果校验与同键互斥（承接 next/qcode.db.md 条 31，MED）

现状
- 守卫面：全部分层读写臂经 wedb/wkv/src/range_index/stub.rs:265 acquire_tree_read 取条带共享读锁
  （返回 RwLockReadGuard，见 wedb/wkv/src/range_index/mod.rs:143 TreeReadGuard），无写锁变体；
  tiered_collection_ops.rs:376/:838/:1085/:1508/:1718/:1812/:1879 均走该共享读面，同键并发两臂可同时进入。
- 各臂「tree_member_state 探测 → tree_del/tree_put → 计数」多步序列整体不原子，删除结果被丢弃：
  - HDEL 臂 wedb/wnode/src/resp/objects/tiered_collection_ops.rs:497 `let _ = tree_del(...)`
    丢弃删除结果即 :498 `removed += 1` → meta.size 减至虚低并回写（HLEN 与树内实存永久背离）；
  - member_expire_arm :130 已到期/过去时刻臂 :140/:141、:156/:157 同型丢弃 tree_del 后 dec_size(1)；
    :160 `tree_payload(...).unwrap_or_default()` 在并发删除后以空载荷重插 → 幽灵成员复活、size 不补记；
  - member_ttl_probe :181、member_persist_arm :220 同型；member_persist_arm :215 亦 unwrap_or_default 复活；
  - Zrem 臂 :1283/:1284 同型；
  - collect_expired_members :259 `let _ = tree.delete(k)`、:264 `dec_size(collected)`（被并发 HDEL
    抢先删除的成员仍按 collected 全额扣减）；
  - 两臂各持独立 load 的 meta 副本，save_bftree_meta_stub 整体覆写 → 并发 HDEL+HSET 丢 size/next_expiry 更新。
- 计数失真随元记录回写固化。

C# 参考
- 无分层对位（分层是 wedb 自定义扩展，SKILL.md:27-30）；串行化参照：对象域同键写经 Tsavorite
  记录独占锁串行（garnet/libs/storage/Tsavorite/.../TsavoriteKV.cs RMW InPlaceUpdater 前置记录 X 锁）；
  RI 域参照 garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:30-32（每命令单树操作原子，
  无多步序列窗口）。

优先级
功能缺口（同键并发下 meta.size 永久虚减、HEXPIRE 重写臂复活幽灵成员，计数与实存背离并固化）。

方向
- 第一步（结果驱动计数）：tree_del/tree.delete 返回 Success 才计入 removed/dec_size；payload 缺失时
  重探或放弃重写，禁 unwrap_or_default 复活（unwrap_or_default 复活面与 task/ing/envelope-payload-fail-fast.md
  条 33 同源，共用错误面口径）。
- 第二步（互斥面）：stub 层补 acquire_tree_write（条带写锁 RAII）承载全部多步写臂（读臂维持共享锁），
  或把写臂收敛为「单点探测 + 单操作生效 + 结果驱动计数」的无多步窗口形态；save_bftree_meta_stub
  前经互斥面消除 meta 丢更新。
- 与插入侧同族面合并处置：本票为删除结果丢弃 + 多步序列并发竞态（需同键并发触发），插入结果丢弃
  为静态可达面；落地时统一为「树内写漏斗成对校验 + 写臂互斥面」一张工单（插入侧旧在途档
  tiered-write-count-insert-result.md 当前不在 task/ing，若重建则本票交叉引用之）。
- 验收：双核并发同键 HDEL/HEXPIRE/HSET 混压后 HLEN、RI.COUNT 与树内实存一致，重复执行无漂移。

分拣补记（主仓 HEAD 4b0d462d 复核，next 两份同域单已核销并入本票，不另立单）

- 插入侧（原 next/tiered-write-count-insert-result.md，并入本票第一步）：「先计数、tree_put 结果丢弃」
  在 HEAD 仍全部未门控——HSET/HMSET :421-424（:422 计数、:424 丢结果、:428 入 size）、HSETNX :399-401、
  SADD :847-849、ZADD 新成员 :1171-1173、ZINCRBY :1339，原单漏列的同型两支 HINCRBY :655-657、
  HINCRBYFLOAT :717-718。可达面须纠正：分层记录经 wcol/src/types/member_ttl.rs:37-43 编码恒带
  FLAG_PLAIN 前缀（≥1B），故 wbftree/src/service/bulk.rs:76-78 的空值整批拒绝对这几支不可达，
  原单举的 HSET bigkey field "" 一例不成立；真可达的是引擎超限分支（bf-tree tree.rs:1120-1143
  Key too large / Record too large，集合建树 min_record_size=2 见 wbftree/src/types.rs:102，
  cb_max_record_size 由 manager/lifecycle.rs:78 取 max_record_size+1，单条 insert 亦经
  service/ops.rs:79-84 转 bulk_load）。后果不变：被拒写入仍计 size 并回假新增，且 size 偏大使
  drain_or_save :324 的 size==0 删空自愈永不命中；应答口径取 wkv/src/ri.rs:81、:93 既有
  InvalidKV→CollectionError::KeyTooLong 单点映射，不再 let _ = 吞掉。
- 折叠（原 next/tiered-batch-fold-and-arm-counting.md 甲，与本票改臂同段，合并落地勿各写一半）：
  HSET :418-425、SADD :846-851、SREM :865-870、HDEL :495-500、ZADD :1136-1209 五段仍按命令入参原序
  逐成员下树，未套用已落地的排序批量内核 wbftree/src/service/bulk.rs:53 upsert（内核 :60-87 栈上排序
  + 单次借用 + 同键保末值，先例 wkv/src/ri.rs:79、:91 与 wkv/src/session/mod.rs:721-731）。
  upsert 返回值即真实新增键数（bulk.rs:129 逐条前查只计新增），恰可同时充当上一条「插成功才计数」
  的单点判据，两事一处落；但其 :130 分支在批中途回 Err 即丢弃已得 counted，多成员命令按 C# 逐成员
  语义需「已落成员保留并计入应答」，落地时须补该可观测面，勿造成第二次计数背离。
- 删除面双树下行（原同单乙，仅留指针）：HDEL :496-498、SREM :866-868、ZREM :1283-1285 仍前探 +
  tree_del 两趟下行；去前探需 wbftree 命中态删除原语（同一借用内回报 existed，模板即 bulk.rs:53
  内核），该缺口在册于在途主题 tiered-command-arm-coverage（其 task/ing 档随该分支，主仓无副本），
  本票第二步的互斥面若先落地可顺带收为该原语，勿另立第三套。原单自否的「忽略删除结果致 size 虚减」
  在单线程口径确不成立（BfTreeDeleteResult 无 NotFound 变体，types.rs:150-155；delete 借用成功即恒
  Success，ops.rs:190-197），仅同键并发竞态面归本票本体。
- 原单丙「读臂盲信 meta.size 预写 RESP 帧头」已落地，不再挂账：HGETALL :560-570（实扫 pairs 后
  cs::write_map_len）、HKEYS :582-589、HVALS :602-609、SMEMBERS :905-912 均已帧头与条目同源，
  全文已无一处帧头取自 ctx.meta.size（单趟计数写头随 commit cfa9176d 分层四臂帧型对齐落地）。

## 细化方案（f46 分支，认领时主仓 dev 实况复核）

### 现状核销（票据原文行号有漂移，按认领时 HEAD 重核）
- 插入侧「插成功才计数」已全部落地：tree_put_ok/tiered_precheck/tree_put_rejected
  三漏斗在位，HSET/HMSET/HSETNX/SADD/ZADD/ZINCRBY/HINCRBY/HINCRBYFLOAT 全部已
  门控且应答不再吞结果，本票不改插入应答口径，仅做批量折叠。
- 待修面确认仍在：HDEL/SREM/ZREM 前探+删除两趟且结果丢弃（:571/:977/:1428）、
  member_expire_arm :192/:208 与 member_ttl_probe :233 与 member_persist_arm :272
  删除结果丢弃后 dec_size、:212/:267 unwrap_or_default 空载荷复活、
  collect_expired_members :311 `let _ = tree.delete(k)`、
  全部分层臂仅 acquire_tree_read 共享锁（stub.rs:256）、meta 装载于锁外
  （rmw_helpers.rs:76）后 save_bftree_meta_stub 整体覆写。

### 改动文件与要点
1. wkv/src/range_index/mod.rs：TreeWriteGuard（条带写锁 RAII）+ TreeGuard
   enum（Read/Write 统一 tree() 访问）。
2. wkv/src/range_index/stub.rs：
   - acquire_tree_write：与 acquire_tree_read 同骨架（wait_tree_checkpoint →
     write 锁 → get_tree → flushed 先放锁再 promote → 恢复慢路径先放锁再阻塞
     卸载 → 重试）。C# 对位 Locking.cs ExclusiveRangeIndexLock；语义差异注释：
     C# RI 数据写每命令单树操作故共享锁即可，rust 分层写臂是多步序列，独占
     串行对位 C# 对象域 RMW 记录锁（TsavoriteKV RMW InPlaceUpdater 前置 X 锁）。
   - refresh_tiered_meta：写臂锁内重读 meta_k，live 记录覆盖 ctx.meta，
     消除「装载于锁外」的 save 覆写丢更新；非 live（键已被并发 drain）返回假。
3. wbftree/src/service/bulk.rs：bulk_delete 内核（单次 with_tree 借用 + 栈上
   排序下标 + 同批去重 + 前查存在性 + 删，返回真实删除键数；与 upsert 前查
   同口径，是 O(1) 计数规约的必要成本）。
4. wnode tiered_collection_ops.rs：
   - tiered_guard 单点 helper：按 needs_tree_write(op) 取写/读守卫，写臂锁内
     refresh_tiered_meta，键消亡返回 None 由调用臂穿透（四族 arm Ok(false)、
     exec_tiered_collect Ok(None)）。
   - needs_tree_write 每族一处：hash 写含 collect 校正面（Hlen/Hgetall/Hkeys/
     Hvals 亦写）；set：Sadd/Srem/Spop（Srandmember 读）；zset：Zadd/Zrem/
     Zincrby/Zexpire/Zttl/Zpersist/Zcard；list：四 push + Lpop/Rpop。纯读臂
     （Hget/Hmget/Hexists/Hstrlen/Sismmember/Smismmember/Scard/Smembers/
     Zscore/Zmscore/Llen/Lindex/Lrange）与穿透臂维持共享读锁。
   - 结果驱动计数：member_expire_arm/member_persist_arm 的 tree_payload 改
     读不到即放弃重写（回 KeyNotFound 口径，零计数零变更），禁 unwrap_or_default
     复活；collect_expired_members 删除改经批量删除漏斗（真实删除数入账）。
   - 批量折叠（并入单甲段）：tree_put_batch 漏斗（编码整批 + 单次 upsert +
     置脏，返回真实新增数）承载 HSET/HMSET/SADD；tree_del_batch 漏斗（单次
     bulk_delete + 置脏）承载 HDEL/SREM/SPOP/finalize_pop/collect_expired_members。
   - drain_or_save 签名改 TreeGuard：save 在守卫仍持有时执行（写锁内），
     drain 前显式 drop（既有 drop 顺序不变）。
   - ZADD 不折叠：NX/GT/LT/INCR/CH/NaN 早退的逐成员决策依赖前值读取与中途
     应答，批量 upsert 无法表达 C# 逐成员语义；其计数已由 tree_put_ok 单点
     承接。LPUSH/RPUSH 逐条保持（并入单未点名，序号分配已在臂内单点）。

### 边界（不做）
- 命中态单条删除原语（前探+删两趟下行）：归在途 tiered-command-arm-coverage；
  写锁内前探已准确，两趟是性能非正确性。
- 批内中途 Err 丢 counted 面：分层批量漏斗前置全量 tiered_precheck 封死
  （InvalidKV 批内不可达），Err 兜底回 tree_put_rejected 错误、零计数入账，
  不引入第二次计数背离；内核签名不动（不波及 RI 面）。
- 穿透臂物化 None → Err 的窄窗口（键在执行中消亡）：既有行为，不因本票恶化。
- RI 面命令锁语义不动（C# 数据操作共享锁，每命令单树操作）。

### 验收
- cargo check -p wbftree -p wkv -p wnode（独立 target）零 error 零新增 warning。
- 正确性三重保证：写臂全程条带写锁互斥 + 锁内刷新 meta + 结果驱动计数；
  HLEN/SCARD/ZCARD/LLEN 与树内实存一致，save 覆写不再丢更新。

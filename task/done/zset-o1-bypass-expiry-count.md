ZCARD / zset 内存计数兑现严格 O(1)：与 hlen 票同口径补 zset 半边旁路过期计数

来源：next/glm.my.md 第 13 轮条（自定义优化上下游打通审查）。取证基线：主仓 /Users/z/git/db/wedb dev 工作树，行号为当下实况。
去重：并发分拣把同一原文照搬成六行 stub /Users/z/git/db/wedb/next/zcard-zset-o1-count.md，本档是其细化版，
同题只此一份；认领开发时删该 stub。与 /Users/z/git/db/wedb/task/ing/zset-aggregate-member-ttl-filter.md
（聚合内核不过滤成员级 TTL，来源 glm.data 条 7）是同文件不同面，两票不得互抄计数口径。

## 现状

承诺面：/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:47-48「HLEN / SCARD / ZCARD / LLEN：
直读 wcol 内存对象计数 O(1)」；/Users/z/git/db/wedb/doc/zh/collection.md:72-76 §6 全部计数命令
「严格保证 O(1)」，其中第 3 条明写「字段级失效旁路校正：针对携带字段级 TTL 的集合，维护旁路过期计数抵扣」。

zset 半边未兑现：/Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object.rs:563-570 count()
在 expiration_times 在册时走 `times.keys().filter(|k| self.is_expired(k)).count()`，
即 O(带 TTL 成员数) 逐键过滤；而成员级 TTL 命令面在位
（/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:572 ZEXPIRE 入口，
对标 C# SortedSetExpire），可令 expiration_times 常态化非空。

消费面（同一 O(K) 版）：
- ZCARD 内存操作臂 /Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object_impl.rs:312-320
  sorted_set_length 直调 count()——全成员 ZEXPIRE 的大 zset 每次 ZCARD O(K)；
- 阻塞面 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_commands/blocking.rs:64、:68
  BZPOPMIN/BZPOPMAX 每次 obj.count()（同一次调用还数两遍）；
- 写命令面 write.rs:306、:410、:413、:716、:922 与
  /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/collection_item_source.rs:240。

同命令两态口径分裂：ZCARD 的磁盘/分层两态已是 O(1)——
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:413-476 obj_length_sync
优先直读 MetaValue.size（next_expiry 水位快路径），信封态直读 4B 头部计数
（/Users/z/git/db/wedb/wedb/wcol/src/zset/sorted_set_object.rs:900-906 to_blob 头部写 serialize_wire
过滤后的计数）。同一命令内存态 O(K)、磁盘态 O(1)。

同名两语义并存：trait 实现 /Users/z/git/db/wedb/wedb/wcol/src/types/garnet_object.rs:317-319 对 zset 取
`self.sorted_set_dict.len()` raw len（升阶判定 should_promote/should_demote 的输入，O(1) 不过滤），
与具体类型的固有过滤版 count() 同名不同义（hash 侧同型论述见下票，zset 侧无写热路径 O(K) 问题但更易误用）。

C# 对照：garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:606-618 Count() 同为 O(K) 过滤——
rust 忠实转写，无转写缺口；缺口是本仓自研承诺未兑现。

## 修法

与 next/hlen-o1-bypass-expiry-count.md（hash_object.rs:563 同型面，在册未认领）同批同口径实施，
两域共用一套「旁路过期计数」范式，禁两套存活计数口径：

1. zset 侧维护 expired_pending 抵扣标量：ZEXPIRE/ZPERSIST 与成员 TTL 登记/续期处递增/递减
   （过期时刻落在过去刻度即计入），count() 的在册分支改直读 `len - expired_pending`；
   堆序 purge（delete_expired_items 一类）物理出账后清零抵扣。
   或选 hlen 票方向 1 的第二形态：count() 先走堆序 purge（只弹到期前缀，摊还 O(弹过量)、稳态 peek 短路）
   再直读 len。两形态与 hash 侧必须同择其一，一处收口（可抽 wcol 共用的成员级 TTL 抵扣 helper，
   hash/zset 两对象复用），禁 hash 一套 zset 另一套。
2. 精度与 C# 逐值对照：ZCARD 应答、should_promote/should_demote 输入、BZPOP 的 pop_count 上界三处
   口径一致；trait 的 raw len 版与具体类型的存活计数版若同批无法合并，至少在 garnet_object.rs:317 处
   注释标明「升阶判定用未剔过期 raw len」，并与 hlen 票方向 2 对 hash 的处置同步（同名两语义不得长期并存）。
3. 消费面不改：ZCARD / BZPOP / ZADD / ZMPOP / ZUNIONSTORE / ZRANK 与集合项源随 count() 单点修复自动受益，
   禁在调用点各自加缓存计数（会造第二套口径）。
4. to_blob 头部计数（:900-906）与 MetaValue.size / next_expiry 抵扣（分层态口径见已归档
   tiered-field-ttl-accounting）随本票统一到同一存活计数源，落盘信封头与内存直读不得分歧。

## 边界

不重开 hlen 票的 hash 半边（hash_object.rs 由该票承接，两票若被不同代理认领，以同一 helper 收口为准，
后落地者删自己那份）；SCARD/LLEN 不涉本条（C# SetObject 无 expirationTimes，rust set/list 的 count
取 raw len 即正确）；分层态字段级 TTL 抵扣本身已落地，本票只做口径统一不做新机制。

## 验收判据

- 复杂度探针：全成员 ZEXPIRE 的 10 万成员 zset 连续 ZCARD/ZADD 无 O(K) 时间增长（或计数断言
  count() 内不再遍历 expiration_times）。
- ZCARD、BZPOPMIN pop_count、ZADD 后升阶判定输入三处与惰性过滤参考实现逐值一致（含过期恰在当下边界）。
- 落盘后重启（to_blob 头部）与内存直读同值；分层/内存两态 ZCARD 同值。
- cargo check --workspace --all-targets 绿；中文注释、禁 #[allow]、不新增依赖。

优先级：P2（大键热路径复杂度与文档承诺违约，无数据正确性风险；宜与 hlen 票同代理同批改，一次收口）。

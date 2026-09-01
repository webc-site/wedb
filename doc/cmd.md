# WeDB 命令规格与兼容性对照表

> **生成时间**：2026-08-29T06:05:55.504Z  
> **命令总数**：`384`（标准命令：`245`，扩展命令：`139`）  
> **不支持命令**：`216`（置于文末分类展示）

---

## 目录

### 一、支持与扩展命令
- [1. 字符串与位图](#1-字符串与位图)
- [2. 哈希字典](#2-哈希字典)
- [3. 双向列表](#3-双向列表)
- [4. 无序集合](#4-无序集合)
- [5. 有序集合](#5-有序集合)
- [6. 空间地理位置](#6-空间地理位置)
- [7. 基数统计](#7-基数统计)
- [8. 消息流与消费者组](#8-消息流与消费者组)
- [9. 通用键与生命周期](#9-通用键与生命周期)
- [10. 连接与服务器管理](#10-连接与服务器管理)
- [11. 发布订阅](#11-发布订阅)
- [12. 事务与批量引擎](#12-事务与批量引擎)
- [13. 分布式集群与共识](#13-分布式集群与共识)
- [14. 布隆与布谷鸟过滤器](#14-布隆与布谷鸟过滤器)
- [15. JSON 文档引擎](#15-json文档引擎)
- [16. 全文与向量检索](#16-全文与向量检索)
- [17. 高性能时序引擎](#17-高性能时序引擎)
- [18. 分位数统计草图](#18-分位数统计草图)
- [19. 紧凑有序整型集合](#19-紧凑有序整型集合)
- [20. 多租户命名空间隔离](#20-多租户命名空间隔离)

### 二、不支持的命令
- [末尾.1 服务运维与权限 (72)](#末尾1-服务运维与权限)
- [末尾.2 集群运维与槽位迁移 (32)](#末尾2-集群运维与槽位迁移)
- [末尾.3 哨兵系统 (22)](#末尾3-哨兵系统)
- [末尾.4 连接追踪与控制 (19)](#末尾4-连接追踪与控制)
- [末尾.5 脚本与函数库 (19)](#末尾5-脚本与函数库)
- [末尾.6 数组扩展 (18)](#末尾6-数组扩展)
- [末尾.7 流消息运维 (14)](#末尾7-流消息运维)
- [末尾.8 通用键运维 (7)](#末尾8-通用键运维)
- [末尾.9 发布订阅控制 (7)](#末尾9-发布订阅控制)
- [末尾.10 哈希扩展 (5)](#末尾10-哈希扩展)
- [末尾.11 基数统计扩展 (1)](#末尾11-基数统计扩展)

---

## 1. 字符串与位图

提供高吞吐的字符串存储、数值原子增减、位图与任意位宽位段操作，并内置支持条件增减 (INCREX)、条件删除 (DELEX)、原子 CAS/CAD 与极速哈希 (DIGEST) 等扩展命令。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `APPEND` | `APPEND key value` | ✅ |
| `BITCOUNT` | `BITCOUNT key [start end [BYTE \| BIT]]` | ✅ |
| `BITFIELD` | `BITFIELD key [[encoding offset] \| [[WRAP \| SAT \| FAIL] [[encoding offset value] \| [encoding offset increment]]]] [operation ...]` | ✅ |
| `BITFIELD_RO` | `BITFIELD_RO key [encoding offset] [get-block ...]` | ✅ |
| `BITOP` | `BITOP [AND \| OR \| XOR \| NOT \| DIFF \| DIFF1 \| ANDOR \| ONE] destkey key [key ...]` | ✅ |
| `BITPOS` | `BITPOS key bit [start [end [BYTE \| BIT]]]` | ✅ |
| `CAD` | `CAD key oldval` | 🌟 |
| `CAS` | `CAS key oldval newval [EX sec \| PX ms]` | 🌟 |
| `DECR` | `DECR key` | ✅ |
| `DECRBY` | `DECRBY key decrement` | ✅ |
| `DELEX` | `DELEX key [IFEQ ifeq-value \| IFNE ifne-value \| IFDEQ ifdeq-digest \| IFDNE ifdne-digest]` | 🌟 |
| `DIGEST` | `DIGEST key` | 🌟 |
| `GET` | `GET key` | ✅ |
| `GETBIT` | `GETBIT key offset` | ✅ |
| `GETDEL` | `GETDEL key` | ✅ |
| `GETEX` | `GETEX key [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| PERSIST]` | ✅ |
| `GETRANGE` | `GETRANGE key start end` | ✅ |
| `GETSET` | `GETSET key value` | ✅ |
| `INCR` | `INCR key` | ✅ |
| `INCRBY` | `INCRBY key increment` | ✅ |
| `INCRBYFLOAT` | `INCRBYFLOAT key increment` | ✅ |
| `INCREX` | `INCREX key [BYFLOAT float \| BYINT integer] [SATURATE] [LBOUND lowerbound] [UBOUND upperbound] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| PERSIST] [ENX]` | 🌟 |
| `LCS` | `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min-match-len] [WITHMATCHLEN]` | ✅ |
| `MGET` | `MGET key [key ...]` | ✅ |
| `MSET` | `MSET [key value] [data ...]` | ✅ |
| `MSETEX` | `MSETEX numkeys [key value] [data ...] [NX \| XX] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| KEEPTTL]` | ✅ |
| `MSETNX` | `MSETNX [key value] [data ...]` | ✅ |
| `PSETEX` | `PSETEX key milliseconds value` | ✅ |
| `SET` | `SET key value [NX \| XX \| IFEQ ifeq-value \| IFNE ifne-value \| IFDEQ ifdeq-digest \| IFDNE ifdne-digest] [GET] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| KEEPTTL]` | ✅ |
| `SETBIT` | `SETBIT key offset value` | ✅ |
| `SETEX` | `SETEX key seconds value` | ✅ |
| `SETNX` | `SETNX key value` | ✅ |
| `SETRANGE` | `SETRANGE key offset value` | ✅ |
| `STRLEN` | `STRLEN key` | ✅ |
| `SUBSTR` | `SUBSTR key start end` | ✅ |

---

## 2. 哈希字典

基于 LSM-Tree 二级索引编码的紧凑哈希表结构，支持字段级独立 TTL 过期机制 (HEXPIRE/HTTL)、原子提取删除 (HGETDEL)、原子提取延期 (HGETEX) 以及字典序前缀范围扫描 (HRANGEBYLEX)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `HDEL` | `HDEL key field [field ...]` | ✅ |
| `HEXISTS` | `HEXISTS key field` | ✅ |
| `HEXPIRE` | `HEXPIRE key seconds [NX \| XX \| GT \| LT] [numfields field [field ...]]` | ✅ |
| `HEXPIREAT` | `HEXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT] [numfields field [field ...]]` | ✅ |
| `HEXPIRETIME` | `HEXPIRETIME key [numfields field [field ...]]` | ✅ |
| `HGET` | `HGET key field` | ✅ |
| `HGETALL` | `HGETALL key` | ✅ |
| `HGETDEL` | `HGETDEL key [numfields field [field ...]]` | 🌟 |
| `HGETEX` | `HGETEX key [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| PERSIST] [numfields field [field ...]]` | 🌟 |
| `HINCRBY` | `HINCRBY key field increment` | ✅ |
| `HINCRBYFLOAT` | `HINCRBYFLOAT key field increment` | ✅ |
| `HKEYS` | `HKEYS key` | ✅ |
| `HLEN` | `HLEN key` | ✅ |
| `HMGET` | `HMGET key field [field ...]` | ✅ |
| `HMSET` | `HMSET key [field value] [data ...]` | ✅ |
| `HPERSIST` | `HPERSIST key [numfields field [field ...]]` | ✅ |
| `HPEXPIRE` | `HPEXPIRE key milliseconds [NX \| XX \| GT \| LT] [numfields field [field ...]]` | ✅ |
| `HPEXPIREAT` | `HPEXPIREAT key unix-time-milliseconds [NX \| XX \| GT \| LT] [numfields field [field ...]]` | ✅ |
| `HPEXPIRETIME` | `HPEXPIRETIME key [numfields field [field ...]]` | ✅ |
| `HPTTL` | `HPTTL key [numfields field [field ...]]` | ✅ |
| `HRANDFIELD` | `HRANDFIELD key [count [WITHVALUES]]` | ✅ |
| `HRANGEBYLEX` | `HRANGEBYLEX key min max [LIMIT offset count]` | 🌟 |
| `HSCAN` | `HSCAN key cursor [MATCH pattern] [COUNT] [NOVALUES]` | ✅ |
| `HSET` | `HSET key [field value] [data ...]` | ✅ |
| `HSETEX` | `HSETEX key [FNX \| FXX] [EX seconds \| PX milliseconds \| EXAT unix-time-seconds \| PXAT unix-time-milliseconds \| KEEPTTL] [numfields [field value] [data ...]]` | ✅ |
| `HSETEXPIRE` | `HSETEXPIRE [args ...]` | ✅ |
| `HSETNX` | `HSETNX key field value` | ✅ |
| `HSTRLEN` | `HSTRLEN key field` | ✅ |
| `HTTL` | `HTTL key [numfields field [field ...]]` | ✅ |
| `HVALS` | `HVALS key` | ✅ |

---

## 3. 双向列表

支持双端队列的高速推入弹出 (LPUSH/RPOP)、阻塞弹出 (BLPOP/BRPOP)、多列表弹出 (LMPOP/BLMPOP)、范围截取 (LRANGE/LTRIM) 与跨列表原子移动 (LMOVE/BLMOVE)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `BLMOVE` | `BLMOVE source destination [LEFT \| RIGHT] [LEFT \| RIGHT] timeout` | ✅ |
| `BLMOVEM` | `BLMOVEM source destination [LEFT \| RIGHT] [LEFT \| RIGHT] timeout [[COUNT \| EXACTLY] [OBO \| BULK]]` | ✅ |
| `BLMPOP` | `BLMPOP timeout numkeys key [key ...] [LEFT \| RIGHT] [COUNT]` | ✅ |
| `BLPOP` | `BLPOP key [key ...] timeout` | ✅ |
| `BRPOP` | `BRPOP key [key ...] timeout` | ✅ |
| `BRPOPLPUSH` | `BRPOPLPUSH source destination timeout` | ✅ |
| `LINDEX` | `LINDEX key index` | ✅ |
| `LINSERT` | `LINSERT key [BEFORE \| AFTER] pivot element` | ✅ |
| `LLEN` | `LLEN key` | ✅ |
| `LMOVE` | `LMOVE source destination [LEFT \| RIGHT] [LEFT \| RIGHT]` | ✅ |
| `LMOVEM` | `LMOVEM source destination [LEFT \| RIGHT] [LEFT \| RIGHT] [[COUNT \| EXACTLY] [OBO \| BULK]]` | ✅ |
| `LMPOP` | `LMPOP numkeys key [key ...] [LEFT \| RIGHT] [COUNT]` | ✅ |
| `LPOP` | `LPOP key [count]` | ✅ |
| `LPOS` | `LPOS key element [RANK] [COUNT num-matches] [MAXLEN len]` | ✅ |
| `LPUSH` | `LPUSH key element [element ...]` | ✅ |
| `LPUSHX` | `LPUSHX key element [element ...]` | ✅ |
| `LRANGE` | `LRANGE key start stop` | ✅ |
| `LREM` | `LREM key count element` | ✅ |
| `LSET` | `LSET key index element` | ✅ |
| `LTRIM` | `LTRIM key start stop` | ✅ |
| `RPOP` | `RPOP key [count]` | ✅ |
| `RPOPLPUSH` | `RPOPLPUSH source destination` | ✅ |
| `RPUSH` | `RPUSH key element [element ...]` | ✅ |
| `RPUSHX` | `RPUSHX key element [element ...]` | ✅ |

---

## 4. 无序集合

支持高性能集合成员判定 (SISMEMBER/SMISMEMBER)、随机抽样 (SRANDMEMBER/SPOP)、增量游标迭代 (SSCAN)，以及多集合交集/并集/差集的基数计算与存储 (SINTER/SUNION/SDIFF)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `SADD` | `SADD key member [member ...]` | ✅ |
| `SCARD` | `SCARD key` | ✅ |
| `SDIFF` | `SDIFF key [key ...]` | ✅ |
| `SDIFFCARD` | `SDIFFCARD numkeys key [key ...] [LIMIT]` | ✅ |
| `SDIFFSTORE` | `SDIFFSTORE destination key [key ...]` | ✅ |
| `SINTER` | `SINTER key [key ...]` | 🌟 |
| `SINTERCARD` | `SINTERCARD numkeys key [key ...] [LIMIT]` | 🌟 |
| `SINTERSTORE` | `SINTERSTORE destination key [key ...]` | 🌟 |
| `SISMEMBER` | `SISMEMBER key member` | 🌟 |
| `SMEMBERS` | `SMEMBERS key` | ✅ |
| `SMISMEMBER` | `SMISMEMBER key member [member ...]` | ✅ |
| `SMOVE` | `SMOVE source destination member` | ✅ |
| `SPOP` | `SPOP key [count]` | ✅ |
| `SRANDMEMBER` | `SRANDMEMBER key [count]` | ✅ |
| `SREM` | `SREM key member [member ...]` | ✅ |
| `SSCAN` | `SSCAN key cursor [MATCH pattern] [COUNT]` | ✅ |
| `SUNION` | `SUNION key [key ...]` | ✅ |
| `SUNIONCARD` | `SUNIONCARD numkeys key [key ...] [APPROX] [LIMIT]` | ✅ |
| `SUNIONSTORE` | `SUNIONSTORE destination key [key ...]` | ✅ |

---

## 5. 有序集合

基于跳表与分数-成员双向复合键编码的高性能有序集合，支持按分数 (BYSCORE)、字典序 (BYLEX) 与索引范围检索，支持多集合加权聚合 (ZUNION/ZINTER/ZDIFF) 与阻塞极值弹出 (BZPOPMIN/BZMPOP)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `BZMPOP` | `BZMPOP timeout numkeys key [key ...] [MIN \| MAX] [COUNT]` | ✅ |
| `BZPOPMAX` | `BZPOPMAX key [key ...] timeout` | ✅ |
| `BZPOPMIN` | `BZPOPMIN key [key ...] timeout` | ✅ |
| `ZADD` | `ZADD key [NX \| XX] [GT \| LT] [CH change] [INCR increment] [score member] [data ...]` | ✅ |
| `ZCARD` | `ZCARD key` | ✅ |
| `ZCOUNT` | `ZCOUNT key min max` | ✅ |
| `ZDIFF` | `ZDIFF numkeys key [key ...] [WITHSCORES]` | ✅ |
| `ZDIFFSTORE` | `ZDIFFSTORE destination numkeys key [key ...]` | ✅ |
| `ZINCRBY` | `ZINCRBY key increment member` | ✅ |
| `ZINTER` | `ZINTER numkeys key [key ...] [WEIGHTS weight] [weight ...] [SUM \| MIN \| MAX \| COUNT] [WITHSCORES]` | ✅ |
| `ZINTERCARD` | `ZINTERCARD numkeys key [key ...] [LIMIT]` | ✅ |
| `ZINTERSTORE` | `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight] [weight ...] [SUM \| MIN \| MAX \| COUNT]` | ✅ |
| `ZLEXCOUNT` | `ZLEXCOUNT key min max` | ✅ |
| `ZMPOP` | `ZMPOP numkeys key [key ...] [MIN \| MAX] [COUNT]` | ✅ |
| `ZMSCORE` | `ZMSCORE key member [member ...]` | ✅ |
| `ZPOPMAX` | `ZPOPMAX key [count]` | ✅ |
| `ZPOPMIN` | `ZPOPMIN key [count]` | ✅ |
| `ZRANDMEMBER` | `ZRANDMEMBER key [count [WITHSCORES]]` | ✅ |
| `ZRANGE` | `ZRANGE key start stop [BYSCORE \| BYLEX] [REV] [offset count] [WITHSCORES]` | ✅ |
| `ZRANGEBYLEX` | `ZRANGEBYLEX key min max [offset count]` | ✅ |
| `ZRANGEBYSCORE` | `ZRANGEBYSCORE key min max [WITHSCORES] [offset count]` | ✅ |
| `ZRANGESTORE` | `ZRANGESTORE dst src min max [BYSCORE \| BYLEX] [REV] [offset count]` | ✅ |
| `ZRANK` | `ZRANK key member [WITHSCORE]` | ✅ |
| `ZREM` | `ZREM key member [member ...]` | ✅ |
| `ZREMRANGEBYLEX` | `ZREMRANGEBYLEX key min max` | ✅ |
| `ZREMRANGEBYRANK` | `ZREMRANGEBYRANK key start stop` | ✅ |
| `ZREMRANGEBYSCORE` | `ZREMRANGEBYSCORE key min max` | ✅ |
| `ZREVRANGE` | `ZREVRANGE key start stop [WITHSCORES]` | ✅ |
| `ZREVRANGEBYLEX` | `ZREVRANGEBYLEX key max min [offset count]` | ✅ |
| `ZREVRANGEBYSCORE` | `ZREVRANGEBYSCORE key max min [WITHSCORES] [offset count]` | ✅ |
| `ZREVRANK` | `ZREVRANK key member [WITHSCORE]` | ✅ |
| `ZSCAN` | `ZSCAN key cursor [MATCH pattern] [COUNT]` | ✅ |
| `ZSCORE` | `ZSCORE key member` | ✅ |
| `ZUNION` | `ZUNION numkeys key [key ...] [WEIGHTS weight] [weight ...] [SUM \| MIN \| MAX \| COUNT] [WITHSCORES]` | ✅ |
| `ZUNIONSTORE` | `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight] [weight ...] [SUM \| MIN \| MAX \| COUNT]` | ✅ |

---

## 6. 空间地理位置

基于 52 位 Geohash 编码与 Haversine 大圆距离公式，支持经纬度坐标写入、位置距离与哈希值计算、以及现代矩形与半径范围搜索 (GEOSEARCH/GEOSEARCHSTORE)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `GEOADD` | `GEOADD key [NX \| XX] [CH change] [longitude latitude member] [data ...]` | ✅ |
| `GEODIST` | `GEODIST key member1 member2 [m \| km \| ft \| mi]` | ✅ |
| `GEOHASH` | `GEOHASH key [member] [member ...]` | ✅ |
| `GEOPOS` | `GEOPOS key [member] [member ...]` | ✅ |
| `GEORADIUS` | `GEORADIUS key longitude latitude radius [m \| km \| ft \| mi] [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT [ANY]] [ASC \| DESC] [STORE storekey \| STOREDIST storedistkey]` | ✅ |
| `GEORADIUS_RO` | `GEORADIUS_RO key longitude latitude radius [m \| km \| ft \| mi] [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT [ANY]] [ASC \| DESC]` | ✅ |
| `GEORADIUSBYMEMBER` | `GEORADIUSBYMEMBER key member radius [m \| km \| ft \| mi] [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT [ANY]] [ASC \| DESC] [STORE storekey \| STOREDIST storedistkey]` | ✅ |
| `GEORADIUSBYMEMBER_RO` | `GEORADIUSBYMEMBER_RO key member radius [m \| km \| ft \| mi] [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT [ANY]] [ASC \| DESC]` | ✅ |
| `GEOSEARCH` | `GEOSEARCH key [FROMMEMBER member \| [longitude latitude]] [[BYRADIUS radius [m \| km \| ft \| mi]] \| [BYBOX width height [m \| km \| ft \| mi]]] [ASC \| DESC] [COUNT [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]` | ✅ |
| `GEOSEARCHSTORE` | `GEOSEARCHSTORE destination source [FROMMEMBER member \| [longitude latitude]] [[BYRADIUS radius [m \| km \| ft \| mi]] \| [BYBOX width height [m \| km \| ft \| mi]]] [ASC \| DESC] [COUNT [ANY]] [STOREDIST]` | ✅ |

---

## 7. 基数统计

基于 14 位寄存器分桶与偏差修正算法的概率性基数统计引擎，支持稀疏与密集编码自适应转换，支持千万级独立元素去重统计与多键合并 (PFMERGE)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `PFADD` | `PFADD key [element] [element ...]` | ✅ |
| `PFCOUNT` | `PFCOUNT key [key ...]` | ✅ |
| `PFMERGE` | `PFMERGE destkey [sourcekey] [sourcekey ...]` | ✅ |
| `PFSELFTEST` | `PFSELFTEST` | ✅ |

---

## 8. 消息流与消费者组

支持持久化消息追加、毫秒级唯一 ID 自动递增、多流阻塞订阅 (XREAD)、消费者组与待处理条目表 PEL 管理 (XREADGROUP/XPENDING/XCLAIM/XAUTOCLAIM)，以及原子确认删除 (XACKDEL) 扩展命令。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `XACK` | `XACK key group ID [ID ...]` | ✅ |
| `XACKDEL` | `XACKDEL key group [KEEPREF \| DELREF \| ACKED] [numids id [id ...]]` | 🌟 |
| `XADD` | `XADD key [NOMKSTREAM] [KEEPREF \| DELREF \| ACKED] [IDMPAUTO pid \| [pid iid]] [[MAXLEN \| MINID] [= equal \| ~ approximately] threshold [LIMIT count]] [* auto-id \| id] [field value] [data ...]` | ✅ |
| `XAUTOCLAIM` | `XAUTOCLAIM key group consumer min-idle-time start [COUNT] [JUSTID]` | ✅ |
| `XCLAIM` | `XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME unix-time-milliseconds] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID]` | ✅ |
| `XDEL` | `XDEL key ID [ID ...]` | ✅ |
| `XDELEX` | `XDELEX key [KEEPREF \| DELREF \| ACKED] [numids id [id ...]]` | 🌟 |
| `XLEN` | `XLEN key` | ✅ |
| `XNACK` | `XNACK key group [SILENT \| FAIL \| FATAL] [numids id [id ...]] [RETRYCOUNT count] [FORCE]` | 🌟 |
| `XPENDING` | `XPENDING key group [[IDLE min-idle-time] start end count [consumer]]` | ✅ |
| `XRANGE` | `XRANGE key start end [COUNT]` | ✅ |
| `XREAD` | `XREAD [COUNT] [MAXCOUNT] [MAXSIZE] [BLOCK milliseconds] [key [key ...] ID [ID ...]]` | ✅ |
| `XREADGROUP` | `XREADGROUP [group consumer] [COUNT] [MAXCOUNT] [MAXSIZE] [BLOCK milliseconds] [CLAIM min-idle-time] [NOACK] [key [key ...] ID [ID ...]]` | ✅ |
| `XREVRANGE` | `XREVRANGE key end start [COUNT]` | ✅ |
| `XSETID` | `XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]` | ✅ |
| `XTRIM` | `XTRIM key [[MAXLEN \| MINID] [= equal \| ~ approximately] threshold [LIMIT count] [KEEPREF \| DELREF \| ACKED]]` | ✅ |

---

## 9. 通用键与生命周期

管理键级别的生命周期，包含毫秒级 TTL/PTTL 过期、持久化、重命名、类型检测、通用排序 (SORT)、以及基于底层 LSM 前缀索引的高速扫描扩展命令 (SCANPREFIX)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `COPY` | `COPY source destination [DB destination-db] [REPLACE]` | ✅ |
| `DBSIZE` | `DBSIZE` | ✅ |
| `DEL` | `DEL key [key ...]` | ✅ |
| `EXISTS` | `EXISTS key [key ...]` | ✅ |
| `EXPIRE` | `EXPIRE key seconds [NX \| XX \| GT \| LT]` | ✅ |
| `EXPIREAT` | `EXPIREAT key unix-time-seconds [NX \| XX \| GT \| LT]` | ✅ |
| `EXPIRETIME` | `EXPIRETIME key` | ✅ |
| `FLUSHALL` | `FLUSHALL [ASYNC \| SYNC]` | ✅ |
| `FLUSHDB` | `FLUSHDB [ASYNC \| SYNC]` | ✅ |
| `KEYS` | `KEYS pattern` | ✅ |
| `KMETADATA` | `KMETADATA key` | 🌟 |
| `OBJECT` | `OBJECT` | ✅ |
| `PERSIST` | `PERSIST key` | ✅ |
| `PEXPIRE` | `PEXPIRE key milliseconds [NX \| XX \| GT \| LT]` | ✅ |
| `PEXPIREAT` | `PEXPIREAT key unix-time-milliseconds [NX \| XX \| GT \| LT]` | ✅ |
| `PEXPIRETIME` | `PEXPIRETIME key` | ✅ |
| `PREFIX` | `PREFIX [args ...]` | ✅ |
| `PTTL` | `PTTL key` | ✅ |
| `RANDOMKEY` | `RANDOMKEY` | ✅ |
| `RENAME` | `RENAME key newkey` | ✅ |
| `RENAMENX` | `RENAMENX key newkey` | ✅ |
| `SCAN` | `SCAN cursor [MATCH pattern] [COUNT] [TYPE]` | ✅ |
| `SCANPREFIX` | `SCANPREFIX prefix cursor [COUNT count]` | 🌟 |
| `SORT` | `SORT key [BY by-pattern] [offset count] [GET get-pattern] [get-pattern ...] [ASC \| DESC] [ALPHA sorting] [STORE destination]` | ✅ |
| `SORT_RO` | `SORT_RO key [BY by-pattern] [offset count] [GET get-pattern] [get-pattern ...] [ASC \| DESC] [ALPHA sorting]` | ✅ |
| `TOUCH` | `TOUCH key [key ...]` | ✅ |
| `TTL` | `TTL key` | ✅ |
| `TYPE` | `TYPE key` | ✅ |
| `UNLINK` | `UNLINK key [key ...]` | ✅ |

---

## 10. 连接与服务器管理

包含客户端连接保活 (PING)、鉴权 (AUTH)、多库切换 (SELECT)、运行时配置动态调整 (CONFIG)、集群角色查询 (ROLE)、LSM 物理压缩 (COMPACT) 与实例统计信息 (INFO)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `APPLYBATCH` | `APPLYBATCH [args ...]` | ✅ |
| `AUTH` | `AUTH [username] password` | ✅ |
| `BGSAVE` | `BGSAVE [SCHEDULE]` | ✅ |
| `COMMAND` | `COMMAND` | ✅ |
| `COMPACT` | `COMPACT` | 🌟 |
| `DEBUG` | `DEBUG` | ✅ |
| `DISK` | `DISK [args ...]` | ✅ |
| `DUMP` | `DUMP key` | ✅ |
| `ECHO` | `ECHO message` | ✅ |
| `EVAL` | `EVAL script numkeys [key] [key ...] [arg] [arg ...]` | ✅ |
| `EVAL_RO` | `EVAL_RO script numkeys [key] [key ...] [arg] [arg ...]` | ✅ |
| `EVALSHA` | `EVALSHA sha1 numkeys [key] [key ...] [arg] [arg ...]` | ✅ |
| `EVALSHA_RO` | `EVALSHA_RO sha1 numkeys [key] [key ...] [arg] [arg ...]` | ✅ |
| `FLUSHBACKUP` | `FLUSHBACKUP [args ...]` | ✅ |
| `FLUSHBLOCKCACHE` | `FLUSHBLOCKCACHE [args ...]` | ✅ |
| `FLUSHMEMTABLE` | `FLUSHMEMTABLE [args ...]` | ✅ |
| `HELLO` | `HELLO [protover [username password] [SETNAME clientname]]` | ✅ |
| `INFO` | `INFO [section] [section ...]` | ✅ |
| `KPROFILE` | `KPROFILE [args ...]` | ✅ |
| `LASTSAVE` | `LASTSAVE` | ✅ |
| `LATENCY` | `LATENCY` | ✅ |
| `MEMORY` | `MEMORY` | ✅ |
| `MONITOR` | `MONITOR` | ✅ |
| `MOVE` | `MOVE key db` | ✅ |
| `MOVEX` | `MOVEX key src_db dst_db` | 🌟 |
| `PERFLOG` | `PERFLOG [args ...]` | ✅ |
| `PING` | `PING [message]` | ✅ |
| `POLLUPDATES` | `POLLUPDATES [args ...]` | ✅ |
| `QUIT` | `QUIT` | ✅ |
| `RDB` | `RDB [args ...]` | ✅ |
| `REPLICAOF` | `REPLICAOF [[host port] \| [NO ONE]]` | ✅ |
| `RESET` | `RESET` | ✅ |
| `RESTORE` | `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]` | ✅ |
| `ROLE` | `ROLE` | ✅ |
| `SELECT` | `SELECT index` | ✅ |
| `SHUTDOWN` | `SHUTDOWN [NOSAVE \| SAVE] [NOW] [FORCE] [ABORT]` | ✅ |
| `SLAVEOF` | `SLAVEOF [[host port] \| [NO ONE]]` | ✅ |
| `SLOWLOG` | `SLOWLOG` | ✅ |
| `SST` | `SST [args ...]` | ✅ |
| `STATS` | `STATS [args ...]` | ✅ |
| `SWAPDB` | `SWAPDB index1 index2` | ✅ |
| `TIME` | `TIME` | ✅ |

---

## 11. 发布订阅

支持经典频道与模式匹配广播 (PUBLISH/SUBSCRIBE/PSUBSCRIBE)、分片发布订阅 (SPUBLISH/SSUBSCRIBE)，以及单网络往返多频道批量发布扩展命令 (MPUBLISH)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `MPUBLISH` | `MPUBLISH channel message [channel message ...]` | 🌟 |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | ✅ |
| `PUBLISH` | `PUBLISH channel message` | ✅ |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern] [pattern ...]` | ✅ |
| `SPUBLISH` | `SPUBLISH shardchannel message` | ✅ |
| `SSUBSCRIBE` | `SSUBSCRIBE shardchannel [shardchannel ...]` | ✅ |
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | ✅ |
| `SUNSUBSCRIBE` | `SUNSUBSCRIBE [shardchannel] [shardchannel ...]` | ✅ |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel] [channel ...]` | ✅ |

---

## 12. 事务与批量引擎

提供乐观锁事务块 (MULTI/EXEC/WATCH) 以及单往返原子多操作批量写入执行引擎 (BATCH) 与跨分片分布式事务扩展命令 (TXN)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `DISCARD` | `DISCARD` | ✅ |
| `EXEC` | `EXEC` | ✅ |
| `MULTI` | `MULTI` | ✅ |
| `UNWATCH` | `UNWATCH` | ✅ |
| `WATCH` | `WATCH key [key ...]` | ✅ |

---

## 13. 分布式集群与共识

完全兼容 Redis Cluster 拓扑发现协议 (CLUSTER NODES/SLOTS/SHARDS)，底层深度集成 Raft 强一致性共识状态机扩展命令 (RAFT LEADER/CLUSTER_INFO/MEMBERSHIP)。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `ASKING` | `ASKING` | ✅ |
| `BATCH` | `BATCH <op> <args...> [OP ...]` | 🌟 |
| `CLUSTERX` | `CLUSTERX [args ...]` | ✅ |
| `PSYNC` | `PSYNC replicationid offset` | ✅ |
| `RAFT ADD_LEARNER` | `RAFT ADD_LEARNER node_id addr` | 🌟 |
| `RAFT CHANGE_MEMBERSHIP` | `RAFT CHANGE_MEMBERSHIP [node_id ...]` | 🌟 |
| `RAFT CLUSTER_INFO` | `RAFT CLUSTER_INFO` | 🌟 |
| `RAFT HEALTH` | `RAFT HEALTH` | 🌟 |
| `RAFT JOIN` | `RAFT JOIN node_id addr` | 🌟 |
| `RAFT LEADER` | `RAFT LEADER` | 🌟 |
| `RAFT LEAVE` | `RAFT LEAVE node_id` | 🌟 |
| `RAFT MEMBERS` | `RAFT MEMBERS` | 🌟 |
| `RAFT METRICS` | `RAFT METRICS` | 🌟 |
| `RAFT PURGE` | `RAFT PURGE [upto]` | 🌟 |
| `RAFT SNAPSHOT` | `RAFT SNAPSHOT` | 🌟 |
| `RAFT SNAPSHOT_STATUS` | `RAFT SNAPSHOT_STATUS` | 🌟 |
| `RAFT STATUS` | `RAFT STATUS` | 🌟 |
| `RAFT.BATCH` | `RAFT.BATCH [args ...]` | 🌟 |
| `RAFT.HEALTH` | `RAFT.HEALTH [args ...]` | 🌟 |
| `RAFT.JOIN` | `RAFT.JOIN [args ...]` | 🌟 |
| `RAFT.LEAVE` | `RAFT.LEAVE [args ...]` | 🌟 |
| `RAFT.MEMBERS` | `RAFT.MEMBERS [args ...]` | 🌟 |
| `RAFT.METRICS` | `RAFT.METRICS [args ...]` | 🌟 |
| `RAFT.PURGE` | `RAFT.PURGE [args ...]` | 🌟 |
| `RAFT.SNAPSHOT` | `RAFT.SNAPSHOT [args ...]` | 🌟 |
| `RAFT.SNAPSHOT_STATUS` | `RAFT.SNAPSHOT_STATUS [args ...]` | 🌟 |
| `RAFT.STATUS` | `RAFT.STATUS [args ...]` | 🌟 |
| `RAFT.TXN` | `RAFT.TXN [args ...]` | 🌟 |
| `READONLY` | `READONLY` | ✅ |
| `READWRITE` | `READWRITE` | ✅ |
| `REPLCONF` | `REPLCONF` | ✅ |
| `TXN` | `TXN` | 🌟 |
| `WAIT` | `WAIT numreplicas timeout` | ✅ |

---

## 14. 布隆与布谷鸟过滤器

提供可伸缩链式布隆过滤器 (Bloom Filter) 与支持动态删除的布谷鸟过滤器 (Cuckoo Filter) 扩展命令，以极小内存开销实现超大规模数据集的高速存在性过滤。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `BF.ADD` | `BF.ADD [args ...]` | 🌟 |
| `BF.CARD` | `BF.CARD [args ...]` | 🌟 |
| `BF.EXISTS` | `BF.EXISTS [args ...]` | 🌟 |
| `BF.INFO` | `BF.INFO [args ...]` | 🌟 |
| `BF.INSERT` | `BF.INSERT [args ...]` | 🌟 |
| `BF.MADD` | `BF.MADD [args ...]` | 🌟 |
| `BF.MEXISTS` | `BF.MEXISTS [args ...]` | 🌟 |
| `BF.RESERVE` | `BF.RESERVE [args ...]` | 🌟 |
| `CF.ADD` | `CF.ADD [args ...]` | 🌟 |
| `CF.ADDNX` | `CF.ADDNX [args ...]` | 🌟 |
| `CF.COUNT` | `CF.COUNT [args ...]` | 🌟 |
| `CF.DEL` | `CF.DEL [args ...]` | 🌟 |
| `CF.EXISTS` | `CF.EXISTS [args ...]` | 🌟 |
| `CF.INFO` | `CF.INFO [args ...]` | 🌟 |
| `CF.INSERT` | `CF.INSERT [args ...]` | 🌟 |
| `CF.INSERTNX` | `CF.INSERTNX [args ...]` | 🌟 |
| `CF.MEXISTS` | `CF.MEXISTS [args ...]` | 🌟 |
| `CF.RESERVE` | `CF.RESERVE [args ...]` | 🌟 |

---

## 15. JSON 文档引擎

提供符合 RFC 7396 / RFC 6901 标准的原生 JSON 文档存储扩展命令，支持复杂的 JSONPath 表达式查询、局部路径就地修改、数字乘加与类型转换。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `JSON.ARRAPPEND` | `JSON.ARRAPPEND [args ...]` | 🌟 |
| `JSON.ARRINDEX` | `JSON.ARRINDEX [args ...]` | 🌟 |
| `JSON.ARRINSERT` | `JSON.ARRINSERT [args ...]` | 🌟 |
| `JSON.ARRLEN` | `JSON.ARRLEN [args ...]` | 🌟 |
| `JSON.ARRPOP` | `JSON.ARRPOP [args ...]` | 🌟 |
| `JSON.ARRTRIM` | `JSON.ARRTRIM key path start stop` | 🌟 |
| `JSON.CLEAR` | `JSON.CLEAR [args ...]` | 🌟 |
| `JSON.DEBUG` | `JSON.DEBUG [args ...]` | 🌟 |
| `JSON.DEL` | `JSON.DEL [args ...]` | 🌟 |
| `JSON.FORGET` | `JSON.FORGET [args ...]` | 🌟 |
| `JSON.GET` | `JSON.GET [args ...]` | 🌟 |
| `JSON.INFO` | `JSON.INFO key` | 🌟 |
| `JSON.MERGE` | `JSON.MERGE [args ...]` | 🌟 |
| `JSON.MGET` | `JSON.MGET [args ...]` | 🌟 |
| `JSON.MSET` | `JSON.MSET key path value [key path value ...]` | 🌟 |
| `JSON.NUMINCRBY` | `JSON.NUMINCRBY [args ...]` | 🌟 |
| `JSON.NUMMULTBY` | `JSON.NUMMULTBY [args ...]` | 🌟 |
| `JSON.OBJKEYS` | `JSON.OBJKEYS [args ...]` | 🌟 |
| `JSON.OBJLEN` | `JSON.OBJLEN [args ...]` | 🌟 |
| `JSON.RESP` | `JSON.RESP key [path]` | 🌟 |
| `JSON.SET` | `JSON.SET [args ...]` | 🌟 |
| `JSON.STRAPPEND` | `JSON.STRAPPEND [args ...]` | 🌟 |
| `JSON.STRLEN` | `JSON.STRLEN [args ...]` | 🌟 |
| `JSON.TOGGLE` | `JSON.TOGGLE [args ...]` | 🌟 |
| `JSON.TYPE` | `JSON.TYPE [args ...]` | 🌟 |

---

## 16. 全文与向量检索

支持对 Hash 和 JSON 文档建立倒排索引、数值与标签过滤、同义词解析、聚合分析管道，以及基于 HNSW 图索引的高维向量近邻相似度检索扩展命令。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `FT._LIST` | `FT._LIST` | 🌟 |
| `FT.ALIASADD` | `FT.ALIASADD [args ...]` | 🌟 |
| `FT.ALIASDEL` | `FT.ALIASDEL [args ...]` | 🌟 |
| `FT.CREATE` | `FT.CREATE [args ...]` | 🌟 |
| `FT.DROPINDEX` | `FT.DROPINDEX [args ...]` | 🌟 |
| `FT.EXPLAIN` | `FT.EXPLAIN [args ...]` | 🌟 |
| `FT.EXPLAINSQL` | `FT.EXPLAINSQL index query` | 🌟 |
| `FT.INFO` | `FT.INFO [args ...]` | 🌟 |
| `FT.LIST` | `FT.LIST [args ...]` | 🌟 |
| `FT.SEARCH` | `FT.SEARCH [args ...]` | 🌟 |
| `FT.SEARCHSQL` | `FT.SEARCHSQL index query` | 🌟 |
| `FT.TAGVALS` | `FT.TAGVALS index field_name` | 🌟 |

---

## 17. 高性能时序引擎

专为时序数据设计的高吞吐分块存储，内置 Gorilla 双重增量压缩算法、多时间序列标签过滤、滑动窗口聚合与自动降采样连续聚合规则扩展命令。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `TS.ADD` | `TS.ADD [args ...]` | 🌟 |
| `TS.ALTER` | `TS.ALTER [args ...]` | 🌟 |
| `TS.CREATE` | `TS.CREATE [args ...]` | 🌟 |
| `TS.CREATERULE` | `TS.CREATERULE [args ...]` | 🌟 |
| `TS.DECRBY` | `TS.DECRBY [args ...]` | 🌟 |
| `TS.DEL` | `TS.DEL [args ...]` | 🌟 |
| `TS.GET` | `TS.GET [args ...]` | 🌟 |
| `TS.INCRBY` | `TS.INCRBY [args ...]` | 🌟 |
| `TS.INFO` | `TS.INFO [args ...]` | 🌟 |
| `TS.MADD` | `TS.MADD [args ...]` | 🌟 |
| `TS.MGET` | `TS.MGET [args ...]` | 🌟 |
| `TS.MRANGE` | `TS.MRANGE [args ...]` | 🌟 |
| `TS.MREVRANGE` | `TS.MREVRANGE [args ...]` | 🌟 |
| `TS.QUERYINDEX` | `TS.QUERYINDEX [args ...]` | 🌟 |
| `TS.RANGE` | `TS.RANGE [args ...]` | 🌟 |
| `TS.REVRANGE` | `TS.REVRANGE [args ...]` | 🌟 |

---

## 18. 分位数统计草图

基于质心聚类与缩放函数的流式高精度分位数估计扩展命令，支持极值准确保留、百分位数估计、累积分布概率 (CDF)、排位估计与草图合并。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `TDIGEST.ADD` | `TDIGEST.ADD [args ...]` | 🌟 |
| `TDIGEST.BYRANK` | `TDIGEST.BYRANK [args ...]` | 🌟 |
| `TDIGEST.BYREVRANK` | `TDIGEST.BYREVRANK [args ...]` | 🌟 |
| `TDIGEST.CDF` | `TDIGEST.CDF [args ...]` | 🌟 |
| `TDIGEST.CREATE` | `TDIGEST.CREATE [args ...]` | 🌟 |
| `TDIGEST.INFO` | `TDIGEST.INFO [args ...]` | 🌟 |
| `TDIGEST.MAX` | `TDIGEST.MAX [args ...]` | 🌟 |
| `TDIGEST.MERGE` | `TDIGEST.MERGE [args ...]` | 🌟 |
| `TDIGEST.MIN` | `TDIGEST.MIN [args ...]` | 🌟 |
| `TDIGEST.QUANTILE` | `TDIGEST.QUANTILE [args ...]` | 🌟 |
| `TDIGEST.RANK` | `TDIGEST.RANK [args ...]` | 🌟 |
| `TDIGEST.RESET` | `TDIGEST.RESET [args ...]` | 🌟 |
| `TDIGEST.REVRANK` | `TDIGEST.REVRANK [args ...]` | 🌟 |
| `TDIGEST.TRIMMED_MEAN` | `TDIGEST.TRIMMED_MEAN [args ...]` | 🌟 |

---

## 19. 紧凑有序整型集合

专为千万级 64 位无符号大整数（ID 集合）深度优化的紧凑去重与排序扩展命令，支持按值开闭区间极速分页查询与存在性统计。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `SIADD` | `SIADD key id [id ...]` | 🌟 |
| `SICARD` | `SICARD key` | 🌟 |
| `SIEXISTS` | `SIEXISTS key id [id ...]` | 🌟 |
| `SIRANGE` | `SIRANGE key offset count` | 🌟 |
| `SIRANGEBYVALUE` | `SIRANGEBYVALUE key min max [LIMIT offset count]` | 🌟 |
| `SIREM` | `SIREM key id [id ...]` | 🌟 |
| `SIREVRANGE` | `SIREVRANGE key offset count` | 🌟 |
| `SIREVRANGEBYVALUE` | `SIREVRANGEBYVALUE key max min [LIMIT offset count]` | 🌟 |

---

## 20. 多租户命名空间隔离

原生内置企业级多租户虚拟化隔离扩展命令，支持为不同租户分配独立命名空间、Token 鉴权、配额控制与整租户数据毫秒级原子销毁。

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |

---

## 末尾：不支持的命令

以下命令为 Redis 官方定义但 WeDB 当前尚未支持的命令：

### 末尾.1 服务运维与权限

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `ACL` | `ACL` |  |
| `ACL CAT` | `ACL CAT [category]` |  |
| `ACL DELUSER` | `ACL DELUSER username [username ...]` |  |
| `ACL DRYRUN` | `ACL DRYRUN username command [arg] [arg ...]` |  |
| `ACL GENPASS` | `ACL GENPASS [bits]` |  |
| `ACL GETUSER` | `ACL GETUSER username` |  |
| `ACL HELP` | `ACL HELP` |  |
| `ACL LIST` | `ACL LIST` |  |
| `ACL LOAD` | `ACL LOAD` |  |
| `ACL LOG` | `ACL LOG [count \| RESET]` |  |
| `ACL SAVE` | `ACL SAVE` |  |
| `ACL SETUSER` | `ACL SETUSER username [rule] [rule ...]` |  |
| `ACL USERS` | `ACL USERS` |  |
| `ACL WHOAMI` | `ACL WHOAMI` |  |
| `BACKUP` | `BACKUP` |  |
| `BACKUP ABORT` | `BACKUP ABORT` |  |
| `BACKUP CLEANUP` | `BACKUP CLEANUP` |  |
| `BACKUP HELP` | `BACKUP HELP` |  |
| `BACKUP LIST` | `BACKUP LIST` |  |
| `BACKUP SEAL` | `BACKUP SEAL` |  |
| `BACKUP START` | `BACKUP START` |  |
| `BACKUP STATUS` | `BACKUP STATUS` |  |
| `BGREWRITEAOF` | `BGREWRITEAOF` |  |
| `COMMAND COUNT` | `COMMAND COUNT` |  |
| `COMMAND DOCS` | `COMMAND DOCS [command-name] [command-name ...]` |  |
| `COMMAND GETKEYS` | `COMMAND GETKEYS command [arg] [arg ...]` |  |
| `COMMAND GETKEYSANDFLAGS` | `COMMAND GETKEYSANDFLAGS command [arg] [arg ...]` |  |
| `COMMAND HELP` | `COMMAND HELP` |  |
| `COMMAND INFO` | `COMMAND INFO [command-name] [command-name ...]` |  |
| `COMMAND LIST` | `COMMAND LIST [MODULE module-name \| ACLCAT category \| PATTERN]` |  |
| `CONFIG` | `CONFIG` |  |
| `CONFIG GET` | `CONFIG GET parameter [parameter ...]` |  |
| `CONFIG HELP` | `CONFIG HELP` |  |
| `CONFIG RESETSTAT` | `CONFIG RESETSTAT` |  |
| `CONFIG REWRITE` | `CONFIG REWRITE` |  |
| `CONFIG SET` | `CONFIG SET [parameter value] [data ...]` |  |
| `FAILOVER` | `FAILOVER [host port [FORCE]] [ABORT] [TIMEOUT milliseconds]` |  |
| `HOTKEYS` | `HOTKEYS` |  |
| `HOTKEYS GET` | `HOTKEYS GET` |  |
| `HOTKEYS HELP` | `HOTKEYS HELP` |  |
| `HOTKEYS RESET` | `HOTKEYS RESET` |  |
| `HOTKEYS START` | `HOTKEYS START [count [CPU] [NET]] [COUNT k] [DURATION seconds] [SAMPLE ratio] [count slot [slot ...]]` |  |
| `HOTKEYS STOP` | `HOTKEYS STOP` |  |
| `LATENCY DOCTOR` | `LATENCY DOCTOR` |  |
| `LATENCY GRAPH` | `LATENCY GRAPH event` |  |
| `LATENCY HELP` | `LATENCY HELP` |  |
| `LATENCY HISTOGRAM` | `LATENCY HISTOGRAM [COMMAND] [COMMAND ...]` |  |
| `LATENCY HISTORY` | `LATENCY HISTORY event` |  |
| `LATENCY LATEST` | `LATENCY LATEST` |  |
| `LATENCY RESET` | `LATENCY RESET [event] [event ...]` |  |
| `LOLWUT` | `LOLWUT [VERSION]` |  |
| `MALLOC STATS` | `MALLOC STATS` |  |
| `MEMORY DOCTOR` | `MEMORY DOCTOR` |  |
| `MEMORY HELP` | `MEMORY HELP` |  |
| `MEMORY PURGE` | `MEMORY PURGE` |  |
| `MEMORY STATS` | `MEMORY STATS` |  |
| `MEMORY USAGE` | `MEMORY USAGE key [SAMPLES count]` |  |
| `MODULE` | `MODULE` |  |
| `MODULE HELP` | `MODULE HELP` |  |
| `MODULE LIST` | `MODULE LIST` |  |
| `MODULE LOAD` | `MODULE LOAD path [arg] [arg ...]` |  |
| `MODULE LOADEX` | `MODULE LOADEX path [name value] [configs ...] [ARGS] [args ...]` |  |
| `MODULE UNLOAD` | `MODULE UNLOAD name` |  |
| `RESTORE ASKING` | `RESTORE ASKING key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]` |  |
| `SAVE` | `SAVE` |  |
| `SFLUSH` | `SFLUSH [slot-start slot-last] [data ...] [ASYNC \| SYNC]` |  |
| `SLOWLOG GET` | `SLOWLOG GET [count]` |  |
| `SLOWLOG HELP` | `SLOWLOG HELP` |  |
| `SLOWLOG LEN` | `SLOWLOG LEN` |  |
| `SLOWLOG RESET` | `SLOWLOG RESET` |  |
| `SYNC` | `SYNC` |  |
| `TRIMSLOTS` | `TRIMSLOTS [numranges [startslot endslot] [slots ...]]` |  |

### 末尾.2 集群运维与槽位迁移

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `CLUSTER` | `CLUSTER` |  |
| `CLUSTER ADDSLOTS` | `CLUSTER ADDSLOTS slot [slot ...]` |  |
| `CLUSTER ADDSLOTSRANGE` | `CLUSTER ADDSLOTSRANGE [start-slot end-slot] [range ...]` |  |
| `CLUSTER BUMPEPOCH` | `CLUSTER BUMPEPOCH` |  |
| `CLUSTER COUNTKEYSINSLOT` | `CLUSTER COUNTKEYSINSLOT slot` |  |
| `CLUSTER DELSLOTS` | `CLUSTER DELSLOTS slot [slot ...]` |  |
| `CLUSTER DELSLOTSRANGE` | `CLUSTER DELSLOTSRANGE [start-slot end-slot] [range ...]` |  |
| `CLUSTER FAILOVER` | `CLUSTER FAILOVER [FORCE \| TAKEOVER]` |  |
| `CLUSTER FLUSHSLOTS` | `CLUSTER FLUSHSLOTS` |  |
| `CLUSTER FORGET` | `CLUSTER FORGET node-id` |  |
| `CLUSTER GETKEYSINSLOT` | `CLUSTER GETKEYSINSLOT slot count` |  |
| `CLUSTER HELP` | `CLUSTER HELP` |  |
| `CLUSTER INFO` | `CLUSTER INFO` |  |
| `CLUSTER KEYSLOT` | `CLUSTER KEYSLOT key` |  |
| `CLUSTER LINKS` | `CLUSTER LINKS` |  |
| `CLUSTER MEET` | `CLUSTER MEET ip port [cluster-bus-port]` |  |
| `CLUSTER MIGRATION` | `CLUSTER MIGRATION [[start-slot end-slot] [import ...] \| [ID task-id \| ALL] \| [[ID task-id] \| [ALL]]]` |  |
| `CLUSTER MYID` | `CLUSTER MYID` |  |
| `CLUSTER MYSHARDID` | `CLUSTER MYSHARDID` |  |
| `CLUSTER NODES` | `CLUSTER NODES` |  |
| `CLUSTER REPLICAS` | `CLUSTER REPLICAS node-id` |  |
| `CLUSTER REPLICATE` | `CLUSTER REPLICATE node-id` |  |
| `CLUSTER RESET` | `CLUSTER RESET [HARD \| SOFT]` |  |
| `CLUSTER SAVECONFIG` | `CLUSTER SAVECONFIG` |  |
| `CLUSTER SETSLOT` | `CLUSTER SETSLOT slot [IMPORTING \| MIGRATING \| NODE \| STABLE]` |  |
| `CLUSTER SHARDS` | `CLUSTER SHARDS` |  |
| `CLUSTER SLAVES` | `CLUSTER SLAVES node-id` |  |
| `CLUSTER SLOTS` | `CLUSTER SLOTS` |  |
| `CLUSTER SYNCSLOTS` | `CLUSTER SYNCSLOTS [[task-id [start-slot end-slot] [slot-range ...]] \| RDBCHANNEL task-id \| SNAPSHOT-EOF \| STREAM-EOF \| [state offset] \| FAIL error \| [option [option ...] value [value ...]]]` |  |
| `COUNT FAILURE REPORTS` | `COUNT FAILURE REPORTS node-id` |  |
| `SET CONFIG EPOCH` | `SET CONFIG EPOCH config-epoch` |  |
| `SLOT STATS` | `SLOT STATS [[start-slot end-slot] \| [metric [LIMIT] [ASC \| DESC]]]` |  |

### 末尾.3 哨兵系统

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `GET MASTER ADDR BY NAME` | `GET MASTER ADDR BY NAME master-name` |  |
| `INFO CACHE` | `INFO CACHE nodename [nodename ...]` |  |
| `IS MASTER DOWN BY ADDR` | `IS MASTER DOWN BY ADDR ip port current-epoch runid` |  |
| `PENDING SCRIPTS` | `PENDING SCRIPTS` |  |
| `SENTINEL` | `SENTINEL` |  |
| `SENTINEL CKQUORUM` | `SENTINEL CKQUORUM master-name` |  |
| `SENTINEL CONFIG` | `SENTINEL CONFIG [[parameter value] [set ...] \| GET parameter [parameter ...]]` |  |
| `SENTINEL DEBUG` | `SENTINEL DEBUG [parameter value] [data ...]` |  |
| `SENTINEL FAILOVER` | `SENTINEL FAILOVER master-name` |  |
| `SENTINEL FLUSHCONFIG` | `SENTINEL FLUSHCONFIG` |  |
| `SENTINEL HELP` | `SENTINEL HELP` |  |
| `SENTINEL MASTER` | `SENTINEL MASTER master-name` |  |
| `SENTINEL MASTERS` | `SENTINEL MASTERS` |  |
| `SENTINEL MONITOR` | `SENTINEL MONITOR name ip port quorum` |  |
| `SENTINEL MYID` | `SENTINEL MYID` |  |
| `SENTINEL REMOVE` | `SENTINEL REMOVE master-name` |  |
| `SENTINEL REPLICAS` | `SENTINEL REPLICAS master-name` |  |
| `SENTINEL RESET` | `SENTINEL RESET pattern` |  |
| `SENTINEL SENTINELS` | `SENTINEL SENTINELS master-name` |  |
| `SENTINEL SET` | `SENTINEL SET master-name [option value] [data ...]` |  |
| `SENTINEL SLAVES` | `SENTINEL SLAVES master-name` |  |
| `SIMULATE FAILURE` | `SIMULATE FAILURE [crash-after-election \| crash-after-promotion \| help] [mode ...]` |  |

### 末尾.4 连接追踪与控制

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `CLIENT` | `CLIENT` |  |
| `CLIENT CACHING` | `CLIENT CACHING [YES \| NO]` |  |
| `CLIENT GETNAME` | `CLIENT GETNAME` |  |
| `CLIENT GETREDIR` | `CLIENT GETREDIR` |  |
| `CLIENT HELP` | `CLIENT HELP` |  |
| `CLIENT ID` | `CLIENT ID` |  |
| `CLIENT INFO` | `CLIENT INFO` |  |
| `CLIENT KILL` | `CLIENT KILL [old-format \| [[ID client-id] \| [normal \| master \| slave \| replica \| pubsub] \| [USER username] \| [ADDR] \| [LADDR] \| [YES \| NO] \| [MAXAGE]] [new-format ...]]` |  |
| `CLIENT LIST` | `CLIENT LIST [normal \| master \| replica \| pubsub] [ID client-id] [client-id ...]` |  |
| `CLIENT PAUSE` | `CLIENT PAUSE timeout [WRITE \| ALL]` |  |
| `CLIENT REPLY` | `CLIENT REPLY [ON \| OFF \| SKIP]` |  |
| `CLIENT SETINFO` | `CLIENT SETINFO [lib-name libname \| lib-ver libver]` |  |
| `CLIENT SETNAME` | `CLIENT SETNAME connection-name` |  |
| `CLIENT TRACKING` | `CLIENT TRACKING [ON \| OFF] [REDIRECT client-id] [PREFIX] [prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]` |  |
| `CLIENT TRACKINGINFO` | `CLIENT TRACKINGINFO` |  |
| `CLIENT UNBLOCK` | `CLIENT UNBLOCK client-id [TIMEOUT \| ERROR]` |  |
| `CLIENT UNPAUSE` | `CLIENT UNPAUSE` |  |
| `NO EVICT` | `NO EVICT [ON \| OFF]` |  |
| `NO TOUCH` | `NO TOUCH [ON \| OFF]` |  |

### 末尾.5 脚本与函数库

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `FCALL` | `FCALL function numkeys [key] [key ...] [arg] [arg ...]` |  |
| `FCALL_RO` | `FCALL_RO function numkeys [key] [key ...] [arg] [arg ...]` |  |
| `FUNCTION` | `FUNCTION` |  |
| `FUNCTION DELETE` | `FUNCTION DELETE library-name` |  |
| `FUNCTION DUMP` | `FUNCTION DUMP` |  |
| `FUNCTION FLUSH` | `FUNCTION FLUSH [ASYNC \| SYNC]` |  |
| `FUNCTION HELP` | `FUNCTION HELP` |  |
| `FUNCTION KILL` | `FUNCTION KILL` |  |
| `FUNCTION LIST` | `FUNCTION LIST [LIBRARYNAME library-name-pattern] [WITHCODE]` |  |
| `FUNCTION LOAD` | `FUNCTION LOAD [REPLACE] function-code` |  |
| `FUNCTION RESTORE` | `FUNCTION RESTORE serialized-value [FLUSH \| APPEND \| REPLACE]` |  |
| `FUNCTION STATS` | `FUNCTION STATS` |  |
| `SCRIPT` | `SCRIPT` |  |
| `SCRIPT DEBUG` | `SCRIPT DEBUG [YES \| SYNC \| NO]` |  |
| `SCRIPT EXISTS` | `SCRIPT EXISTS sha1 [sha1 ...]` |  |
| `SCRIPT FLUSH` | `SCRIPT FLUSH [ASYNC \| SYNC]` |  |
| `SCRIPT HELP` | `SCRIPT HELP` |  |
| `SCRIPT KILL` | `SCRIPT KILL` |  |
| `SCRIPT LOAD` | `SCRIPT LOAD script` |  |

### 末尾.6 数组扩展

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `ARCOUNT` | `ARCOUNT key` |  |
| `ARDEL` | `ARDEL key index [index ...]` |  |
| `ARDELRANGE` | `ARDELRANGE key [start end] [range ...]` |  |
| `ARGET` | `ARGET key index` |  |
| `ARGETRANGE` | `ARGETRANGE key start end` |  |
| `ARGREP` | `ARGREP key start end [[EXACT string] \| [MATCH string] \| [GLOB pattern] \| [RE pattern]] [predicate ...] [AND \| OR \| LIMIT \| WITHVALUES \| NOCASE] [options ...]` |  |
| `ARINFO` | `ARINFO key [FULL]` |  |
| `ARINSERT` | `ARINSERT key value [value ...]` |  |
| `ARLASTITEMS` | `ARLASTITEMS key count [REV]` |  |
| `ARLEN` | `ARLEN key` |  |
| `ARMGET` | `ARMGET key index [index ...]` |  |
| `ARMSET` | `ARMSET key [index value] [data ...]` |  |
| `ARNEXT` | `ARNEXT key` |  |
| `AROP` | `AROP key start end [SUM \| MIN \| MAX \| AND \| OR \| XOR \| [MATCH value] \| USED]` |  |
| `ARRING` | `ARRING key size value [value ...]` |  |
| `ARSCAN` | `ARSCAN key start end [LIMIT]` |  |
| `ARSEEK` | `ARSEEK key index` |  |
| `ARSET` | `ARSET key index value [value ...]` |  |

### 末尾.7 流消息运维

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `XCFGSET` | `XCFGSET key [IDMP-DURATION] [IDMP-MAXSIZE]` |  |
| `XGROUP` | `XGROUP` |  |
| `XGROUP CREATE` | `XGROUP CREATE key group [id \| $ new-id] [MKSTREAM] [ENTRIESREAD]` |  |
| `XGROUP CREATECONSUMER` | `XGROUP CREATECONSUMER key group consumer` |  |
| `XGROUP DELCONSUMER` | `XGROUP DELCONSUMER key group consumer` |  |
| `XGROUP DESTROY` | `XGROUP DESTROY key group` |  |
| `XGROUP HELP` | `XGROUP HELP` |  |
| `XGROUP SETID` | `XGROUP SETID key group [id \| $ new-id] [ENTRIESREAD]` |  |
| `XIDMPRECORD` | `XIDMPRECORD key pid iid stream-id` |  |
| `XINFO` | `XINFO` |  |
| `XINFO CONSUMERS` | `XINFO CONSUMERS key group` |  |
| `XINFO GROUPS` | `XINFO GROUPS key` |  |
| `XINFO HELP` | `XINFO HELP` |  |
| `XINFO STREAM` | `XINFO STREAM key [FULL [COUNT]]` |  |

### 末尾.8 通用键运维

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `MIGRATE` | `MIGRATE host port [key \| "" empty-string] destination-db timeout [COPY] [REPLACE] [AUTH \| [username password]] [KEYS] [keys ...]` |  |
| `OBJECT ENCODING` | `OBJECT ENCODING key` |  |
| `OBJECT FREQ` | `OBJECT FREQ key` |  |
| `OBJECT HELP` | `OBJECT HELP` |  |
| `OBJECT IDLETIME` | `OBJECT IDLETIME key` |  |
| `OBJECT REFCOUNT` | `OBJECT REFCOUNT key` |  |
| `WAITAOF` | `WAITAOF numlocal numreplicas timeout` |  |

### 末尾.9 发布订阅控制

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `PUBSUB` | `PUBSUB` |  |
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` |  |
| `PUBSUB HELP` | `PUBSUB HELP` |  |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` |  |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel] [channel ...]` |  |
| `PUBSUB SHARDCHANNELS` | `PUBSUB SHARDCHANNELS [pattern]` |  |
| `PUBSUB SHARDNUMSUB` | `PUBSUB SHARDNUMSUB [shardchannel] [shardchannel ...]` |  |

### 末尾.10 哈希扩展

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `HIMPORT` | `HIMPORT` |  |
| `HIMPORT DISCARD` | `HIMPORT DISCARD fieldset-name` |  |
| `HIMPORT DISCARDALL` | `HIMPORT DISCARDALL` |  |
| `HIMPORT PREPARE` | `HIMPORT PREPARE fieldset-name field [field ...]` |  |
| `HIMPORT SET` | `HIMPORT SET key fieldset-name value [value ...]` |  |

### 末尾.11 基数统计扩展

| 命令 | 语法 | 状态 |
| :--- | :--- | :---: |
| `PFDEBUG` | `PFDEBUG subcommand key` |  |


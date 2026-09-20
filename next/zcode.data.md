# 轮8 数据类型、Redis 命令与参数、TTL 语义审查报告

审查范围：
对照 garnet 支持数据类型（Hash/Set/ZSet/List/Geo/Bitmap/HLL/JSON/Vector）、Redis 命令全族、参数支持与校验、TTL 设计（SessionApis、KeyAdminCommands、Expiration 机制与成员级 TTL）。
rust 对位 wnode/resp、wcol、wkv、wbitmap、whyperlog、wext_json、wext_roaring、wcustom。
检查遗漏、缺失、行为偏离、参数未校验或隐式 panic。

结论概要：
整体主干命令与复杂参数状态机（ZADD 互斥矩阵、GEOSEARCH 多维解析、BITFIELD 溢出控制、HLL 稀疏稠密转码与 RMW TTL 保留、分层集合成员级 TTL 穿透与惰性出账）对齐完备度极高。
本轮审查发现 5 项涉及客户端挂起、错误码偏离、写门漏检破坏索引、大小写不一致与裸 unwrap 隐式 panic 的具体问题。文末附各数据类型与 TTL 全景无缺口确认。


3. MSET 与 MSETNX 批量写入绕过 RangeIndex 写门导致孤儿索引树索引树与元数据孤儿损坏

具体问题：
在基础字符串单键写入口 network_set 中，前置了 ri_write_gate 检查，若目标键存在存活的 RangeIndex 元记录（KeyTag::Meta 且 collection_type == RangeIndex），则拒绝覆写并返回 WRONGTYPE，防止破坏 BfTree 树文件与留下孤儿元数据。
但在 network_mset（快路径）与 slow::mset（慢路径）中，直接将全量键值对送入 store.try_upsert_batch_sync 与 storage.upsert_string，完全绕过了 ri_write_gate 检查。
客户端若调用 MSET ri_key value，会直接向存储写入普通 String 记录，覆盖 RangeIndex 存根但保留磁盘索引树，造成元数据不一致和孤儿索引树文件损坏。

rust 文件与函数：
wedb/wnode/src/resp/array_commands.rs: network_mset 与 slow::mset

c# 对应文件与函数：
libs/server/Resp/Parser/RespCommand.cs: IsLegalOnRangeIndex
libs/server/Storage/Session/MainStore/MainStoreOps.cs: MSET



6. 数据类型与 TTL 全族无缺口确认清单（后续审查免重查）

String 族：
SET 全选项状态机（EX/PX/EXAT/PXAT/KEEPTTL/NX/XX/GET）、GETEX 四态与溢出钳制、GETDEL 原子删除退回旧值、GETRANGE/SUBSTR 负下标与越界截断、SETRANGE 512MB 上限与原位扩写、APPEND 扩容机制、INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT 溢出检查、MGET 批量预取、MSETNX 全有或全无原子语义，除上述 MSET 的 RI 写门外对位完整。

Hash 族：
HSET/HSETNX/HMSET 多字段写入、HGET/HMGET/HGETALL 字段读取、HDEL 字段删除与删空清退、HEXISTS/HLEN/HSTRLEN 计数与长度、HINCRBY/HINCRBYFLOAT 浮点增减与 NaN 拦截、HRANDFIELD(WITHVALUES) 随机抽取、HSCAN(MATCH/COUNT/NOVALUES) 游标遍历、HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT 选项组合(NX/XX/GT/LT)与字段级过期、HTTL/HPTTL/HPERSIST/HEXPIRETIME/HPEXPIRETIME 字段级 TTL 查询与持久化，对位完整。

Set 族：
SADD/SREM 成员增删与计数、SPOP/SRANDMEMBER 正负 count 语义与多抽样、SMEMBERS/SCARD/SISMEMBER/SMISMEMBER 成员判定与多成员匹配、SINTER/SUNION/SDIFF 集合多路运算、SINTERSTORE/SUNIONSTORE/SDIFFSTORE 运算结果落盘、SINTERCARD(LIMIT) 限制交集计数，除上述裸 unwrap 外对位完整。

ZSet 族：
ZADD 全选项互斥状态机（NX/XX/GT/LT/CH/INCR）、ZREM 成员删除、ZINCRBY 分值增减、ZCARD/ZSCORE/ZMSCORE 查询、ZCOUNT/ZLEXCOUNT 区间计数、ZRANGE(BYSCORE/BYLEX/REV/LIMIT) 组合查询、ZRANGESTORE 结果转存、ZREMRANGEBYRANK/ZREMRANGEBYSCORE/ZREMRANGEBYLEX 区间删除、ZRANK/ZREVRANK 排名获取、ZPOPMIN/ZPOPMAX/ZMPOP/BZMPOP 多键弹出与阻塞等待、ZRANDMEMBER 随机成员、ZINTER/ZUNION/ZDIFF 多路加权聚合运算、ZINTERCARD 限制交集计数、ZSCAN 游标遍历、ZEXPIRE 族成员级过期，对位完整。

List 族：
LPUSH/RPUSH/LPUSHX/RPUSHX 双端压入、LPOP/RPOP 单多项弹出、LMPOP/BLMPOP 多键弹出与阻塞经纪接线、LLEN/LINDEX/LINSERT/LSET/LRANGE/LTRIM/LREM 元素修剪与索引读写、LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH 双端迁移与阻塞迁移，除上述 LPOS 选项大小写外对位完整。

Geo 族：
GEOADD(NX/XX/CH) 坐标写入与更新计数、GEODIST 距离计算与四单位换算、GEOHASH 52位编码转Base32、GEOPOS 经纬度还原与F6格式化、GEOSEARCH/GEOSEARCHSTORE(FROMMEMBER/FROMLONLAT/BYRADIUS/BYBOX/WITHCOORD/WITHDIST/WITHHASH/COUNT/ANY/ASC/DESC) 全参数搜索与存储变体、GEORADIUS(_RO)/GEORADIUSBYMEMBER(_RO) 兼容变体，对位完整。

Bitmap 族：
SETBIT/GETBIT 位级读写与上限检查、BITCOUNT(BYTE/BIT) 区间计数、BITPOS 查找首个置位、BITOP(AND/OR/XOR/NOT) 多键位运算与单源限制、BITFIELD/BITFIELD_RO(GET/SET/INCRBY/OVERFLOW WRAP/SAT/FAIL) 任意位宽整型操作，除 dest 键 RI 门已保护外对位完整。

HLL 族：
PFADD 元素基数登记与稀疏/稠密自适应编码、PFCOUNT 单键与多键虚拟并集估算、PFMERGE 零源与多源择大并入、冷数据异步装载 RMW 保留既有 TTL、非法载荷直接 WRONGTYPE 判定，对位完整。

JSON 族：
JSON.SET(NX/XX/$/根路径) 写入状态机、JSON.GET(INDENT/NEWLINE/SPACE/多路径) 格式化读取、JSON.DEL 路径删除、JSON.TYPE/JSON.NUMINCRBY/JSON.NUMMULTBY/JSON.STRAPPEND/JSON.STRLEN/JSON.ARRAPPEND/JSON.ARRINDEX/JSON.ARRINSERT/JSON.ARRLEN/JSON.ARRPOP/JSON.ARRTRIM/JSON.OBJKEYS/JSON.OBJLEN/JSON.TOGGLE/JSON.CLEAR 路径操作与 Sonic 引擎对位完整。

Vector 族：
VADD 向量索引创建与属性挂载、VSIM 向量余弦/欧氏/点积近似近邻检索、VEMB 向量嵌入查询、VCARD 维度内向量计数、VDIM 维度查询、VGETATTR/VSETATTR 动态属性存取、VINFO 索引元数据、VISMEMBER 存在判定、VLINKS 图连接查询、VRANDMEMBER 随机采样、VREM 向量删除，对位完整。

TTL 机制：
EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 全选项（NX/XX/GT/LT）状态机、TTL/PTTL/EXPIRETIME/PEXPIRETIME -2（不存在）/-1（无TTL）语义规范、PERSIST 独立清除、过期时间戳物理删键驱动、RENAME/RENAMENX 随键迁移 TTL 与 ETag、SCAN MATCH/COUNT/TYPE 过滤、EXPDELSCAN 互斥门与后台扫描联动、双域（String/Envelope/Meta）一致性读与过期自动惰性清退闭环。

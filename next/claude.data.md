[类型兼容] 移除 RangeIndex 在 GarnetObjectType 中的错误归属
c#: garnet/libs/server/Objects/Types/GarnetObjectType.cs:GarnetObjectType.GarnetObjectType()
rust: wedb/wval/src/tag.rs:98 (GarnetObjectType)
现状: Rust 端将 RangeIndex = 5 混入 GarnetObjectType 枚举，而 Garnet 原生未包含，导致序列化越界。
方案: 从 wval/src/tag.rs 中移除 RangeIndex 成员，并在 KeyTag 物理键前缀中为其单独分配 0x0D，将 RangeIndex 的底层序列化与常规对象彻底解耦。

[性能调优] 修复 Hash 集合对象 count() 的 O(N) 性能违规问题
c#: garnet/libs/server/Objects/Hash/HashObject.cs:HashObject.Count()
rust: wedb/wcol/src/hash/hash_object.rs:534 (count)
现状: Rust 端 count(&self) 在每次调用时 O(N) 遍历 times.keys() 过滤过期键，导致巨型对象下引发严重阻塞。
方案: 改造 wcol/src/hash/hash_object.rs 以及 set/zset 的 count 统计。设计一个带缓存的字段记录内部过期项或采用惰性删除方式，使 count 恒为 len - expired_count 直接计算的 O(1) 耗时，不再进行整体迭代。

[特性补全] 全面补全 Stream 数据结构及其核心相关命令
c#: garnet/libs/server/Objects/Stream/StreamObject.cs:StreamObject.Operate()
rust: wedb/wcol/src/stream:缺失 (缺失)
现状: 尚未发现 Stream 类型（XADD/XREAD/XRANGE）的存储层结构与指令解析。
方案: 新建 wcol/src/stream 模块，使用 Radix Tree 结合列表结构存储消息，并将对应命令路由接入 wnode/src/resp 层的会话对象调度。

[指令对齐] 对齐 TTL 与 ExpireOption 边界控制在 Rust 与 C# 中的行为
c#: garnet/libs/server/Objects/Hash/HashObject.cs:HashObject.SetExpiration(ExpirationWithOption)
rust: wedb/wcol/src/hash/hash_object.rs:568 (hash_expire)
现状: Rust 中对 NX/XX/GT/LT 判定虽然存在，但与 C# 的 ExpireResult 响应状态码及边界条件的容错对齐不够。
方案: 细化 wcol/src/hash/hash_object.rs 的 hash_expire 判断分支，统一 TTL 响应与 wval/src/ttl.rs 的 Ticks 解析，严格按照 C# 返回 KeyNotFound/ConditionNotMet 状态码处理容错。

[指令对齐] 检查并优化跨键命令 (ZDIFF/ZUNION) 零拷贝
c#: garnet/libs/server/Resp/Objects/SortedSetCommands.cs:RespServerSession.ZDIFF()
rust: wedb/wnode/src/storage/session/objectstore/sorted_set_ops.rs:317 (zdiff)
现状: 当前 wnode 已实现该系列操作，但在多轮交集或差集的底层链路上可能存在大量中间 Vec 分配，未完全符合 SKILL.md 要求。
方案: 在 wnode/src/storage/session/objectstore/ 中审查 zdiff/zunion 等函数，摒弃多余收集步骤。引入高效迭代器链（Iterator Chain），结合 BTreeMap 确保全程零拷贝并避免大内存储存。

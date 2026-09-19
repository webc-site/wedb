优先级：中

问题
ACL 物理键直写逻辑命名空间，与日志紧缩判死域冲突可误删存活用户。AclStore 以调用方逻辑 ns 直接编码物理前缀 SessionPrefixBuf::new(ns, 0)（不经 vdb 映射虚号域），记录的物理 vns 槽位即逻辑 ns 数值。紧缩谓词 is_deleted 将物理 vns/vdb 对 gc_dead 死亡账本判死（KeyTag::DbMeta 有豁免、KeyTag::Acl 无）：某退役虚拟命名空间 ID 数值恰等于某租户逻辑 ns 时（虚号自 1 自增、逻辑 ns 为任意 u64，碰撞现实可达），该租户全部存活 ACL 记录在紧缩中被物理抹除，且无任何日志告警面。

取证（dev 当下代码重取）
wedb/wnode/src/resp/acl_store.rs:42-50 prefix/physical_key（SessionPrefixBuf::new(ns, ACL_DB=0)、NamespaceDbCodec::encode_tagged_key(ns, 0, KeyTag::Acl, username)——ns 参数即 caller_namespace 逻辑值，acl_commands.rs 调用处传 ctx.caller_namespace）。wedb/wkv/src/compact.rs:197-212 is_deleted：:203-205 仅豁免 KeyTag::DbMeta，:206-212 is_virtual_id_dead_and_expired(rec_vns, rec_vdb, now) 对 Acl 记录同样生效。wedb/wkv/src/vdb.rs:1019-1027/:1095-1103 gc_dead 收录退役 vns/vdb。

C# 对标
garnet/libs/server/ACL/AccessControlList.cs（C# ACL 为内存全局字典 + 可选文件，无紧缩交互；ACL 落盘存储为 SKILL「KeyTag::Acl 0x0D 存入底层存储、零全局内存」自定义设计）。

修法建议
推荐：is_deleted 将 KeyTag::Acl 加入与 KeyTag::DbMeta 同级的紧缩判死豁免（ACL 域的 ns 是逻辑号、永不换号退役，判死域对其不适用；注释写明两域差异）。备选：AclStore 经 vdb 映射虚号域编码（代价：每次认证/改权限多一次映射查询与冷装载路径，收益仅概念统一）。禁两案并存。

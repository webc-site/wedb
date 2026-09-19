拒件：迁移 sketch 按 key 字节哈希分槽与库级定槽两哈希域分叉易误读，主张头注写清

来源：next/muse.my.md 条 13。判定：不成立（C# 同形；模块边界注释已在位）。

拒绝原因
1 C# 对位同形：garnet/libs/cluster/Server/Migration/Sketch.cs 的迁移门控 bloom bitmap 本就按 key 哈希置位/探测（C# 全键空间 CRC16 槽位体系下的迁移去重结构）；rust wedb/wedb/src/server/migration/sketch.rs probe_with_seed 按 key 哈希探测与 C# 一一对应，非「键哈希定槽回潮」。
2 库级定槽单点在 wedb/wbase/src/hash_slot.rs slot_of（SKILL「集群以 namespace -> db 为唯一分片」），与 sketch 的去重哈希是两个已分离的单点；sketch.rs:8-11 模块头注已声明边界（「传输与待删清单由调用方结构承担…不承载 C# argSliceVector / Keys 的收集去重职责」）。
3 「头注写清 sketch 不参与定槽」属一句话注释打磨，无缺口无行为面，不立项；如顺手可并入任一触及迁移模块的在途票。

引证
wedb/wedb/src/server/migration/sketch.rs:7-11/:78-79；wedb/wbase/src/hash_slot.rs slot_of；garnet/libs/cluster/Server/Migration/Sketch.cs、garnet/libs/cluster/Utils/HashSlotUtils.cs:GetSlot。

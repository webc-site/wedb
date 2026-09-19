拒件：降阶预筛只有条目维、体积维物化后判定，「大体积小条目树永不降阶」

来源：next/muse.my.md 条 2。判定：不成立（前提错误 + 头注如实，非「自称与实现矛盾」）。

拒绝原因
1 「大体积小条目树永不降阶」是双门限迟滞设计的正确行为而非缺陷：should_demote 要求 count AND heap_bytes 双低（SKILL「条目数（65536/32768）与内存体积（4MB/2MB）双维度双门限迟滞死区」），体积超 2MB 的树本就不应降阶——永不降阶恰是设计意图。
2 「头注自称双维单点与实现矛盾」不实：wcol 分层头注与 tiered_demote.rs:13-16 如实写明「体积维决策复用同一谓词单点 should_demote（count AND heap_bytes 双维），经分层物化单源通道构造内存对象后判定…绝不全树扫体积（MetaValue 无体积标量，wval/src/meta.rs）」——即明示体积维在物化后判定，无自称矛盾。
3 预筛补体积水位需 MetaValue 增体积标量：头注已论证 MetaValue 无体积标量且 24B 头部布局受单缓存行双记录口径约束（tiered_collection_ops.rs list_head_seq 文档同论证），属设计变更非缺口。
4 该条唯一有价值的残余（hopeless 候选每轮反复物化挤占 16 名额、饿死真候选的饥饿面）已并入 next/my-tiered-demote-root-domain-scan.md 修法建议，不在本拒件丢失。

引证
wedb/wnode/src/resp/objects/tiered_demote.rs:11-16/:135-188；wedb/wcol/src/types/garnet_object.rs should_demote 谓词；doc/zh/collection.md 3.2/3.3。

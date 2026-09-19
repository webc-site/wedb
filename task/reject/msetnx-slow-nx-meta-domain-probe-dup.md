重复：task/ing/msetnx-slow-path-meta-domain-probe.md（关键符号 MSETNX/Meta 域探测/双域键 命中）
优先级：高

MSETNX 慢路径 NX 判定只探 String/ObjectEnvelope 两域漏 Meta 域，升阶冷键被误判不存在后写入 String 值造成双域键
    快路径 probe_alive_with_prefix 三域判活（String→ObjectEnvelope→Meta 依序），升阶键的 Meta 元记录为磁盘候选时返回 Ok(None) 整体降级慢路径；慢路径 !resume 判定段仅 read_tag_with 探 KeyTag::String 与 KeyTag::ObjectEnvelope 两域，而升阶键信封已在 promote_collection_to_bftree 内删除（仅 upsert_raw 元记录 + delete_raw 信封），Meta 域未探 → 判"键不存在" → 写入循环 upsert_string 落 String 值并回 :1。C# MSET_Conditional 的 EXISTS 走 unified 域，任意记录（含对象）非 NOTFOUND 即判存在回 :0 且零写入。后果：String 值与大集合 Meta 元记录并存，后续读 String 域优先命中遮蔽原集合，bftree 树文件成孤儿。降级触发条件（Meta 磁盘候选）与缺口耦合：一旦 MSETNX 因升阶冷键降级，慢路径必判其不存在，缺口近乎必现。修法：判定循环补第三探（read_tag_with KeyTag::Meta，口径对齐同文件 StorageSession::exists 三域）。
    rust：wedb/wnode/src/resp/garnet_api/slow.rs:124-148（Msetnx !resume 判定循环）；三域单点对照 wedb/wnode/src/storage/session/common/ttl_sync.rs:482-517 probe_alive_domain_with_prefix、wedb/wnode/src/storage/session/storage_session.rs:231-256 exists；升阶删信封 wedb/wnode/src/resp/objects/object_store_utils.rs:1041-1042
    C#：garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:349-397 MSET_Conditional（unified 域 EXISTS 判定，非 NOTFOUND 即 error=true 全批不写）

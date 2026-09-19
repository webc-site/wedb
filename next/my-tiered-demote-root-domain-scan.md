优先级：中

问题
后台懒降阶评估轮硬编码只扫主库根域 (ns 0, db 0)。tiered_demote_round 用 store.new_session() 默认域会话，collect_demote_candidates 以该会话 prefix_slice 做 strip_prefix 过滤，全库 hlog 扫描只匹配根域前缀。所有 ns > 0 租户与 db > 0 库的分层集合永远不进候选，跌入迟滞死区之下的分层树无法被周期任务降阶释放，违背 SKILL「双门限迟滞死区与懒降阶双向转换保护」对全体键的承诺。模块头注:78 自述「会话默认域（主库 ns0/db0）」即自认局限。

取证（dev 当下代码重取）
wedb/wnode/src/resp/objects/tiered_demote.rs:79-84 tiered_demote_round（store.new_session + StorageSession::new_readonly，默认根域）；:139-149 collect_demote_candidates `let prefix = storage.batch.session_prefix()` + `key.strip_prefix(prefix_slice)`；:151-176 仅对命中前缀的 KeyTag::Meta 记录预筛。对照同仓跨域扫描先例：wedb/wkv/src/compact.rs:198 NamespaceDbCodec::decode_tagged_key 解 (rec_vns, rec_vdb, tag, user_key) 无会话前缀约束。

C# 对标
garnet/libs/server/Databases/DatabaseManagerBase.cs:ExecuteObjectCollection（C# 单租户单域，无跨域对位；多租户多库为 rust 自定义设计，doc/zh/db.md）。

修法建议
预筛移除会话前缀约束，物理键直接 decode 出 (vns, vdb, tag)，跨全量租户与数据库收集候选；单候选评估与写回（obj_save / handle_bftree_drain_and_delete）须先 set_virtual_context(vns, vdb) 落到条目域执行，禁用会话域前缀拼键。顺带评估饥饿面：预筛只有条目维，体积超限永不降阶的键（count 低 bytes 高）每轮反复物化挤占 16 个名额且永不出局——加负缓存或失败水位标注，避免 hopeless 候选饿死他域真候选（体积维不进预筛本身是 MetaValue 无体积标量的既定设计，见 task/reject/my-demote-volume-prefilter.md）。

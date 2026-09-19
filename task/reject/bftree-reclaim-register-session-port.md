# 换号回收旁表登记的会话侧单点收口（承接 next/qcode8.db.md 条 2，该文件已核销删除）

已失效（2026-09-19 核验）：票据描述的三参十站现状在当前 dev 基线中不存在，方案四步已全部落地——
会话一参口 wedb/wkv/src/session/mod.rs:389 `register_bftree_key(&self, key)` 与 :400
`unregister_bftree_key(&self, key)`（体内一次 load 两原子转调内核）；九个会话站全部一参调用
（stub.rs:56/157/181/295、ops.rs:90/104、migration.rs:56/187、session/raw/write/mod.rs:144，
行号为当前基线）；恢复站 store/cpr_host.rs:241 保留三参直调 store 内核；reclaim.rs:35-51
内核 doc 已写明「会话态取当前域 / 恢复态取解出域」两形态分工。票据系对照旧快照生成
（guard-recheck 982ad2b8 前后行号全漂移），无剩余工作量。

## 现状

被调方签名仍是三参：wedb/wkv/src/store/reclaim.rs:40 `register_bftree_key(&self, vns: u64,
vdb: u64, key: &[u8])`、:55 `unregister_bftree_key(&self, vns, vdb, key)`。
调用侧每处手写「取两个会话原子 + 传三元组」，实测 10 站（原报 7 站，随开发已增 3 站，扩散中）：

1. wedb/wkv/src/range_index/stub.rs:51-54（handle_bftree_drain_and_delete 注销）
2. wedb/wkv/src/range_index/stub.rs:156-159（promote_collection_to_bftree 注册）
3. wedb/wkv/src/range_index/stub.rs:184-187（emit 失败回滚注销）
4. wedb/wkv/src/range_index/stub.rs:302-305（acquire_tree_read 路径注册）
5. wedb/wkv/src/range_index/migration.rs:56-59（publish_migrated_range_index）
6. wedb/wkv/src/range_index/migration.rs:191-194
7. wedb/wkv/src/range_index/ops.rs:89-92（range_index_create 注册）
8. wedb/wkv/src/range_index/ops.rs:107-110（upsert 失败回滚注销）
9. wedb/wkv/src/session/raw/write/mod.rs:144-147
10. wedb/wkv/src/store/cpr_host.rs:210-212（恢复期登记，域来自
    NamespaceDbCodec::decode_tagged_key，形态与其余九站不同，见方案第 3 步）

每站重复 `self.active_vns.load(Ordering::Relaxed)` 与 `self.active_vdb.load(Ordering::Relaxed)`，
两参顺序颠倒或漏传一域无编译期防护，后果是 FLUSHDB 换号后同名重建被 IndexExists 拦截
（旁表残留 pending 注册）。

## C# 参考

garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:396 RegisterIndex、:457 RegisterPending、
:471 UnregisterIndex——一律只收 keyBytes（外加 bfTree/keyHash），身份在单点内部派生：
:398-400 与 :459-461 走 `HashKeyToPrefix(keyBytes)` + `KeyId(keyBytes)`，:473-474 注销同样自算
keyId。调用方无从传递任何派生量，故 C# 不存在「漏传身份参数」这一失败模式。

## 方案

1. 在持有 active_vns/active_vdb 的会话类型（wkv session 侧，即上述 1-9 站 `self` 的类型）补两个
   一参会话方法：`register_bftree_key(&self, key: &[u8])`、`unregister_bftree_key(&self, key: &[u8])`，
   体内一次性 load 两原子（Relaxed，与现口径一致）后转调 store::reclaim 内核。
2. 1-9 站全部改为一参调用，删除各站的两次原子 load 与三元组样板；方法名与 store 侧内核同名，
   靠接收者区分层（会话 vs WedbStore），不改 reclaim.rs 的 pub(crate) 三参内核——它仍是唯一落表点。
3. 恢复站 cpr_host.rs:210-212 的域来自物理键前缀解码（彼时 vdb 映射尚未重建，不能用会话原子），
   保留三参直调 store 内核，并在 reclaim.rs:40 的 doc 里写明两形态分工（会话态取当前域、
   恢复态取解出域），杜绝后来者把恢复站也改成一参而错取活跃域。
4. 不做向下兼容处理，不保留旧的三参会话可见口（会话侧三参入口若存在即删）。

## 验收

1. `grep -rn "active_vns.load" wedb/wkv/src/range_index wedb/wkv/src/session` 零命中。
2. 既有用例保持通过：wkv 的 FLUSHDB 换号后同名重建、RI 升阶/迁移注册面（含 emit 失败回滚臂）。
3. clippy 无新增告警（禁写 allow）。

优先级：重复/多套架构（同一身份取数样板十份、无编译期防护、随开发在扩散）。

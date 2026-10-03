## 终态注记（合入哈希：0b7baea / 收口形态：删除 is_replaying 孤儿死字段）
- 合入哈希：0b7baea
- 收口形态：删 transaction_manager.rs:164 声明及两处 new/reset 仅置 false 代码，集群归属校验由 verify_cluster_txn_keys 独立承接，消除零读零置真孤儿字段。

审核结论：通过，定案删（P3 治理）

分席勘误与确证（供 fix 直接消费）：
- 订正：rust 事务侧实有集群归属闸 verify_cluster_txn_keys（wtxn/src/txn_session.rs:97 定义、wnode cluster_session.rs 承接），只是该闸不消费回放标志；正文「无对应承接臂」据此订正为「承接臂已另立且不依赖回放标志」。
- 置真点零命中、消费点零命中，字段纯悬挂死位；本单非重复立案（台账裁决的是 VerifyKeyOwnership 本体，未收口此残留字段）。

优化执行方案：删 transaction_manager.rs:164 声明及 :204/:230 两处 is_replaying 置 false；编译期零引用自证，无行为改动。

TransactionManager.is_replaying 系零写真点零消费的孤儿字段（C# 唯一消费面 VerifyKeyOwnership 未承接）

问题分析：
1. Garnet 契约对齐（C# 原型行为）
C# TransactionManager.IsReplaying（garnet/libs/server/Transaction/TransactionManager.cs:135）在 AOF/复制回放路径置真（TransactionManager.cs:291 IsReplaying = isReplaying），全仓唯一消费面为 TxnKeyManager.VerifyKeyOwnership（garnet/libs/server/Transaction/TxnKeyManager.cs:46-48）：if (!clusterEnabled || IsReplaying) return，即回放期跳过集群键归属校验。置位与消费闭环。

2. 工程现状确证（Rust 实现路径）
rust TransactionManager 携 pub is_replaying: bool（wedb/wtxn/src/transaction_manager.rs:164），仅在 new（:204）与 reset（:230）置 false，全仓 grep 无处置真、无处读取。对应 C# 唯一消费面 VerifyKeyOwnership 的集群归属校验在 rust 事务侧无对应承接臂，该字段遂成悬挂：既无写方（AOF/复制回放不置位）也无读方（无 VerifyKeyOwnership 消费）。

3. 逻辑危害确证
属治理面而非运行时危害：pub 字段零调用零消费，触 task/review.md 板块 1「零死代码与假桩清退」与 rust_review 运行时纪律「pub API 孤儿（零调用导出）定期清理」。留之误导后续对账者以为存在回放归属闸，实为死位。
严重度 P3：仅台账与死码收口，不改行为。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/transaction_manager.rs:TransactionManager.is_replaying（:164 声明，:204/:230 仅置 false，零读零置真）

对应 c# 文件与函数：
libs/server/Transaction/TransactionManager.cs:IsReplaying（:135/:291）
libs/server/Transaction/TxnKeyManager.cs:VerifyKeyOwnership（:46-48，唯一消费面）

精炼执行方案：
1. 二选一，禁留半态：若集群归属校验（VerifyKeyOwnership）本域已在别处（如 wcoord/cluster 域）另立承接臂，则删除 is_replaying 字段及其 new/reset 两处置 false；
2. 若该闸确属未承接的应有契约，则连同回放置真点与消费判据一并接线，勿只留字段。
3. 测试验证点：删则编译期即证零引用（cargo build/clippy 无残留）；接则补回放期跳过归属、非回放期执行归属的对拍用例。
分诊须先 grep 全仓确认归属校验承接臂之有无，据此在删/接二案中定案。

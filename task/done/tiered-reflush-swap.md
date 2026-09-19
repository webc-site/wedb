分层重灌臂改先建后拆：复用 publish_tree_from_snapshot_locked 原子换入，消除键蒸发窗口

认领自 next/tiered-reflush-atomic-swap.md（票面及 stub 件 tiered-reload-atomic-publish-swap.md 均已删）。
取证基线：主仓 /Users/z/git/db/wedb dev 工作树 f50bb41，票面行号已按当下代码重取（重灌臂早已自
object_store_utils.rs 迁至 rmw_helpers.rs）。

甄别结论：票面主张全部成立。现证据：
- 重灌臂先拆后建：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/rmw_helpers.rs:377-398，
  tiered 时 :381-385 先 handle_bftree_drain_and_delete(key, true)（元记录墓碑 + delete_index 摘注册并
  投递树文件删除，wkv/src/range_index/stub.rs:67-89），随后 :391 promote_collection_to_bftree 重建。
  drain 成功而 promote 失败（或两步之间崩溃）即丢树：分层态信封本就不存在，键整体蒸发，
  连墓碑兜底也只保「无旧数据可读」，不是数据无损。姊妹票（排空臂信封幂等墓碑）已落地
  （stub.rs:45-66 头注与 collection.rs:7-11 头注），换序主张仍优于墓碑兜底形态，采纳。
- 同题第二处：到期整值重灌臂 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs
  :338-347（expire_sweep_or_rebuild 非空臂）同型 drain(…, true) 后 promote。票面正文未点名，但其
  验收 grep 口径「重灌路径不再调用 handle_bftree_drain_and_delete(…, true) 做销毁重建」覆盖该臂，
  同一机制一并收口。
- 原子换入内核在位：/Users/z/git/db/wedb/wedb/wbftree/src/manager/lifecycle.rs:483-549
  publish_tree_from_snapshot_locked(key, snapshot_path, replace)（票面行号 :440-470 漂移，本体在位）：
  锁内 remove_and_take_tree + dispose_bf_tree_deferred 排空旧树 → rename 原子换入 → fsync 双屏障 →
  恢复注册。生产消费现仅 wkv/src/range_index/migration.rs:50 一处。
- create_bftree_internal 的 IndexExists 拦截在 lifecycle.rs:104-108（防重门保留，RI.CREATE 专用）。
- C# 依据：对象记录重写单日志记录原子、HasExpiration 前移零 TTL 事件
  garnet/libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:33-44 GetRMWModifiedFieldInfo；
  发布件 garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:352 附近
  PublishMigratedIndex（其内部 TODO「Propagate replaceOption in the migrated stream」在 rust 侧
  由本票承接——重灌流必须携 replace，副本才可在旧树在位时回放，见修法四）。

方案（单一机制，先建后拆，杜绝两条建树工序）：

一、wbftree 新增建树+快照单内核（manager/lifecycle.rs）：
  build_collection_tree_snapshot(entries, tuning) -> Result<(快照路径, 去重条数)>：
  在 migration-tmp 下 instantiate 未注册临时工作树 → bulk_load → cpr_snapshot 至独立临时文件 →
  dispose 临时树并删除工作文件（migration-tmp 启动即 remove_dir_all，残件无泄漏类）。
  instantiate_tree 由 hash_prefix 参数改收显式数据路径（Disk 传入派生路径、Memory 传 None），
  create_bftree_internal 与 get_or_open_tree 两既有调用点同批改指，不留第二构造口。
  bulk_load 拒绝新增 Error::LoadRejected(BfTreeInsertResult) 变体上抛，wkv 侧按既有口径映射
  KeyTooLong / InvalidArgument。
  publish_tree_from_snapshot_locked 微调：快照文件存在性检查前移到摘旧树之前（现 :506 在
  :499-504 摘除之后，失败即旧树已摘、新树未立的自伤窗）；其余内核不动。

二、wkv 重铸 promote_collection_to_bftree（wkv/src/range_index/stub.rs:133-230），加末参
  replace: bool（首升阶 false / 分层重灌 true），执行序改为：
  1. 卸载阻塞线程 build_collection_tree_snapshot 得 (snap, count)（建树全程不触注册表，旧树
     原态可读，杜绝现「先删文件再重建」窗）。
  2. 构造发布存根 RangeIndexStub::from_tuning(0, tuning, Disk)（句柄瞬态，消费侧 rebind_stub
     重绑，与迁移流同口径）。
  3. 先发 RangeIndexStream AOF 数据通道事件（携 replace 标志，见三），同步读快照文件灌块；
     入队失败即删快照文件上抛——旧树、旧 meta、TTL、信封全原态，回滚不再「拆完才报错」。
  4. 卸载阻塞线程持键条带写锁 publish_tree_from_snapshot_locked(key, snap, replace)：
     replace=true 锁内摘旧树延迟释放 + rename 原子换入 + 恢复注册，换入窗口无文件缺失间隙；
     replace=false 保留 IndexExists 防重门（票面修法4 的门从 create_bftree 挪至此，判据同
     live_indexes 单源，仍一锁一源、不新增强制覆盖旁路）。失败删残件上抛（此时旧树在位）。
  5. register_bftree_key 换号旁表登记（set 插入幂等，重灌臂因不再 drain 也就不再 unregister，
     全程在册）。
  6. 新树句柄重绑存根 + MetaValue::new_with_expiry(key_id, tag, count, next_expiry)，
     upsert_raw 元记录。
  7. delete_raw 信封域幂等墓碑（首升阶删实存信封；重灌臂探针落空零写，与现 promote 尾步同口）。
  既「建树+灌入+快照」与「发布+落 meta+清信封」前后段在首升阶与重灌两态共用同一函数，
  仅 replace 一参分流，禁另写第二份建树工序；「先发流后落 meta」不变量保持（流在 3、meta 在 6）。

三、调用侧改点：
  - rmw_helpers.rs apply_rmw_post_operate 重灌分支：删去 :378-386 的 tiered 前置 drain，promote
    传 replace = tiered；keep_ttl 语义天然成立（不再有任何 drain → 零 TTL 事件、零 RangeIndexDrop）；
    WATCH 显式推进一臂不变（promote 仍只 upsert_raw/delete_raw 物理原语）。头注同步。
  - tiered_collection_ops.rs expire_sweep_or_rebuild 非空重灌臂：删 drain(…, true)，promote 传
    replace=true；drop(tree_guard) 前置次序不变（publish 自取同键条带写锁）；删空臂 keep_ttl=false
    整键回收不动。
  - handle_bftree_drain_and_delete 头注（stub.rs:61-64）去掉对重灌臂的指涉（keep_ttl=true 现仅
    服务降阶/目标清退臂）。
  - collection.rs 头注 :7-11 设计口径改述：升阶发布换用原子换入内核，重灌臂无销毁重建窗；
    首升阶三步流（流→meta→删信封）残留兜底表述保留。

四、副本/AOF 回放面（重灌流可回放的必要配套，非新机制）：
  Stream 事件不再前导 RangeIndexDrop，副本 live_indexes 旧树必在册，固定 replace=false 的
  publish 会以 AlreadyExists 拒回放。给既有流通道补 replace 一位：
  - StoreEvent::RangeIndexStream 加 replace: bool（wkv/src/store/event.rs:84-91）。
  - wnode/src/service.rs:316-345 透传至 RangeIndexStreamArgs。
  - range_index_manager_replication.rs：流块 arg1 标志位新增 REPLACE bit（现 IS_LAST=1、
    IS_FIRST=2，pack/unpack 扩一位，每块携载与 obj_type 同形）；process_stream_chunk 完成臂
    改 publish_migrated_index(..., replace) 并就地核销 :435 TODO(RangeIndex)（该 TODO 的
    user-RI 迁移流通道 wconn frame 不动，仅 AOF 流通道编码，无格式兼容诉求）。
  副本收敛链不变：流块 publish（replace）→ 其后 meta 域 StoreUpsert 条目回放到终态
  （next_expiry / count 以 master 元记录为准）。

五、旧代码直删：promote 内 create_bftree + snapshot_tree_to_path_locked + 装载失败 delete_index
  回滚臂、emit 失败「delete_index + unregister」回滚臂（新形态回滚即删快照残件，旧树未动过）
  一并移除；不做双轨开关，不留兼容分支。create_bftree 本体保留（RI.CREATE 唯一出口）。

测试面：既有 promote 调用测试补 replace 实参（首升阶 false；重灌形态用例改传 true 并删前置
drain）：wkv/tests/store/{tiered_drain_envelope,range_index,flush_database}.rs、
wnode/tests/{tiered_promote_demote_ttl,tiered_field_ttl,tiered_watch_fence}.rs；
wnode/tests/range_index_replication.rs 的 RangeIndexStreamArgs 构造与 flags 编解码用例补
replace 位断言；wnode/tests/tiered_promote_aof_replay.rs 增加重灌流回放（同名二次升阶
replace=true 回放不报 AlreadyExists）断言。票面验收判据（kill -9 / 模拟发布前失败重启键仍
可读、往返无孤儿文件、grep 取证）随上述形态成立；集成用例覆盖可判定的后半（重启可读、
注册表/文件在册计数）。

边界与交叉引用（同域在册票，避让声明）：
- task/ing/wkv-range-index-stub-file-split.md（在途）：该票计划把 stub.rs 按域拆为
  promote.rs/drain.rs 等四件，与本票同改 promote_collection_to_bftree 函数体。机制不同（该票
  纯搬移、本票改换入内核），让位方向为本票先落，该票落地时以搬移后位置为准；若其先落，
  本票合并时冲突按「函数体取本票、文件位置取该票」只留一套解决。
- task/ing/rmw-atomic-read-modify-write-window.md（在途）：管 RMW 读-算-写窗口同键互斥，
  不改 apply_rmw_post_operate 内臂序；本票不动其范围。
- task/ing/my-tiered-zset-range-materialize.md（在途）：管分层 zset 范围命令原生树内化，
  落地后其写臂穿透仍汇入本票重灌臂，无判据交叠。
- next/ 未认领票 tiered-collection-ops-file-split、my-zadd-tiered-batch-fold、
  my-tiered-count-expiry-scan 可能指涉本票改过的行，后续认领者按行号重取即可。
- 树文件删除的纪元/守卫前置检查归 next/bftree-release-detached-guard-recheck.md，不重开。

验收：只跑 cargo check（worktree 内 -p wbftree -p wkv -p wnode 及全 workspace --all-targets 面
由合并前自查）；不跑 test.sh / clippy.sh；中文注释、禁 #[allow]。

release_detached 世代守卫补延迟时点复查，堵排空窗口内同名重建的误删

来源：next/glm.db.md 条 1（该文件已清空删除）。逐条按主仓 HEAD 复核后判定成立且待做。

结论
两段式释放的世代守卫只在登记延迟动作之前查一次注册表，延迟动作真正 unlink 时不再复查；
而本仓的摘注册与释放跨队列投递分离后，这个窗口从纪元排空量级拉大到后台轮询量级。窗口内
同 key 重建会把新世代数据文件在 unlink 时删掉（Unix 下句柄仍可向孤儿 inode 写，新数据全丢，
重启惰性恢复报数据文件缺失）。守卫是自加件但论点成立，须把复查推到 unlink 前并与重建串行化。

现状（主仓 HEAD 实测行号）

1. wedb/wbftree/src/manager/lifecycle.rs:368 release_detached：:374
   `data_path.filter(|_| !self.live_indexes.pin().contains_key(&key_id))` 为唯一一次世代判定，
   随后 :380-382 才把 dispose_and_delete_files_deferred 挂进 bump_current_epoch_action；
   删除内核 wedb/wbftree/src/manager/lifecycle.rs:312（:313-324 体）先 dispose 后 :322
   `fs::remove_file`，全程不再看注册表。方法头 :357-367 注释自陈窗口意识（「摘注册与释放跨队列
   投递分离后窗口远超纪元排空本身」），守卫时点未覆盖排空等待期。
2. 投递与消费链：wedb/wkv/src/store/reclaim.rs:104 reclaim_bftree_keys（投递时 detach_tree 摘注册）
   → wedb/wkv/src/store/reclaim.rs:120 drain_bftree_release →
   wedb/wkv/src/gc.rs:130 spawn_bftree_reclaimer，轮询间隔 wedb/wkv/src/gc.rs:86
   RELEASE_POLL_MS = 200ms。同一 key 在此 200ms 至数秒内可完成重建。
3. 重建侧注册点：wedb/wbftree/src/manager/lifecycle.rs:97 create_bftree_internal（:105 注释
   「调用方已持条带写锁，同 key 并发被串行化」，:126 插入注册）与
   wedb/wbftree/src/manager/lifecycle.rs:130 get_or_open_tree。即注册在条带写锁内、
   unlink 在锁外，两者毫无次序关系，这是误删的根因形状。
4. 现有测试只锁住「登记在先即跳过 unlink」这一时点语义
   （wedb/wkv/tests/store/flush_database.rs:210 注释自陈），无排空期重建的对照用例。

C# 参考

garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:240 DisposeTreeUnderLock：
锁内摘注册、:296 storeEpoch.BumpCurrentEpoch(() => DisposeAndDeleteFilesDeferred(...))、
:313 DisposeAndDeleteFilesDeferred 直接删文件、无世代守卫——C# 摘除到排空在调用栈上紧邻、
窗口窄，裸删可幸存。rust 把两半拆开跨队列后自加守卫，属自有增强的收口不完整。

修法

1. 世代复查移到 unlink 之前：延迟动作内先取该键条带写锁、再判 live_indexes 是否已有同
   key_id 条目，无条目才 dispose + remove_file。条目带 key_hash 才能落回同一条带，故给
   wedb/wbftree/src/manager/mod.rs:82 DetachedTree 增一字段承载 detach_tree 已算出的
   key_hash（lifecycle.rs:334 起、:335 取 hash），不改键派生逻辑。
2. 闭包需拿到管理器：把 release_detached 的接收者改为 `self: &Arc<Self>` 并 clone 入闭包
   （生产唯一消费点 wedb/wkv/src/store/reclaim.rs:128 的接收者本就是
   Arc<RangeIndexManager>，见 wedb/wkv/src/store/mod.rs:95；dispose_tree_under_lock
   wedb/wbftree/src/manager/lifecycle.rs:393 同步别名随之改形态，测试侧构造点按 Arc 对齐）。
   若不宜改接收者，备选同一内核由 reclaimer 线程（持 Arc）统一收割、闭包只投递待删项——
   二者取一，目标是删除动作与重建注册落在同一条带锁内，不留第二套裁决口径。
3. :374 的登记前 filter 保留：它是「已确认新世代在用」的快速短路，省掉无谓的纪元注册；
   延迟时点复查是补齐而非替换。
4. 测试：wedb/wkv/tests/store/flush_database.rs 增一例换号投递后、释放前排空期同 key 重建，
   断言新树数据文件仍在且可读写；现有 :210 世代守卫用例保持通过。

优先级
功能缺口（正确性洞，后果是静默丢数据与复活期报错；不涉及重复实现，也不属在途主题）。

边界
同域在册票 bftree-reclaim-register-session-port 的射程是登记/注销调用点的三元组样板收口
（reclaim.rs:40/:55 内核签名与会话侧一参口），不改裁决时点；本单只改释放动作的复查时点与
锁序，两单在 reclaim.rs 的改动面不重叠（该票不碰 :104/:120 两函数体）。
机制口径的文档表述已由 task/done/ 归档的换号回收裁定与票 spec-doc-drift-gcbarrier-ri-promote
承接，本单不改文档。

验收
1. 排空窗口内同名重建不再触发对新世代数据文件的 unlink。
2. unlink 前的世代判定与 create_bftree_internal 的注册同处一条带写锁保护，无时序洞。
3. 现有换号回收、RI 升阶/驱逐用例全绿；clippy 无新增告警（禁写 allow）。

落地记录（分支 bftree-release-recheck，合入 dev f536d49）

1. wbftree/src/manager/mod.rs：DetachedTree 增 key_hash 字段（detach_tree 已算出，
   不改键派生）；管理器增 release_retries 退让队列。
2. wbftree/src/manager/lifecycle.rs：删除内核改为 settle_detached_release，持该键
   条带写锁在同一处复查 live_indexes 后 dispose + unlink，与 create_bftree_internal
   的注册同锁同判据；登记前 filter 保留为快速短路。release_detached /
   dispose_tree_under_lock / delete_index 接收者收为 &Arc<Self>（原
   dispose_and_delete_files_deferred 裸删内核删除，无第二套口径）。
3. 收割线程的条带锁态不可确知（LightEpoch 的 prev_action 槽位复用与收尾 help_drain
   会在持锁线程上内联执行延迟动作，如 publish_tree_from_snapshot_locked 锁内
   dispose_bf_tree_deferred、树会话条带读锁），故删除时点用 wbase 新增的
   StripedRwLock::try_write 非阻塞取锁：被占即退让排队，由释放驱动线程经
   harvest_release_retries 重投同一内核（wkv/src/store/reclaim.rs
   drain_bftree_release 承接），绝不用阻塞加锁换自死锁、也不引入超时假设。
4. 测试：wkv/tests/store/flush_database.rs
   test_flush_reclaim_generation_guard_at_unlink_time（长读会话钉住旧纪元，把
   「已登记未收割」窗口确定性打开后同名重建；把守卫退回登记前判一次即红，已实测）；
   wbftree/tests/manager_and_stub/manager.rs
   test_release_detached_defers_on_stripe_contention（持锁退让—重投—落地闭环）。
5. 门禁：cargo check --workspace --all-targets 零告警；nextest wbftree 135/135；
   wkv RI/flush 面 25/26（唯一红 test_flush_atomic_batch_survives_rebuild 为 dev
   基线既有 dbmeta 红，基线同测复现）；wnode range_index_tests 35/35。


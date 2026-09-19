checkpoint_store purge 段回正 C# 语义：只清磁盘孤儿，不改内存链表

现象
dev 基线红用例 wedb::server::replication::checkpoint_store::tests::
test_purge_all_except_entry_cleans_orphan_files。purge 后断言
entry_count() == 1 恒不成立：被测 store 的 entries 为空，keep 是一个未登记的
Arc<CheckpointEntry>，retain 谓词在无条目的链表上什么也留不下，purge 之后
链表仍为 0，故 entry_count() == 1 必红。而磁盘段（seed token 1/2/9，keep
token 2）断言 list == vec![2] 本身是成立的。

C# 对位
garnet/libs/cluster/Server/Replication/CheckpointStore.cs:77
PurgeAllCheckpointsExceptEntry(CheckpointEntry entry = null) 的函数体只有
:82 一次 PurgeAllCheckpointsExceptTokens(entry.metadata.storeHlogToken,
entry.metadata.storeIndexToken) 调用（局部函数 :84，内部 :89-96 逐个
DeleteLogCheckpoint、:99-106 逐个 DeleteIndexCheckpoint），全程不写 head/tail。
内存链表的重建由调用方负责：同文件 :40 Initialize 自身赋 head = tail =
GetLatestCheckpointEntryFromDisk()，:56 才调 purge；副本侧
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:336
purge 之后紧跟 :340 InitializeCheckpointStore() 重扫重建。C# 的 purge 里
既没有 retain，也没有 clear，更没有「按 metadata 相等保留」这一层身份判定。

裁决
实现错，测试也错，但以实现对位为唯一基准：rust 侧 entries.retain(...) 与
None => entries.clear() 都是 C# 没有的额外复杂度（retain 的
e.metadata == keep.metadata 分支还凭空引入「元数据相等即同一条目」的 identity
语义），删除链表改写；随之该用例原断言 entry_count() == 1 所预设的「purge 会把
keep 挂进链表」这一行为不存在，测试断言改为 C# 语义下可验证的事实。不存在
「用链表 purge 替代 C# purge+reinit」的刻意设计证据：本仓 initialize()
（同文件 :78-89）已按 C# Initialize 的次序先 clear 再 push 后 purge，
replication_manager.rs:1025 initialize_checkpoint_store 亦先扫盘组装条目再
store.initialize，purge 的链表改写对任何现役调用方都是死重量。

修法
wedb/wedb/src/server/replication/checkpoint_store.rs
- purge_all_checkpoints_except_entry 由 (&mut self, Option<&Arc<CheckpointEntry>>)
  收为 (&self, &CheckpointEntry)：只转调 purge_checkpoint_files_except 做磁盘
  孤儿清理，删除 retain 与 clear 两条链表改写；Option 形态（C# 的 null 兜底是
  扫盘取最新，rust 无该面且零调用方）直接删，不留旧形态。
- 唯一现役调用方 initialize() 改传 &arc_entry。
- 方法文档与结构体文档补真实锚点：CheckpointStore.cs:82/:94/:104（磁盘清理段）、
  :38-57（Initialize 自赋链表）、:52-54（无读者论证）、
  ReplicaDiskbasedSync.cs:336 → :340（清理后重建），并注明内存链表的裁剪只
  发生在 delete_outdated_checkpoints（C# DeleteOutdatedCheckpoints 对位）。
- 用例同名保留，seed 面不动（磁盘 1/2/9、keep token 2），改为先
  add_checkpoint_entry 预登记一条陈旧条目（单条目不触发登记期过期淘汰），purge
  后断言 entry_count() == 1（链表不受 purge 影响）与
  list_checkpoints() == vec![2]（keep 之外孤儿连文件回收）。

验收
- 分支 cp-purge-csemantics（worktree /tmp/fork/cp-purge-csemantics，提交 72c0fc3）。
- CARGO_TARGET_DIR=/tmp/rs-cppurge cargo check --workspace --all-targets：exit 0，
  零 warning 零 error。
- CARGO_TARGET_DIR=/tmp/rs-cppurge cargo nextest run -p wedb --no-fail-fast
  checkpoint_store（同一私有 target）：4 passed / 0 failed，其中
  test_purge_all_except_entry_cleans_orphan_files 由红转绿，
  test_delete_outdated_purges_disk_except_reader_held 与
  test_checkpoint_store_add_and_delete、wedb_test 的
  test_checkpoint_store_reader_suspension_and_token_pruning 均无回归。
- rustfmt --check（wedb/rustfmt.toml）该文件零 diff；test.sh 与 ./sh/clippy.sh
  按 fixloop 规程留主代理合并后统一跑。

状态
已合入 dev（快进至 68cf470，代码提交 ce42276，分支 fix-checkpoint-purge-signature，
worktree /tmp/fork/fix-checkpoint-purge-signature）。

分两棒落地：前棒（cp-purge-csemantics / 72c0fc3）删掉链表 retain 与 clear 改写、
purge 收为纯磁盘转调；本棒收残留面——签名由 (&self, Option<&Arc<CheckpointEntry>>)
收为 (&self, &CheckpointEntry)，删除 `let Some(keep) .. else { return }` 空兜底分支
（C# :79 的 null 兜底是扫盘取最新，rust 无该面且零调用方，按「Option 形态直接删，
不留旧形态」处置），initialize 唯一现役调用点改传 &arc_entry（Arc 靠 deref 借用，
零克隆），方法与结构体文档补齐票面锚点 :82/:94/:104、:38-57、:52-54、
ReplicaDiskbasedSync.cs:336 → :340，并写明内存链表的裁剪只发生在
delete_outdated_checkpoints。

前棒对修法末条留了偏差：用例只断言 entry_count() == 0（空表恒空，证不出「链表不受
purge 影响」），本棒按票面改为预登记一条陈旧条目（token 9，单条目不触发登记期过期
淘汰），purge 后断言 entry_count() == 1 且 list_checkpoints() == vec![2]——链上条目
的磁盘快照照样被回收，两轨分离由此可验。

本棒门禁：CARGO_TARGET_DIR=/tmp/target-fix-cpsig cargo check --workspace
--all-targets 在合入 dev 后的树上 exit 0、零 warning 零 error（含 --all-targets 的
测试面编译）；cargo fmt --check -p wedb 零 diff。./test.sh 与 ./sh/clippy.sh 按
fixloop 规程留主代理合并后统一跑。initialize 内 :79 的 entries.clear() 属初始化
语义（C# :40/:42-45 自赋 head = tail 的对位），按主代理裁定保留。

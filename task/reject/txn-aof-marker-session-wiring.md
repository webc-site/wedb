事务 AOF 标记接线：会话装配注入真实 GarnetLog，MULTI/EXEC 落 TxnStart/TxnCommit、RUNTXP 落 StoredProcedure

结论一句话：标记发射逻辑（enqueue_txn_marker/log_proc + GarnetLog impl TxnAofLog）dev 已具，但会话侧 attach_transaction_components 恒传 aof_log=None，导致后端不在场、PerformWrites 门控的标记永不落盘；本条把 service 持有的 GarnetLog 经 SessionDependencies 注入 TransactionManager，使 MULTI/EXEC 真实成对落 TxnStart+TxnCommit，与 C# CreateDatabaseSession 绑 functionsState.appendOnlyFile 一一对应。属功能缺口（下游链路未打通），排在死代码/多套架构之后。

优先级：中（功能缺口）。前置阻塞：wtxn 事务 AOF 后端收敛条（next/wtxn-aof-log-dyn-backend.md，在途分支 wave6-a-wtxn-aof-log 单提交 6e18c93e，dev 尚未合入）合入后，后端为单一 Option<Arc<dyn TxnAofLog>>，会话字段无 <L> 传染，注入即 aof_log: Some(Arc::clone(&log))。前置未合入前本条不得开工：当前 resp_server_session.rs:412 txn_manager: Option<TransactionManager> 即 TransactionManager<()>，会话链硬锁空后端，注入必须先解泛型。

判定基线：主仓 dev，取证时 HEAD 415e0d0e（startup-assembly-single-source 已合入，service.rs 装配段行号整体后移；文档旧引 sha 与旧行号全失效，判落地只认当前 grep）。

一、复核过的 rust 现状（缺口 = 注入缺席，非发射缺席）
- 发射面已在（本条不动，仅接线）：wedb/wtxn/src/transaction_manager.rs TransactionManager<L: TxnAofLog = ()> :344、字段 aof_log: Option<L> :362、aof_enabled() :412-414（对标 C# :49 AofEnabled => appendOnlyFile != null）、run 落 TxnStart :478-483、commit 落 TxnCommit :492-498、enqueue_txn_marker :590（形参 log: &L）、log_proc 落 StoredProcedure :658-660；三处门控形态均为 perform_writes && aof_log.is_some()（run/commit 另加 !stored_proc_mode）。
- 后端 impl 已在（本条不动）：wedb/wnode/src/aof/garnet_log/mod.rs:319 impl wtxn::TxnAofLog for GarnetLog（enqueue_txn :343 → 单日志臂 garnet_log/single_log_branch.rs:359）；判别值映射 txn_entry_type_to_aof mod.rs:311-317；waof 侧 AofEntryType::{TxnStart=0x20, TxnCommit=0x21, StoredProcedure=0x50}（wedb/waof/src/aof/entry_type.rs:23/:25/:33，TryFrom<u8> :53）。
- 回放臂已在（本条只验「不二次入账」，不重写回放）：wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:299/:310/:324 消化 TxnStart/TxnCommit/StoredProcedure；stored_proc_replay.rs:94-97 回放事务管理器第三参仍 None（is_replaying 短路落盘，与 C# 恢复会话 recordToAof:false 对齐，见 :90-93 注）。
- 注入缺席（本条要补的断点，四处）：
  - wedb/wnode/src/resp/session_dependencies.rs 全 1-44 行：SessionDependencies 无 aof_log 字段（该文件自 init 起未变）。
  - wedb/wnode/src/service.rs:1457-1473 session_dependencies() 构造 SessionDependencies，无 aof_log。可取源字段在场：service.rs:914 aof: Option<Arc<GarnetAppendOnlyFile>>，访问器 service.rs:1325 aof() -> Option<&Arc<GarnetAppendOnlyFile>>（另有 :408 的非 Option aof() 属另一结构，接线按 provider 上下文取 :1325），句柄取器 wedb/wnode/src/aof/garnet_append_only_file.rs:95 log() -> &Arc<GarnetLog>。
  - wedb/wnode/src/resp/resp_server_session.rs:576-585 attach_transaction_components 无 aof_log 形参、:582 TransactionManager::new(lock_table, watch_version_map, None)；:621 inject_dependencies 仅透传 watch_version_map/lock_table。
  - wedb/wnode/src/resp/resp_session_consumer.rs:106-114 attach_transaction_components 仅透传两参。
- 端到端覆盖缺席：wedb/wnode/tests/ 无 txn_aof_wiring.rs；现有 TxnStart/TxnCommit/StoredProcedure 断言全部由测试直接 log.enqueue_txn 手撒标记构造（aof_replay.rs:291/:302/:631/:649、aof_store_rmw_replay.rs:124-126、aof_stored_proc_replay.rs:76），无一经会话事务链产生，故接线与否当前无测试可红。

二、复核过的 C# 锚点（逐条核实在场）
- garnet/libs/server/Transaction/TransactionManager.cs:173 this.appendOnlyFile = functionsState.appendOnlyFile;（会话/事务管理器构造即绑真实日志）；:49 AofEnabled => appendOnlyFile != null;；:375-376 PerformWrites && appendOnlyFile != null → EnqueueStoredProc(StoredProcedure)；:392-395 / :513-516 PerformWrites && appendOnlyFile != null && !functionsState.StoredProcMode → EnqueueTxn(TxnCommit) / EnqueueTxn(TxnStart)。rust 三处发射门控与之逐字对应。
- 会话侧绑定源头：garnet/libs/server/Resp/RespServerSession.cs:1618 CreateDatabaseSession → :1640 new TransactionManager(storeWrapper, this, dbGarnetApi, ...)，真实 appendOnlyFile 经 storeWrapper/functionsState 流入事务管理器（对标 :173 注入语义）。rust 等价 = service.rs:1457 session_dependencies() 把 self.aof 的 log 句柄灌进 attach_transaction_components。

三、修订方案（前置条合入后开工；本条不改 wtxn 后端形态，只装配）
1. session_dependencies.rs：SessionDependencies 增字段 aof_log（Option 后端句柄，句柄类型名以前置条收敛结果为准：Option<Arc<dyn TxnAofLog>>，若前置条定名 TxnAofLogHandle 则直接引用该别名，全仓只留一种写法）；None = 未点亮 AOF。字段随既有装配语义放置，勿复制后端句柄之外的会话状态。
2. service.rs:1457-1473 session_dependencies()：aof_log: self.aof.as_ref().map(|aof| Arc::clone(aof.log()) as _)（log() -> &Arc<GarnetLog> 再 clone，向上转型为前置条的后端句柄）。
3. resp_server_session.rs:576-585 attach_transaction_components 增 aof_log 形参并透传到 TransactionManager::new(lock_table, watch_version_map, aof_log)；会话字段 :412 随前置条去 <()> 后即为持真实后端的事务管理器；:621 inject_dependencies 传 deps.aof_log。:572-575 文档注释随之改写（现注「AOF 事务日志由 wtxn 默认无日志形态承接」接线后即失真，须删）。
4. resp_session_consumer.rs:106-114 attach_transaction_components 同步透传 aof_log。
5. 无 AOF 形态：aof_log=None，TransactionManager::new 第三参 None（前置条收敛后类型可推断，不再 None::<()>）。构造点复核：service.rs 装配链与 stored_proc_replay.rs:94-97 —— 回放 replayer 保持不注入（is_replaying 短路落盘），本条不为其新增后端句柄，避免「在线/回放」两套 AOF 出口。
6. attach_transaction_components 两参调用点补第三参 None（现 HEAD 全量清单，落地前重读行号）：生产侧 resp_server_session.rs:621、resp_session_consumer.rs:113；测试侧 wedb/wnode/tests/transaction_session_test.rs:280、resp_server_session_tests.rs:1073/:1086/:1144/:1159、tiered_watch_fence.rs:429/:465；wedb/wedb/tests/replication_assembly_e2e.rs:337、cluster_resp_session.rs:121、diskless_loop_convergence.rs:124、diskless_sync_ri_vector.rs:152、diskless_sync_ttl.rs:137、diskless_sync_anchor_window.rs:208、primary_live_repl_offset.rs:219、cluster_migration.rs:533/:2891/:2961、cluster_iterative_slot_verify.rs:102、checkpoint_import.rs:155/:729。不得为省改测试而另留「不带后端的第二套装配入口」（重载/builder 均算两套）。

四、验收（DoD：新建 wedb/wnode/tests/txn_aof_wiring.rs，端到端、仅经服务面驱动）
- multi_set_exec_enqueues_txn_markers_and_recovers：MULTI/SET k1 v1/EXEC → AOF 成对落 TxnStart+TxnCommit（同 store_version、同 session_id、store_version>0）+ 一条 StoreUpsert；drop 后同 WAL open_recovered_with_config_and_aof 回放，GET k1 → v1。回放臂消费已在（aof_replay_coordinator.rs:299/:310/:324 消化 TxnStart/TxnCommit/StoredProcedure；stored_proc_replay.rs:90-97 is_replaying 短路落盘；transaction_manager.rs run/commit 受 perform_writes 门控），本用例即验「接线后不二次入账」。
- read_only_transaction_writes_no_markers：MULTI/GET/EXEC 走 PerformWrites 门控（只读）→ 零事务标记（transaction_manager.rs:478/:492 perform_writes 为假即不落）。
- txn_without_aof_commits_silently：open_with_config（无 AOF 门控、aof_log=None）→ 事务照常提交、无标记、无报错。
- 对标 C# 行为：标记 txnVersion/sessionID 语义与 EnqueueTxn 同形；StoredProcedure 仅 RUNTXP/存储过程路径（log_proc :658-660）落，普通 MULTI/EXEC 不落后端。
- 测试源逐字转录（原为死亡代理未跟踪文件，防丢失；移植前按前置条 + 本节 API 对齐最新 dev：仅经 StorageSessionProvider/RespSessionConsumer 服务面驱动，不直接引用 wtxn 泛型，后端形态变化对本文件透明；对 dev 的硬要求是 session_dependencies() 已注入真实 GarnetLog，否则 multi_set_exec_* 因无标记而红）。已在 HEAD 核实的 API：AofHeader::parse（waof/src/aof/header.rs:166 const fn 返回 Option）、字段 op_type:u8/store_version:i64/session_id:i32（:78/:84/:86）、scan_single_with(sublog_idx, begin, end, 闭包)（wnode/src/aof/garnet_log/addresses.rs:138）、record.payload（waof/src/wal/record.rs:15）、commit_flush_async（wnode/src/aof/garnet_append_only_file.rs:368）、MessageConsumerFace 的 take_recv_scratch/return_recv_scratch/try_consume_messages_into（wnode/src/traits.rs:70/:73/:38，由 resp_session_consumer.rs:165/:198/:203 实现）、SessionProviderFace/WireFormat（wnode/src/lib.rs:67 再导出）、open_with_config/open_with_config_and_aof/open_recovered_with_config_and_aof（service.rs:946/:1074/:1186，恢复臂为 async）、装饰器签名 F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>（service.rs:934-935 类型级实绑，SessionProviderFace 的 impl 另加 Send + Sync，:1477-1478；与转录一致）、StoreGarnetApi（wnode/src/resp/garnet_api/mod.rs:338，模块 resp/mod.rs:13 已 pub）。转录源的一处偏差已就地修正：test_store_config 定义在 wtest_base/src/config.rs:28（非 wedb_test —— 该 crate 只导出 NodeAssembly/cluster_decorate/start_node），且 wnode dev-deps 只含 wtest_base，故下方 use 行为 wtest_base。

```rust
//! 主侧事务 AOF 标记入账与恢复回放端到端测试
//!
//! 对标 C# 链路：RespServerSession 的 CreateDatabaseSession 构造事务管理器即
//! 绑 functionsState.appendOnlyFile → TransactionManager 的 Run / Commit 内
//! `appendOnlyFile.Log.EnqueueTxn(TxnStart / TxnCommit)`（PerformWrites 门控），
//! 重启恢复由 AofProcessor 的事务协调臂消费该标记。
//!
//! rust 同链路验证：会话装配（StorageSessionProvider::session_dependencies）把
//! GarnetLog 注入 TransactionManager，MULTI/SET/EXEC 提交的事务真实落
//! TxnStart + 主存条目 + TxnCommit；只读事务不落标记；AOF 关闭形态无标记；
//! 同一 WAL 重启恢复后事务数据齐备（回放臂激活）。

use std::sync::Arc;

use aok::Result;
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofEntryType, AofHeader};
use wdev::SegmentedDevice;
use wtest_base::test_store_config;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};

/// 会话装饰器以函数项承接（F 可命名，助手函数免泛型传染）
fn decorate(sender_id: u64, api: StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    sender_id,
    RespServerSessionOptions::default(),
    api,
  ))
}

/// 装饰器函数指针（单态化为 fn 指针，Provider 类型可命名）
const TXN_DECORATE: fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> =
  decorate;

type Provider =
  StorageSessionProvider<fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>>;

/// AOF 门控点亮的服务基座（单物理日志形态，与 service_aof.rs 同径；
/// TempDir 与 provider 分离，便于仅释放 provider 后按同路径恢复）
async fn open_provider(recover: bool, dir: &TempDir) -> Result<Arc<Provider>> {
  let data_path = dir.path().join("data").join("txn.db");
  let provider = if recover {
    StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      TXN_DECORATE,
    )
    .await?
  } else {
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      TXN_DECORATE,
    )?
  };
  Ok(Arc::new(provider))
}

/// 会话内提交一条流水线命令，返回应答字节
fn pipeline(provider: &Provider, req: &[u8]) -> Vec<u8> {
  let mut consumer = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(req);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  consumer.try_consume_messages_into(&mut resp);
  resp
}

/// 全量提交落盘（恢复前置）
async fn flush_aof(provider: &Provider) {
  Arc::clone(
    provider
      .aof()
      .expect("装配期已注入 AOF 门面，事务标记方有落点"),
  )
  .commit_flush_async()
  .await;
}

/// 扫描单物理日志全部 AOF 条目的 (判别值, 事务版本, 会话号) 序列
///
/// 事务标记携 txnVersion 与 sessionID（C# EnqueueTxn 同形），
/// 数据条目该二元组语义不同，此处仅记录不判定。
fn scan_entries(provider: &Provider) -> Vec<(AofEntryType, i64, i32)> {
  let mut out = Vec::new();
  provider
    .aof()
    .expect("AOF 在场")
    .log()
    .scan_single_with(0, 0, i64::MAX, |record| {
      if let Some(header) = AofHeader::parse(&record.payload)
        && let Ok(op_type) = AofEntryType::try_from(header.op_type)
      {
        out.push((op_type, header.store_version, header.session_id));
      }
      true
    });
  out
}

/// 事务标记子集（TxnStart / TxnCommit）
fn txn_markers(provider: &Provider) -> Vec<(AofEntryType, i64, i32)> {
  scan_entries(provider)
    .into_iter()
    .filter(|(op, ..)| matches!(op, AofEntryType::TxnStart | AofEntryType::TxnCommit))
    .collect()
}

/// 对标 C# 事务管理器的 Commit 入账形态（实现登记在 wtxn 侧）——
/// 写事务成对落标记，且恢复回放后数据齐备
#[test]
fn multi_set_exec_enqueues_txn_markers_and_recovers() -> Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = open_provider(false, &dir).await?;

    let resp = pipeline(
      &provider,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n",
    );
    assert_eq!(resp, b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");
    flush_aof(&provider).await;

    let markers = txn_markers(&provider);
    assert_eq!(
      markers
        .iter()
        .map(|(op, ..)| *op)
        .collect::<Vec<_>>()
        .as_slice(),
      [AofEntryType::TxnStart, AofEntryType::TxnCommit].as_slice(),
      "EXEC 提交成对落 TxnStart / TxnCommit"
    );
    assert_eq!(markers[0].1, markers[1].1, "标记同事务版本");
    assert_eq!(markers[0].2, markers[1].2, "标记同会话号");
    assert!(markers[0].1 > 0, "事务版本非零（C# txnVersion）");
    let upserts = scan_entries(&provider)
      .into_iter()
      .filter(|(op, ..)| *op == AofEntryType::StoreUpsert)
      .count();
    assert_eq!(upserts, 1, "事务内 SET 落一条主存条目");

    // 恢复回放臂：同一 WAL 重启后事务条目被消化，数据可读
    drop(provider);
    let reopened = open_provider(true, &dir).await?;
    let resp = pipeline(&reopened, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
    assert_eq!(resp, b"$2\r\nv1\r\n", "AOF 事务条目恢复后数据齐备");
    Ok(())
  })
}

/// C# PerformWrites 门控：只读事务不落事务标记
#[test]
fn read_only_transaction_writes_no_markers() -> Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = open_provider(false, &dir).await?;

    let resp = pipeline(
      &provider,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n*1\r\n$4\r\nEXEC\r\n",
    );
    assert_eq!(resp, b"+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n");
    flush_aof(&provider).await;

    assert!(
      txn_markers(&provider).is_empty(),
      "只读事务 PerformWrites 为假，不得落事务标记，实际 {:?}",
      scan_entries(&provider)
    );
    Ok(())
  })
}

/// 装配期未注入 AOF（关闭门控）：事务照常提交，无标记、无报错
#[test]
fn txn_without_aof_commits_silently() -> Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("data").join("noaof.db"),
      TXN_DECORATE,
    )?;
    assert!(provider.aof().is_none(), "未点亮 AOF 门控");

    let resp = pipeline(
      &provider,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n*1\r\n$4\r\nEXEC\r\n",
    );
    assert_eq!(resp, b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");
    let resp = pipeline(&provider, b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n");
    assert_eq!(resp, b"$2\r\nv2\r\n", "无 AOF 形态事务照常提交");
    Ok(())
  })
}
```

五、门禁
- ./fork.sh txn-aof-marker-session-wiring 基于最新 dev（须已含 wtxn-aof-log-dyn-backend 合入，否则先等或先落前置条）。
- ./sh/clippy.sh 零警告、禁 allow；./test.sh 全绿（失败隔离重跑 3 次排 flake）。
- 硬指标 grep 归零：rg "TransactionManager::new\(lock_table, watch_version_map, None\)" wedb/wnode/src 无命中（会话装配不得再恒传 None）；rg "aof_log" wedb/wnode/src/resp/session_dependencies.rs 命中字段与注入两处，全仓会话侧只此一条注入路径。
- StoredProcedure/TxnStart/TxnCommit 写入点与 C# 一一对应（run/commit/log_proc 三处，不新增第四处）。
- bun js/check.js：缺失 0 / 重复 0；本条为 .md + 端到端测试，不新增映射登记、不动 ignore 语料。

六、串行依赖与撞车
- 前置：wtxn 后端收敛条（next/wtxn-aof-log-dyn-backend.md，分支 wave6-a-wtxn-aof-log）合入 dev。该条拥有 wtxn/src/*、resp/txn_resp_commands.rs 与 wnode/tests/transaction_*.rs 的去 <L> 改动；本条只拥有 session_dependencies.rs、service.rs::session_dependencies、resp_server_session.rs 的 attach_transaction_components/inject_dependencies 段、resp_session_consumer.rs，以及新测试 txn_aof_wiring.rs。两单不交叉，勿双改后端形态。
- 同域：RUNTXP/存储过程回放（stored_proc_replay.rs、AofEntryType::StoredProcedure）回放臂已在，本条只做「不二次入账」的端到端验证，不重写回放、不给回放 replayer 注入后端。
- 测试文件撞车（在途分支已认领）：migration-frame-import-core 正改 wedb/wedb/tests/diskless_sync_ttl.rs、tiered-command-arm-coverage 正改 wedb/wnode/tests/tiered_watch_fence.rs，均在三.6 补参清单内。本条改动这两文件前须先 rebase 到含其合入的 dev，避免双改。
- 旧引 task/ing/wnode-session-slot-verify-split.md 已不存在（不在 next/、task/ing/、task/done/），该段「同文件不同段可并行」的约定失效；本条落地时按最新 dev 重读 resp_server_session.rs 行号即可。

交叉引用：本条落地（合入 dev）即解除 task/ing/wnode-service-split.md 的唯一未满足门禁——本条改的 service.rs:1457-1473 session_dependencies() 与 :1325/:1615 aof 访问器正落在该档待搬的 session_provider 段内，两档不得并开。

七、fixloop 查重命中（2026-09-19）
- 命中证据：/tmp/fork/txn-aof-marker-session-wiring worktree（git worktree list 在册，分支 txn-aof-marker-session-wiring）已有提交 9eac5f30 "feat(wnode): 会话装配注入真实 GarnetLog 事务 AOF 后端"，提交时间 2026-09-19 15:56:31（本代理认领票据后数秒内产生），提交标题与内容即本票第三节装配方案（session_dependencies 注入 GarnetLog）。
- 结论：同题已被并行会话认领且实现已提交，本票 reject，避免双改 session_dependencies.rs / service.rs 装配段。

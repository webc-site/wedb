//! --recover 启动恢复端到端测试
//!
//! 对标 C# Garnet.test/TestProcessBase 重启恢复语义（Options.cs:139 Recover
//! → StoreWrapper.RecoverAsync：RecoverCheckpointAsync + RecoverAOFAsync +
//! ReplayAOF）：
//! 1. 写数据 → SAVE（检查点 + 版本推进 + AOF 截断）→ 停服；
//! 2. --recover 重启（open_recovered_with_config_and_aof，端点 accept 之前完成恢复）；
//! 3. 检查点内数据与 SAVE 后增量（AOF 重放）均可读。
//!
//! 覆盖两处刻意差异的回归面：
//! - SAVE 必须经管理器内核推进存储版本，否则恢复重放会按版本基线误跳过
//!   SAVE 后的增量条目；
//! - 收尾帧慢命令的网络泵挂起检查（SAVE 恰为批次收尾帧时 pending_slow
//!   仍须被驱动）。

use std::{str::from_utf8, sync::Arc};

use compio::{BufResult, io::AsyncRead, net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    recover::aof_recover::AofRecover,
  },
  rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
  resp::{
    garnet_api::{GarnetApiFace, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::{StorageSessionProvider, open_node_with_config},
  storage::session::storage_session::StorageSession,
};
use wnode_test::{read_bulk_reply, read_line_reply, send_cmd, session_factory, start_server};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

/// 写数据 → SAVE → 停服 → --recover 重启 → 数据可读（检查点 + AOF 增量）
#[test]
fn recover_checkpoint_and_aof_after_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("recover.db");

  // ---- 第一代进程：SET → SAVE → 增量 SET（走 AOF）→ 停服
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"ck", b"v1"])
      .await
      .expect("set ck");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
    send_cmd(&mut stream, &[b"SAVE"]).await.expect("save");
    assert!(
      read_line_reply(&mut stream).await.starts_with(b"+OK"),
      "SAVE 须成功（检查点 + 版本推进 + AOF 截断）"
    );
    // SAVE 后增量：仅存在于 AOF（恢复靠重放追平）
    send_cmd(&mut stream, &[b"SET", b"inc", b"v2"])
      .await
      .expect("set inc");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
    // 模拟 commit_frequency 周期提交：物理刷盘（设备面恢复以落盘为准）
    provider
      .aof()
      .expect("aof enabled")
      .commit_flush_async()
      .await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代进程：--recover 重启，数据面恢复先于端点 accept
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      session_factory,
    ))
    .expect("open recovered"),
  );
  // --recover 后位点回填值（对标 C# RecoverCheckpointAndAOFAsync 尾段
  // replicationOffset.SetValue(ref replayedUntil) 的回填值来源）：等于重放
  // 后 AOF 尾地址，且已越过初始位点（SAVE 后增量经重放推进）
  let recovered_tail = provider2
    .recovered_aof_tail()
    .expect("--recover 形态须点亮恢复位点");
  assert_eq!(
    recovered_tail,
    provider2.aof().expect("aof enabled").log().tail_address(),
    "恢复位点须等于重放后 AOF 尾"
  );
  assert!(
    recovered_tail.get(0).unwrap_or(0) > 0,
    "SAVE 后增量重放须推进恢复位点（空日志判据 begin == tail，非空即越过 0）"
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut stream, &[b"GET", b"ck"])
      .await
      .expect("get ck");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v1".to_vec()),
      "检查点内数据须可读"
    );
    send_cmd(&mut stream, &[b"GET", b"inc"])
      .await
      .expect("get inc");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v2".to_vec()),
      "SAVE 后 AOF 增量须可读（版本基线不得误跳过）"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}

/// --recover 空目录冷启动：无检查点静默回退空库（对标 C# RecoverAsync
/// 对空检查点目录的静默语义）
#[test]
fn recover_cold_start_without_checkpoint() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("cold.db");

  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config(
      test_store_config(),
      &data_path,
      session_factory,
    ))
    .expect("open recovered on empty dir"),
  );
  let session = provider.store().new_session().expect("session");
  rt.block_on(async {
    assert_eq!(session.read(b"any").await.expect("read"), None);
  });
}

/// 判定累积字节是否已含一条完整 RESP 应答（按类型逐帧消耗；GETWITHETAG /
/// SETIFMATCH 的多帧数组应答可能单次 read 一次到齐，逐行读取会吞帧错位）
fn reply_frame_complete(d: &[u8]) -> bool {
  let Some(&kind) = d.first() else {
    return false;
  };
  let Some(nl) = d.iter().position(|&b| b == b'\n') else {
    return false;
  };
  match kind {
    b'+' | b'-' | b':' => nl == d.len() - 2 && d.ends_with(b"\r\n"),
    b'$' => {
      // bulk 长度为有符号数：-1（$-1\r\n 空串）本身即完整帧，无载荷行
      let Some(len): Option<isize> = from_utf8(&d[1..nl])
        .ok()
        .and_then(|s| s.trim().parse().ok())
      else {
        return false;
      };
      if len < 0 {
        return nl == d.len() - 2 && d.ends_with(b"\r\n");
      }
      d.len() >= nl + 1 + len as usize + 2
    }
    b'*' => {
      // 数组长度为有符号数：-1（*-1\r\n 空数组）本身即完整帧
      let Some(n): Option<isize> = from_utf8(&d[1..nl])
        .ok()
        .and_then(|s| s.trim().parse().ok())
      else {
        return false;
      };
      if n < 0 {
        return nl == d.len() - 2 && d.ends_with(b"\r\n");
      }
      let mut rest = &d[nl + 1..];
      for _ in 0..n {
        let Some(p) = rest.iter().position(|&b| b == b'\n') else {
          return false;
        };
        match rest[0] {
          b'+' | b'-' | b':' => rest = &rest[p + 1..],
          b'$' => {
            let Some(len): Option<isize> = from_utf8(&rest[1..p])
              .ok()
              .and_then(|s| s.trim().parse().ok())
            else {
              return false;
            };
            if len < 0 {
              rest = &rest[p + 1..];
              continue;
            }
            let end = p + 1 + len as usize + 2;
            if rest.len() < end {
              return false;
            }
            rest = &rest[end..];
          }
          _ => return false,
        }
      }
      true
    }
    _ => d.ends_with(b"\r\n"),
  }
}

/// 读取一条完整 RESP 应答帧
async fn read_complete_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  loop {
    if reply_frame_complete(&acc) {
      return acc;
    }
    let buf = vec![0u8; 512];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => acc.extend_from_slice(&returned[..n]),
    }
  }
}

/// 解析 GETWITHETAG / SETIFMATCH 族应答：`$-1`（键不存在）返回 None；
/// `[etag, value]` 数组返回 (etag, Some(value))；`[etag, nil]` 返回 (etag, None)
fn parse_etag_array(frame: &[u8]) -> Option<(i64, Option<Vec<u8>>)> {
  if frame.starts_with(b"$-1") {
    return None;
  }
  let nl = frame.iter().position(|&b| b == b'\n')?;
  let n: usize = from_utf8(&frame[1..nl]).ok()?.trim().parse().ok()?;
  assert_eq!(n, 2, "GETWITHETAG 须回 [etag, value] 二元数组");
  let mut rest = &frame[nl + 1..];
  let p = rest.iter().position(|&b| b == b'\n')?;
  let etag: i64 = from_utf8(&rest[1..p]).ok()?.trim().parse().ok()?;
  rest = &rest[p + 1..];
  if rest.starts_with(b"$-1") {
    return Some((etag, None));
  }
  let p = rest.iter().position(|&b| b == b'\n')?;
  let len: usize = from_utf8(&rest[1..p]).ok()?.trim().parse().ok()?;
  let val = rest.get(p + 1..p + 1 + len)?.to_vec();
  Some((etag, Some(val)))
}

/// 读取 GETWITHETAG / SETIFMATCH 族应答（帧完整读取，防多帧错位）
async fn read_etag_array(stream: &mut TcpStream) -> Option<(i64, Option<Vec<u8>>)> {
  parse_etag_array(&read_complete_reply(stream).await)
}

/// ETag 旁路记录恢复链端到端（缺陷回归：ETag 旁路记录无 AOF 通路时，
/// --recover 后 etag 恒为 NoETag 0，条件写 DELIFGREATER 主副判定漂移）
///
/// 序列：SETWITHETAG → SAVE（etag 记录进检查点）→ SETIFMATCH 推进（仅 AOF）
/// → DEL 级联清退（etag 墓碑仅 AOF）→ RENAME 搬迁 → 停服 → --recover →
/// GETWITHETAG / DELIFGREATER / SETWITHETAG 断言。覆盖点：
/// 1. 检查点后 etag 推进经 AOF 恢复（修复前恒回 0）；
/// 2. 检查点内 etag 记录随 DEL 级联清除，恢复后无残留复活（SETWITHETAG
///    从 NoETag 起步而非复活旧值）；
/// 3. RENAME 整记录搬迁（值 + 绝对 etag）；
/// 4. P0 缺陷场景：恢复后 DELIFGREATER 条件判定与主端一致，不误删。
#[test]
fn recover_etag_chain_after_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("etag_recover.db");

  // ---- 第一代进程：写入 → SAVE → 推进/级联/搬迁（仅 AOF）→ 停服
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // SAVE 前：ek / dk 各带 etag=1（将随检查点落盘）
    send_cmd(&mut stream, &[b"SETWITHETAG", b"ek", b"v1"])
      .await
      .expect("setwithetag ek");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"SETWITHETAG", b"dk", b"w"])
      .await
      .expect("setwithetag dk");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"SAVE"]).await.expect("save");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    // SAVE 后增量：SETIFMATCH 推进（命中条件 etag==1 → 新 etag 2，仅 AOF）
    send_cmd(&mut stream, &[b"SETIFMATCH", b"ek", b"v2", b"1"])
      .await
      .expect("setifmatch ek");
    assert_eq!(read_etag_array(&mut stream).await, Some((2, None)));
    send_cmd(&mut stream, &[b"SETIFMATCH", b"dk", b"w2", b"1"])
      .await
      .expect("setifmatch dk");
    assert_eq!(read_etag_array(&mut stream).await, Some((2, None)));

    // DEL 级联清退 dk（etag 墓碑 + 值墓碑均仅 AOF；检查点内旧 etag 记录
    // 须被清除条目覆盖，否则恢复后复活 etag=2）
    send_cmd(&mut stream, &[b"DEL", b"dk"])
      .await
      .expect("del dk");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());

    // RENAME 搬迁（值 + 绝对 etag 一体迁移，仅 AOF）
    send_cmd(&mut stream, &[b"SETWITHETAG", b"mk", b"mv"])
      .await
      .expect("setwithetag mk");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"RENAME", b"mk", b"mk2"])
      .await
      .expect("rename mk");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    // 物理刷盘 + 停服
    provider
      .aof()
      .expect("aof enabled")
      .commit_flush_async()
      .await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代进程：--recover 重启 → etag 全链断言
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      session_factory,
    ))
    .expect("open recovered"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");

    // 1. 检查点后推进的 etag 经 AOF 恢复（修复前恒为 0）
    send_cmd(&mut stream, &[b"GETWITHETAG", b"ek"])
      .await
      .expect("getwithetag ek");
    assert_eq!(
      read_etag_array(&mut stream).await,
      Some((2, Some(b"v2".to_vec()))),
      "SAVE 后 SETIFMATCH 推进的 etag 须经 AOF 恢复"
    );

    // 2. DEL 级联后 etag 无残留复活：重建设读值从 NoETag 起步（修复前
    //    检查点内旧 etag=2 复活，首写回 :3）
    send_cmd(&mut stream, &[b"SETWITHETAG", b"dk", b"w3"])
      .await
      .expect("setwithetag dk");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b":1\r\n".to_vec(),
      "DEL 级联清退后 etag 记录不得复活"
    );

    // 3. RENAME 搬迁恢复
    send_cmd(&mut stream, &[b"GETWITHETAG", b"mk2"])
      .await
      .expect("getwithetag mk2");
    assert_eq!(
      read_etag_array(&mut stream).await,
      Some((1, Some(b"mv".to_vec()))),
      "RENAME 搬迁的值与 etag 须经 AOF 恢复"
    );

    // 4. P0 缺陷场景：DELIFGREATER 判定与主端一致（etag=2 时 given=1 不
    //    命中不删；修复前 etag=0 → 1>0 误删 → 副本数据丢失）
    send_cmd(&mut stream, &[b"DELIFGREATER", b"ek", b"1"])
      .await
      .expect("delifgreater ek");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b":0\r\n".to_vec(),
      "恢复后 etag=2，given=1 不命中不得删除"
    );
    send_cmd(&mut stream, &[b"GET", b"ek"])
      .await
      .expect("get ek");
    assert_eq!(read_bulk_reply(&mut stream).await, Some(b"v2".to_vec()));

    // etag 继续正常推进（2 → 3）
    send_cmd(&mut stream, &[b"SETWITHETAG", b"ek", b"v3"])
      .await
      .expect("setwithetag ek");
    assert_eq!(read_line_reply(&mut stream).await, b":3\r\n".to_vec());
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}

/// 副本形态 AOF 重放等价：主端经真实 RESP 命令 + 写监听端口产生的 AOF 流
/// 重放进全新空库（副本增量重放路径），值与 etag 状态与主端逐键一致；
/// 回放后条件写族（DELIFGREATER / SETIFGREATER / SETIFMATCH）判定与主端
/// 一致——P0 缺陷面：etag 恢复缺失时副本上 DELIFGREATER 误删数据、
/// SETIFGREATER 判定翻转
#[test]
fn aof_replay_etag_equivalence_replica_form() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("etag_replica.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      None,
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // 主端写入序列：推进 / 级联删除 / 搬迁
    send_cmd(&mut stream, &[b"SETWITHETAG", b"a", b"va"])
      .await
      .expect("setwithetag a");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"SETIFMATCH", b"a", b"va2", b"1"])
      .await
      .expect("setifmatch a");
    assert_eq!(read_etag_array(&mut stream).await, Some((2, None)));
    send_cmd(&mut stream, &[b"SETWITHETAG", b"b", b"vb"])
      .await
      .expect("setwithetag b");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"DEL", b"b"]).await.expect("del b");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"SETWITHETAG", b"c", b"vc"])
      .await
      .expect("setwithetag c");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n".to_vec());
    send_cmd(&mut stream, &[b"RENAME", b"c", b"c2"])
      .await
      .expect("rename c");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
    provider
      .aof()
      .expect("aof enabled")
      .commit_flush_async()
      .await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);

  // ---- 副本形态：AOF 流重放进全新空库（端口注册但不镜像回写）
  rt.block_on(async {
    let aof = Arc::clone(provider.aof().expect("aof enabled"));
    let replica_path = dir.path().join("node").join("etag_replica_target.db");
    // 小预算测试配置注入（生产缺省 open_node 走 StoreConfig::auto）
    let (store, _broker, _vm) =
      open_node_with_config(test_store_config(), replica_path).expect("open replica store");
    let target_session = store.new_session().expect("replica session");
    let _pause = store.pause_aof_listeners();
    let batch = target_session.enter_batch();
    let storage = StorageSession::new(batch);
    let mut processor = AofProcessor::new(Arc::clone(&aof));
    processor.set_range_index_manager(Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      &store.range_index,
    ))));
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&store),
      store_version: store.current_version(),
    };
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
      .await
      .expect("replay");
    assert!(replayed > 0, "副本重放须消化主端条目");

    // 逐键断言：值与 etag 与主端一致（修复前 etag 恒 0 / DEL 后复活）
    let rs = store.new_session().expect("assert session");
    assert_eq!(
      (
        rs.read(b"a").await.expect("read a"),
        rs.etag_of(b"a").await.expect("etag a")
      ),
      (Some(b"va2".to_vec()), Some(2)),
      "SETIFMATCH 推进后的值与 etag 须一致"
    );
    assert_eq!(
      (
        rs.read(b"b").await.expect("read b"),
        rs.etag_of(b"b").await.expect("etag b")
      ),
      (None, None),
      "DEL 级联后值与 etag 均须清除"
    );
    assert_eq!(
      (
        rs.read(b"c2").await.expect("read c2"),
        rs.etag_of(b"c2").await.expect("etag c2")
      ),
      (Some(b"vc".to_vec()), Some(1)),
      "RENAME 搬迁的值与 etag 须一致"
    );
    assert_eq!(
      rs.etag_of(b"c").await.expect("etag c"),
      None,
      "旧键 etag 须随搬迁清退"
    );

    // 回放后条件写判定与主端一致（etag 真值源已恢复，条件基线不再归零）
    let cs = store.new_session().expect("verdict session");
    let api = StoreGarnetApi::new(cs);
    let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
    // DELIFGREATER a 1：1 不严格大于恢复后的 etag 2 → 不删（修复前 etag=0
    // → 1 > 0 误删，副本数据丢失）
    api.exec(&mut s, RespCommand::Delifgreater, &[b"a", b"1"]);
    assert_eq!(s.output, b":0\r\n", "回放后 DELIFGREATER 判定不得误删");
    s.output.clear();
    // SETIFGREATER a x 5：5 > 2 命中 → 新 etag 5，应答 [5, nil]
    api.exec(&mut s, RespCommand::Setifgreater, &[b"a", b"x", b"5"]);
    assert_eq!(
      s.output, b"*2\r\n:5\r\n$-1\r\n",
      "回放后 SETIFGREATER 命中判定"
    );
    s.output.clear();
    // SETIFGREATER a y 3：3 不严格大于 5 不命中 → [5, 旧值 x]
    api.exec(&mut s, RespCommand::Setifgreater, &[b"a", b"y", b"3"]);
    assert_eq!(
      s.output, b"*2\r\n:5\r\n$1\r\nx\r\n",
      "回放后 SETIFGREATER 不命中判定"
    );
    s.output.clear();
    // SETIFMATCH a va3 5：5 == 5 命中 → etag 推进 6 → [6, nil]
    api.exec(&mut s, RespCommand::Setifmatch, &[b"a", b"va3", b"5"]);
    assert_eq!(
      s.output, b"*2\r\n:6\r\n$-1\r\n",
      "回放后 SETIFMATCH 命中推进"
    );
    s.output.clear();
    // 推进后的 etag 真值可读
    api.exec(&mut s, RespCommand::Getwithetag, &[b"a"]);
    assert_eq!(s.output, b"*2\r\n:6\r\n$3\r\nva3\r\n");
  });
}

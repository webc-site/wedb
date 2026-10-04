//! AOF 撕裂残尾恢复（对标 garnet/test/standalone/Garnet.test/RespAofTornTailTests.cs:37
//! TornTailCommitRecordRecovery）：非优雅停机留下最后一帧半写，恢复不得崩溃，
//! 且重放覆盖至最后一条完整记录；恢复后日志可继续写，新数据可二次恢复。
//!
//! rust 恢复端「CRC 记录链扫描自同步定位尾部」无独立提交元数据文件（waof
//! recover.rs 在册架构差异），故 C# 的删 log-commits 元数据步骤无对位——直接
//! 对 wal 文件做尾截断即残尾注入的唯一形态，恢复端按「EOF/校验和失败 → 保守
//! 截断至最后一条完整记录」容错臂承接。

use std::{
  fs::{OpenOptions, metadata, read_dir},
  sync::Arc,
};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode_test::{
  open_aof_provider, open_recovered_provider, read_line_reply, send_cmd, start_server,
};

/// 残尾截断字节数（对齐 C# tornBytes=12：掉 CommitNum(8) + cookieLength(4)
/// 帧尾，校验和必败）；C# numKeys=1000 收敛为 100，保持撕裂臂判定精度不变
const TORN_BYTES: u64 = 12;
const NUM_KEYS: usize = 100;

/// 撕裂残尾恢复：恢复不炸 + 已提交键全量回读 + 恢复后可续写 + 二次恢复收敛
#[test]
fn torn_tail_commit_record_recovery() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("torn_tail.db");
  let wal_path = data_path.parent().unwrap().join("wal").join("wal.log");

  // 1. AOF 全开节点：100 键逐条 SET（auto_commit 开，enqueue 同步推提交位），
  //    收尾显式等提交位点覆盖尾（C# commitWait 形态：每条 ack 即持久）
  let provider = open_aof_provider(&data_path);
  // 截断前日志尾（撕裂臂只允许吞尾帧，恢复位点须不越过它）
  let (server, addr) = start_server(Arc::clone(&provider));
  let pre_tail = rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    for i in 0..NUM_KEYS {
      let key = format!("key{i}");
      let value = format!("value{i}");
      send_cmd(&mut stream, &[b"SET", key.as_bytes(), value.as_bytes()])
        .await
        .expect("set");
      assert!(
        read_line_reply(&mut stream).await.starts_with(b"+OK"),
        "key{i} 须写入成功"
      );
    }
    let aof = provider.aof().expect("aof enabled");
    aof
      .log()
      .wait_for_commit_all_async(0)
      .await
      .expect("提交位点推进");
    // 落盘收口（C# Dispose(false) 优雅段对位：环形缓冲刷盘 + 尾追加提交
    // 指纹帧——该帧即撕裂候选，正对 C# 残尾事故形态「撕裂的是 commit 记录」）
    aof.dispose_async().await;
    aof.log().tail_address().max()
  });

  // 2. 停机不删文件（C# server.Dispose(false) 对位）
  server.stop();
  drop(server);
  drop(provider);

  // 3. 注入残尾：尾截 12 字节（C# SimulateTornTailWrite 的 (b) 步；无元数据
  //    文件域，(a) 步删除动作在 rust 自同步恢复架构下天然成立）。
  //    段文件命名 `<base>.<段号32进制>`（segment_path），残尾必落在最大段号段
  // 截断后文件长度（观测面：恢复位点断言经地址域 pre_tail 承接，此处仅
  // 校验截断动作本身落地）
  let _torn_len = {
    let mut segments: Vec<_> = read_dir(wal_path.parent().unwrap())
      .expect("wal 目录在位")
      .map(|e| e.expect("readdir").path())
      .filter(|p| {
        p.file_name()
          .is_some_and(|n| n.to_string_lossy().starts_with("wal.log"))
      })
      .collect();
    segments.sort();
    let tail_seg = segments.last().expect("至少一个段文件");
    let len = metadata(tail_seg).expect("wal 文件在位").len();
    assert!(len > TORN_BYTES, "日志尾须有余量可截");
    OpenOptions::new()
      .write(true)
      .open(tail_seg)
      .expect("open wal")
      .set_len(len - TORN_BYTES)
      .expect("truncate tail");
    len - TORN_BYTES
  };

  // 4. 恢复装配必须成功（C# Assert.DoesNotThrow(server.Start) 对位：
  //    残尾走保守截断容错臂，绝不上抛）
  let provider2 = rt.block_on(open_recovered_provider(&data_path));

  // 5. 已提交操作回读至倒数第二条（C# 断言 0..numKeys-1 口径；豁免候选仅为
  //    残尾吞掉的尾帧——本形态各 SET 已逐条持久，尾帧是提交指纹帧，键集合
  //    实际完整在场，断言保持宽容向）。恢复位点双向夹逼：非零且不越过截断前尾
  let recovered = provider2.recovered_aof_tail().expect("恢复位点点亮");
  assert!(
    recovered.get(0).is_some_and(|a| a > 0 && a <= pre_tail),
    "恢复位点 {recovered:?} 须落在 (0, {pre_tail}] 内"
  );

  let session = provider2.store().new_session().expect("session");
  for i in 0..NUM_KEYS - 1 {
    let key = format!("key{i}");
    assert_eq!(
      rt.block_on(session.read(key.as_bytes())).expect("read"),
      Some(format!("value{i}").into_bytes()),
      "已提交 {key} 须自 AOF 恢复"
    );
  }

  // 6. 恢复后 AOF 可继续写（C# numKeys..numKeys*2 续写对位）
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("connect2");
    for i in NUM_KEYS..NUM_KEYS * 2 {
      let key = format!("key{i}");
      let value = format!("value{i}");
      send_cmd(&mut stream, &[b"SET", key.as_bytes(), value.as_bytes()])
        .await
        .expect("set2");
      assert!(
        read_line_reply(&mut stream).await.starts_with(b"+OK"),
        "恢复后 {key} 须可写"
      );
    }
    let aof = provider2.aof().expect("aof enabled2");
    aof
      .log()
      .wait_for_commit_all_async(0)
      .await
      .expect("二次提交位点推进");
  });
  server2.stop();
  drop(server2);
  drop(provider2);

  // 7. 二次恢复：原键与新键全部存活（C# 第二轮 DoesNotThrow + 双段回读对位）
  let provider3 = rt.block_on(open_recovered_provider(&data_path));
  let session = provider3.store().new_session().expect("session3");
  for i in 0..NUM_KEYS * 2 {
    if i == NUM_KEYS - 1 {
      continue; // 撕裂臂允许丢弃的唯一候选
    }
    let key = format!("key{i}");
    assert_eq!(
      rt.block_on(session.read(key.as_bytes())).expect("read3"),
      Some(format!("value{i}").into_bytes()),
      "{key} 须跨二次恢复存活"
    );
  }
}

//! 假迁移目标端单源（脚本化应答：按连接分配脚本逐帧弹答）
//!
//! 收口 cluster_migration / cluster_migration_domain / migrate_deleting_unheld_claim /
//! migrate_epoch_drain_failclose / migrate_fail_inject 五册近逐字同形的
//! scripted_migrate_target + reserve_reply。差异面（RESERVE 合成臂基数、seen
//! 载荷字节数后缀、逐帧原文留档槽）以 [`ScriptedTargetOptions`] 暴露。宿主册
//! 直挂（沿用 resp_drive_scratch 先例）：
//!
//! ```text
//! #[path = "common/scripted_migrate_target.rs"]
//! mod scripted_migrate_target_core;
//! use scripted_migrate_target_core::{ScriptedTargetOptions, scripted_migrate_target};
//! ```
//!
//! 帧解析走 `wtest_base::parse_frame_slices` 单源。不进
//! common/mod.rs 聚合面——按册直挂裁项，避免不消费册招 per-binary
//! dead_code（primary_assets 先例警示）。

use std::{
  collections::VecDeque,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
};
use parking_lot::Mutex;
use wtest_base::{parse_frame_slices, resp_frame};

/// 假目标端行为差异面（各册按语义取舍；全 Copy 可入 static）
#[derive(Clone, Copy, Default)]
pub struct ScriptedTargetOptions<'a> {
  /// CLUSTER RESERVE 合成就地应答的上下文 id 发号基数（None = 不启用合成臂，
  /// RESERVE 帧照常耗脚本；脚本 +OK 非合法预留应答，向量集收尾需真实 id
  /// 完成重映射）
  pub reserve_ctx_base: Option<u64>,
  /// seen 记录追加第 6 参载荷字节数（` payload=N` 后缀，供载荷非空断言）
  pub seen_with_payload_len: bool,
  /// 逐帧原文留档槽（Some 槽逐帧 push 原始帧字节，供用例按键名检索「发没发」
  /// ——跨用例并发追加，断言侧只用各用例独有键名过滤，互不污染）
  pub frame_archive: Option<&'a Mutex<Vec<Vec<u8>>>>,
}

/// CLUSTER RESERVE VECTOR_SET_CONTEXTS 合成应答：按请求数即时产出上下文
/// 字符串数组（基数起自增发号）
fn reserve_reply(args: &[&[u8]], ctx_ids: &AtomicU64) -> Vec<u8> {
  let count: usize = args
    .get(3)
    .and_then(|a| from_utf8(a).ok())
    .and_then(|s| s.parse().ok())
    .unwrap_or(0);
  let ids: Vec<Vec<u8>> = (0..count)
    .map(|_| {
      ctx_ids
        .fetch_add(1, Ordering::Relaxed)
        .to_string()
        .into_bytes()
    })
    .collect();
  let refs: Vec<&[u8]> = ids.iter().map(Vec::as_slice).collect();
  resp_frame(&refs)
}

/// 假迁移目标端（模式对标 tests/appendlog_reject_disconnect.rs 的
/// reject_after_handshake_node）：监听 127.0.0.1:0，按连接分配脚本——第 i 个
/// 连接用第 i 段脚本，逐帧解析 RESP2 数组按脚本弹答（+OK/-ERR），该段脚本耗尽
/// 后本连接保持静默（模拟目标挂起）；脚本段耗尽后的新连接同样静默（模拟重连
/// 无应答）。每帧前三参记入 seen 供用例断言帧序；CLUSTER RESERVE 合成就地
/// 应答不耗脚本。返回监听地址串。
pub async fn scripted_migrate_target(
  conn_replies: Vec<Vec<&'static [u8]>>,
  seen: Arc<Mutex<Vec<String>>>,
  opts: ScriptedTargetOptions<'static>,
) -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let scripts = Arc::new(Mutex::new(VecDeque::from(
    conn_replies
      .into_iter()
      .map(VecDeque::from)
      .collect::<Vec<_>>(),
  )));
  let ctx_ids = Arc::new(AtomicU64::new(opts.reserve_ctx_base.unwrap_or(0)));
  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      let scripts = Arc::clone(&scripts);
      let seen = Arc::clone(&seen);
      let ctx_ids = Arc::clone(&ctx_ids);
      spawn(async move {
        // 每连接独立脚本：无脚本段 → 连接直读不答（重连也无应答）
        let mut script = scripts.lock().pop_front().unwrap_or_default();
        let mut acc: Vec<u8> = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          acc.extend_from_slice(&buf[..n]);
          while let Some((frame_len, args)) = parse_frame_slices(&acc) {
            // RESERVE 合成就地应答（不弹脚本，见 reserve_reply）；其余帧按脚本弹答
            let is_reserve = opts.reserve_ctx_base.is_some()
              && args.len() >= 4
              && args[0].eq_ignore_ascii_case(b"CLUSTER")
              && args[1].eq_ignore_ascii_case(b"RESERVE");
            let reply = if is_reserve {
              Some(reserve_reply(&args, &ctx_ids))
            } else {
              script.pop_front().map(|r| r.to_vec())
            };
            let head = args
              .iter()
              .take(3)
              .map(|a| String::from_utf8_lossy(a).into_owned())
              .collect::<Vec<_>>()
              .join(" ");
            let seen_line = if opts.seen_with_payload_len {
              format!("{head} payload={}", args.get(5).map_or(0, |p| p.len()))
            } else {
              head
            };
            if let Some(archive) = opts.frame_archive {
              archive.lock().push(acc[..frame_len].to_vec());
            }
            acc.drain(..frame_len);
            seen.lock().push(seen_line);
            // 脚本耗尽 → 静默（目标挂起，驱动停等只能超时）
            if let Some(reply) = reply
              && stream.write_all(reply).await.is_err()
            {
              return;
            }
          }
        }
      })
      .detach();
    }
  })
  .detach();
  addr
}

//! 客户端 dispose 拆连面集成测试
//!（对标 C# `libs/client/GarnetClient.cs:Dispose(bool)` :521-532 无条件拆 fd 契约）
//!
//! 立案触发链（task/issue/
//! wconn-client-dispose-read-pump-hang-silent-peer-leak.md）：timeout 旋钮关闭
//!（timeout_millis=0）+ 对端 accept 后静默不断（半开连接）时，旧实现被 dispose
//! 连接的常驻读唯一唤醒源依赖 progress 超时判成，判成不启用即读泵 task、读半 fd、
//! 池借出缓冲永久滞留。修复后 dispose 面双向 shutdown 令常驻读即刻以 EOF/err 内核
//! 落定，读泵沿既有读收场支退出。断言八组：半开 dispose 收场、最后 Arc 落下 Drop
//! 兜底收场、在途命令拆连结算（旋钮开/关两态不误伤超时口径）、正常请求-响应链
//! dispose 零回归、二次 connect_async 的换代拆旧守卫、dispose 后 reconnect_async
//! 恒拒、dispose 后命令面即刻 Disposed（含发出即忘形不得 Ok）。

use std::{future::pending, sync::Arc, time::Duration};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use wbase::pool::{DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool};
use wconn::{Error, client::GarnetClient, network::READ_CHUNK};

/// 收场总时限：双向拆连为内核即刻唤醒（毫秒级），写泵存活轮询粒度 250ms，
/// 5s 覆盖重载机器调度抖动（client_timeout.rs 同档先例）
const SETTLE_BUDGET: Duration = Duration::from_secs(5);

/// 有界轮询断言面：粒度 25ms、上界 [`SETTLE_BUDGET`]，到期谓词仍假即调用方断言失败
async fn poll_until(cond: impl Fn() -> bool) -> bool {
  for _ in 0..200 {
    if cond() {
      return true;
    }
    sleep(Duration::from_millis(25)).await;
  }
  cond()
}

/// 静默对端：accept 后持有 socket 永久静默（裸半开形态，不发不断）。
/// 注意必须持有 accept 出的 socket——其随任务 drop 会立即回 RST/EOF，
/// 半开前提即消失
async fn silent_server() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (_sock, _peer) = listener.accept().await.unwrap();
    // 半开保持：不读不写不回，TCP 层无 FIN 无 RST
    pending::<()>().await;
  })
  .detach();
  addr
}

/// 注入池 + 静默对端建连（timeout 旋钮关闭形态），返回已连接的客户端与池
async fn half_open_client(timeout_millis: u64) -> (Arc<GarnetClient>, Arc<LimitedFixedBufferPool>) {
  let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let mut client =
    GarnetClient::new(silent_server().await, None, None, None, 16, timeout_millis).unwrap();
  client.set_network_pool(Some(Arc::clone(&pool)));
  client.connect_async().await.unwrap();
  assert!(client.is_connected(), "半开建连应报告已连接");
  // 有界等待读泵进入稳态挂读：池借出就位为唯一连接级单块
  //（network_socket_integration.rs 稳态挂读同判据）
  assert!(
    poll_until(|| pool.borrowed_count() == 1).await,
    "读泵未进入稳态挂读（池借出未就绪）"
  );
  (Arc::new(client), pool)
}

/// dispose 收场主断言：timeout 旋钮关闭 + 静默对端半开连接，dispose 后读泵
/// 任务即时收场——池借出缓冲 RAII 归池、连接态翻假、后续命令以断连失败不挂起
///（本用例红即立案泄漏窗复现）
#[compio::test]
async fn dispose_settles_read_pump_on_silent_peer() {
  let (client, pool) = half_open_client(0).await;

  client.dispose();
  assert!(
    !client.is_connected(),
    "dispose 后 is_connected 应即刻翻假（幂等位）"
  );

  // 读泵任务必须收场：池借出缓冲归零、唯一接收块回池可复用
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "读泵任务未在 {SETTLE_BUDGET:?} 内收场：池借出缓冲未归还（立案泄漏窗）"
  );
  assert_eq!(pool.free_count(), 1, "接收缓冲必须归池复用");

  // 后续命令在入通道前即被 dispose 幂等位拒绝（即刻失败，不挂起、不被动结算）
  let next = timeout(
    SETTLE_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await;
  assert!(
    matches!(next, Ok(Err(Error::Disposed))),
    "dispose 后后续命令应即刻以 Disposed 拒绝，实际 {next:?}"
  );

  // 幂等：二次 dispose 无害（对标 C# Interlocked 守卫）
  client.dispose();
}

/// 最后 Arc 落下兜底：Drop 仅转发同一拆连面——facade 建连限时放弃臂
///（wedb/src/client.rs:155-179 局部 client 落下）与消费点自然放尽同形态收场
#[compio::test]
async fn last_arc_drop_settles_pump_on_silent_peer() {
  let (client, pool) = half_open_client(0).await;

  drop(client);
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "Drop 兜底未在 {SETTLE_BUDGET:?} 内令读泵收场：池借出缓冲未归还"
  );
  assert_eq!(pool.free_count(), 1, "接收缓冲必须归池复用");
}

/// 在途命令拆连结算（旋钮关闭）：dispose 令在途 oneshot 随在途通道销毁断连，
/// roundtrip 以既有断连错误 ResponseChannelClosed 结算，不挂起、不误判 Timeout
#[compio::test]
async fn dispose_settles_in_flight_command_as_disconnect() {
  let (client, _pool) = half_open_client(0).await;

  let late = Arc::clone(&client);
  let discharger = spawn(async move {
    sleep(Duration::from_millis(50)).await;
    late.dispose();
  });
  let res = timeout(
    SETTLE_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await
  .unwrap_or_else(|_| panic!("在途命令未在 {SETTLE_BUDGET:?} 内随拆连收场"));
  discharger.await.unwrap();

  let err = res.expect_err("在途命令应随拆连以断连错误结算，不应 Ok");
  assert!(
    matches!(err, Error::ResponseChannelClosed),
    "拆连结算应为断连错误形态 ResponseChannelClosed，实际 {err:?}"
  );
}

/// 在途命令拆连结算（旋钮开启）：progress 语义回归纯超时判成——判成尚未到
/// 周期时 dispose 收场同样按断连错误结算，与 client_timeout.rs 的判成 Timeout
/// 口径互不误伤（分类锁测）
#[compio::test]
async fn dispose_with_timeout_knob_on_settles_as_disconnect_not_timeout() {
  // 判成周期 60s：50ms 处的 dispose 远早于任何判成，Timeout 结算即为误伤
  let (client, _pool) = half_open_client(60_000).await;

  let late = Arc::clone(&client);
  let discharger = spawn(async move {
    sleep(Duration::from_millis(50)).await;
    late.dispose();
  });
  let res = timeout(
    SETTLE_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await
  .unwrap_or_else(|_| panic!("在途命令未在 {SETTLE_BUDGET:?} 内随拆连收场"));
  discharger.await.unwrap();

  let err = res.expect_err("在途命令应随拆连以断连错误结算，不应 Ok");
  assert!(
    matches!(err, Error::ResponseChannelClosed),
    "旋钮开启态 dispose 收场应按断连错误结算（判成语义不承载拆连），实际 {err:?}"
  );
}

/// 正对照零回归：正常请求-响应链（假 RESP 端点逐帧回 +PONG）多轮往返后
/// dispose——应答全部正常结算、连接态翻假、池借出归还、拆连后命令即刻失败
#[compio::test]
async fn dispose_after_normal_roundtrip_zero_regression() {
  // 假 RESP 端点：读到命令帧即回 +PONG，对端拆连后读落定退出
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let endpoint = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (mut sock, _peer) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; 4096];
    loop {
      let BufResult(res, next) = sock.read(buf).await;
      buf = next;
      match res {
        Ok(n) if n > 0 => {}
        // 对端拆连收场（EOF/err）：端点退场
        _ => break,
      }
      let BufResult(res, _) = sock.write_all(b"+PONG\r\n".to_vec()).await;
      if res.is_err() {
        break;
      }
    }
  })
  .detach();

  let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let mut client = GarnetClient::new(endpoint, None, None, None, 16, 0).unwrap();
  client.set_network_pool(Some(Arc::clone(&pool)));
  client.connect_async().await.unwrap();

  // 正常多轮请求-响应：应答逐一结算，读泵稳态恒为单块池借出
  for i in 0..8 {
    let resp = client
      .execute_for_string_result_async(&["PING"])
      .await
      .unwrap_or_else(|e| panic!("第 {i} 轮正常往返不应回归: {e}"));
    assert_eq!(resp, "PONG", "第 {i} 轮应答文本应逐轮结算");
    assert_eq!(pool.borrowed_count(), 1, "稳态恒为连接级单次池借出");
  }

  client.dispose();
  assert!(!client.is_connected(), "dispose 后 is_connected 应翻假");
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "正常连接 dispose 后读泵未收场：池借出缓冲未归还"
  );
  assert_eq!(pool.allocated_count(), 1, "零回归：稳态未产生第二块新分配");

  let next = client.execute_for_string_result_async(&["PING"]).await;
  assert!(
    matches!(next, Err(Error::Disposed)),
    "拆连后命令应即刻以 Disposed 拒绝，实际 {next:?}"
  );
}

/// 换代拆旧守卫（面一，资源泄漏）：未经 dispose 直调二次 connect_async（pub API
/// 面——门面与 reconnect_async 均先 dispose，故仓内不可达）必须先拆净旧代，
/// 否则旧拆连句柄被 `*dispose_slot = Some(..)` 静默覆盖：只关 dup fd、不下发
/// shutdown，旧连拆除全赖旧 tx 断→writer_done 路。本用例把旧写泵钉在挂起态
///（在途闸 2 满、静默对端不应答 → 写泵 await 在途入队即永久挂起、永不回到收
/// 命令臂），该路随之失效：旧读写泵、fd、池借出缓冲永不回收，且再无句柄可拆
///（Drop 兜底只触新代）。判据取两代各自注入的一块独立池：旧池借出归零 = 旧读写
/// 泵确已收场、新池借出就位 = 新代确已挂读，两判据互不混淆（共池下新旧同数，
/// 判别力失真）
#[compio::test]
async fn second_connect_async_settles_previous_generation() {
  let old_pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let new_pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  // 在途闸 2：两条带应答命令入在途队列即满，第三条钉死写泵
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 2, 0).unwrap();
  client.set_network_pool(Some(Arc::clone(&old_pool)));
  client.connect_async().await.unwrap();
  assert!(
    poll_until(|| old_pool.borrowed_count() == 1).await,
    "旧代读泵未进入稳态挂读（池借出未就绪）"
  );

  // 三条命令逐条限时发起（应答永不到来即到期弃约，帧已入通道由写泵接手）
  for _ in 0..3 {
    let res = timeout(
      Duration::from_millis(100),
      client.execute_for_string_result_async(&["PING"]),
    )
    .await;
    assert!(res.is_err(), "静默对端下命令应按限时到期弃约，实际 {res:?}");
  }
  // 写泵钉在在途满挂起点、读泵稳态挂读的判据走轮询预算（25ms 粒度、5s 上界），
  // 不押注固定留量——慢机上写泵未及挂起即换代会让守卫失真
  assert!(
    poll_until(|| old_pool.borrowed_count() == 1).await,
    "旧代写泵应钉在在途满挂起点、读泵稳态挂读（旧池借出单块）"
  );

  // 换代：改端点、换池后直调二次建连，不走任何 dispose 前置
  client.end_point = silent_server().await;
  client.set_network_pool(Some(Arc::clone(&new_pool)));
  client.connect_async().await.unwrap();
  assert!(client.is_connected(), "新代建连后应报告已连接");
  assert!(
    poll_until(|| old_pool.borrowed_count() == 0 && new_pool.borrowed_count() == 1).await,
    "旧代读写泵未在 {SETTLE_BUDGET:?} 内收场（旧池借出未归还）：换代静默丢句柄泄漏窗复现"
  );
}

/// dispose 恒拒（面二，守卫语义）：已 dispose 实例的 reconnect_async 一律以
/// Disposed 拒绝（对标 C# `if (Disposed) throw disposeException;`），不得经
/// connect_async 换新幂等位而复活；恒拒路径零副作用（不改连接态、不建第二块缓冲）
#[compio::test]
async fn reconnect_async_rejects_disposed_instance() {
  let pool = LimitedFixedBufferPool::new(READ_CHUNK, DEFAULT_MAX_ENTRIES_PER_LEVEL);
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 16, 0).unwrap();
  client.set_network_pool(Some(Arc::clone(&pool)));
  client.connect_async().await.unwrap();
  assert!(
    poll_until(|| pool.borrowed_count() == 1).await,
    "读泵未进入稳态挂读"
  );

  client.dispose();
  assert!(
    poll_until(|| pool.borrowed_count() == 0).await,
    "dispose 后读泵未收场"
  );

  let err = client
    .reconnect_async()
    .await
    .expect_err("已 dispose 实例的 reconnect_async 应恒拒，不得复活");
  assert!(
    matches!(err, Error::Disposed),
    "恒拒错误形制应为 Disposed，实际 {err:?}"
  );
  assert!(!client.is_connected(), "恒拒不得改动连接态");
  assert_eq!(pool.borrowed_count(), 0, "恒拒不得新建接收块");
  assert_eq!(pool.allocated_count(), 1, "恒拒不得产生第二块新分配");

  // 恒拒可重复（对标 C# 单例异常，二次调用同形）
  let again = client
    .reconnect_async()
    .await
    .expect_err("恒拒是稳定态而非一次性窗");
  assert!(matches!(again, Error::Disposed), "实际 {again:?}");
}

/// dispose 后命令面错误形制（面三）：五种带应答形与发出即忘形在入通道前即判
/// dispose 幂等位，一律即刻 `Error::Disposed`（对标 C# 发送路径槽位填满后六处
/// 同形的 GarnetClientDisposedException 即时判定），不再以
/// ResponseChannelClosed/ReadPumpExited 被动结算；发出即忘形尤其不得在 250ms
/// 限时窗内 send 成功返 Ok 而帧永不达对端
#[compio::test]
async fn dispose_makes_every_execute_face_return_disposed() {
  let (client, _pool) = half_open_client(0).await;
  client.dispose();

  let str_res = client.execute_for_string_result_async(&["PING"]).await;
  assert!(
    matches!(str_res, Err(Error::Disposed)),
    "字符串应答形应为 Disposed，实际 {str_res:?}"
  );
  let bytes_res = client
    .execute_for_bytes_result_async(&[b"PING".as_slice()])
    .await;
  assert!(
    matches!(bytes_res, Err(Error::Disposed)),
    "字节应答形应为 Disposed，实际 {bytes_res:?}"
  );
  let arr_res = client
    .execute_for_string_array_result_async(&["PING"])
    .await;
  assert!(
    matches!(arr_res, Err(Error::Disposed)),
    "字符串数组应答形应为 Disposed，实际 {arr_res:?}"
  );
  let attach_res = client.execute_cluster_attach_sync(&[0u8; 8]).await;
  assert!(
    matches!(attach_res, Err(Error::Disposed)),
    "CLUSTER ATTACH_SYNC 帧形应为 Disposed，实际 {attach_res:?}"
  );

  // 发出即忘形：不得 Ok（帧永不达对端即假完成），亦不得挂起
  let no_resp = timeout(
    SETTLE_BUDGET,
    client.execute_no_response_async(&[b"PING".as_slice()]),
  )
  .await
  .unwrap_or_else(|_| panic!("发出即忘形不应挂起"));
  assert!(
    matches!(no_resp, Err(Error::Disposed)),
    "发出即忘形应即刻 Disposed 而非 Ok，实际 {no_resp:?}"
  );
}

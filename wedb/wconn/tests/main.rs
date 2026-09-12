use aok::{OK, Void};
use compio::{
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
};
use log::info;
use wconn::{GarnetClient, GarnetClientSession, InfoMetricsType, SortedSetPairCollection};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test_garnet_client_and_session_apis() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = addr.to_string();

    spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        spawn(async move {
          let mut buf = vec![0u8; 4096];
          loop {
            let compio::BufResult(res, b) = stream.read(buf).await;
            buf = b;
            let n = match res {
              Ok(n) if n > 0 => n,
              _ => break,
            };
            let req = &buf[..n];
            let resp: &[u8] = if req.windows(6).any(|w| w.eq_ignore_ascii_case(b"LRANGE")) {
              b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
            } else if req.windows(9).any(|w| w.eq_ignore_ascii_case(b"REPLICAOF")) {
              b"+OK\r\n"
            } else if req.windows(5).any(|w| w.eq_ignore_ascii_case(b"LPUSH"))
              || req.windows(4).any(|w| w.eq_ignore_ascii_case(b"LLEN"))
            {
              b":2\r\n"
            } else if req.windows(5).any(|w| w.eq_ignore_ascii_case(b"RPUSH")) {
              b":3\r\n"
            } else if req.windows(5).any(|w| w.eq_ignore_ascii_case(b"ZCARD"))
              || req
                .windows(4)
                .any(|w| w.eq_ignore_ascii_case(b"ZADD") || w.eq_ignore_ascii_case(b"ZREM"))
              || req.windows(3).any(|w| w.eq_ignore_ascii_case(b"DEL"))
            {
              b":1\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"PING")) {
              b"+PONG\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"INCR")) {
              b":42\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"DECR")) {
              b":41\r\n"
            } else if req.windows(4).any(|w| w.eq_ignore_ascii_case(b"INFO")) {
              b"$4\r\ninfo\r\n"
            } else if req.windows(3).any(|w| w.eq_ignore_ascii_case(b"GET")) {
              b"$5\r\nhello\r\n"
            } else {
              b"+OK\r\n"
            };
            let compio::BufResult(res, _) = stream.write_all(resp.to_vec()).await;
            if res.is_err() {
              break;
            }
          }
        })
        .detach();
      }
    })
    .detach();

    let mut client = GarnetClient::new(endpoint.clone(), None, None, None, 64);
    client.connect_async().await?;

    // 基础 RESP 命令
    assert_eq!(client.ping_async().await?, "PONG");
    assert_eq!(client.string_get_async("key").await?, "hello");
    assert!(client.string_set_async("key", "val").await?);
    assert!(client.key_delete_async("key").await?);
    assert_eq!(client.string_increment("num").await?, 42);
    assert_eq!(client.string_decrement("num").await?, 41);

    // 管理命令
    assert!(client.save().await?);
    assert_eq!(client.info(InfoMetricsType::Server).await?, "info");
    assert_eq!(client.replica_of("127.0.0.1", 6379).await?, "OK");

    // List 命令
    assert_eq!(client.list_left_push_async("list", &["e1", "e2"]).await?, 2);
    assert_eq!(client.list_right_push_async("list", &["e3"]).await?, 3);
    assert_eq!(client.list_length_async("list").await?, 2);
    let range = client.list_range_async("list", 0, -1).await?;
    assert_eq!(range, vec!["a", "b"]);

    // Sorted Set 命令
    assert_eq!(client.sorted_set_add_async("zset", "m1", 1.5).await?, 1);
    let pair_col = SortedSetPairCollection {
      entries: vec![(2.0, "m2".to_string())],
    };
    assert_eq!(
      client
        .sorted_set_add_collection_async("zset", &pair_col)
        .await?,
      1
    );
    assert_eq!(client.sorted_set_remove_async("zset", "m1").await?, 1);
    assert_eq!(client.sorted_set_length_async("zset").await?, 1);
    assert_eq!(client.quit_async().await?, "OK");

    // 会话透传命令
    let mut session = GarnetClientSession::new(endpoint, None, None, None);
    session.connect_async().await?;
    assert_eq!(session.execute_async(&["PING"]).await?, "PONG");
    let bytes_res = session.execute_for_bytes_async(&[b"GET", b"k"]).await?;
    assert_eq!(bytes_res, b"hello");
    let arr_res = session
      .execute_for_array_async(&["LRANGE", "list", "0", "-1"])
      .await?;
    assert_eq!(arr_res, vec!["a", "b"]);

    info!("GarnetClient 与 GarnetClientSession 全 API 链路验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

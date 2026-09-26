//! INFO uptime 起点装配期预热端到端（task/ing/zcode-r27-boottimeline 发现二）
//!
//! C# 对标 libs/server/StoreWrapper.cs:StoreWrapper 构造函数 :215 赋值
//! startupTimestamp，构造点 GarnetServer.cs:298 先于 Start :530 的
//! RecoverAsync：uptime 自存储装配起表，恢复时长与启动静默期天然计入。
//! rust 修复前 `startup_ticks` OnceLock 仅在 INFO 服务路径懒取——装配后
//! 静默期全部漏计，首条 INFO 报 uptime≈0。
//!
//! 本用例以真实节点装配 → 已知静默间隔 → INFO server 断言
//! uptime_in_seconds >= 间隔，钉死「起表先于 accept」契约。

use std::{thread::sleep, time::Duration};

use compio::{net::TcpStream, runtime::Runtime};
use wnode_test::{read_reply, send_cmd, start_node};

/// 装配后静默间隔：uptime_in_seconds 秒粒度（四舍五入），2.2s 保证
/// 修复前（起点 = 首条 INFO 时刻）报 0、修复后报 >= 2 的判别差
const QUIESCENCE: Duration = Duration::from_millis(2_200);

/// bulk 帧载荷文本（INFO 应答）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = String::from_utf8_lossy(&frame[1..nl - 1])
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// SERVER 段 uptime_in_seconds 字段取值
fn uptime_seconds(info: &str) -> i64 {
  info
    .split("\r\n")
    .find(|l| l.starts_with("uptime_in_seconds:"))
    .unwrap_or_else(|| panic!("INFO server 须含 uptime_in_seconds 行: {info}"))
    .split(':')
    .nth(1)
    .unwrap()
    .parse()
    .unwrap()
}

#[test]
fn info_uptime_counts_assembly_quiescence() -> aok::Result<()> {
  let rt = Runtime::new()?;
  // 装配节点：起表点在 StorageSessionProvider 构造期（先于本测试静默期）
  let (_dir, _server, addr) = start_node();
  // 装配后静默期：uptime 须覆盖（修复前起点懒取恒漏计本段）
  sleep(QUIESCENCE);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await?;
    // 预热：泵在建连后首个数据帧才创建并注册会话
    send_cmd(&mut s, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut s).await, b"+PONG\r\n");

    send_cmd(&mut s, &[b"INFO", b"server"]).await?;
    let info = bulk_body(&read_reply(&mut s).await);
    let uptime = uptime_seconds(&info);
    assert!(
      uptime >= 2,
      "uptime 须计入装配后静默期（>=2s），实得 {uptime}s"
    );
    Ok(())
  })
}

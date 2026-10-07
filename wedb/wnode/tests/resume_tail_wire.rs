#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 慢路径续跑尾参线格式集成测试（自 wnode/src/resp/resp_server_session/resume.rs
//! 内联 mod tests 迁入，断言与覆盖原样保留）
//!
//! 线格式钉版对拍：期望字节序列按旧手写实现逐字节展开（非往返自证），
//! 单源 TailWriter / TailReader 改造后字节漂移即在此爆。

use wnode::resp::{EtagResume, MsetnxResume, TtlLeg, TtlResume};

/// 25 字节尾参（TtlResume 线形）按旧实现的字节序手工展开
fn ttl_wire(mode: u8, ticks: i64, vns: u64, vdb: u64) -> Vec<u8> {
  let mut t = vec![mode];
  t.extend_from_slice(&ticks.to_le_bytes());
  t.extend_from_slice(&vns.to_le_bytes());
  t.extend_from_slice(&vdb.to_le_bytes());
  t
}

/// 33 字节尾参（EtagResume 线形）按旧实现的字节序手工展开
fn etag_wire(mode: u8, new_etag: i64, ticks: i64, vns: u64, vdb: u64) -> Vec<u8> {
  let mut t = vec![mode];
  t.extend_from_slice(&new_etag.to_le_bytes());
  t.extend_from_slice(&ticks.to_le_bytes());
  t.extend_from_slice(&vns.to_le_bytes());
  t.extend_from_slice(&vdb.to_le_bytes());
  t
}

#[test]
fn msetnx_tail_byte_wire() {
  assert_eq!(MsetnxResume::Replay.tail_byte(), b'0');
  assert_eq!(MsetnxResume::Continue.tail_byte(), b'1');
  assert_eq!(MsetnxResume::Rollback.tail_byte(), b'r');
  assert_eq!(MsetnxResume::from_tail(Some(b"1")), MsetnxResume::Continue);
  assert_eq!(MsetnxResume::from_tail(Some(b"r")), MsetnxResume::Rollback);
  assert_eq!(MsetnxResume::from_tail(Some(b"0")), MsetnxResume::Replay);
  // 非单字节形态按 Replay 兜底
  assert_eq!(MsetnxResume::from_tail(None), MsetnxResume::Replay);
  assert_eq!(MsetnxResume::from_tail(Some(b"")), MsetnxResume::Replay);
  assert_eq!(MsetnxResume::from_tail(Some(b"10")), MsetnxResume::Replay);
}

#[test]
fn ttl_tail_bytes_wire() {
  // Full：b'0' + 24 零字节
  assert_eq!(TtlResume::Full.tail_bytes(), ttl_wire(b'0', 0, 0, 0));
  // 三臂同腿：仅模式字节区分
  let leg = TtlLeg {
    ticks: -1,
    domain: (7, 9),
  };
  assert_eq!(
    TtlResume::Pending(leg).tail_bytes(),
    ttl_wire(b'1', -1, 7, 9)
  );
  assert_eq!(
    TtlResume::KeepTtl(leg).tail_bytes(),
    ttl_wire(b'2', -1, 7, 9)
  );
  assert_eq!(
    TtlResume::ReplyEcho(leg).tail_bytes(),
    ttl_wire(b'3', -1, 7, 9)
  );
  // 边界值位保真
  let edge = TtlLeg {
    ticks: i64::MIN,
    domain: (u64::MAX, u64::MAX),
  };
  assert_eq!(
    TtlResume::Pending(edge).tail_bytes(),
    ttl_wire(b'1', i64::MIN, u64::MAX, u64::MAX)
  );
}

#[test]
fn ttl_from_tail_wire() {
  let leg = TtlLeg {
    ticks: 0x0123_4567_89ab_cdef,
    domain: (1, 2),
  };
  assert_eq!(
    TtlResume::from_tail(Some(&ttl_wire(b'1', leg.ticks, 1, 2))),
    TtlResume::Pending(leg)
  );
  assert_eq!(
    TtlResume::from_tail(Some(&ttl_wire(b'2', leg.ticks, 1, 2))),
    TtlResume::KeepTtl(leg)
  );
  assert_eq!(
    TtlResume::from_tail(Some(&ttl_wire(b'3', leg.ticks, 1, 2))),
    TtlResume::ReplyEcho(leg)
  );
  // 形态不符 / 未知模式字节一律 Full 兜底
  for bad in [
    None,
    Some(&b""[..]),
    Some(&ttl_wire(b'1', 0, 0, 0)[..24]),
    Some(&ttl_wire(b'1', 0, 0, 0)),
  ] {
    let expect = match bad {
      Some(t) if t.len() == 25 && t[0] == b'1' => TtlResume::Pending(TtlLeg {
        ticks: 0,
        domain: (0, 0),
      }),
      _ => TtlResume::Full,
    };
    assert_eq!(TtlResume::from_tail(bad), expect);
  }
  for mode in [b'0', b'4', 0xff] {
    assert_eq!(
      TtlResume::from_tail(Some(&ttl_wire(mode, 5, 6, 7))),
      TtlResume::Full
    );
  }
}

#[test]
fn etag_tail_bytes_wire() {
  // Full：b'0' + 32 零字节
  assert_eq!(EtagResume::Full.tail_bytes(), etag_wire(b'0', 0, 0, 0, 0));
  let leg = TtlLeg {
    ticks: -42,
    domain: (3, 4),
  };
  // found 真假仅模式字节区分（b'1'/b'2'）
  assert_eq!(
    EtagResume::Pending {
      ttl: leg,
      new_etag: -7,
      found: true
    }
    .tail_bytes(),
    etag_wire(b'1', -7, -42, 3, 4)
  );
  assert_eq!(
    EtagResume::Pending {
      ttl: leg,
      new_etag: -7,
      found: false
    }
    .tail_bytes(),
    etag_wire(b'2', -7, -42, 3, 4)
  );
}

#[test]
fn etag_from_tail_wire() {
  assert_eq!(
    EtagResume::from_tail(Some(&etag_wire(b'1', -7, -42, 3, 4))),
    EtagResume::Pending {
      ttl: TtlLeg {
        ticks: -42,
        domain: (3, 4)
      },
      new_etag: -7,
      found: true
    }
  );
  assert_eq!(
    EtagResume::from_tail(Some(&etag_wire(b'2', i64::MIN, i64::MAX, u64::MAX, 0))),
    EtagResume::Pending {
      ttl: TtlLeg {
        ticks: i64::MAX,
        domain: (u64::MAX, 0)
      },
      new_etag: i64::MIN,
      found: false
    }
  );
  // 长度门 + 模式门：恰 33 字节但模式非法（含 b'0'/b'3'）一律 Full
  for mode in [0u8, b'0', b'3', b'x'] {
    assert_eq!(
      EtagResume::from_tail(Some(&etag_wire(mode, 1, 2, 3, 4))),
      EtagResume::Full
    );
  }
  assert_eq!(EtagResume::from_tail(None), EtagResume::Full);
  assert_eq!(
    EtagResume::from_tail(Some(&etag_wire(b'1', 1, 2, 3, 4)[..32])),
    EtagResume::Full
  );
}

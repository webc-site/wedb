//! R.* 四条命令端到端集成测试（对标 garnet/test/standalone/Garnet.test/
//! RespRoaringBitmapTests.cs）
//!
//! 经真实 RESP 协议帧驱动 StorageSessionProvider + RespSessionConsumer 全链
//! （解析匹配 → 注册表解析 → 存储执行域四接口分派）。纯数据结构面
//! （RoaringBitmapDataTests.cs）由 wext_roaring 单元测试承接。

use std::collections::HashSet;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wedb_test::{resp_frame, test_store_config};
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};

/// 装配测试会话：临时库（内置 RoaringBitmap 特性开箱即用）
fn setup(db: &str) -> RespSessionConsumer {
  let dir = tempdir().expect("tempdir");
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(db),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        api,
      ))
    },
  )
  .expect("provider open");
  provider
    .get_session(WireFormat::Ascii, 1)
    .expect("session create")
}

/// 单命令执行（帧进 → 应答出）
fn exec(consumer: &mut RespSessionConsumer, parts: &[&[u8]]) -> Vec<u8> {
  let req = resp_frame(parts);
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&req);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "命令须整段消费: {parts:?}");
  resp
}

#[test]
fn get_bit_on_missing_key_returns_0() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_missing_getbit.db");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"12345"]), b":0\r\n");
    // 缺键读不得建键（C# ClassicAssert.IsFalse(db.KeyExists)）
    assert_eq!(exec(&mut s, &[b"EXISTS", b"rb"]), b":0\r\n");
    aok::OK
  })
}

#[test]
fn bit_count_on_missing_key_returns_0() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_missing_bitcount.db");
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":0\r\n");
    assert_eq!(exec(&mut s, &[b"EXISTS", b"rb"]), b":0\r\n");
    aok::OK
  })
}

#[test]
fn bit_pos_on_missing_key_bit1_returns_minus1() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_missing_bitpos1.db");
    assert_eq!(exec(&mut s, &[b"R.BITPOS", b"rb", b"1"]), b":-1\r\n");
    assert_eq!(exec(&mut s, &[b"EXISTS", b"rb"]), b":0\r\n");
    aok::OK
  })
}

#[test]
fn bit_pos_on_missing_key_bit0_returns_from_or_zero() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_missing_bitpos0.db");
    assert_eq!(exec(&mut s, &[b"R.BITPOS", b"rb", b"0"]), b":0\r\n");
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"0", b"100"]),
      b":100\r\n"
    );
    aok::OK
  })
}

#[test]
fn set_bit_get_bit_basics() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_set_get.db");
    assert_eq!(exec(&mut s, &[b"R.SETBIT", b"rb", b"42", b"1"]), b":0\r\n");
    assert_eq!(exec(&mut s, &[b"R.SETBIT", b"rb", b"42", b"1"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"42"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"41"]), b":0\r\n");
    // 清位
    assert_eq!(exec(&mut s, &[b"R.SETBIT", b"rb", b"42", b"0"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"42"]), b":0\r\n");
    aok::OK
  })
}

#[test]
fn bit_count_basics() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_bitcount.db");
    for i in 0..10u32 {
      assert_eq!(
        exec(
          &mut s,
          &[b"R.SETBIT", b"rb", i.to_string().as_bytes(), b"1"]
        ),
        b":0\r\n"
      );
    }
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":10\r\n");
    assert_eq!(exec(&mut s, &[b"R.SETBIT", b"rb", b"5", b"0"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":9\r\n");
    aok::OK
  })
}

#[test]
fn bit_pos_basics() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_bitpos.db");
    for off in [100u32, 200, 70000] {
      exec(
        &mut s,
        &[b"R.SETBIT", b"rb", off.to_string().as_bytes(), b"1"],
      );
    }
    assert_eq!(exec(&mut s, &[b"R.BITPOS", b"rb", b"1"]), b":100\r\n");
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"1", b"150"]),
      b":200\r\n"
    );
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"1", b"300"]),
      b":70000\r\n"
    );
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"1", b"70001"]),
      b":-1\r\n"
    );
    // 全置位区间后的首个未置位
    assert_eq!(exec(&mut s, &[b"R.BITPOS", b"rb", b"0"]), b":0\r\n");
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"0", b"100"]),
      b":101\r\n"
    );
    aok::OK
  })
}

#[test]
fn errors_bad_offset() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_err_offset.db");
    assert_eq!(
      exec(&mut s, &[b"R.SETBIT", b"rb", b"notanumber", b"1"]),
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );
    assert_eq!(
      exec(&mut s, &[b"R.SETBIT", b"rb", b"-5", b"1"]),
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );
    assert_eq!(
      exec(&mut s, &[b"R.SETBIT", b"rb", b"5", b"2"]),
      b"-ERR bit value must be 0 or 1\r\n"
    );
    // 畸形命令不建键（NeedInitialUpdate 防空墓碑）
    assert_eq!(exec(&mut s, &[b"EXISTS", b"rb"]), b":0\r\n");
    aok::OK
  })
}

#[test]
fn string_key_and_custom_object_key_are_separate() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_wrongtype.db");
    exec(&mut s, &[b"SET", b"rb", b"hello"]);
    assert_eq!(
      exec(&mut s, &[b"R.SETBIT", b"rb", b"10", b"1"]),
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );
    assert_eq!(exec(&mut s, &[b"GET", b"rb"]), b"$5\r\nhello\r\n");
    aok::OK
  })
}

#[test]
fn errors_bit_pos_bad_bit() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_err_bit.db");
    assert_eq!(
      exec(&mut s, &[b"R.BITPOS", b"rb", b"2"]),
      b"-ERR bit must be 0 or 1\r\n"
    );
    aok::OK
  })
}

#[test]
fn errors_wrong_arity() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_arity.db");
    let r1 = exec(&mut s, &[b"R.SETBIT", b"rb", b"1"]);
    assert!(r1.starts_with(b"-ERR wrong number of arguments"), "{r1:?}");
    let r2 = exec(&mut s, &[b"R.BITCOUNT", b"rb", b"extra"]);
    assert!(r2.starts_with(b"-ERR wrong number of arguments"), "{r2:?}");
    aok::OK
  })
}

#[test]
fn delete_and_recreate() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_del_recreate.db");
    exec(&mut s, &[b"R.SETBIT", b"rb", b"5", b"1"]);
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"DEL", b"rb"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":0\r\n");
    exec(&mut s, &[b"R.SETBIT", b"rb", b"9", b"1"]);
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"5"]), b":0\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"9"]), b":1\r\n");
    aok::OK
  })
}

#[test]
fn large_offsets_and_chunk_boundaries() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_large.db");
    // 分块边界偏移（含 uint32 极值）
    let interesting: [u32; 7] = [0, 65535, 65536, 131071, 131072, i32::MAX as u32, u32::MAX];
    for &off in &interesting {
      assert_eq!(
        exec(
          &mut s,
          &[b"R.SETBIT", b"rb", off.to_string().as_bytes(), b"1"]
        ),
        b":0\r\n"
      );
      assert_eq!(
        exec(&mut s, &[b"R.GETBIT", b"rb", off.to_string().as_bytes()]),
        b":1\r\n"
      );
    }
    assert_eq!(
      exec(&mut s, &[b"R.BITCOUNT", b"rb"]),
      format!(":{}\r\n", interesting.len()).as_bytes()
    );
    assert_eq!(exec(&mut s, &[b"R.BITPOS", b"rb", b"1"]), b":0\r\n");
    assert_eq!(
      exec(
        &mut s,
        &[
          b"R.BITPOS",
          b"rb",
          b"1",
          (i32::MAX as u32 + 1).to_string().as_bytes()
        ]
      ),
      format!(":{}\r\n", u32::MAX).as_bytes()
    );
    aok::OK
  })
}

#[test]
fn oracle_parity_random_mix() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_oracle.db");
    // 确定性 LCG（C# Random(1234) 的独立等价 oracle 驱动）
    let mut state: u64 = 1234;
    let mut next = move || {
      state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
      (state >> 33) as u32
    };
    let mut oracle = HashSet::new();
    for _ in 0..5000 {
      let off = next() % 200_000;
      let set = next() % 2 == 1;
      let prev_expected = if oracle.contains(&off) { 1 } else { 0 };
      let resp = exec(
        &mut s,
        &[
          b"R.SETBIT",
          b"rb",
          off.to_string().as_bytes(),
          if set { b"1" } else { b"0" },
        ],
      );
      assert_eq!(resp, format!(":{prev_expected}\r\n").as_bytes());
      if set {
        oracle.insert(off);
      } else {
        oracle.remove(&off);
      }
    }
    assert_eq!(
      exec(&mut s, &[b"R.BITCOUNT", b"rb"]),
      format!(":{}\r\n", oracle.len()).as_bytes()
    );
    for _ in 0..200 {
      let off = next() % 200_000;
      let expected = if oracle.contains(&off) { 1 } else { 0 };
      assert_eq!(
        exec(&mut s, &[b"R.GETBIT", b"rb", off.to_string().as_bytes()]),
        format!(":{expected}\r\n").as_bytes()
      );
    }
    aok::OK
  })
}

#[test]
fn dense_bitmap_promotion_stays_correct() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("rb_promotion.db");
    // 单分块 > 4096 置位触发 bitmap 容器升级
    for i in 0..5000u32 {
      exec(
        &mut s,
        &[b"R.SETBIT", b"rb", i.to_string().as_bytes(), b"1"],
      );
    }
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":5000\r\n");
    // 回收到 4096 降级回 array 容器
    for i in 4096..5000u32 {
      exec(
        &mut s,
        &[b"R.SETBIT", b"rb", i.to_string().as_bytes(), b"0"],
      );
    }
    assert_eq!(exec(&mut s, &[b"R.BITCOUNT", b"rb"]), b":4096\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"4095"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"R.GETBIT", b"rb", b"4096"]), b":0\r\n");
    aok::OK
  })
}

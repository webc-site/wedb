//! 向量选项关键字 ASCII 折叠字母域收窄钉测（doc/zh/deviations.md §187）
//!
//! C# 二参版 AsciiUtils.EqualsUpperCaseSpanIgnoringCase 逐字节
//! `b1 == b2 || b1 - 32 == b2` 无字母域限制（`b2 is >= 65 and <= 90` 仅
//! Debug.Assert，Release 无效），含非字母位（数字/连字符）的关键字全部暴露
//! +32 偏移病形面：'X'-32=0x38='8'、'R'-32=0x32='2'、'M'-32=0x2D='-'。
//! rust 走 wbase::ascii::eq_ascii_case（u8::eq_ignore_ascii_case，仅字母位
//! 折叠），病形词法一律拒收，属修复型收窄分叉（同谱 §18/§32），严禁按 C#
//! 病形回改放宽。
//!
//! C# 消费面锚（garnet 现树）：Q8（RespServerSessionVectors.cs:232）、L2
//! （:392）、FILTER-EF（:801）、XU8/XB8/XI8（:128-149）、XPREQ8（:256）均为
//! 二参版；XNOQUANT_U8 等三参版（allowNonAlphabeticChars: true）非字母位要求
//! 精确相等，与 rust 行为一致无分叉。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorManager, VectorManagerOptions},
};
use wnode_test::bound_domain;
use wval::SessionPrefixBuf;
use wvector::{Callbacks, store::StoreCallbacks};

/// 内存零值桩：本文件钉测全部在解析期返回错误帧，零存储触达，
/// 桩仅满足 [`StoreCallbacks`] 契约（各臂取与 resp_vector_set.rs 桩
/// 「不触达即等价」的零值应答）。
struct LexQuirkStore;

impl StoreCallbacks for LexQuirkStore {
  async fn read_multi<F>(&self, _context: u64, _keys: &[u8], _length_hint: usize, _f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    true
  }

  async fn read<F>(&self, _context: u64, _key: &[u8], _f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    false
  }

  async fn write(&self, _context: u64, _key: &[u8], _value: &[u8]) -> bool {
    true
  }

  async fn delete(&self, _context: u64, _key: &[u8]) -> bool {
    false
  }

  async fn rmw<F>(&self, _context: u64, _key: &[u8], _write_len: usize, _f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    true
  }

  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    false
  }

  async fn purge_context(&self, _context: u64) -> bool {
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn session() -> RespServerSessionVectors<LexQuirkStore> {
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(LexQuirkStore)),
  ));
  RespServerSessionVectors::new(manager)
}

fn err_text(r: VectorReply) -> String {
  match r {
    VectorReply::Error(e) => from_utf8(&e).unwrap().to_owned(),
    other => panic!("期望错误应答，实际 {other:?}"),
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// VADD 选项位病形钉测：C# "QX" 命中 Q8（尾位 'X'-32=0x38='8'）、
/// "XPREQX" 命中 XPREQ8 别名（:256 二参版尾位同算术）、"LR" 命中
/// XDISTANCE_METRIC 的 L2 值（'R'-32=0x32='2'）——rust 一律拒收，
/// 落解析期原错误帧，严禁按 C# 病形回改放宽（§187）。
#[test]
fn vadd_option_lex_plus32_quirk_rejected() {
  // 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let v = f32_bytes(&[1.0]);

    // 量化器 "QX"：C# 二参版识别为 Q8，rust 拒收
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v, b"e", b"QX"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid option after element"
    );

    // XPREQ8 别名尾位病形 "XPREQX"：C# :256 二参版含数字位 8，'X'-32 命中，
    // 识别为 XNOQUANT_U8；rust 拒收（活病形钉死）
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v, b"e", b"XPREQX"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid option after element"
    );

    // 同族词面 "XPREXX"：第 5 位 'X'-32=0x38≠'Q'=0x51，C# 亦拒——非活病形，
    // 钉 rust 拒收词面防回改连带（甄别订正复核不成立形的在测见证）
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v, b"e", b"XPREXX"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid option after element"
    );

    // XDISTANCE_METRIC 值参 "LR"：C# 二参版识别为 L2，rust 拒收
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v, b"e", b"XDISTANCE_METRIC", b"LR"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid XDISTANCE_METRIC"
    );
  })
}

/// VADD 取参格式位病形钉测：C# 二参版 "XUX"/"XBX"/"XIX" 尾位 'X'-32=0x38
/// 命中 '8'，分别识别为 XU8/XB8/XI8——rust 一律落 invalid vector
/// specification，严禁按 C# 病形回改放宽（§187）。
#[test]
fn vadd_value_kind_lex_plus32_quirk_rejected() {
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();

    for kw in [b"XUX".as_slice(), b"XBX".as_slice(), b"XIX".as_slice()] {
      assert_eq!(
        err_text(
          sess
            .network_vadd(
              SessionPrefixBuf::ROOT.as_slice(),
              &[b"k", kw, b"ab", b"e"],
              SLOT0,
              false,
            )
            .await
        ),
        "ERR invalid vector specification"
      );
    }
  })
}

/// VSIM 选项位病形钉测：C# 二参版 "FILTERMEF" 中 'M'-32=0x2D 命中 '-'，
/// 识别为 FILTER-EF——rust 拒收落 Unknown option，严禁按 C# 病形回改
/// 放宽（§187）。
#[test]
fn vsim_option_lex_plus32_quirk_rejected() {
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let v = f32_bytes(&[1.0]);

    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v, b"FILTERMEF"],
            false,
          )
          .await
      ),
      "Unknown option"
    );
  })
}

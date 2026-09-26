//! 升阶键 RENAME 迁移窗内 TTL 写面封堵回归测试（票 zcode-r15-expire 发现三）
//!
//! 缺陷背景：RENAME Meta 域臂（rename_slow → wkv `rename_range_index`）收口
//! 前在窗前快照旧键 TTL，迁移完成后按快照回填新键——窗内并发 EXPIRE/PERSIST
//! 的 TTL 变更落在旧键上被段五排空清退吞掉（续期静默蒸发），或 PERSIST 已
//! 撤销的 TTL 被快照借尸还魂到新键。封堵面漏掉 TTL 写者即 migration.rs
//! 「被拒写零副作用」承诺未闭环。修复：
//! - wkv `expire_at`/`persist` 头部加 `migration_claim_busy` 复合判点（Meta
//!   在场探测 + claim 在册），命中 [`wkv::Error::MigrationBusy`] 零副作用
//!   上抛，客户端重试即线性化到迁移完成后；
//! - dst 侧 claim 无条件登记（旧「dst 有存活记录才 claim」判据退役），
//!   claim 窗即 C# RENAME 双键排他锁窗的完整对位；
//! - TTL 随迁下沉段五 claim 窗内现势读取（排空前读旧键 TTL 即随迁终态），
//!   调用方零快照消费。
//!
//! C# 对位：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME
//! （241-265 双键 Exclusive 事务锁）把并发 EXPIRE/PERSIST 串行化到迁移完成后
//!（UnifiedStore/RMWMethods.cs InPlaceUpdater 锁内读改写）。
//!
//! 验证点（票面发现三第 3 条）：并发压测「升阶键 RENAME 迁移窗内
//! EXPIRE/PERSIST，断言 :1 应答后新键 TTL 必为该命令终值、PERSIST 后新键无
//! TTL」；claim 命中时 EXPIRE/PERSIST 应回可重试错误而非静默成功（只统计
//! 已回执口径下的整数回执，错误帧不计）。

use std::{sync::Arc, thread::spawn};

use aok::Void;
use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

struct Node {
  store: Arc<WedbStore<SegmentedDevice>>,
  _dir: TempDir,
}

fn open_node(tag: &str) -> aok::Result<Node> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok(Node { store, _dir: dir })
}

fn api_of(store: &Arc<WedbStore<SegmentedDevice>>) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 灌 8000 × 600B hash 字段（总体积 ≈4.8MB > 4MB TIERED_PROMOTE_BYTES，
/// 单块 HSET 触发就地升阶；与 collection_adaptive_tiering 同构造）
fn fill_big_hash(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8]) {
  const FIELDS: usize = 8000;
  const VAL_BYTES: usize = 600;
  let mut args: Vec<Vec<u8>> = Vec::with_capacity(FIELDS * 2 + 1);
  args.push(key.to_vec());
  let mut buf = ItoaBuffer::new();
  let val = vec![b'v'; VAL_BYTES];
  for i in 1..=FIELDS {
    args.push(format!("f{}", buf.format(i)).into_bytes());
    args.push(val.clone());
  }
  let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  auto_exec(api, rt, s, RespCommand::Hset, &slices);
}

/// 升阶断言前置：键已为树态 hash
async fn assert_promoted(node: &Node, key: &[u8], size: usize) -> Void {
  let sess = node.store.new_session()?;
  let (meta, _) = sess
    .load_collection_stub(key)
    .await?
    .expect("键应已就地升阶为树态");
  assert_eq!(meta.collection_type, GarnetObjectType::Hash);
  assert_eq!(meta.size as usize, size);
  Ok(())
}

/// 基线（确定性）：升阶键 RENAME 后——带 TTL 键的现势 TTL 随迁新键（段五窗
/// 内现势读取）、旧键 TTL 随排空清退；无 TTL 键新键不借尸还魂
#[test]
fn rename_meta_carries_live_ttl_and_absent_ttl() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let node = open_node("rename-ttl-baseline")?;
    let api = api_of(&node.store)?;
    let mut s = session_with(&api);

    fill_big_hash(&api, &rt, &mut s, b"rh:h");
    assert_promoted(&node, b"rh:h", 8000).await?;
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[b"rh:h", b"3600"]),
      b":1\r\n",
      "升阶键 EXPIRE 须 :1（wkv 判点无迁移时不误伤）"
    );
    fill_big_hash(&api, &rt, &mut s, b"rh:p");
    assert_promoted(&node, b"rh:p", 8000).await?;

    // RENAME h → h2（Meta 域慢路径）
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Rename, &[b"rh:h", b"rh:h2"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"rh:h2"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"rh:h"]),
      b":0\r\n",
      "旧键必须消失"
    );
    // 现势 TTL 随迁：窗前 EXPIRE 写入的 3600s 迁到新键（非 100s 陈旧快照、
    // 非丢失）
    let ttl = auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"rh:h2"]);
    assert!(
      ttl == b":3600\r\n" || ttl == b":3599\r\n",
      "新键须携带随迁 TTL ≈3600s：{ttl:?}"
    );

    // 无 TTL 升阶键：新键无 TTL（旧实现快照 None → del_ttl 幂等；窗内现势
    // 读取同为 None——双向一致，杜绝借尸还魂回归面）
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Rename, &[b"rh:p", b"rh:p2"]),
      b"+OK\r\n"
    );
    let ttl = auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"rh:p2"]);
    assert_eq!(ttl, b":-1\r\n", "无 TTL 键迁移后新键不得出现 TTL：{ttl:?}");

    Ok(())
  })
}

/// 并发压测：RENAME（大树迁移窗）× EXPIRE/PERSIST 同键交叉，可串行化不变式
/// ——EXPIRE 回执 :1 ⇒ 新键终态 TTL 必为其写入值；PERSIST 回执 :1 ⇒ 新键
/// 终态无 TTL；迁移窗内的被拒写为可重试错误（错误帧不计入已回执）
#[test]
fn migrate_window_serializes_expire_and_persist() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let node = open_node("rename-ttl-race")?;
    let rounds: Vec<(&[u8], &[u8])> = vec![
      (b"EXPIRE", b"30000"),
      (b"PERSIST", b""),
      (b"EXPIRE", b"30000"),
      (b"PERSIST", b""),
    ];

    for (round, (cmd, arg)) in rounds.iter().enumerate() {
      let key = format!("rw:h{round}");
      let dst = format!("rw:g{round}");
      {
        let api = api_of(&node.store)?;
        let mut s = session_with(&api);
        fill_big_hash(&api, &rt, &mut s, key.as_bytes());
        assert_promoted(&node, key.as_bytes(), 8000).await?;
        // 预置旧 TTL 100s：PERSIST 有可删之物、EXPIRE NX 有在场旧值可判
        assert_eq!(
          auto_exec(
            &api,
            &rt,
            &mut s,
            RespCommand::Expire,
            &[key.as_bytes(), b"100"]
          ),
          b":1\r\n"
        );
      }

      // 真并发：A 线程 RENAME（慢路径迁移窗），B 线程 EXPIRE/PERSIST
      let store = Arc::clone(&node.store);
      let key_a = key.clone();
      let dst_a = dst.clone();
      let a = spawn(move || {
        let rt = Runtime::new().unwrap();
        let api = api_of(&store).unwrap();
        let mut s = session_with(&api);
        rt.block_on(async {
          auto_exec(
            &api,
            &rt,
            &mut s,
            RespCommand::Rename,
            &[key_a.as_bytes(), dst_a.as_bytes()],
          )
        })
      });
      let store = Arc::clone(&node.store);
      let key_b = key.clone();
      let cmd_b = (*cmd).to_vec();
      let arg_b = (*arg).to_vec();
      let b = spawn(move || {
        let rt = Runtime::new().unwrap();
        let api = api_of(&store).unwrap();
        let mut s = session_with(&api);
        rt.block_on(async {
          let mut args: Vec<Vec<u8>> = vec![key_b.as_bytes().to_vec()];
          if !arg_b.is_empty() {
            args.push(arg_b);
          }
          let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
          let cmd = if cmd_b.as_slice() == b"EXPIRE" {
            RespCommand::Expire
          } else {
            RespCommand::Persist
          };
          auto_exec(&api, &rt, &mut s, cmd, &slices)
        })
      });

      let rename_reply = a.join().unwrap();
      let ttl_reply = b.join().unwrap();
      assert_eq!(rename_reply, b"+OK\r\n", "第 {round} 轮 RENAME 须闭环 +OK");

      // 旧键消失、新键树态存活
      let api = api_of(&node.store)?;
      let mut s = session_with(&api);
      assert_eq!(
        auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[key.as_bytes()]),
        b":0\r\n",
        "第 {round} 轮旧键必须消失"
      );
      assert_eq!(
        auto_exec(&api, &rt, &mut s, RespCommand::Type, &[dst.as_bytes()]),
        b"+hash\r\n",
        "第 {round} 轮新键须为树态 hash"
      );

      let ttl = auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[dst.as_bytes()]);
      let is_persist = *cmd == b"PERSIST";
      if !is_persist && ttl_reply == b":1\r\n" {
        assert_eq!(
          ttl, b":30000\r\n",
          "第 {round} 轮：EXPIRE 回执 :1 而新键终态 TTL 是 {ttl:?}\
          （应为写入值 30000s——续期被窗前快照回填吞掉）"
        );
      }
      if is_persist && ttl_reply == b":1\r\n" {
        assert_eq!(
          ttl, b":-1\r\n",
          "第 {round} 轮：PERSIST 回执 :1 而新键终态 TTL 是 {ttl:?}\
           （已撤销的 TTL 被窗前快照借尸还魂）"
        );
      }
      // 非整数回执（:0 或 MigrationBusy 错误帧）= 可重试拒绝，不计终态
    }

    Ok(())
  })
}

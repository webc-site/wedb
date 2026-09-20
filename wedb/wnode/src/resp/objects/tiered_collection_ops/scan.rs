use core::str;

use itoa::Buffer as ItoaBuffer;
use wbase::time::now_ticks;
use wbftree::ScanReturnField;
use wcol::types::member_ttl::{decode_member, member_expired_at};
use wdev::Device;
use wkv::{BatchStoreSession, StoreSession, SwapInWindowGuard};
use wresp::{cmd_strings as cs, ext::RespVecExt, resp_memory_writer::format_double};
use wval::GarnetObjectType;
use zmij::Buffer as ZmijBuffer;

use super::common::{SweepOutcome, TieredCtx, expire_sweep_or_rebuild, scan_count, tiered_guard};

/// 分层树物化为内存信封载荷（对标 C# 单记录对象域：Garnet 对象常驻存储层，
/// 任意命令对任意规模对象语义恒定；rust 分层态未实现树内原语的操作经本通道
/// 一次性物化回 wcol 对象，走对象层单源求值后按升阶/降阶判据回写）
///
/// meta/stub 由调用方预载传入：只读物化（慢路径求值）传门禁装载
/// [`load_collection_stub`](wkv::StoreSession::load_collection_stub) 结果即可；
/// 写回面（求值后经 [`apply_rmw_post_operate`](super::super::rmw_helpers)
/// 换入/清退）一律走封窗变体 [`tiered_materialize_blob_sealed`]，窗内自装载
/// 交 [`load_collection_stub_in_window`](wkv::StoreSession::load_collection_stub_in_window)
/// 预载，杜绝被自身 claim 拒绝。`Ok(Some(blob))` 物化载荷（与信封剥壳后格式
/// 一致，可直接喂 `from_blob`）；`Ok(None)` 键非分层态或类型不符（调用方维持
/// 既有装载路径）；`Err(())` 存储 IO 失败或树记录载荷损坏（fail-fast 中止
/// 物化，调用方不得回写，杜绝固化成员丢失）
pub(crate) async fn tiered_materialize_blob<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag: GarnetObjectType,
  meta: &wval::MetaValue,
  stub: &wbftree::RangeIndexStub,
) -> Result<Option<Vec<u8>>, ()> {
  use wcol::{
    hash::hash_object::HashObject, list::list_object::ListObject,
    object_payload::GarnetObjectPayload, set::set_object::SetObject,
    zset::sorted_set_object::SortedSetObject,
  };

  if meta.collection_type != tag {
    return Ok(None);
  }
  let tree_guard = session.acquire_tree_read(key, stub).await.map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 物化承载字段级 TTL：挂 TTL 成员还原进对象 expiration 结构（未到期），
  // 已到期成员剔除不复活（对齐 C# 对象构造函数装载口径：SortedSetObject.cs
  // 的 ExpirationBitMask 分支 canAddItem = expiration >= UtcNow.Ticks）；
  // 往返「内存态 → 升阶 → 物化 → 再升阶」逐字段 TTL 保真。扫描 Err 经
  // [`scan_count`] 上抛（fail-fast 中止物化，调用方不写回）——静默截断的
  // 物化载荷会被整值写回固化成员丢失，比输出臂截断更重
  let now = now_ticks();
  let blob = match tag {
    GarnetObjectType::Hash => {
      let mut obj = HashObject::new();
      scan_count(tree.scan_with_count_callback(
        &[0u8],
        usize::MAX,
        ScanReturnField::KeyAndValue,
        |k, v| {
          let (expiry, payload) = decode_member(v);
          if expiry.is_some_and(|ticks| ticks < now) {
            return true;
          }
          let item = k.to_vec();
          obj.update_size(&item, payload, true);
          obj.hash.insert(item.clone(), payload.to_vec());
          if let Some(ticks) = expiry {
            obj.insert_expiration(item, ticks);
          }
          true
        },
      ))?;
      obj.to_blob()
    }
    GarnetObjectType::Set => {
      let mut obj = SetObject::new();
      scan_count(tree.scan_with_count_callback(
        &[0u8],
        usize::MAX,
        ScanReturnField::Key,
        |k, _| {
          obj.set.insert(k.to_vec());
          true
        },
      ))?;
      obj.to_blob()
    }
    GarnetObjectType::SortedSet => {
      let mut obj = SortedSetObject::from_entries(Vec::with_capacity(meta.size as usize));
      // 树记录分值载荷恒为编码侧单源落下的 8B f64 大端；非 8B 即编码损坏或
      // codec 缺陷——fail-fast 中止物化（Err 上抛，调用方不写回），与
      // exec_tiered_scan 扫描臂的显式失败口径共用错误面，严禁静默剔除成员后
      // 照常回写固化丢失（对照 C# GarnetObjectSerializer.DeserializeInternal
      // 抛异常可见失败）
      let mut corrupt = false;
      scan_count(tree.scan_with_count_callback(
        &[0u8],
        usize::MAX,
        ScanReturnField::KeyAndValue,
        |k, v| {
          let (expiry, payload) = decode_member(v);
          if expiry.is_some_and(|ticks| ticks < now) {
            return true;
          }
          let Ok(arr) = <[u8; 8]>::try_from(payload) else {
            log::error!(
              "tiered_materialize_blob: corrupted zset score payload, key='{}' member={:?}",
              String::from_utf8_lossy(key),
              k
            );
            corrupt = true;
            return false;
          };
          let score = f64::from_be_bytes(arr);
          let member = k.to_vec();
          obj.update_size(&member, true);
          obj.sorted_set_dict.insert(member.clone(), score);
          if let Some(ticks) = expiry {
            obj.insert_expiration(member, ticks);
          }
          true
        },
      ))?;
      if corrupt {
        return Err(());
      }
      obj.to_blob()
    }
    GarnetObjectType::List => {
      let mut obj = ListObject::new();
      scan_count(tree.scan_with_count_callback(
        &[0u8],
        usize::MAX,
        ScanReturnField::KeyAndValue,
        |_, v| {
          // 树内键为序号（大端保序），扫描序即元素序
          obj.list.push_back(decode_member(v).1.to_vec());
          true
        },
      ))?;
      obj.to_blob()
    }
    _ => return Ok(None),
  };
  Ok(Some(blob))
}

/// 写回面物化封窗单点（物化降级臂唯一入口，一处定义、调用点转引）：登记自迁移
/// 安全换入窗（[`StoreSession::try_swap_in_window`]，同键并发稳态写自此被四探测
/// 门拒绝）后未门禁装载 + 全扫物化，守卫随载荷交出——调用方必须持守卫跨过
/// 「对象层求值 → 写回收尾（promote 换入 / 懒降阶清退 / 删空排空）」全程，杜绝
/// 「已 ACK 落旧树随 replace=true 换入被整树顶替」的丢失形与镜像 AOF 乱序。
///
/// `Ok(None)` = 键不在分层态（claim 窗内键不可被并发排空——DEL/判活被 load_meta
/// 门禁视同不存在；窗内 None 仅余本命令臂先行删空自愈的键消亡态，调用方落信封
/// 通道按 Missing 新建承接）；`Err(())` = 同键 claim 被并发迁移持有（存储忙，
/// 客户端重试收敛）或存储 IO 失败
pub(crate) async fn tiered_materialize_blob_sealed<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<Option<(Vec<u8>, SwapInWindowGuard)>, ()> {
  let Some(window) = session.try_swap_in_window(key) else {
    return Err(());
  };
  let Some((meta, stub)) = session
    .load_collection_stub_in_window(key)
    .await
    .map_err(|_| ())?
  else {
    // 键已消亡（封窗前树内臂删空穿透）：出窗失败，交调用方信封通道承接
    return Ok(None);
  };
  Ok(
    tiered_materialize_blob(session, key, tag, &meta, &stub)
      .await?
      .map(|blob| (blob, window)),
  )
}

/// 分层键字段级到期收集执行体（显式 HCOLLECT / ZCOLLECT 单键、`*` 全库周期
/// 对象收集任务与计数慢路径校正共用；到期重灌内核 [`expire_sweep_or_rebuild`]
/// 唯一内核）
///
/// `Ok(Some(size))` 分层键已处理并返回校正后存活计数（含零到期零写）；
/// `Ok(None)` 键不在分层态或类型不符；`Err(())` 存储 IO 失败
pub(crate) async fn exec_tiered_collect<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<Option<u64>, ()> {
  let Some((mut meta, mut stub)) = session.load_collection_stub(key).await.map_err(|_| ())? else {
    return Ok(None);
  };
  if meta.collection_type != tag {
    return Ok(None);
  }
  let mut ctx = TieredCtx::new(&mut meta, &mut stub);
  // 收集执行体是写臂（到期成员出账重灌）：独占写锁 + 锁内刷新元记录，
  // 键已被并发排空回收则按「键非分层态」穿透
  let Some(tree_guard) = tiered_guard(session, key, &mut ctx, true).await? else {
    return Ok(None);
  };
  // 守卫在 Below 出口原样奉还后即释放；Swept 出口重灌/回写已在内完成。
  // 推进判据只认实际到期出账（与四族计数臂 Swept{0} 不置脏不推进同口径）：
  // 零到期仅水位前移是 FOLLOW-UP 内部元记录收敛（水位滞留旧值的一次扫正，
  // 树内容零变更），非客户端可见变更，不推进——否则纯内部修复误夭折并发
  // WATCH 事务（对齐既有「零到期零写不推进」文注）
  let changed = match expire_sweep_or_rebuild(session, key, &mut ctx, tree_guard, |_| {}).await? {
    SweepOutcome::Below(_) => false,
    SweepOutcome::Swept { expired } => expired > 0,
  };
  if changed {
    // 到期成员物理出账即客户端可见变更，推进 WATCH 版本栅栏（对标 C#
    // HashCollect 走 RMW 写钩子 IncrementVersion；零到期零写不推进）
    session.bump_watch_version(key);
  }
  Ok(Some(ctx.meta.size))
}

/// 分层态 SCAN 族（HSCAN/SSCAN/ZSCAN）树内游标扫描
///
/// 对标 SharedObjectCommands.cs 的 ObjectScan 分层态臂——C# 对任意规模
/// 对象恒可用，升阶键不得回内部错误。游标复刻 wcol 对象层 scan 口径（
/// hash_object.rs:scan / set_object.rs:scan / sorted_set_object.rs:scan 单源）：
/// 起始游标 = 已扫条目计数，单轮收集匹配项至 count 截断（成员+值对计数），
/// 扫至树尾光标归零；MATCH 全局通配、COUNT 钳制 OBJECT_SCAN_COUNT_LIMIT、
/// NOVALUES（zset 对齐对象层忽略该旗标）逐项一致。树内流式输出，内存
/// O(单轮 items)，不做全量物化
///
/// 栈深度口径（勿按「COUNT 截断」误读为成本有界）：起始游标以「跳过 start 条」
/// 实现，故本臂自树头起遍历，**遍历**条数与单帧深度都不受 COUNT 约束——底层游标
/// 对墓碑的尾递归自调必须在单次 `next()` 内跳完游标后的整段连跑（≈680B/条），
/// 回调早停与 count 均来不及生效（实测 task/reject/tiered-zset-demote-stack.md
/// 二.表 S4）。本臂的安全性来自「树内零墓碑」的写形不变量，不来自本函数的截断
///
/// `Ok(true)` 已闭环应答；`Ok(false)` 键非分层态（调用方维持既有路径）；
/// `Err(())` 存储 IO 失败
pub(crate) async fn exec_tiered_scan<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  object_type: GarnetObjectType,
  args: &[&[u8]],
  scan_count_limit: i32,
  output: &mut Vec<u8>,
  resp_protocol_version: u8,
) -> Result<bool, ()> {
  use wbase::glob::glob_match;
  use wcol::types::scan_input::read_scan_input;

  let Some((meta, stub)) = session.load_collection_stub(key).await.map_err(|_| ())? else {
    return Ok(false);
  };
  if meta.collection_type != object_type {
    cs::write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
    return Ok(true);
  }

  // ReadScanInput 单源解析（与对象层同一解析函数）
  let params = match read_scan_input(args, scan_count_limit) {
    Ok(params) => params,
    Err(msg) => {
      // 错误文本为 ASCII 常量（INVALIDCURSOR/SYNTAX/NOT_INTEGER）
      cs::write_error_raw(output, str::from_utf8(msg).unwrap_or(""));
      return Ok(true);
    }
  };
  let start = params.cursor;
  // 对象层截断口径：hash/zset 每成员占 2 项（成员 + 分值/值）
  let want = match object_type {
    GarnetObjectType::Hash if !params.is_no_value => params.count * 2,
    GarnetObjectType::SortedSet => params.count * 2,
    _ => params.count,
  };
  let return_field = if object_type == GarnetObjectType::Hash && params.is_no_value {
    ScanReturnField::Key
  } else {
    ScanReturnField::KeyAndValue
  };

  let tree_guard = session
    .acquire_tree_read(key, &stub)
    .await
    .map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 纯读面不回写 meta（水位由计数臂 / 收集执行体收敛），到期成员只过滤
  // 不出产出（C# 对象层 Scan 前经 DeleteExpiredItems 的过滤等价）；扫描 Err
  // 经 [`scan_count`] 上抛（应答尚未落帧，上游闭环成 RESP 错误帧），严禁
  // 折成空游标页——存储故障不得伪装成「扫描完毕」
  let now = now_ticks();
  // None 项 = 分值文本化失败（对齐对象层 RESP null 项回写）
  let mut items: Vec<Option<Vec<u8>>> = Vec::new();
  let mut skipped = 0_i64;
  let mut scanned = 0_i64;
  let mut expired = 0_i64;
  scan_count(
    tree.scan_with_count_callback(&[0u8], usize::MAX, return_field, |k, v| {
      if object_type != GarnetObjectType::List && member_expired_at(v, now) {
        // 到期成员计入光标基数（对齐 C# scan 的 expiredKeysCount 口径，
        // hash_object.rs:scan 尾段 cursor + expired_keys_count == len 判定）
        expired += 1;
        return true;
      }
      // 起始游标之前的有效存活条目跳过（不产出，推进已跳过计数）
      if skipped < start {
        skipped += 1;
        return true;
      }
      if params.pattern.is_empty() || glob_match(params.pattern, k) {
        match object_type {
          GarnetObjectType::SortedSet => {
            let (_, payload) = decode_member(v);
            items.push(Some(k.to_vec()));
            // 分值文本化失败（±inf/NaN）以 None 表达（C# Utf8Formatter 失败写 null）
            items.push(if payload.len() == 8 {
              if let Ok(bytes) = <[u8; 8]>::try_from(payload) {
                let score = f64::from_be_bytes(bytes);
                if score.is_finite() {
                  let mut fbuf = ZmijBuffer::new();
                  Some(format_double(score, &mut fbuf).as_bytes().to_vec())
                } else {
                  None
                }
              } else {
                None
              }
            } else {
              None
            });
          }
          GarnetObjectType::Hash => {
            items.push(Some(k.to_vec()));
            if !params.is_no_value {
              items.push(Some(decode_member(v).1.to_vec()));
            }
          }
          _ => items.push(Some(k.to_vec())),
        }
      }
      scanned += 1;
      // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历；count=0 首个
      // 未命中条目即停的上游怪癖一并 1:1 保留）
      (items.len() as i64) != want
    }),
  )?;

  // 扫至树尾光标归零（对象层 cursor + expired_keys_count == size 口径）
  let next_cursor = if start + scanned + expired >= meta.size as i64 {
    0
  } else {
    start + scanned
  };

  output.write_resp_array_len(2);
  let mut cur_buf = ItoaBuffer::new();
  output.write_resp_bulk_string(cur_buf.format(next_cursor).as_bytes());
  if items.is_empty() {
    output.extend_from_slice(cs::RESP_EMPTYLIST);
  } else {
    output.write_resp_array_len(items.len());
    for item in &items {
      match item {
        Some(bytes) => output.write_resp_bulk_string(bytes),
        None => output.write_resp_null_ver(resp_protocol_version),
      }
    }
  }
  Ok(true)
}

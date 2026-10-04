//! SET 条件写共同体慢臂（NetworkSET_Conditional 的异步对偶，SET 选项形态 /
//! GETSET 条件写共用；对标 C# BasicCommands.cs SET_Conditional 单记录锁内全程）

use wbase::time::now_ticks;
use wdev::Device;
use wkv::is_expired;
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, ext::RespVecExt};
use wval::KeyTag;

use super::common::{REPLY_OK, apply_set_with_expiry_async, clear_vector_registry, read_cold};
use crate::{
  resp::{TtlResume, basic_commands::set::SetOptions, vector::vector_manager::VectorManager},
  storage::session::{
    common::ttl_sync::{keep_ttl_probe_async, registry_alive},
    storage_session::StorageSession,
  },
};

/// NetworkSET_Conditional 的异步对偶（SET 选项形态 / GETSET 条件写共同体），
/// 序次对齐快路径 `network_set_conditional`：取窗 → 第四态复验 → 域探针 →
/// RI 门 → 对象键分支 → 条件裁决 → KEEPTTL 旧值 → 写共同体 → 应答
///
/// 向量登记表第四态窗内统一裁决（票 zcode-r163c-setguard 案二，判据单源
/// [`registry_alive`]，与快臂同一折叠式）：NX 命中第四态即「键在」，直出
/// nil 终态零副作用、登记保留（与 SETNX 慢臂
/// [`crate::storage::session::common::ttl_sync::probe_alive_with_registry_async`]
/// 同一裁决源）；XX / 无条件 / KEEPTTL
/// 命中第四态且条件成立，持窗临界区内 [`clear_vector_registry`] 清退后
/// 覆写（对位 C# 锁内 DELETE+SET_Conditional 重投终态，BasicCommands.cs:
/// 788-796）；GET 形态命中第四态对位 C# getValue 臂（:832-835）无 DELETE
/// 重试，直出 -WRONGTYPE 登记保留。窗外破坏性预清退就此删除——旧序
/// 「窗外 clear → 窗内三域探针」使 NX 误判缺写 +OK 毁登记、XX 判缺回 nil
/// 却已静默摧毁存活向量索引（NX/XX 语义反转），本臂收拢为窗内一次折叠终裁
pub(super) async fn slow_set_conditional(
  storage: &StorageSession<'_, impl Device>,
  opts: &SetOptions<'_>,
  vector: Option<&VectorManager>,
  resume: TtlResume,
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let SetOptions {
    key,
    cmd,
    get_value,
    ..
  } = *opts;
  // 条件写共同体整段同窗（第四态复验 → 存活探测 → RI 门 → 对象键分支 →
  // 条件裁决 → KEEPTTL 读旧 → 写共同体；对标快路径 network_set_conditional
  // 同一窗口契约，C# 单记录 RMW 锁内全程）
  let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
  // 第四态判据窗内单次折叠取值（registry_alive 全链单源），存活判定
  // exists = 三域命中 ∨ 第四态在场，与快臂 probe_alive_with_registry 同式
  let prefix = storage.batch.session_prefix();
  let vector_alive = registry_alive(vector, prefix.as_slice(), key);
  if !get_value {
    let (must_exist, must_absent) = (cmd.is_xx(), cmd.is_nx());

    // NX 命中第四态：键在，nil 出且绝不写值、绝不摘除登记；终态「键在」
    // → found 恰一帧（与快臂 NX 第四态命中同形，票
    // wnode-string-bitmap-found-notfound-accounting-matrix）
    if must_absent && vector_alive {
      output.write_resp_null_ver(resp_version);
      storage.record_read_outcome(true);
      return Ok(());
    }

    // 双域存活探测（三域异步裁决，对标快路径 probe_alive_domain）；静默
    // 对偶口 + 终态单点补账（票 wnode-string-bitmap-found-notfound-
    // accounting-matrix：簿记档逐域入账使缺失键计 3、对象键计 2，与 C#
    // SET_Conditional 单帧口径失联，MainStoreOps.cs:279/:284）
    let found = storage
      .probe_alive_domain_quiet_with_prefix(prefix.as_slice(), key)
      .await
      .map_err(|_| ())?;
    if matches!(found, Some(KeyTag::Meta))
      && storage.ri_write_gate_quiet(key).await.map_err(|_| ())?
    {
      // 存活 RangeIndex 元记录：字符串写一律拒（对标快路径 ri_write_gate）；
      // WRONGTYPE 臂零入账（C# WRONGTYPE 臂无 incr，MainStoreOps.cs:273）
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }

    // 对象键（内存信封 / 经上方 RI 门放行的升阶 Meta 域）：与快路径
    // network_set_conditional 对象键臂对称双域匹配
    if matches!(found, Some(KeyTag::ObjectEnvelope) | Some(KeyTag::Meta)) {
      if must_exist {
        // C# WRONGTYPE 分支 XX 族：promote DELETE 重试必 NOTFOUND → 回 nil；
        // 先删对象键（用户键级联删除，含随键 TTL；Meta 域经 delete_string
        // 降级臂完整树清退）再答 nil，与快路径 try_delete_sync 同终态；
        // 重试形末态 NOTFOUND → notfound 恰一帧（首次 WRONGTYPE 零计）
        storage.delete_string(key).await.map_err(|_| ())?;
        output.write_resp_null_ver(resp_version);
        storage.record_read_outcome(false);
        return Ok(());
      }
      // NX / 无条件 / KEEPTTL：覆写且 keep_ttl 恒 None（upsert_string 自带
      // 信封 / Meta 树覆写清退，旧 TTL 随清退消失，杜绝 object 时代幽灵 TTL）；
      // C# DELETE+SET_Conditional 重试形末态缺席写入 → notfound 恰一帧
      apply_set_with_expiry_async(
        storage,
        opts.key,
        opts.val,
        (opts.expiry, opts.exp_high_precision),
        None,
      )
      .await?;
      output.write_resp_simple_string(REPLY_OK);
      storage.record_read_outcome(false);
      return Ok(());
    }

    let exists = found.is_some() || vector_alive;
    if (must_exist && !exists) || (must_absent && exists) {
      // 条件不满足：C# 以 nil 表失败；按存在性折叠 found / notfound 恰一帧
      output.write_resp_null_ver(resp_version);
      storage.record_read_outcome(exists);
      return Ok(());
    }
    if vector_alive {
      // XX / 无条件 / KEEPTTL 命中第四态且条件成立：持窗临界区内先清退
      // 登记再覆写（C# DELETE+SET_Conditional 重投同终态，杜绝双域并存）
      clear_vector_registry(storage, vector, key).await;
    }

    // KEEPTTL：按旧值回填（upsert 自带同步清 TTL）；尾参刻度与当前解析域
    // 现比——同域直取刻度（重放读已清墓碑面免疫），跨换号失配弃刻度改现读
    // 当前域 ttl_of（Full 臂既有现读式，回到 C# 线性化终态新键无 TTL，
    // 杜绝死域刻度盖进新域键成幽灵 TTL）
    let keep_ttl = if cmd.is_keep_ttl() {
      match resume {
        TtlResume::KeepTtl(leg) if leg.domain == storage.batch.virtual_domain() => {
          // 捕获→重放窗刻度跨期防线（零成本，GET 形臂同款）：捕获时存活、
          // 重放时已跨期的死刻度同样不回填
          if is_expired(leg.ticks, now_ticks()) {
            Some(None)
          } else {
            Some(Some(leg.ticks))
          }
        }
        // 过期残留刻度不回填（快臂同语义：C# 新记录无过期，杜绝落地即死）
        _ => Some(keep_ttl_probe_async(storage, key).await?),
      }
    } else {
      None
    };
    apply_set_with_expiry_async(
      storage,
      opts.key,
      opts.val,
      (opts.expiry, opts.exp_high_precision),
      keep_ttl,
    )
    .await?;
    output.write_resp_simple_string(REPLY_OK);
    // 写成功终态：按存在性折叠 found（覆写存活键）/ notfound（缺席写入）
    // 恰一帧（票 wnode-string-bitmap-found-notfound-accounting-matrix）
    storage.record_read_outcome(exists);
    return Ok(());
  }

  // GET 形态命中第四态：-WRONGTYPE 登记保留（C# getValue 臂无 DELETE 重试，
  // 旧值不可读即无覆写义务）
  if vector_alive {
    output.write_resp_error(RESP_ERR_WRONG_TYPE);
    return Ok(());
  }

  // GET 形态：回旧值（不存在则 nil），条件语义同上；双域读判定对象键 WRONGTYPE
  // （[`read_cold`] 折叠入账整值冷读）
  let Some(old) = read_cold(storage, key, output).await? else {
    return Ok(());
  };

  let should_set = if cmd.is_keep_ttl() {
    !cmd.is_xx() || old.is_some()
  } else if cmd.is_nx() {
    old.is_none()
  } else if cmd.is_xx() {
    old.is_some()
  } else {
    true
  };

  // GET 形 KEEPTTL 与非 GET 臂同一域比对纪律：同域直取尾参刻度，跨换号
  // 失配改现读当前域 ttl_of（Full 臂既有现读式，新键无 TTL，杜绝幽灵 TTL）。
  // 过期残留刻度不回填（快臂同语义：C# 新记录无过期，杜绝新值落地即死）
  let keep_ttl = if should_set && cmd.is_keep_ttl() {
    match resume {
      TtlResume::KeepTtl(leg) if leg.domain == storage.batch.virtual_domain() => {
        // 捕获→重放窗刻度跨期防线（零成本）：捕获时存活、重放时已跨期的
        // 死刻度同样不回填
        if is_expired(leg.ticks, now_ticks()) {
          Some(None)
        } else {
          Some(Some(leg.ticks))
        }
      }
      _ => match storage.batch.ttl_of(key).await.map_err(|_| ())? {
        Some(ticks) if is_expired(ticks, now_ticks()) => Some(None),
        ttl => Some(ttl),
      },
    }
  } else {
    None
  };
  if should_set {
    apply_set_with_expiry_async(
      storage,
      opts.key,
      opts.val,
      (opts.expiry, opts.exp_high_precision),
      keep_ttl,
    )
    .await?;
  }
  match old {
    Some(old) => output.write_resp_bulk_string(&old),
    None => output.write_resp_null_ver(resp_version),
  }
  Ok(())
}

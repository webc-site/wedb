//! 字符串族 / OBJECT / 位图族慢路径执行段（快路径 `Ok(false)` 降级承接）
//!
//! 对标 C# BasicCommands.cs / BitmapCommands.cs 同步函数体内 CompletePending
//! 就地闭环的应答形态：快路径在环形页翻转 / RI 门 / 存在性探针降级后，本
//! 模块以同一套参数解析单源（快侧纯函数）+ 存储会话异步口重放整条命令，
//! 产出与快路径逐字节一致的应答。参数推导一律转调快侧同一纯函数，
//! 不在本模块重写第二套推导。

use wbase::num::{strict_f64, strict_i32, strict_i64};
use wbitmap::{
  BitFieldSecondaryCommand, BitOpAccumulator, BitmapOperation, bit_count_driver, bit_field_execute,
  bit_pos_driver, new_block_alloc_length_from_type, try_validate_bit_pos_offsets,
};
use wresp::{
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  command::RespCommand,
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag};

use super::{
  ObjectSubCmd,
  get::parse_getex_args,
  incr::{IncrCmd, parse_incr_args, parse_incr_by_float_args},
  set::{SetCmd, SetOptions, parse_set_options, parse_setex_args, parse_setrange_args},
  ttl::{GetexExpiry, expiry_ticks_from_now},
};
use crate::{
  resp::{
    bitmap::bitmap_commands::{parse_bit_args, parse_bit_count_args, parse_bit_pos_args},
    resp_server_session::RespServerSession,
    vector::vector_manager::VectorManager,
  },
  storage::session::{
    common::{UserReadAsync, ttl_sync::meta_collection_type_of},
    storage_session::StorageSession,
  },
};

/// OBJECT 编码名映射（信封内层标签 / Meta collection_type 同表，C#
/// ReadMethods.cs:67 `_ => CmdStrings.hashtable` 默认臂对齐）
fn encoding_of_object_type(obj_type: GarnetObjectType) -> &'static [u8] {
  match obj_type {
    GarnetObjectType::SortedSet => b"skiplist",
    GarnetObjectType::List => b"quicklist",
    _ => b"hashtable",
  }
}

/// SET 写共同体尾部（异步口）：upsert 自带同步清 TTL（对标快路径
/// `apply_set_with_expiry` 的 try_upsert_sync 面），随后按需写新 TTL 或
/// KEEPTTL 回填旧值（裸 ticks，对标 C# TrySetExpiration 裸值路径）
///
/// `keep_ttl` 三态：`None` = 非 KEEPTTL 形态（expiry != 0 时写新 TTL）；
/// `Some(None)` = KEEPTTL 无旧值（保持无 TTL）；`Some(Some(ticks))` = 回填
async fn apply_set_with_expiry_async(
  storage: &StorageSession<'_, impl wdev::Device>,
  opts: &SetOptions<'_>,
  keep_ttl: Option<Option<i64>>,
) -> Result<(), ()> {
  storage
    .upsert_string(opts.key, opts.val)
    .await
    .map_err(|_| ())?;
  if let Some(old) = keep_ttl {
    if let Some(ticks) = old {
      storage
        .batch
        .put_ttl(opts.key, ticks)
        .await
        .map_err(|_| ())?;
    }
    return Ok(());
  }
  if opts.expiry != 0 {
    let expire_at_ticks = expiry_ticks_from_now(opts.expiry, opts.exp_high_precision);
    storage
      .batch
      .put_ttl(opts.key, expire_at_ticks)
      .await
      .map_err(|_| ())?;
  }
  Ok(())
}

/// NetworkSET_Conditional 的异步对偶（SET 选项形态 / GETSET 条件写共同体），
/// 序次对齐快路径 `network_set_conditional`：域探针 → RI 门 → 对象键分支 →
/// 条件裁决 → KEEPTTL 旧值 → 写共同体 → 应答
async fn slow_set_conditional(
  storage: &StorageSession<'_, impl wdev::Device>,
  opts: &SetOptions<'_>,
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let SetOptions {
    key,
    cmd,
    get_value,
    ..
  } = *opts;
  if !get_value {
    let (must_exist, must_absent) = (cmd.is_xx(), cmd.is_nx());

    // 双域存活探测（三域异步裁决，对标快路径 probe_alive_domain）
    let found = storage
      .probe_alive_domain_async(key)
      .await
      .map_err(|_| ())?;
    if matches!(found, Some(KeyTag::Meta))
      && storage.ri_write_gate_async(key).await.map_err(|_| ())?
    {
      // 存活 RangeIndex 元记录：字符串写一律拒（对标快路径 ri_write_gate）
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }

    if matches!(found, Some(KeyTag::ObjectEnvelope)) {
      if must_exist {
        // C# WRONGTYPE 分支 XX 族：promote DELETE 重试必 NOTFOUND → 回 nil；
        // 先删对象键（用户键级联删除，含随键 TTL）再答 nil，与快路径
        // try_delete_sync 同终态
        storage.delete_string(key).await.map_err(|_| ())?;
        output.write_resp_null_ver(resp_version);
        return Ok(());
      }
      // NX / 无条件：直接写（upsert 自带信封覆写清退，旧 TTL 随清）
      apply_set_with_expiry_async(storage, opts, None).await?;
      output.write_resp_simple_string("OK");
      return Ok(());
    }

    let exists = found.is_some();
    if (must_exist && !exists) || (must_absent && exists) {
      // 条件不满足：C# 以 nil 表失败
      output.write_resp_null_ver(resp_version);
      return Ok(());
    }

    // KEEPTTL：按旧值回填（upsert 自带同步清 TTL）
    let keep_ttl = if cmd.is_keep_ttl() {
      Some(storage.batch.ttl_of(key).await.map_err(|_| ())?)
    } else {
      None
    };
    apply_set_with_expiry_async(storage, opts, keep_ttl).await?;
    output.write_resp_simple_string("OK");
    return Ok(());
  }

  // GET 形态：回旧值（不存在则 nil），条件语义同上；双域读判定对象键 WRONGTYPE
  let old = match storage
    .read_user_async(key, |v| v.to_vec())
    .await
    .map_err(|_| ())?
  {
    UserReadAsync::Hit(old) => Some(old),
    UserReadAsync::WrongType => {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    UserReadAsync::Missing => None,
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

  let keep_ttl = if should_set && cmd.is_keep_ttl() {
    Some(storage.batch.ttl_of(key).await.map_err(|_| ())?)
  } else {
    None
  };
  if should_set {
    apply_set_with_expiry_async(storage, opts, keep_ttl).await?;
  }
  match old {
    Some(old) => output.write_resp_bulk_string(&old),
    None => output.write_resp_null_ver(resp_version),
  }
  Ok(())
}

/// SET 族写命令的向量登记表守卫（对标 exec 层 `set_vector_guard` 的写面：
/// 登记命中即预清退后覆写，杜绝 wkv string 域与向量域并存的幽灵双域键；
/// `delete_vector_set` 未命中零操作，幂等）
fn clear_vector_registry(
  storage: &StorageSession<'_, impl wdev::Device>,
  vector: Option<&VectorManager>,
  key: &[u8],
) {
  if let Some(vm) = vector {
    vm.delete_vector_set(storage.batch.session_prefix().as_slice(), key);
  }
}

/// 字符串族 / OBJECT 慢路径执行段入口（exec_slow 分派；`Err(())` 为存储
/// 错误，调用方统一应答）
pub(crate) async fn string_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;
  let resp_version = storage.resp_protocol_version();
  match cmd {
    C::Set | C::Setexnx => {
      // SET 全形态（裸 SET 即无选项默认）与 SETEXNX 同走选项单源；
      // 无 NX/XX 的 EX/PX/无过期走盲写共同体（对标快路径 network_set_ex）
      let Some(opts) = parse_set_options(parse_state, output) else {
        return Ok(());
      };
      if opts.cmd == SetCmd::Set && !opts.get_value {
        // RI 门（盲写无前置读，须在此拦）+ 向量登记预清退后写
        if storage
          .ri_write_gate_async(opts.key)
          .await
          .map_err(|_| ())?
        {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        clear_vector_registry(storage, vector, opts.key);
        apply_set_with_expiry_async(storage, &opts, None).await?;
        output.write_resp_simple_string("OK");
        return Ok(());
      }
      // 选项形态条件写：登记命中 + GET 形态回 WRONGTYPE（对标
      // set_vector_guard），其余写形态预清退放行自然路径
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), opts.key)
          .is_some()
      }) {
        if opts.get_value {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        clear_vector_registry(storage, vector, opts.key);
      }
      slow_set_conditional(storage, &opts, resp_version, output).await
    }
    C::Setex | C::Psetex => {
      let cmd_name = if cmd == C::Setex { "SETEX" } else { "PSETEX" };
      let Some((key, expiry, val)) = parse_setex_args(cmd_name, parse_state, output) else {
        return Ok(());
      };
      let high_precision = cmd == C::Psetex;
      // RI 门（盲写 + 独立写 TTL，无前置读）+ 向量登记预清退
      if storage.ri_write_gate_async(key).await.map_err(|_| ())? {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      clear_vector_registry(storage, vector, key);
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      let expire_at_ticks = expiry_ticks_from_now(expiry, high_precision);
      storage
        .batch
        .put_ttl(key, expire_at_ticks)
        .await
        .map_err(|_| ())?;
      output.write_resp_simple_string("OK");
      Ok(())
    }
    C::Setnx => {
      let (key, val) = match parse_state {
        [key, val] => (*key, *val),
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "SETNX");
          return Ok(());
        }
      };
      // 登记命中即存在（C# NX 对既有键失败）：回 :0 不写入
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_int(0);
        return Ok(());
      }
      // 存在性探测（三域异步裁决，对象键同计存在，C# NX 语义）
      if storage
        .probe_alive_domain_async(key)
        .await
        .map_err(|_| ())?
        .is_some()
      {
        output.write_resp_int(0);
        return Ok(());
      }
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      output.write_resp_int(1);
      Ok(())
    }
    C::Getset => {
      // C# 走 NetworkSET_Conditional(SET, getValue: true)：无条件写入并回旧值；
      // 登记命中即错型不可读旧值，回 -WRONGTYPE 保留登记（exec 层值域门同判）
      let (key, val) = match parse_state {
        [key, val] => (*key, *val),
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "GETSET");
          return Ok(());
        }
      };
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      let opts = SetOptions {
        key,
        val,
        expiry: 0,
        exp_high_precision: false,
        cmd: SetCmd::Set,
        get_value: true,
      };
      slow_set_conditional(storage, &opts, resp_version, output).await
    }
    C::Setrange => {
      let Some((key, offset, val)) = parse_setrange_args(parse_state, output) else {
        return Ok(());
      };
      let new_val = match storage
        .read_user_async(key, |v| {
          let mut existing = v.to_vec();
          let required_len = offset + val.len();
          if existing.len() < required_len {
            existing.resize(required_len, 0);
          }
          existing[offset..offset + val.len()].copy_from_slice(val);
          existing
        })
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(existing) => existing,
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => {
          let mut new_val = vec![0u8; offset + val.len()];
          new_val[offset..].copy_from_slice(val);
          new_val
        }
      };
      let len = new_val.len() as i64;
      storage.rmw_string(key, &new_val).await.map_err(|_| ())?;
      output.write_resp_int(len);
      Ok(())
    }
    C::Append => {
      let (key, val) = match parse_state {
        [key, val] => (*key, *val),
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "APPEND");
          return Ok(());
        }
      };
      let new_val = match storage
        .read_user_async(key, |v| {
          let mut buf = Vec::with_capacity(v.len() + val.len());
          buf.extend_from_slice(v);
          buf
        })
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(mut existing) => {
          existing.extend_from_slice(val);
          existing
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => val.to_vec(),
      };
      let len = new_val.len() as i64;
      storage.rmw_string(key, &new_val).await.map_err(|_| ())?;
      output.write_resp_int(len);
      Ok(())
    }
    C::Incr | C::Decr | C::Incrby | C::Decrby => {
      let incr_cmd = match cmd {
        C::Incr => IncrCmd::Incr,
        C::Decr => IncrCmd::Decr,
        C::Incrby => IncrCmd::IncrBy,
        _ => IncrCmd::DecrBy,
      };
      let Some((key, delta)) = parse_incr_args(incr_cmd, parse_state, output) else {
        return Ok(());
      };
      // 旧值口径对位 C# IsValidNumber → NumUtils.TryReadInt64（拒前导零，
      // 与参数路径 strict_i64 同源单一实现）
      let val = match storage
        .read_user_async(key, strict_i64)
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(Some(v)) => v,
        UserReadAsync::Hit(None) => {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(());
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => 0,
      };
      // C# checked 加法溢出与"非整数旧值"共用 not-integer 错误且不落写
      let Some(next) = val.checked_add(delta) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(());
      };
      let mut buf = itoa::Buffer::new();
      storage
        .rmw_string(key, buf.format(next).as_bytes())
        .await
        .map_err(|_| ())?;
      output.write_resp_int(next);
      Ok(())
    }
    C::Incrbyfloat => {
      let Some((key, incr_by)) = parse_incr_by_float_args(parse_state, output) else {
        return Ok(());
      };
      // C# parseState.TryGetDouble 默认 canBeInfinite: true（INF 白名单 + NaN 拒）
      let val = match storage
        .read_user_async(key, |raw| strict_f64(raw, true))
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(Some(v)) => v,
        UserReadAsync::Hit(None) => {
          abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          return Ok(());
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => 0.0,
      };
      // 对标 C# IsValidDouble：旧值自身非有限 → NaN/Infinity 文案
      if !val.is_finite() {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
        return Ok(());
      }
      let next = val + incr_by;
      // 有限 + 有限相加溢出无穷大与非法旧值同报 not-valid-float
      if !next.is_finite() {
        abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
        return Ok(());
      }
      // 对标 NumUtils.WriteDouble：无指数记法十进制表示，整数结果无小数点
      let mut buf = zmij::Buffer::new();
      let formatted = wresp::resp_memory_writer::format_double(next, &mut buf);
      storage
        .rmw_string(key, formatted.as_bytes())
        .await
        .map_err(|_| ())?;
      output.write_resp_bulk_string(formatted.as_bytes());
      Ok(())
    }
    C::Getex => {
      let Some((key, expiry)) = parse_getex_args(parse_state, output) else {
        return Ok(());
      };
      match storage
        .read_user_async(key, |v| v.to_vec())
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(val) => {
          // 过期应用先于应答闭环（对标快路径同序；慢路径整段原子重放，
          // 无快路径的输出回退窗口）
          match expiry {
            GetexExpiry::None => {}
            // PERSIST：移除键级 TTL（wkv persist 异步口，键已确认存活）
            GetexExpiry::Persist => {
              storage.batch.persist(key).await.map_err(|_| ())?;
            }
            GetexExpiry::At(ticks) => {
              storage.batch.put_ttl(key, ticks).await.map_err(|_| ())?;
            }
          }
          output.write_resp_bulk_string(&val);
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
        }
        UserReadAsync::Missing => {
          output.write_resp_null_ver(resp_version);
        }
      }
      Ok(())
    }
    C::Getrange | C::Substr => {
      let cmd_name = if cmd == C::Getrange {
        "GETRANGE"
      } else {
        "SUBSTR"
      };
      let (key, start_raw, end_raw) = match parse_state {
        [key, start_raw, end_raw] => (*key, *start_raw, *end_raw),
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, cmd_name);
          return Ok(());
        }
      };
      // start/end 须可解析为整数（TryGetInt 口径），否则报 not-integer
      let (Some(start), Some(end)) = (strict_i32(start_raw), strict_i32(end_raw)) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(());
      };
      let (start, end) = (i64::from(start), i64::from(end));
      match storage
        .read_user_async(key, |val| {
          let len = val.len() as i64;
          let (start, end) = RespServerSession::normalize_range(start, end, len);
          if start == end {
            Vec::new()
          } else {
            val[(start as usize)..(end as usize)].to_vec()
          }
        })
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(slice) => output.write_resp_bulk_string(&slice),
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
        }
        // 键缺失回空串（C# NOTFOUND → 空批量串）
        UserReadAsync::Missing => output.write_resp_bulk_string(b""),
      }
      Ok(())
    }
    C::Strlen => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "STRLEN");
          return Ok(());
        }
      };
      match storage
        .read_user_async(key, |v| v.len())
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(len) => output.write_resp_int(len as i64),
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
        }
        UserReadAsync::Missing => output.write_resp_int(0),
      }
      Ok(())
    }
    C::ObjectEncoding | C::ObjectFreq | C::ObjectIdletime | C::ObjectRefcount => {
      let sub_cmd = match cmd {
        C::ObjectEncoding => ObjectSubCmd::Encoding,
        C::ObjectFreq => ObjectSubCmd::Freq,
        C::ObjectIdletime => ObjectSubCmd::Idletime,
        _ => ObjectSubCmd::Refcount,
      };
      object_slow(storage, sub_cmd, parse_state, vector, output).await
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接字符串族与 OBJECT，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// NetworkOBJECT 的异步对偶（四子命令；语义对照 UnifiedStore
/// ReadMethods:HandleObjectEncoding，与快路径 `network_object` 同口径：
/// 向量键登记特判 → String 域 raw / 信封标签映射 / 升阶键 Meta 映射 /
/// 缺失回 nil）
pub(crate) async fn object_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
  sub_cmd: ObjectSubCmd,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let key = match parse_state {
    [key] => *key,
    _ => {
      cs::abort_with_wrong_number_of_arguments(output, sub_cmd.as_str());
      return Ok(());
    }
  };
  let resp_version = storage.resp_protocol_version();

  // 向量键登记特判（快路径同形：raw / 1 / 0 / FREQ 不支持）
  if vector.is_some_and(|vm| {
    vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
      .is_some()
  }) {
    match sub_cmd {
      ObjectSubCmd::Encoding => output.write_resp_bulk_string(b"raw"),
      ObjectSubCmd::Refcount => output.write_resp_int(1),
      ObjectSubCmd::Idletime => output.write_resp_int(0),
      ObjectSubCmd::Freq => {
        abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED);
      }
    }
    return Ok(());
  }

  // 编码判定：String 域命中 → raw；信封域按内层标签映射；升阶键按 Meta
  // collection_type 同款映射；过期键与缺失键一致回 nil
  let encoding = match storage.read_user_async(key, |_| ()).await.map_err(|_| ())? {
    UserReadAsync::Hit(()) => Some(b"raw".as_slice()),
    UserReadAsync::WrongType => {
      let envelope = storage
        .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
          raw
            .first()
            .copied()
            .and_then(GarnetObjectType::from_u8)
            .map(encoding_of_object_type)
        })
        .await
        .map_err(|_| ())?;
      match envelope {
        Some(Some(enc)) => Some(enc),
        _ => storage
          .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
          .await
          .map_err(|_| ())?
          .flatten()
          .map(encoding_of_object_type),
      }
    }
    UserReadAsync::Missing => None,
  };
  match (encoding, sub_cmd) {
    (Some(encoding), ObjectSubCmd::Encoding) => output.write_resp_bulk_string(encoding),
    (Some(_), ObjectSubCmd::Refcount) => output.write_resp_int(1),
    (Some(_), ObjectSubCmd::Idletime) => output.write_resp_int(0),
    (Some(_), ObjectSubCmd::Freq) => {
      abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED);
    }
    // C# 键缺失（status != OK）一律回 nil
    (None, _) => output.write_resp_null_ver(resp_version),
  }
  Ok(())
}

/// 位图族慢路径执行段入口（快路径降级承接；解析一律转调 bitmap_commands
/// 的推导单源，应答形态与快路径逐字节一致）
pub(crate) async fn bitmap_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;

  use crate::resp::bitmap::bitmap_commands::{parse_bitfield_args, parse_bitfield_ro_args};
  match cmd {
    C::Setbit | C::Getbit => {
      let (cmd_name, with_bit) = if cmd == C::Setbit {
        ("SETBIT", true)
      } else {
        ("GETBIT", false)
      };
      let Some((key, offset, bit)) = parse_bit_args(cmd_name, parse_state, output, with_bit) else {
        return Ok(());
      };
      let byte_idx = (offset / 8) as usize;
      let bit_idx = 7 - (offset % 8) as u32;
      if with_bit {
        let mut val = match storage
          .read_user_async(key, |v| {
            let mut vec = Vec::with_capacity(v.len().max(byte_idx + 1));
            vec.extend_from_slice(v);
            vec
          })
          .await
          .map_err(|_| ())?
        {
          UserReadAsync::Hit(v) => v,
          UserReadAsync::Missing => Vec::with_capacity(byte_idx + 1),
          UserReadAsync::WrongType => {
            output.write_resp_error(RESP_ERR_WRONG_TYPE);
            return Ok(());
          }
        };
        if val.len() <= byte_idx {
          val.resize(byte_idx + 1, 0);
        }
        let old_bit = (val[byte_idx] >> bit_idx) & 1;
        if bit == 1 {
          val[byte_idx] |= 1 << bit_idx;
        } else {
          val[byte_idx] &= !(1 << bit_idx);
        }
        storage.rmw_string(key, &val).await.map_err(|_| ())?;
        output.write_resp_int(i64::from(old_bit));
      } else {
        let bit = match storage
          .read_user_async(key, |val| {
            if byte_idx < val.len() {
              (val[byte_idx] >> bit_idx) & 1
            } else {
              0
            }
          })
          .await
          .map_err(|_| ())?
        {
          UserReadAsync::Hit(bit) => bit,
          // C# NOTFOUND → :0
          UserReadAsync::Missing => 0,
          UserReadAsync::WrongType => {
            output.write_resp_error(RESP_ERR_WRONG_TYPE);
            return Ok(());
          }
        };
        output.write_resp_int(i64::from(bit));
      }
      Ok(())
    }
    C::Bitcount => {
      let Some((key, start, end, offset_type)) = parse_bit_count_args(parse_state, output) else {
        return Ok(());
      };
      let total = match storage
        .read_user_async(key, |val| {
          bit_count_driver(start, end, offset_type, val, val.len() as i64)
        })
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(total) => total,
        // C# NOTFOUND → :0
        UserReadAsync::Missing => 0,
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
      };
      output.write_resp_int(total);
      Ok(())
    }
    C::Bitpos => {
      let Some(args) = parse_bit_pos_args(parse_state, output) else {
        return Ok(());
      };
      // 区间越界直接 -1（快路径同判，单点 try_validate_bit_pos_offsets）
      if try_validate_bit_pos_offsets(
        args.start_offset,
        args.end_offset,
        args.offset_type,
        args.has_start_offset,
        args.has_end_offset,
      ) {
        output.write_resp_int(-1);
        return Ok(());
      }
      let search_for = args.search_for;
      match storage
        .read_user_async(args.key, |val| {
          bit_pos_driver(
            val,
            val.len() as i64,
            args.start_offset,
            args.end_offset,
            search_for,
            args.offset_type,
          )
        })
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(pos) => output.write_resp_int(pos),
        // C# NOTFOUND：找 0 回 0，找 1 回 -1
        UserReadAsync::Missing => {
          output.extend_from_slice(if search_for == 0 {
            cs::RESP_RETURN_VAL_0
          } else {
            cs::RESP_RETURN_VAL_N1
          });
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
        }
      }
      Ok(())
    }
    C::BitopAnd | C::BitopOr | C::BitopXor | C::BitopNot | C::BitopDiff => {
      let bit_op = match cmd {
        C::BitopAnd => BitmapOperation::And,
        C::BitopOr => BitmapOperation::Or,
        C::BitopXor => BitmapOperation::Xor,
        C::BitopNot => BitmapOperation::Not,
        _ => BitmapOperation::Diff,
      };
      slow_bit_operation(storage, bit_op, parse_state, vector, output).await
    }
    C::Bitfield | C::BitfieldRo => {
      let (key, secondary_command_args) = if cmd == C::Bitfield {
        let Some((key, args, _)) = parse_bitfield_args(parse_state, output) else {
          return Ok(());
        };
        (key, args)
      } else {
        let Some((key, args)) = parse_bitfield_ro_args(parse_state, output) else {
          return Ok(());
        };
        (key, args)
      };
      if secondary_command_args.is_empty() {
        output.write_resp_array_len(0);
        return Ok(());
      }
      let resp_version = storage.resp_protocol_version();
      let mut value: Option<Vec<u8>> = match storage
        .read_user_async(key, |v| v.to_vec())
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(v) => Some(v),
        UserReadAsync::Missing => None,
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
      };
      output.write_resp_array_len(secondary_command_args.len());
      let mut dirty = false;
      for args in &secondary_command_args {
        let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
        if is_get {
          match value.as_mut() {
            // NOTFOUND + GET → :0
            None => output.write_resp_int(0),
            Some(buf) => match bit_field_execute(args, buf) {
              Some((v, false)) => output.write_resp_int(v),
              // 只读 GET 不产生溢出
              _ => output.write_resp_null_ver(resp_version),
            },
          }
        } else {
          // 写子命令：增长到位域所需长度后执行
          let need = new_block_alloc_length_from_type(args, 0) as usize;
          let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
          if buf.len() < need {
            buf.resize(need, 0);
          }
          match bit_field_execute(args, buf) {
            Some((v, false)) => output.write_resp_int(v),
            Some((_, true)) => output.write_resp_null_ver(resp_version),
            None => output.write_resp_error(RESP_ERR_GENERIC),
          }
          dirty = true;
        }
      }
      // RMW 语义写回（保留既有 key 级 TTL，有写子命令时统一一次）
      if let Some(buf) = dirty.then_some(value).flatten() {
        storage.rmw_string(key, &buf).await.map_err(|_| ())?;
      }
      Ok(())
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接位图族，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// NetworkStringBitOperation 的异步对偶（BITOP AND/OR/XOR/NOT/DIFF；
/// 逐源异步读折叠 + dest 写共同体，语义对齐快路径
/// `network_string_bit_operation`：源键命中向量登记或对象域即整体
/// WRONGTYPE 零写，dest 命中登记在有源命中时预清退再写）
async fn slow_bit_operation(
  storage: &StorageSession<'_, impl wdev::Device>,
  bit_op: BitmapOperation,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use wresp::cmd_strings::{
    RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED, RESP_ERR_BITOP_KEY_LIMIT,
    RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS,
  };
  let count = parse_state.len();
  // 参数过少（parse_state = [destkey, srckey...]）
  if count < 2 {
    abort_with_error_message(output, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
    return Ok(());
  }
  // DIFF 至少两个源
  if bit_op == BitmapOperation::Diff && count < 3 {
    abort_with_error_message(output, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
    return Ok(());
  }
  // NOT 为一元：恰一个源键
  if bit_op == BitmapOperation::Not && count > 2 {
    abort_with_error_message(output, RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
    return Ok(());
  }
  // 源键上限（含 destkey 共 64）
  if count > 64 {
    abort_with_error_message(output, RESP_ERR_BITOP_KEY_LIMIT);
    return Ok(());
  }
  let dest_key = parse_state[0];

  // 逐源异步读折叠：缺失键跳过（C# NOTFOUND continue）；源键命中向量登记
  // 或对象域（信封 / Meta 升阶）即整体 WRONGTYPE 短路（dest 尚未写入，
  // 无副作用）
  let mut acc = BitOpAccumulator::new(bit_op);
  for src_key in &parse_state[1..] {
    if vector.is_some_and(|vm| {
      vm.read_stored_index(storage.batch.session_prefix().as_slice(), src_key)
        .is_some()
    }) {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    match storage
      .read_user_async(src_key, |v| v.to_vec())
      .await
      .map_err(|_| ())?
    {
      UserReadAsync::Hit(src_val) => {
        acc.fold(&src_val);
      }
      UserReadAsync::Missing => {}
      UserReadAsync::WrongType => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
    }
  }

  let result = match acc.finish() {
    Ok(dst) => {
      // C# maxBitmapLen > 0 才 SET；全缺失/全空源回 0 不写（dest 为存活
      // 向量键时同臂保留登记）
      let longest = dst.len();
      if longest > 0 {
        // dest 写共同体前置 RI 门（异步对偶：存活 RangeIndex 上拒写）+
        // 登记预清退（C# dest DELETE+SET 重试臂同终态）
        if storage
          .ri_write_gate_async(dest_key)
          .await
          .map_err(|_| ())?
        {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        clear_vector_registry(storage, vector, dest_key);
        storage
          .upsert_string(dest_key, &dst)
          .await
          .map_err(|_| ())?;
        longest as i64
      } else {
        0
      }
    }
    // C# GarnetException（源被吞并后 DIFF 单源）→ 通用错误应答
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(());
    }
  };
  output.write_resp_int(result);
  Ok(())
}

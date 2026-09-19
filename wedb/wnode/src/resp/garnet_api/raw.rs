//! 存储原语与快慢命令分派表

use wbitmap::BitmapOperation;
use wcol::zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings::{
    RESP_ERR_BUSSYKEY, RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_WRONG_TYPE, write_error_raw,
  },
  command::{RespCommand, is_data_command, is_vector_gate_exempt, vector_gate_scan_all_keys},
  ext::RespVecExt,
};

use crate::resp::{
  RespServerSession,
  basic_commands::{IncrCmd, ObjectSubCmd, parse_set_options},
  key_admin_commands::{ExpireCmd, ExpireTimeCmd, TtlCmd},
  objects::{sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind},
  vector::vector_manager::VectorManager,
};

/// SET 族向量键守卫（对标 C# SET 覆写向量集记录的 RecordType 变更清理）：
/// 键在向量登记表命中时，覆写前清退登记项与 HNSW 上下文，杜绝 wkv string
/// 域与向量域并存的幽灵双域键（KEYS 迁移只迁 string，源端残留幽灵上下文）。
///
/// 裁决臂对位 C# BasicCommands.cs 内 NetworkSET_Conditional 两分支（:772-847，命令级
/// 锚点登记在 resp/basic_commands/set.rs 的 network_set_conditional，此处不复挂）：
/// getValue=false 的写形态撞 WRONGTYPE 即 promote DELETE+SET_Conditional 重投
/// （NX 删后条件成立 → 写入 +OK；XX 删后 NOTFOUND → nil；KEEPTTL 无条件 +OK），
/// rust 以预清退 + 自然路径条件裁决得同终态；getValue=true 臂（GET 形态，含
/// NX GET / XX GET）直接回 -WRONGTYPE 不重试，登记保留。GETSET（NetworkGETSET
/// :426-434 转 NetworkSET GET 标志）同归值域门拒，不入本守卫面。
///
/// 返回 Ok(true) 表示应答已在 output 闭环（SETNX 存在性 :0 / 选项解析失败 /
/// GET 形态 WRONGTYPE，调用方直接返回）；Ok(false) 正常放行。登记表未命中
/// （绝大多数请求）一次 ConcurrentMap 读即返回，零额外开销。
/// 未覆盖的 RMW 族写命令（SETRANGE/APPEND/INCR/MSETNX）不入本守卫面。
pub(crate) fn set_vector_guard(
  vm: &VectorManager,
  prefix: &[u8],
  cmd: RespCommand,
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  use RespCommand as C;
  let (Some(key), true) = (
    args.first(),
    matches!(
      cmd,
      C::Set | C::Setex | C::Psetex | C::Setnx | C::Setexnx | C::Mset
    ),
  ) else {
    return Ok(false);
  };

  match cmd {
    // 无条件覆写（arity 精确匹配对齐 unpack_args，杜绝失败命令误清退）：
    // 登记命中即预清退（delete_vector_set 未命中零操作）
    C::Set if args.len() == 2 => {
      vm.delete_vector_set(prefix, key);
    }
    C::Setex | C::Psetex if args.len() == 3 => {
      vm.delete_vector_set(prefix, key);
    }
    // MSET 逐键覆写。arity 前置对位 C# parse 层 IsReadOnly+ArityHasValid
    // （RespCommand.cs:724-737）先于任何存储访问：奇数/空参零副作用，
    // 清退不得先于 network_mset 的 check_arg_count 失败发生
    C::Mset if args.len() >= 2 && args.len().is_multiple_of(2) => {
      for [k, _] in args.as_chunks::<2>().0 {
        vm.delete_vector_set(prefix, k);
      }
    }
    // SETNX：登记命中即存在（C# NX 对既有键失败），回 :0 不写入
    C::Setnx => {
      if vm.read_stored_index(prefix, key).is_some() {
        output.write_resp_int(0);
        return Ok(true);
      }
    }
    // 选项形态（SET k v ... / SETEXNX k v ...）：GET 形态登记命中即错型不可读
    // 旧值，回 -WRONGTYPE 保留登记（C# getValue 臂 :832-835 无 DELETE 重试）；
    // 其余写形态（NX/XX/KEEPTTL/带过期）预清退后放行自然路径，由
    // network_set_conditional 按缺失键裁决条件，终态与 C# DELETE 重投一致
    C::Set | C::Setexnx if args.len() > 2 && vm.read_stored_index(prefix, key).is_some() => {
      let Some(opts) = parse_set_options(args, output) else {
        // 语法/取值错误已应答：无写入，登记原样
        return Ok(true);
      };
      if opts.get_value {
        write_error_raw(output, RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      vm.delete_vector_set(prefix, key);
    }
    _ => {}
  }
  Ok(false)
}

/// 向量登记表值域门（写面 WRONGTYPE 单点，与读面门 / MSETNX / RESTORE 共用同一判据源）
///
/// 对标 C# 主存三处同判：ReadMethods.cs:115 CheckRecordTypeMismatch、RMWMethods
/// InPlaceUpdater/CopyUpdater 的 `RecordType == VectorManager.RecordType &&
/// !cmd.IsLegalOnVectorSet()`、UpsertMethods InPlaceWriter:67-77 的 WrongType 臂。
/// rust 向量索引驻留 VectorManager 进程内登记表、不落 wkv 值域（wkv 读写原语看不到它），
/// 命令层登记表判据是唯一可用判据，故门挂 RESP 派发层（与 ri_write_gate 同族：一处
/// 定义、消费单一记录判据，仅判据源不同——RI 取 KeyTag::Meta 物理域、向量取登记表）。
/// 适用集 = `is_data_command && !白名单 && !豁免`（豁免集含 SET 族覆写与第四态探针
/// 承接的存活读侧命令），多键命令逐键过同一判据（scan_all_keys 清单）。
///
/// 返回 true 表示应答已写入 output、本轮不得续写。登记表未命中（绝大多数请求）
/// 一次 ConcurrentMap 读即放行，零额外开销。
fn vector_registry_gate(
  vm: &VectorManager,
  prefix: &[u8],
  cmd: RespCommand,
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> bool {
  // MSETNX：任一待写键（args 偶位）命中登记即键存在 → 回 :0 零写入
  //（C# ArrayCommands.cs:76-90 MSET_Conditional NOTFOUND→1、否则 0）
  if cmd == RespCommand::Msetnx {
    if args
      .chunks(2)
      .any(|pair| vm.read_stored_index(prefix, pair[0]).is_some())
    {
      output.write_resp_int(0);
      return true;
    }
    return false;
  }
  // RESTORE：目的键命中登记即键存在 → BUSYKEY（C# KeyAdminCommands.cs:NetworkRESTORE 前置存在判定）
  if cmd == RespCommand::Restore
    && args
      .first()
      .is_some_and(|k| vm.read_stored_index(prefix, k).is_some())
  {
    write_error_raw(output, RESP_ERR_BUSSYKEY);
    return true;
  }
  // 值域 WRONGTYPE 门：数据命令 ∧ 非白名单 ∧ 非豁免，命中登记即拒
  if is_data_command(cmd) && !cmd.is_legal_on_vector_set() && !is_vector_gate_exempt(cmd) {
    let hit = if vector_gate_scan_all_keys(cmd) {
      args
        .iter()
        .any(|k| vm.read_stored_index(prefix, k).is_some())
    } else {
      args
        .first()
        .is_some_and(|k| vm.read_stored_index(prefix, k).is_some())
    };
    if hit {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      return true;
    }
  }
  false
}

/// 分派表 · fast 段（C# ProcessBasicCommands + ProcessArrayCommands 的存储命令
/// switch：字符串族、多键数组族、键管理族与对象集合族；@fast 语义，不置位
/// containsSlowCommand）。未命中命令落入 [`dispatch_slow`]（C# 链式回退）
pub(crate) fn dispatch<D: Device>(
  session: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  batch: &BatchStoreSession<'_, D>,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  use RespCommand as C;
  // 登记表域键单次外提（会话域内寻址，跨库同名键互不可见）
  let prefix = batch.session_prefix();
  let prefix = prefix.as_slice();
  // SET 族向量键守卫（应在其余分派前闭环）
  if let Some(vm) = vector
    && set_vector_guard(vm, prefix, cmd, args, output)?
  {
    return Ok(true);
  }

  // 向量登记表值域门（值域读/写命令 + MSETNX/RESTORE 存在性裁决，对标 C# 主存三处
  // 同判：ReadMethods/RMWMethods/UpsertMethods 的 CheckRecordTypeMismatch / WrongType 臂）。
  // 判据唯一取自登记表命中（read_stored_index），与 set_vector_guard 的 SET 族覆写、
  // 第四态探针的存活读侧同一登记表源，不另起第二套（适用集裁剪见 wresp::command::
  // is_vector_gate_exempt）。SET 族覆写已在上一步闭环，此处仅拦其余值域入口。
  if let Some(vm) = vector
    && vector_registry_gate(vm, prefix, cmd, args, output)
  {
    return Ok(true);
  }

  match cmd {
    // ---- 字符串族（ProcessBasicCommands switch 前段）
    C::Get => session.network_get(args, batch, output),
    C::Getex => session.network_getex(args, batch, output),
    C::Set => session.network_set(args, batch, output),
    C::Setex => session.network_setex(args, batch, output),
    C::Psetex => session.network_psetex(args, batch, output),
    C::Setnx => session.network_setnx(args, batch, output),
    C::Setexnx => session.network_setexnx(args, batch, output),
    C::Getset => session.network_getset(args, batch, output),
    C::Setrange => session.network_set_range(args, batch, output),
    // C# NetworkGetRange 报 cmd.ToString()：GETRANGE/SUBSTR 各实名
    C::Getrange => session.network_get_range(args, batch, output, "GETRANGE"),
    C::Substr => session.network_get_range(args, batch, output, "SUBSTR"),
    C::Append => session.network_append(args, batch, output),
    C::Strlen => session.network_strlen(args, batch, output),
    C::Incr => session.network_increment(IncrCmd::Incr, args, batch, output),
    C::Decr => session.network_increment(IncrCmd::Decr, args, batch, output),
    C::Incrby => session.network_increment(IncrCmd::IncrBy, args, batch, output),
    C::Decrby => session.network_increment(IncrCmd::DecrBy, args, batch, output),
    C::Incrbyfloat => session.network_increment_by_float(args, batch, output),

    // ---- 多键数组族（ArrayCommands.cs:ProcessArrayCommands 前段）
    // 向量集清退随 wkv 删除单点的缺席观测钩子收口，本臂不再传 vector 切面
    C::Del | C::Unlink => session.network_del(args, batch, output),
    C::Mget => session.network_mget(args, batch, output),
    C::Mset => session.network_mset(args, batch, output),
    C::Msetnx => session.network_msetnx(args, batch, output),

    // ---- 键管理族（KeyAdminCommands.cs）
    C::Exists => session.network_exists(args, batch, vector, output),
    C::Expire => session.network_expire(ExpireCmd::Expire, args, batch, output),
    C::Pexpire => session.network_expire(ExpireCmd::Pexpire, args, batch, output),
    C::Expireat => session.network_expire(ExpireCmd::Expireat, args, batch, output),
    C::Pexpireat => session.network_expire(ExpireCmd::Pexpireat, args, batch, output),
    C::Persist => session.network_persist(args, batch, output),
    C::Ttl => session.network_ttl(TtlCmd::Ttl, args, batch, vector, output),
    C::Pttl => session.network_ttl(TtlCmd::Pttl, args, batch, vector, output),
    C::Expiretime => {
      session.network_expiretime(ExpireTimeCmd::Expiretime, args, batch, vector, output)
    }
    C::Pexpiretime => {
      session.network_expiretime(ExpireTimeCmd::Pexpiretime, args, batch, vector, output)
    }
    C::Getdel => session.network_getdel(args, batch, output),
    C::Rename => session.network_rename(args, batch, vector, output),
    C::Renamenx => session.network_renamenx(args, batch, vector, output),
    C::Dump => session.network_dump(args, batch, output),
    C::Restore => session.network_restore(args, batch, output),

    // ---- 库切换族（ArrayCommands.cs:NetworkSELECT / NetworkSWAPDB）
    C::Select => session.network_select(args, batch, output),
    C::Swapdb => session.network_swapdb(args, output),

    // ---- 哈希族（Objects/HashCommands.cs）
    C::Hset => session.hash_set(args, batch, output),
    C::Hsetnx => session.hash_set_nx(args, batch, output),
    C::Hmset => session.hash_set_map(args, batch, output),
    C::Hget => session.hash_get(args, batch, output),
    C::Hgetall => session.hash_get_all(args, batch, output),
    C::Hmget => session.hash_get_multiple(args, batch, output),
    C::Hlen => session.hash_length(args, batch, output),
    C::Hdel => session.hash_delete(args, batch, output),
    C::Hexists => session.hash_exists(args, batch, output),
    C::Hkeys => session.hash_keys(args, batch, output, true),
    C::Hvals => session.hash_vals(args, batch, output),
    C::Hrandfield => session.hash_random_field(args, batch, output),
    C::Hstrlen => session.hash_str_length(args, batch, output),
    C::Hincrby => session.hash_increment(args, batch, output, false),
    C::Hincrbyfloat => session.hash_increment(args, batch, output, true),
    C::Hexpire => session.hash_expire(cmd.into(), args, batch, output, false, false),
    C::Hpexpire => session.hash_expire(cmd.into(), args, batch, output, true, false),
    C::Hexpireat => session.hash_expire(cmd.into(), args, batch, output, false, true),
    C::Hpexpireat => session.hash_expire(cmd.into(), args, batch, output, true, true),
    C::Httl => session.hash_time_to_live(cmd.into(), args, batch, output, false, false),
    C::Hpttl => session.hash_time_to_live(cmd.into(), args, batch, output, true, false),
    C::Hexpiretime => session.hash_time_to_live(cmd.into(), args, batch, output, false, true),
    C::Hpexpiretime => session.hash_time_to_live(cmd.into(), args, batch, output, true, true),
    C::Hpersist => session.hash_persist(args, batch, output),
    C::Hscan => session.network_hscan(args, batch, output),

    // ---- 集合族（Objects/SetCommands.cs）
    C::Sadd => session.set_add(args, batch, output),
    C::Srem => session.set_remove(args, batch, output),
    C::Scard => session.set_length(args, batch, output),
    C::Smembers => session.set_members(args, batch, output),
    C::Sismember => session.set_is_member(args, batch, output),
    C::Smismember => session.set_multi_is_member(args, batch, output),
    C::Spop => session.set_pop(args, batch, output),
    C::Srandmember => session.set_random_member(args, batch, output),
    C::Smove => session.set_move(args, batch, output),
    C::Sinter => session.set_intersect(args, batch, output),
    C::Sinterstore => session.set_intersect_store(args, batch, output),
    C::Sintercard => session.set_intersect_length(args, batch, output),
    C::Sunion => session.set_union(args, batch, output),
    C::Sunionstore => session.set_union_store(args, batch, output),
    C::Sdiff => session.set_diff(args, batch, output),
    C::Sdiffstore => session.set_diff_store(args, batch, output),
    C::Sscan => session.network_sscan(args, batch, output),

    // ---- 有序集合族（Objects/SortedSetCommands.cs）
    C::Zadd => session.sorted_set_add(args, batch, output),
    C::Zscore => session.sorted_set_score(args, batch, output),
    C::Zrem => session.sorted_set_remove(args, batch, output),
    C::Zcard => session.sorted_set_length(args, batch, output),
    C::Zpopmin => session.sorted_set_pop(args, batch, output, true),
    C::Zpopmax => session.sorted_set_pop(args, batch, output, false),
    C::Zrange => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::NONE),
    C::Zrevrange => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::REVERSE),
    C::Zrangebylex => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::BY_LEX),
    C::Zrevrangebylex => session.sorted_set_range(
      args,
      batch,
      output,
      SortedSetRangeOpts::BY_LEX.union(SortedSetRangeOpts::REVERSE),
    ),
    C::Zrangebyscore => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::BY_SCORE),
    C::Zrevrangebyscore => session.sorted_set_range(
      args,
      batch,
      output,
      SortedSetRangeOpts::BY_SCORE.union(SortedSetRangeOpts::REVERSE),
    ),
    C::Zrangestore => session.sorted_set_range_store(args, batch, output),
    C::Zmscore => session.sorted_set_scores(args, batch, output),
    C::Zmpop => session.sorted_set_m_pop(args, batch, output),
    C::Zcount => session.sorted_set_count(args, batch, output),
    C::Zlexcount => session.sorted_set_length_by_value(args, batch, output),
    C::Zincrby => session.sorted_set_increment(args, batch, output),
    C::Zrank => session.sorted_set_rank(args, batch, output, true),
    C::Zrevrank => session.sorted_set_rank(args, batch, output, false),
    C::Zremrangebyrank => {
      session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Rank)
    }
    C::Zremrangebyscore => {
      session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Score)
    }
    C::Zremrangebylex => session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Lex),
    C::Zrandmember => session.sorted_set_random_member(args, batch, output),
    C::Zdiff => session.sorted_set_difference(args, batch, output),
    C::Zdiffstore => session.sorted_set_difference_store(args, batch, output),
    C::Zinter => session.sorted_set_intersect(args, batch, output),
    C::Zintercard => session.sorted_set_intersect_length(args, batch, output),
    C::Zinterstore => session.sorted_set_intersect_store(args, batch, output),
    C::Zunion => session.sorted_set_union(args, batch, output),
    C::Zunionstore => session.sorted_set_union_store(args, batch, output),
    C::Bzpopmin => session.sorted_set_blocking_pop(args, batch, output, true),
    C::Bzpopmax => session.sorted_set_blocking_pop(args, batch, output, false),
    C::Bzmpop => session.sorted_set_blocking_m_pop(args, batch, output),
    C::Zexpire => session.sorted_set_expire(cmd.into(), args, batch, output, false, false),
    C::Zpexpire => session.sorted_set_expire(cmd.into(), args, batch, output, true, false),
    C::Zexpireat => session.sorted_set_expire(cmd.into(), args, batch, output, false, true),
    C::Zpexpireat => session.sorted_set_expire(cmd.into(), args, batch, output, true, true),
    C::Zttl => session.sorted_set_time_to_live(cmd.into(), args, batch, output, false, false),
    C::Zpttl => session.sorted_set_time_to_live(cmd.into(), args, batch, output, true, false),
    C::Zexpiretime => session.sorted_set_time_to_live(cmd.into(), args, batch, output, false, true),
    C::Zpexpiretime => session.sorted_set_time_to_live(cmd.into(), args, batch, output, true, true),
    C::Zpersist => session.sorted_set_persist(args, batch, output),
    C::Zscan => session.network_zscan(args, batch, output),

    // ---- 列表族（Objects/ListCommands.cs）
    C::Lpush => session.list_push(args, batch, output, true),
    C::Rpush => session.list_push(args, batch, output, false),
    C::Lpushx => session.list_push_x(args, batch, output, true),
    C::Rpushx => session.list_push_x(args, batch, output, false),
    C::Lpop => session.list_pop(args, batch, output, true),
    C::Rpop => session.list_pop(args, batch, output, false),
    C::Lpos => session.list_position(args, batch, output),
    C::Lmpop => session.list_pop_multiple(args, batch, output),
    C::Blpop => session.list_blocking_pop(args, batch, output, true),
    C::Brpop => session.list_blocking_pop(args, batch, output, false),
    C::Blmove => session.list_blocking_move(args, batch, output),
    C::Brpoplpush => session.list_blocking_pop_push(args, batch, output),
    C::Llen => session.list_length(args, batch, output),
    C::Ltrim => session.list_trim(args, batch, output),
    C::Lrange => session.list_range(args, batch, output),
    C::Lindex => session.list_index(args, batch, output),
    C::Linsert => session.list_insert(args, batch, output),
    C::Lrem => session.list_remove(args, batch, output),
    C::Lmove => session.list_move(args, batch, output),
    C::Rpoplpush => session.list_right_pop_left_push(args, batch, output),
    C::Lset => session.list_set(args, batch, output),
    C::Blmpop => session.list_blocking_pop_multiple(args, batch, output),

    // ---- 位图族（Bitmap/BitmapCommands.cs）
    C::Setbit => session.network_string_set_bit(args, batch, output),
    C::Getbit => session.network_string_get_bit(args, batch, output),
    C::Bitcount => session.network_string_bit_count(args, batch, output),
    C::Bitpos => session.network_string_bit_position(args, batch, output),
    // BITOP 的向量键裁决在位点内分流（源键命中整体 WRONGTYPE 零写、目的键
    // 命中在有源命中时预清退再写，见 bitmap_commands 与 C# BitmapOps
    // StringBitOperation dest DELETE+SET 重试臂），派发层不整命令扫
    C::BitopAnd => {
      session.network_string_bit_operation(BitmapOperation::And, args, batch, vector, output)
    }
    C::BitopOr => {
      session.network_string_bit_operation(BitmapOperation::Or, args, batch, vector, output)
    }
    C::BitopXor => {
      session.network_string_bit_operation(BitmapOperation::Xor, args, batch, vector, output)
    }
    C::BitopNot => {
      session.network_string_bit_operation(BitmapOperation::Not, args, batch, vector, output)
    }
    C::BitopDiff => {
      session.network_string_bit_operation(BitmapOperation::Diff, args, batch, vector, output)
    }
    C::Bitfield => session.string_bit_field(args, batch, output),
    C::BitfieldRo => session.string_bit_field_read_only(args, batch, output),

    // ---- HyperLogLog 族（Objects/HyperLogLogCommands.cs）
    C::Pfadd => session.hyper_log_log_add(args, batch, output),
    C::Pfcount => session.hyper_log_log_length(args, batch, output),
    C::Pfmerge => session.hyper_log_log_merge(args, batch, output),

    // ---- 地理族（Objects/SortedSetGeoCommands.cs）
    C::Geoadd => session.geo_add(args, batch, output),
    C::Geodist => session.geo_commands(args, batch, output, SortedSetOperation::Geodist),
    C::Geohash => session.geo_commands(args, batch, output, SortedSetOperation::Geohash),
    C::Geopos => session.geo_commands(args, batch, output, SortedSetOperation::Geopos),
    C::Georadius => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadius)
    }
    C::GeoradiusRo => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadiusRo)
    }
    C::Georadiusbymember => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadiusByMember)
    }
    C::GeoradiusbymemberRo => session.geo_search_commands(
      args,
      batch,
      output,
      GeoSearchCommandKind::GeoRadiusByMemberRo,
    ),
    C::Geosearch => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoSearch)
    }
    C::Geosearchstore => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoSearchStore)
    }

    // fast 段未命中 → 慢段（C# ProcessOtherCommands / ProcessAdminCommands 链式回退）
    _ => dispatch_slow(session, cmd, args, batch, vector, output),
  }
}

/// 分派表 · slow 段（C# ProcessOtherCommands + ProcessAdminCommands 的存储命令
/// switch：TYPE/LCS/DBSIZE/KEYS/SCAN/清库族/CONFIG/对象收集/ETAG/OBJECT/
/// MEMORY/RI 族）。入口置位 containsSlowCommand（NET_RS 直方图分桶，C# 同款
/// 首行语义——本段皆 @slow，单点定义杜绝逐臂重复）
pub(crate) fn dispatch_slow<D: Device>(
  session: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  batch: &BatchStoreSession<'_, D>,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  use RespCommand as C;

  session.contains_slow_command = true;
  match cmd {
    // ---- 慢命令族（ProcessOtherCommands：TYPE / LCS / DBSIZE / KEYS / SCAN）
    C::Type => session.network_type(args, batch, vector, output),
    C::Lcs => session.network_lcs(args, batch, output),
    C::Dbsize => session.network_dbsize(args, output),
    C::Keys => session.network_keys(args, output),
    C::Scan => session.network_scan(args, output),
    C::Coscan => session.network_coscan(args, batch, output),

    // ---- INFO 族（ProcessOtherCommands：纯慢段请求（KEYSPACE/HLOGSCAN）
    // 经会话 process_other_commands 放行至此，存储域扫描降级异步闭环）
    C::Info => session.try_info_keyspace_slow_path(args, output),

    // ---- 清库族（ProcessOtherCommands：选项校验同步承接，清库降级异步闭环）
    C::Flushdb => session.network_flushdb(args, output),
    C::Flushall => session.network_flushall(args, output),

    // ---- 会话族（无存储写，仅经统一注入面保持单一路径）
    C::Hello => session.network_hello(args, batch, output),

    // ---- 配置族（ServerConfig.cs:NetworkCONFIG_GET/SET/REWRITE，经
    // 会话共享 runtime_config 实例：CONFIG SET 即时全服务器生效）
    C::ConfigGet => session.network_config_get(args, output),
    C::ConfigSet => session.network_config_set(args, Some(&batch.session.store), output),
    C::ConfigRewrite => session.network_config_rewrite(args, output),

    // ---- 对象收集族（AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT：
    // 显式键清单逐键 RMW；`*` 全库扫描降级异步闭环）
    C::Hcollect => session.network_hcollect(args, batch, output),
    C::Zcollect => session.network_zcollect(args, batch, output),

    // ---- COMMAND 族（ProcessOtherCommands：命令元数据自省，无存储面）
    C::CommandCount => session.network_command_count(args, batch, output),
    C::CommandDocs => session.network_command_docs(args, batch, output),
    C::CommandInfo => session.network_command_info(args, batch, output),
    C::CommandGetkeys => session.network_command_getkeys(args, batch, output),
    C::CommandGetkeysandflags => session.network_command_getkeysandflags(args, batch, output),

    // ---- MEMORY / OBJECT 族（ProcessOtherCommands：BasicCommands.cs）
    C::MemoryUsage => session.network_memory_usage(args, batch, vector, output),
    C::ObjectHelp => session.network_objecthelp(args, batch, output),
    C::ObjectEncoding => {
      session.network_object(ObjectSubCmd::Encoding, args, batch, vector, output)
    }
    C::ObjectFreq => session.network_object(ObjectSubCmd::Freq, args, batch, vector, output),
    C::ObjectIdletime => {
      session.network_object(ObjectSubCmd::Idletime, args, batch, vector, output)
    }
    C::ObjectRefcount => {
      session.network_object(ObjectSubCmd::Refcount, args, batch, vector, output)
    }

    // ---- Etag 族（ProcessOtherCommands：BasicEtagCommands.cs）
    C::Getwithetag => session.network_getwithetag(args, batch, output),
    C::Getifnotmatch => session.network_getifnotmatch(args, batch, output),
    C::Setifmatch => session.network_setifmatch(args, batch, output),
    C::Setifgreater => session.network_setifgreater(args, batch, output),
    C::Setwithetag => session.network_setwithetag(args, batch, output),
    C::Delifgreater => session.network_delifgreater(args, batch, output),

    // ---- 自定义对象命令族（CustomRespCommands.cs:NetworkCustomObjCmd →
    // TryCustomObjectCommand：会话侧经 current_custom_command 引用解析
    // 注册表，四接口执行体分派见 objects/custom_object_commands.rs）
    C::Customobjcmd => session.network_custom_obj_cmd(args, batch, output),

    // ---- RangeIndex 族（ProcessOtherCommands：RespServerSessionRangeIndex.cs；
    // wkv 范围索引走 compio 异步存储路径，同步段直接降级 SlowWait 异步闭环）
    C::Ricreate
    | C::Riset
    | C::Riget
    | C::Ridel
    | C::Riscan
    | C::Rirange
    | C::Riexists
    | C::Riconfig
    | C::Ricount
    | C::Rimetrics => Ok(false),

    // 未接入分派表的命令：按 C# ProcessAdminCommands 尾部兜底明确报错，
    // 绝不静默吞命令
    _ => {
      write_error_raw(output, RESP_ERR_GENERIC_UNK_CMD);
      Ok(true)
    }
  }
}

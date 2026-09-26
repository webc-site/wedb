//! EXISTS / DUMP / RESTORE 类型判定与序列化载荷管理命令（对标 libs/server/Resp/KeyAdminCommands.cs）

use wbase::{
  convert::expire_after_to_ticks, crc64::hash as rdb_crc64_hash, num::strict_i32, time::now_ticks,
};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, unpack_args},
  cmd_strings as cs,
  cmd_strings::{RESP_ERR_GENERIC, abort_with_error_message, write_error_raw, write_raw},
  ext::RespVecExt,
  length::{try_read_length, try_write_length},
  resp_memory_writer::write_prefixed_len_to,
};

use super::super::{resp_server_session::RespServerSession, vector::vector_manager::VectorManager};
use crate::{
  resp::TtlResume,
  storage::session::common::{
    UserRead, read_user_sync,
    ttl_sync::{probe_alive_with_registry, put_ttl_sync},
  },
};

/// DUMP 载荷版本/校验和非法文案（本域多处复用）。
const ERR_DUMP_VERSION_CHECKSUM: &str = "ERR DUMP payload version or checksum are wrong";
/// DUMP 载荷长度格式非法文案（本域两处复用）。
const ERR_DUMP_LENGTH_INVALID: &str = "ERR DUMP payload length format is invalid";
/// DUMP 载荷长度非法应答文案（快慢臂共用，出口见 [`write_dump_payload`]）。
pub(crate) const ERR_DUMP_PAYLOAD_INVALID: &str = "ERR DUMP payload length is invalid";

/// RDB 格式版本（libs/server/Resp/KeyAdminCommands.cs:RDB_VERSION）
pub(crate) const RDB_VERSION: u16 = 11;

/// DUMP 载荷帧组装单源（快慢臂共用，与 [`parse_restore_args`] 的校验互逆，
/// 保证 DUMP→RESTORE 往返成立）：bulk 头 + 类型字节 0x00 + 长度前缀 + 值 +
/// rdb 版本（小端）+ crc64 + CRLF 逐字节写进 output；长度前缀编码失败返回
/// false 且 output 零写入（调用方据此出错误帧）
pub(crate) fn write_dump_payload(value: &[u8], output: &mut Vec<u8>) -> bool {
  let mut encoded_len = [0u8; 5];
  let Some(bytes_written) = try_write_length(value.len() as u32, &mut encoded_len) else {
    return false;
  };
  let encoded_len = &encoded_len[..bytes_written];

  // DUMP 长 = 类型 1 + 长度前缀 + 值 + rdb 版本 2 + crc64 8
  let payload_len = 1 + encoded_len.len() + value.len() + 2 + 8;
  write_prefixed_len_to(output, b'$', payload_len);
  // 类型字节 + 长度前缀 + 值 + rdb 版本（小端）
  output.push(0x00);
  output.extend_from_slice(encoded_len);
  output.extend_from_slice(value);
  output.extend_from_slice(&RDB_VERSION.to_le_bytes());
  // crc64 覆盖类型字节起至版本字节止。刻意偏离 C#：C# DUMP 的 crc 从
  // 类型字节之后起算（KeyAdminCommands.cs:200 的 Slice 越过 0x00），
  // 而其 RESTORE 的 crc 校验含类型字节，C# 自身 DUMP→RESTORE 往返必被
  // "checksum wrong" 拒绝；rust 对齐 RESTORE 口径保证往返成立（doc/zh/deviations.md 第 21 条）
  let framed = output.len() - (payload_len - 8);
  let crc = rdb_crc64_hash(&output[framed..]);
  output.extend_from_slice(&crc);
  output.extend_from_slice(b"\r\n");
  true
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  ///
  /// 与 Redis 规范的已知差异（维持对标 Garnet，勿按 Redis 规范改）：
  /// - ttl 按秒解释：Redis 规范该参数是毫秒（缺省为相对空闲毫秒数，带 ABSTTL 时为绝对
  ///   Unix 毫秒时间戳），Garnet 与 C# 一样按秒换算，客户端若按 Redis 语义传 5000 表示
  ///   「5 秒」，这里会被解释成 5000 秒（TTL 放大 1000 倍）；
  /// - 只收三参 key、ttl、value：Redis 规范的 ABSTTL/IDLETIME/FREQ 修饰符与 REPLACE
  ///   均不支持，C# 同样在参数数不为 3 时直接回参数数错误，覆盖已存在键只能回 BUSYKEY、
  ///   无替换通路。
  ///
  /// 两点都是继承自 Garnet 的真实分叉而非 rust 回归，本实现 1:1 对标 Garnet，
  /// 禁自行改单位或扩参数面（transpile SKILL 的 1:1 对标原则）
  pub fn network_restore<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, expiry, val)) = parse_restore_args(parse_state, output) else {
      return Ok(true);
    };
    // 续跑标记进入即复位（沿 MSETNX resume 复位纪律，杜绝跨命令残留）
    self.ttl_resume = TtlResume::Full;

    // 单键读改写窗口（票 zcode-r15-generic 发现一，对标 C# NetworkRESTORE
    // 尾段单次 SET_Conditional(SETEXNX) 原子条件写：libs/server/Resp/
    // KeyAdminCommands.cs:25 起「存在判定与写入在同一 RMW 锁内」）：闩内完成
    // 「probe 存活探测 → upsert 写入 → TTL 落库」全序列，杜绝 probe 判不存在
    // 后并发 SET 落库再被覆写的 BUSYKEY 契约绕过与并发写丢失。失闩沿既有
    // Ok(false) 降级慢路径同段持窗重放，绝不自旋等闩
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // SET_Conditional(SETEXNX)：仅键不存在时写入（NX 语义）。存活判定走
    // 闩窗内折叠探针单源（三域 + 向量登记表第四态，与 EXISTS/RENAMENX/写面
    // NX 族同一 probe_alive_with_registry；对标 C# Reader 主存单记录，
    // 存活向量记录恒判在 → BUSYKEY。票 zcode-r161c-msetnx 案一：派发层
    // 窗外 BUSYKEY 位删除，存在性唯窗内裁决）
    let prefix = store.session_prefix();
    if probe_alive_or_bail!(store, prefix.as_slice(), key, vector, output) {
      write_error_raw(output, cs::RESP_ERR_BUSSYKEY);
      return Ok(true);
    }

    match store.try_insert_sync(key, val) {
      Ok(Ok(true)) => {}
      // 探针既判逻辑不存在而纯物理 NX 仍撞见 Active 记录，只剩「已过期未清退
      // 残留」一态（持键闩内排除并发竞态），直写 BUSYKEY 即假失败——C# 契约
      // 对过期记录 SETEXNX 臂 CheckExpiry 判死转 ExpireAndResume 复设新值恒
      // +OK（RMWMethods.cs:974-980），存在判定与写入是同一次条件写、单源裁决。
      // 本臂不再自立第二判据，交慢臂同窗重探完整裁决：真存活并发键照常
      // BUSYKEY、过期残留经 upsert 清退重建出 +OK（key_admin_latch_concurrency
      // 在档 BUSYKEY 锁测与本出口无涉，形零漂移）
      Ok(Ok(false)) | Ok(Err(_)) => return Ok(false),
      Err(_) => bail_err_frame!(output),
    }
    if expiry > 0 {
      // C#：DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expiry).Ticks；
      // 换算单点与 EXPIRE 同源（expire_after_to_ticks）。口径为秒，非 Redis 的毫秒，
      // 详见本函数头部与 Redis 规范的差异说明
      let expire_at_ticks = expire_after_to_ticks(now_ticks(), expiry);
      match put_ttl_sync(store, key, expire_at_ticks) {
        Ok(true) => {}
        // 值已同步提交、TTL 遭环形页翻转降级：置「值已提交 + TTL 待投」标记
        // 随 exec 降级快照尾参续跑（票 wnode-nx-conditional-ttl-degrade-replay-selfhit），
        // 慢臂仅补投 TTL 出 +OK——整命令重放自碰已提交值即误回 BUSYKEY、TTL 永缺
        Ok(false) => {
          self.ttl_resume = TtlResume::Pending(expire_at_ticks);
          return Ok(false);
        }
        Err(_) => {
          // 值已于 try_insert_sync 提交而 TTL 落库硬故障（设备级 Err，非环形
          // 页翻转降级）：同闩窗内补偿删本次残值，杜绝「键无 TTL 永存 +
          // 应答错误帧 + 重试恒 BUSYKEY」分裂态（C# SET_Conditional 单记录
          // 条件写值与过期一体落库，IOError 下记录未提交键不存续，无此态）；
          // 回滚单源见 restore_residual_rollback
          restore_residual_rollback(store, key);
          bail_err_frame!(output);
        }
      }
    }
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkDUMP
  pub fn network_dump<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "DUMP") else {
      return Ok(true);
    };

    // DUMP 是终端用户读而非 RMW 前置读：传接会话指标句柄（与 get.rs 同形），
    // 同步漏斗 record_outcome 尾恰一条（Hit=found/Missing=notfound/WrongType
    // 静默，对标 C# MainStoreOps.cs:15-42 GET 单条口径）
    let dump_res = read_user_sync(store, key, self.session_metrics.as_deref(), |value| {
      write_dump_payload(value, output)
    });

    match dump_res {
      Ok(UserRead::Hit(true)) => {}
      Ok(UserRead::Hit(false)) => {
        write_error_raw(output, ERR_DUMP_PAYLOAD_INVALID);
      }
      // 对象键（信封域命中）与键缺失一致：C# WRONGTYPE → nil 同口径
      Ok(UserRead::WrongType | UserRead::Missing) => {
        output.write_resp_null_ver(self.resp_protocol_version)
      }
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXISTS
  ///
  /// 多键计数；任一键须异步裁决（磁盘候选/TTL 待裁决）时整体降级，
  /// 存储错误直接回错，避免计数口径失真
  pub fn network_exists<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "EXISTS");

    let mut exists_count = 0i64;
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    for key in parse_state {
      // 三域探针 + 向量登记表第四态（存活观测单点，对标 C# Reader 无类型门）
      if probe_alive_or_bail!(store, prefix_slice, key, vector, output) {
        exists_count += 1;
      }
    }

    output.write_resp_int(exists_count);
    Ok(true)
  }
}

/// RESTORE TTL 硬故障残值回滚单源（快慢臂共用一小函数，勿两处手抄，对齐
/// [`parse_restore_args`] 单源先例。rust 快慢两步拆分的自有收口，C# 无对位
/// 形——SET_Conditional 单记录条件写值与过期一体落库，无「值已提交、TTL
/// 缺席」分裂态：libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE +
/// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET_Conditional）
///
/// 触发面：值已提交（快臂 `try_insert_sync` / 慢臂 `upsert_string` /
/// Pending 续跑臂的快臂已提交残值）而 put_ttl 硬故障（设备级 Err）。调用方
/// 须持本键读改写窗口内直调——闩内值恒为本次所写，无内容复验面；补偿删走
/// `try_delete_sync_with_prefix`（MSETNX 快臂回滚同款内核）：级联清退随键
/// TTL/ETag 旁路残留（TTL 半提交记录一并摘除），墓碑镜像恰一次入 AOF，
/// 重放/副本终态与主端 absent 收敛（MSETNX 回滚记账口径）。
///
/// 删失败（设备瞬态 / 环形页翻转降级哨兵）残值留痕 log::error，不 panic
/// 不吞错静默（MSETNX 回滚先例同款残余形），应答侧仍出错误帧
pub(crate) fn restore_residual_rollback<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
) {
  let prefix = store.session_prefix();
  match store.try_delete_sync_with_prefix(prefix.as_slice(), key) {
    // 补偿闭环（含键恰被并发清退的缺席观测，幂等）
    Ok(Ok(_)) => {}
    // 环形页翻转/冷数据降级哨兵：同步段零写入，残值留痕
    Ok(Err(degrade)) => {
      log::error!("RESTORE TTL 硬故障补偿删降级残留（键未清退）: {degrade}");
    }
    Err(e) => {
      log::error!("RESTORE TTL 硬故障补偿删失败（键未清退）: {e:?}");
    }
  }
}

/// NetworkRESTORE 的参数与载荷推导单源（快慢路径共用；解析失败时已写出错误
/// 应答并返回 None，返回 `(key, expiry 秒, 载荷内值切片)`）
///
/// 校验序列对标 C# KeyAdminCommands.cs:NetworkRESTORE：ttl 整数（沿用
/// RESP_ERR_TIMEOUT_NOT_VALID_FLOAT 历史文案）→ 类型字节 0x00 → footer
/// （2 字节 rdb 版本 + 8 字节 crc64）→ 长度前缀。C# 对空载荷直接 valueSpan[0]
/// 越界（进程崩溃断连）、Slice 越界抛异常断连；rust 无 panic 约束下按同族
/// 错误降级应答，不复刻崩溃。登记见 doc/zh/deviations.md §114（宗 a）
pub(crate) fn parse_restore_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64, &'p [u8])> {
  let [key, expiry_raw, value] = unpack_args(parse_state, output, "RESTORE")?;
  // C# TryGetInt（index = Count - 2，arity 锁 3 即下标 1；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32）
  let Some(expiry) = strict_i32(expiry_raw) else {
    abort_with_error_message(output, cs::RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
    return None;
  };
  // RESTORE 仅实现字符串类型（类型字节 0x00）
  if value.first() != Some(&0x00) {
    write_error_raw(output, "ERR RESTORE currently only supports string types");
    return None;
  }
  let mut dump_err = || {
    write_error_raw(output, ERR_DUMP_VERSION_CHECKSUM);
    None
  };
  if value.len() < 10 {
    return dump_err();
  }
  // footer = 2 字节 rdb 版本 + 8 字节 crc64
  let footer = &value[value.len() - 10..];
  let rdb_version = u16::from_le_bytes([footer[0], footer[1]]);
  if rdb_version > RDB_VERSION {
    return dump_err();
  }
  // crc 覆盖除末 8 字节外的全部载荷
  let calculated_crc = rdb_crc64_hash(&value[..value.len() - 8]);
  if calculated_crc != footer[2..] {
    return dump_err();
  }
  // 恰 10 字节形（footer 即全载荷）下 1..(len-10) 退化为 1..0 起点越终点，
  // 直取切片触发 checked panic；get 容错解构，None 落同族版本/校验和错误帧并
  // 返回 None（应答存活、会话不断）。len≥11 时 1..(len-10) 恒合法（len=11 为
  // 空切片 1..1），逐字节零漂移。C# 该形经 TryReadLength 放行空载荷落空值键
  // (+OK) 系残余分叉，已登记 doc/zh/deviations.md §114（宗 a），严禁复刻
  let Some(payload_body) = value.get(1..value.len() - 10) else {
    return dump_err();
  };
  let Some((length, payload_start)) = try_read_length(payload_body) else {
    write_error_raw(output, ERR_DUMP_LENGTH_INVALID);
    return None;
  };
  let Some(val) = payload_body
    .get(payload_start..)
    .and_then(|data| data.get(..length as usize))
  else {
    return dump_err();
  };
  Some((key, i64::from(expiry), val))
}

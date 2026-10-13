//! 字符串族慢路径分派入口（SET 全形态 / SETEX / SETNX / GETSET / SETRANGE /
//! APPEND / INCR 族 / GETEX / GETRANGE / STRLEN → OBJECT 转发）

use itoa::Buffer as ItoaBuffer;
use wbase::num::{strict_f64, strict_i64};
use wdev::Device;
use wresp::{
  check_args::parse_i32_arg,
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  command::RespCommand,
  ext::RespVecExt,
  resp_memory_writer::format_double,
};
use zmij::Buffer as ZmijBuffer;

use super::{
  common::{
    apply_set_with_expiry_async, arity, blind_write_gate, read_and_frame, read_cold,
    read_cold_quiet, read_user_num, rmw_write_len,
  },
  object_slow::object_slow,
  set_conditional::slow_set_conditional,
};
use crate::{
  resp::{
    TtlResume,
    basic_commands::{
      ObjectSubCmd,
      get::parse_getex_args,
      incr::{IncrCmd, parse_incr_args, parse_incr_by_float_args},
      set::{
        SetCmd, SetOptions, parse_set_options, parse_setex_args, parse_setrange_args,
        string_record_fits_page,
      },
      ttl::{GetexExpiry, try_get_absolute_expiry_ticks},
    },
    resp_server_session::RespServerSession,
    vector::vector_manager::VectorManager,
  },
  storage::session::{
    common::ttl_sync::probe_alive_with_registry_async_quiet, storage_session::StorageSession,
  },
};

/// 字符串族 / OBJECT 慢路径执行段入口（exec_slow 分派；`Err(())` 为存储
/// 错误，调用方统一应答）
pub(crate) async fn string_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;
  let resp_version = storage.resp_version;
  match cmd {
    C::Set | C::Setexnx => {
      // SET 全形态（裸 SET 即无选项默认）与 SETEXNX 同走选项单源；
      // 无 NX/XX 的 EX/PX/无过期走盲写共同体（对标快路径 network_set_ex）
      // 尾参为快路径「值已提交 + TTL 待投」续跑标记（[`TtlResume::from_tail`]
      // 逆解析，沿 MSETNX/DEL 尾参先例，exec 降级快照恒追加）：Pending = 值
      // 已同步提交、TTL 遭环形页翻转降级，剥尾参后跳过整命令重放自碰已提交
      // 值（SET NX 误回 nil / KEEPTTL 回填读已清 TTL 静默丢，票
      // wnode-nx-conditional-ttl-degrade-replay-selfhit），持窗仅补投 TTL
      // 回 +OK；ReplyEcho = GET 回旧值形值已提交且应答已由快臂成帧保留
      //（票 zcode-r153c-setrangeget 案一），持窗补投 TTL 后出零字节——会话
      // 累积应答由泵先冲出，禁全量重放；Full / KeepTtl = 提交前降级，
      // 照旧整命令全量重放
      let Some((tail, cmd_args)) = parse_state.split_last() else {
        cs::abort_with_wrong_number_of_arguments(output, "SET");
        return Ok(());
      };
      let resume = TtlResume::from_tail(Some(tail));
      let Some(opts) = parse_set_options(cmd_args, output) else {
        return Ok(());
      };
      if let TtlResume::Pending(leg) | TtlResume::ReplyEcho(leg) = resume {
        // 快臂已持同一窗口契约提交值，Pending 降级零应答 / ReplyEcho 应答
        // 已保留；本臂复取同窗补投裸 ticks（对标 C# 单记录 RMW 锁内值与
        // 过期一体落库的终态）：Pending 出 +OK，ReplyEcho 不出任何帧。
        // 刻度不加二次过期滤（刻意）：线性化点是快臂提交时刻——KEEPTTL
        // 起源刻度捕获时已滤活、EX 起源本为命令绝对到期，补投跨期死刻度
        // 落库与 C# 原子口径终态一致（值于 T 到期→nil）；leg 不携来源旗标，
        // 盲加过滤反使 EX 起源腿漂移（持久化 ≠ 落库即死）。
        // 跨换号域比对失配即跳补投只按原契约出帧——值已随死域退役，禁把
        // 旧域刻度落进新域成孤 TTL 旁路（幽灵 TTL 收口，域钉族推广）
        let _window = storage.batch.rmw_window(opts.key).await.map_err(|_| ())?;
        if leg.domain == storage.batch.virtual_domain() {
          storage
            .batch
            .put_ttl(opts.key, leg.ticks)
            .await
            .map_err(|_| ())?;
        }
        if matches!(resume, TtlResume::Pending(_)) {
          cs::write_raw(output, cs::RESP_OK);
        }
        return Ok(());
      }
      // 前置门仅取换算成败(与快臂 network_setexnx 同款双门,真消费在慢臂 apply 内重算)
      if opts.expiry != 0
        && try_get_absolute_expiry_ticks(opts.expiry, opts.exp_high_precision).is_none()
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return Ok(());
      }
      if opts.cmd == SetCmd::Set && !opts.get_value {
        // 盲写共同体：取窗 → RI 门 → 登记窗内清退（窗口契约单源见
        // [`blind_write_gate`]，清退次序对齐 MSET 窗内标准）
        let Some(_window) = blind_write_gate(storage, vector, opts.key, output).await? else {
          return Ok(());
        };
        apply_set_with_expiry_async(
          storage,
          opts.key,
          opts.val,
          (opts.expiry, opts.exp_high_precision),
          None,
        )
        .await?;
        cs::write_raw(output, cs::RESP_OK);
        return Ok(());
      }
      // 选项形态条件写：第四态裁决（NX 判在出 nil 保留登记 / GET 形态
      // -WRONGTYPE / XX 与 KEEPTTL 覆写形窗内清退）已收拢 slow_set_conditional
      // 持窗临界区内一次折叠终裁（票 zcode-r163c-setguard 案二），本臂不再
      // 于窗外 reg_hit 预清退
      slow_set_conditional(storage, &opts, vector, resume, resp_version, output).await
    }
    C::Setex | C::Psetex => {
      let cmd_name = if cmd == C::Setex { "SETEX" } else { "PSETEX" };
      let Some((key, expiry, val)) = parse_setex_args(cmd_name, parse_state, output) else {
        return Ok(());
      };
      let high_precision = cmd == C::Psetex;
      let Some(expire_at_ticks) = try_get_absolute_expiry_ticks(expiry, high_precision) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return Ok(());
      };
      // 盲写共同体（取窗 → RI 门 → 登记窗内清退 +「清 TTL + 值写 + 新 TTL
      // 写」整段同窗收口，契约单源见 [`blind_write_gate`]，对标 C#
      // NetworkSETEX 单记录一次 CAS 落库）
      let Some(_window) = blind_write_gate(storage, vector, key, output).await? else {
        return Ok(());
      };
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      storage
        .batch
        .put_ttl(key, expire_at_ticks)
        .await
        .map_err(|_| ())?;
      cs::write_raw(output, cs::RESP_OK);
      Ok(())
    }
    C::Setnx => {
      let Some([key, val]) = arity(parse_state, "SETNX", output) else {
        return Ok(());
      };
      // 条件写整段同窗（快路径 network_setnx 同一窗口契约，票
      // zcode-r32-rmwmatrix 立项三：探测与写入一体，杜绝并发 SET 落两步间隙
      // 丢已确认写）
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 存在性探测（闩窗内折叠探针单源：三域异步裁决 + 向量登记表第四态，
      // 对象键同计存在，C# NX 语义；票 zcode-r161c-msetnx 案一：原窗外
      // 手抄 is_some_and(read_stored_index) 位删除，判据收拢至本窗内一次
      // 折叠，与快臂 probe_alive_with_registry 同一折叠式，ttl_sync.rs
      // 「同一判据源勿再手抄」纪律归一）
      //
      // 恰一帧终态补账（票 wnode-string-bitmap-found-notfound-accounting-
      // matrix）：探针改静默对偶口（簿记档逐域入账使缺失键计 3，与 C#
      // SETNX→SET_Conditional 单帧口径失联，MainStoreOps.cs:279/:284），
      // 存活 found / 缺席写成功 notfound 经 record_read_outcome 单点折叠；
      // 存储错误 Err(()) 臂零入账（对位 C# 异常臂无 incr，沿 getex 先例）
      let prefix = storage.batch.session_prefix();
      if probe_alive_with_registry_async_quiet(storage, prefix.as_slice(), key, vector)
        .await
        .map_err(|_| ())?
      {
        storage.record_read_outcome(true);
        output.write_resp_int(0);
        return Ok(());
      }
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      storage.record_read_outcome(false);
      output.write_resp_int(1);
      Ok(())
    }
    C::Getset => {
      // C# 走 NetworkSET_Conditional(SET, getValue: true)：无条件写入并回旧值；
      // 登记命中即错型不可读旧值，回 -WRONGTYPE 保留登记（exec 层值域门同判，
      // 窗内折叠终裁收拢 slow_set_conditional，票 zcode-r163c-setguard 案二）
      let Some([key, val]) = arity(parse_state, "GETSET", output) else {
        return Ok(());
      };
      let opts = SetOptions {
        key,
        val,
        expiry: 0,
        exp_high_precision: false,
        cmd: SetCmd::Set,
        get_value: true,
      };
      slow_set_conditional(
        storage,
        &opts,
        vector,
        TtlResume::Full,
        resp_version,
        output,
      )
      .await
    }
    C::Setrange => {
      let Some((key, offset, val)) = parse_setrange_args(parse_state, output) else {
        return Ok(());
      };
      // 与快臂 network_set_range 同一前置判据源（[`string_record_fits_page`]）：
      // 超窗终值在取窗与整值冷读回之前即按快臂 generic 同帧形收口，杜绝慢臂
      // RecordTooLarge 经 ? 上抛的次级落点与快臂帧形分叉
      if !string_record_fits_page(&storage.batch, key, offset + val.len()) {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      // 冷读旧值前先取本键读改写原子窗口（持本键桶排他闩贯穿读—算—写回全程，
      // 对标 C# InternalRMW 的 ephemeral 独占闩）；前置读走 [`read_cold_quiet`]
      // 零入账口（RMW 前置读不入账纪律，同 C# MainStoreOps SETRANGE RMW 口）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 窗内向量第四态复验（票 zcode-r163c-setguard 家族推广，RMW 写族收口）：
      // 派发层 vector_registry_gate 窗外放行与取窗之间的并发 VADD TOCTOU
      // 天窗——命中即与门同帧 -WRONGTYPE 拒写，绝不落 String 域成幽灵双域键
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      let Some(old) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      // 补零增长 + 拷贝覆写（缺失键空旧值同形折叠，对位 C# SetAndCopyTo 追加补零）
      let mut new_val = old.unwrap_or_default();
      let required_len = offset + val.len();
      if new_val.len() < required_len {
        new_val.resize(required_len, 0);
      }
      new_val[offset..offset + val.len()].copy_from_slice(val);
      rmw_write_len(storage, &window, &new_val, output).await?;
      Ok(())
    }
    C::Append => {
      let Some([key, val]) = arity(parse_state, "APPEND", output) else {
        return Ok(());
      };
      // 同 SETRANGE：追加读改写全程持本键桶排他闩，前置读同走零入账口
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 窗内向量第四态复验（同 SETRANGE 慢臂，RMW 写族收口）
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      let Some(old) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      let mut new_val = old.unwrap_or_default();
      new_val.extend_from_slice(val);
      // 与快臂 network_append 回落臂同一前置判据源（[`string_record_fits_page`]）：
      // 超窗终值在写回前按快臂 generic 同帧形收口，免走注定失败的引擎写回
      //（Hit 臂整值冷回读本身系 C# CopyUpdater 同构成本，不收进本门射程；
      // 磁盘候选记录经本臂整值读—重建即 C# CopyUpdater 形态，空载荷与之同构）
      if !string_record_fits_page(&storage.batch, key, new_val.len()) {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      rmw_write_len(storage, &window, &new_val, output).await?;
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
      // 读—算—写回全程持本键桶排他闩；旧值口径对位 C# IsValidNumber →
      // NumUtils.TryReadInt64：拒前导零（含 '+' 前缀形态，C# IsValidNumber
      // 存在 '+' 绕过缺陷放行 "+007"，见 deviations 条目 32）；前置读走
      // read_user_quiet 零入账口（RMW 前置读不入账纪律，对位快臂 incr.rs
      // read_user_sync 传 None 与 C# MainStoreOps.cs:Increment 全链零计数）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 窗内向量第四态复验（同 SETRANGE 慢臂，RMW 写族收口：INCR/DECR 族
      // 同为 RMW 语义写，窗外门放行至取窗间的并发 VADD 天窗同帧 -WRONGTYPE）
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      let Some(val) = read_user_num(
        storage,
        key,
        strict_i64,
        0,
        cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
        output,
      )
      .await?
      else {
        return Ok(());
      };
      // C# checked 加法溢出与"非整数旧值"共用 not-integer 错误且不落写
      let Some(next) = val.checked_add(delta) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(());
      };
      let mut buf = ItoaBuffer::new();
      storage
        .rmw_string(&window, buf.format(next).as_bytes())
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
      // 前置读同走 read_user_quiet 零入账口（同 INCR 族臂纪律）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 窗内向量第四态复验（同 INCR 族臂，RMW 写族收口）
      if vector.is_some_and(|vm| {
        vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
          .is_some()
      }) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      let Some(val) = read_user_num(
        storage,
        key,
        |raw| strict_f64(raw, true),
        0.0,
        cs::RESP_ERR_NOT_VALID_FLOAT,
        output,
      )
      .await?
      else {
        return Ok(());
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
      let mut buf = ZmijBuffer::new();
      let formatted = format_double(next, &mut buf);
      storage
        .rmw_string(&window, formatted.as_bytes())
        .await
        .map_err(|_| ())?;
      output.write_resp_bulk_string(formatted.as_bytes());
      Ok(())
    }
    C::Getex => {
      let Some((key, expiry)) = parse_getex_args(parse_state, output) else {
        return Ok(());
      };
      // 读值 + TTL 应用整段同窗收口（对标快路径 network_getex 同一窗口契约；
      // C# 单次 RMW 锁内一体完成）。persist_key / expire_at_ticks 包装自取
      // 本键桶闩（wkv persist/expire_at 键闩与本窗口同址互斥），持窗内禁调
      // ——窗内改 batch 裸写 + 同款 WATCH 推进尾巴：At 恒写恒推进（对齐
      // expire_at_ticks_opt 的 applied > 0）；Persist 真实删除才推进（对齐
      // del_ttl_sync 纪律）。读面 Hit 已含 TTL 裁决，wkv persist/expire_at
      // 的到期 purge 面与过去时间戳面（parse 层 target > now）均不可达
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let Some(old) = read_cold(storage, key, output).await? else {
        return Ok(());
      };
      match old {
        Some(val) => {
          // 过期应用先于应答闭环（对标快路径同序）
          match expiry {
            GetexExpiry::None => {}
            // PERSIST：移除键级 TTL（裸写单点）
            GetexExpiry::Persist => {
              if storage.batch.has_ttl_tag(key).map_err(|_| ())? {
                storage.batch.del_ttl(key).await.map_err(|_| ())?;
                storage.batch.bump_watch_version(key);
              }
            }
            // 绝对过期刻度（裸写单点）
            GetexExpiry::At(ticks) => {
              storage.batch.put_ttl(key, ticks).await.map_err(|_| ())?;
              storage.batch.bump_watch_version(key);
            }
          }
          output.write_resp_bulk_string(&val);
        }
        // 键缺失（C# NOTFOUND / 过期键）回版本 nil
        None => output.write_resp_null_ver(resp_version),
      }
      Ok(())
    }
    C::Getrange | C::Substr => {
      let cmd_name = if cmd == C::Getrange {
        "GETRANGE"
      } else {
        "SUBSTR"
      };
      let Some([key, start_raw, end_raw]) = arity(parse_state, cmd_name, output) else {
        return Ok(());
      };
      // start/end 须可解析为整数（溢出走 not-integer 对齐 C# TryGetInt；前导零拒收与现 C# ParseUtils.TryReadInt allowLeadingZeros:false 全等，见 doc/zh/deviations.md §32），否则报 not-integer
      let Some(start) = parse_i32_arg(start_raw, output).map(i64::from) else {
        return Ok(());
      };
      let Some(end) = parse_i32_arg(end_raw, output).map(i64::from) else {
        return Ok(());
      };
      read_and_frame(
        storage,
        key,
        |val| {
          let len = val.len() as i64;
          let (start, end) = RespServerSession::normalize_range(start, end, len);
          // 对标 C# CopyRespTo 半边防御：normalize_range 的反转区间（start > end）
          // 与相等同样回空，杜绝安全切片 panic
          if start >= end {
            Vec::new()
          } else {
            val[(start as usize)..(end as usize)].to_vec()
          }
        },
        output,
        |out, slice| out.write_resp_bulk_string(&slice),
        // 键缺失回空串（C# NOTFOUND → 空批量串）
        |out| out.write_resp_bulk_string(b""),
      )
      .await?;
      Ok(())
    }
    C::Strlen => {
      let Some([key]) = arity(parse_state, "STRLEN", output) else {
        return Ok(());
      };
      read_and_frame(
        storage,
        key,
        |v| v.len(),
        output,
        |out, len| out.write_resp_int(len as i64),
        |out| out.write_resp_int(0),
      )
      .await?;
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

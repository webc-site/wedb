//! 自定义对象命令执行面（对标 libs/server/Custom/CustomRespCommands.cs）
//!
//! C# `TryCustomObjectCommand`：按命令类型分流 RMW_ObjectStore /
//! Read_ObjectStore，框架负责对象装载/回写，命令类四接口
//! （NeedInitialUpdate / Updater / Reader / NotFound）经
//! `customObjectCommand.functions` 触发；rust 侧对象以
//! `[类型标签][载荷]` 信封落库，装载/回写内聚本文件：
//! - RMW：NeedInitialUpdate 防空墓碑校验 → Updater 就地改写 →
//!   空对象整键回收（wedb 严格删空公理，对照 C# 工厂初建空对象常驻的
//!   刻意差异：空载荷不落库，杜绝幽灵空信封）
//! - Read：Reader 命中只读借用零拷贝直喂（杜绝整载荷堆分配）；NotFound 缺键应答（读不建键）
//! - MultiKeyRead：静态清单 [`wcustom::KeyScope::MultiRead`] 形态位驱动的
//!   逐键 Read 循环（RedisJSON JSON.MGET 兼容面，C# 无对位——
//!   `modules/GarnetJSON/JsonModule.cs` 只注册 JSON.SET / JSON.GET）；与
//!   单键读共用同一探测面与同一 `reader` / `not_found` 执行体，会话执行臂
//!   不再按命令名比串特判（转写规范：键形态知识入编译期静态清单）
//!
//! WRONGTYPE 语义：键存在但信封标签不符（含字符串键），与 C# 统一
//! 存储上自定义对象命令作用于字符串键的 WRONGTYPE 口径一致。
//!
//! 分流三条求值路径（同步快路与冷键降级异步重放同款分型）：
//! - Read 臂：经 try_read_tag_sync / read_tag_with 闭包内借用切片直喂 reader，消除 ObjLoad<Vec<u8>> 中转
//! - RMW 臂：装载 owned Vec 供 Updater 就地改写；与 rmw_helpers 骨架同一套
//!   写回面保护（装载前 RmwWindow 用户键桶排他闩 + 落笔前 obj_save_recheck
//!   域归属复验，对标 C# RMWMethods.cs 记录 X 锁内求值，禁第二套裁决）
//! - 多键读臂：数组头 + 逐键 Read 探测；任一键为磁盘候选即整体降级重放；
//!   reader 错误帧禁入元素位（回退改协议 nil）

use core::cell::RefCell;

use wcol::object_payload::obj_encode_custom;
use wcustom::{CommandType, CustomArgs, CustomCommandMeta, CustomObjectFns, RespVersion};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::RespVecExt,
};
use wval::{CustomObjectType, KeyTag};

use super::object_store_utils::{
  EnvStep, ObjLoad, env_decode_object, obj_load_custom, obj_load_custom_sync,
  obj_save_custom_notified, obj_save_recheck_async, obj_save_recheck_sync, probe_tag_async,
  probe_tag_sync, read_string_domain_async, read_string_domain_sync, step_envelope,
};
use crate::storage::session::storage_session::StorageSession;

/// 自定义对象命令同步执行结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomObjOutcome {
  /// 已闭环（应答/错误帧已写 output）
  Done,
  /// 磁盘候选降级：须异步重放（未写任何输出）
  Degrade,
}

/// 自定义对象命令修改动作
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CustomObjMutation {
  /// 删空回收键
  Delete,
  /// 回写新载荷
  Save(Vec<u8>),
}

/// 自定义对象命令 RMW 分派求值阶段
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CustomObjStep {
  /// 已闭环（成功帧或错误帧已写 output）
  Done,
  /// 磁盘候选降级：须异步重放（未写任何输出）
  Degrade,
  /// 产生变更动作（删空或写回）
  Mutate(CustomObjMutation),
}

/// 自定义对象只读探测的键级结局（单键臂与多键循环臂共用）
enum ReadProbe {
  /// 已产出应答帧（命中喂 reader，键缺失走 not_found）
  Handled,
  /// 键存在但类型不符（信封标签非本类型 / 落在 String 域；探测面零出帧，
  /// 单键臂回 WRONGTYPE、批量臂按逐元素口径回 nil）
  WrongType,
  /// 磁盘候选 / 存储故障：须降级异步重放（未写任何字节）
  Degrade,
}

/// 装载结局 → 只读探测结局（sync/async 两孪生共用）：键缺失落 `not_found` 执行体，
/// 其余与 [`ObjLoad`] 一一对应（Present = reader 已出帧）
#[inline]
fn read_probe_of(
  loaded: ObjLoad<()>,
  ctx: &CustomObjCtx<'_>,
  args: &[&[u8]],
  out: &mut Vec<u8>,
) -> ReadProbe {
  match loaded {
    ObjLoad::Present(()) => ReadProbe::Handled,
    ObjLoad::WrongType => ReadProbe::WrongType,
    ObjLoad::Degrade => ReadProbe::Degrade,
    ObjLoad::Missing => {
      dispatch_custom_object_read(ctx.fns, None, args, out, ctx.resp_version);
      ReadProbe::Handled
    }
  }
}

/// 自定义对象命令执行上下文（信封标签 + 静态执行体 + 会话协议版本）
///
/// 三条臂共用同一组不变入参，免逐臂重复传参；标签在信封编解码边界
/// 收窄为 u8 线域（`obj_decode_custom` 同收标准段与扩展段标签）。
#[derive(Clone, Copy)]
struct CustomObjCtx<'a> {
  /// 信封内层类型标签（wval::CustomObjectType 分配单点）
  tag: CustomObjectType,
  fns: &'a CustomObjectFns,
  resp_version: RespVersion,
}

impl CustomObjCtx<'_> {
  /// 信封线域标签：仅在此一处把枚举收窄为 u8（`wcol::object_payload` 的
  /// 信封编解码收标准段/扩展段共用的 u8 线域，见其模块注释）
  #[inline]
  fn wire_tag(self) -> u8 {
    self.tag.as_u8()
  }
}

/// 自定义对象只读命令分派求值原语（直接借用底层存储切片，零堆分配）
#[inline]
pub(crate) fn dispatch_custom_object_read(
  fns: &CustomObjectFns,
  payload: Option<&[u8]>,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_version: u8,
) {
  match payload {
    Some(p) => {
      let _ = (fns.reader)(p, args, output, resp_version);
    }
    None => {
      (fns.not_found)(args, output, resp_version);
    }
  }
}

/// 自定义对象读写改（RMW）求值原语
///
/// 对标 libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand (RMW_ObjectStore)
///
/// 集中承载 NeedInitialUpdate 校验、Updater 变更、is_empty 判定与删空回收/防空墓碑逻辑
pub(crate) fn dispatch_custom_object_rmw(
  fns: &CustomObjectFns,
  loaded: ObjLoad<Vec<u8>>,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_version: u8,
) -> CustomObjStep {
  let (mut payload, existed) = match loaded {
    ObjLoad::Degrade => return CustomObjStep::Degrade,
    ObjLoad::WrongType => return CustomObjStep::Done,
    ObjLoad::Missing => {
      if !(fns.need_initial_update)(args, output, resp_version) {
        return CustomObjStep::Done;
      }
      (Vec::new(), false)
    }
    ObjLoad::Present(p) => (p, true),
  };

  if !(fns.updater)(&mut payload, args, output, resp_version) {
    return CustomObjStep::Done;
  }

  if (fns.is_empty)(&payload) {
    if existed {
      CustomObjStep::Mutate(CustomObjMutation::Delete)
    } else {
      CustomObjStep::Done
    }
  } else {
    CustomObjStep::Mutate(CustomObjMutation::Save(payload))
  }
}

/// 同步执行自定义对象命令的调用入参结构体（收敛入参，消除 too-many-arguments）
pub(crate) struct CustomObjectCall<'a> {
  /// 命令类型（Read / ReadModifyWrite；多键形态下不参与分派，见 [`CustomArgs::Multi`]）
  pub cmd_type: CommandType,
  /// 键与命令入参（会话侧按静态清单键作用域 [`CustomArgs`] 拆定的形态）
  pub args: CustomArgs<'a>,
  /// 信封内层类型标签（parse→exec 全程保持枚举，仅跨信封编解码时收窄）
  pub tag: CustomObjectType,
  /// 静态执行体
  pub fns: &'a CustomObjectFns,
  /// 会话 RESP 协议版本
  pub resp_version: RespVersion,
}

/// 同步执行一条自定义对象命令（按静态清单键作用域与 CommandType 分派）
pub(crate) fn try_custom_object_command<D: Device>(
  store: &BatchStoreSession<'_, D>,
  call: CustomObjectCall<'_>,
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  let ctx = CustomObjCtx {
    tag: call.tag,
    fns: call.fns,
    resp_version: call.resp_version,
  };
  match call.args {
    CustomArgs::Single { key, args } => match call.cmd_type {
      CommandType::Read => try_custom_object_read_sync(store, &ctx, key, args, output),
      CommandType::ReadModifyWrite => try_custom_object_rmw_sync(store, &ctx, key, args, output),
    },
    CustomArgs::Multi { keys, args } => {
      try_custom_object_multi_read_sync(store, &ctx, keys, args, output)
    }
  }
}

/// 同步单键只读探测（唯一读探测面，多键循环臂逐键复用）：与装载族同一决策核
/// （信封解码 [`env_decode_object`] + [`step_envelope`] + String 域反探），本函数
/// 只发起读与归一化；磁盘候选回 Degrade（异步重放段无更冷层，恒不产出）
///
/// 决策核按装载族口径出帧（单键 WRONGTYPE、批量逐元素 nil），本族口径不同，故把
/// 核的帧写入丢弃型 sink，出帧权留在调用臂——与收敛前「探测面零出帧」一致
fn probe_custom_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  out: &mut Vec<u8>,
) -> ReadProbe {
  // 信封命中即零拷贝直喂 fns.reader（读侧降级约定：磁盘候选 / TTL 待裁决）
  let env = probe_tag_sync(store, key, KeyTag::ObjectEnvelope, |raw| {
    env_decode_object(raw, ctx.wire_tag(), |payload| {
      dispatch_custom_object_read(ctx.fns, Some(payload), args, out, ctx.resp_version);
      Some(())
    })
  });
  let mut sink = Vec::new();
  let loaded = match step_envelope(env, key, ctx.wire_tag(), &mut sink, ObjLoad::Present) {
    EnvStep::Done(loaded) => loaded,
    // 信封域确认缺失（内存墓碑/无候选）：反探 String 域
    EnvStep::StringDomain => read_string_domain_sync(store, key, &mut sink),
  };
  read_probe_of(loaded, ctx, args, out)
}

/// 同步 Read 臂：单键只读探测，类型不符按装载族同口径回 WRONGTYPE
fn try_custom_object_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  match probe_custom_read_sync(store, ctx, key, args, output) {
    ReadProbe::Handled => CustomObjOutcome::Done,
    ReadProbe::WrongType => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      CustomObjOutcome::Done
    }
    ReadProbe::Degrade => CustomObjOutcome::Degrade,
  }
}

/// 逐键元素位收尾（票 zcode-r22-wcustom 发现三）：reader 产出的命令级错误帧
/// 禁入数组元素位（畸形 RESP：批量应答元素位只允许值帧或 nil），回退改写协议
/// nil。单键 GET 顶层错误契约不受影响——错误帧即整条命令应答，不落元素位；
/// 命令级错误帧仅在数组头之前短路写出（多键臂无数组头前出帧路径，装载降级
/// 经 truncate(base) 连头一并回退）
#[inline]
fn error_element_to_nil(output: &mut Vec<u8>, el: usize, resp_version: u8) {
  if output.get(el) == Some(&b'-') {
    output.truncate(el);
    output.write_resp_null_ver(resp_version);
  }
}

/// 同步多键读臂（[`wcustom::KeyScope::MultiRead`] 形态）：数组头 + 逐键只读
/// 探测，键缺失与类型不符均按逐元素口径回 nil（单元素失败不拖垮整条批量读）；
/// 任一键为磁盘候选即丢弃本次输出整体降级异步重放（降级契约：零字节残留）
fn try_custom_object_multi_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  keys: &[&[u8]],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  // 本条应答起点（同批可含前序命令输出，降级时按此回退而非另立草稿缓冲）
  let base = output.len();
  output.write_resp_array_len(keys.len());
  for &key in keys {
    // 元素位起点：reader 出错帧时据此回退改 nil（禁错误帧入元素位）
    let el = output.len();
    match probe_custom_read_sync(store, ctx, key, args, output) {
      ReadProbe::Handled => error_element_to_nil(output, el, ctx.resp_version),
      ReadProbe::WrongType => output.write_resp_null_ver(ctx.resp_version),
      ReadProbe::Degrade => {
        output.truncate(base);
        return CustomObjOutcome::Degrade;
      }
    }
  }
  CustomObjOutcome::Done
}

/// 同步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并按需回写或删空
///
/// C# 对位 libs/server/Storage/Functions/ObjectStore/RMWMethods.cs：NeedInitialUpdate /
/// InPlaceUpdater / CopyUpdater 四钩子全在记录 X 锁内求值，「装载 → 写回」间隙不存在。
/// rust 同款写回面保护（rmw_helpers 头注写回面保护统一登记，复用同一套判定核，绝不
/// 另立第二套裁决，票 zcode-r22-wcustom 发现一）：装载前 [`wkv::BatchStoreSession::
/// try_rmw_window`] 用户键桶排他闩挡并发同键 RMW 交错顶替（自旋预算内不得闩即降级，
/// 由慢路径 [`custom_object_rmw_async`] 让核等待臂承接）；落笔前
/// [`obj_save_recheck_sync`] 复验域归属——窗口只挡 RMW 方，对面 DEL/SET 走物理记录键
/// 桶闩，装载时旧视图未经复验绝不落笔，复验不过弃写走既有 Degrade（异步重放按当前态
/// 重新装载求值，应答与新状态自洽，绝不复活已 ACK 删除的键或造 String/信封双域并存）
fn try_custom_object_rmw_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  // 本条应答起点（同批可含前序命令输出；复验弃写经降级重放，按此回退本臂已写帧）
  let base = output.len();
  let Some(_window) = store.try_rmw_window(key) else {
    return CustomObjOutcome::Degrade;
  };
  let loaded = obj_load_custom_sync(store, key, ctx.wire_tag(), output, |p| Some(p.to_vec()));
  let existed = matches!(loaded, ObjLoad::Present(_));
  let step = dispatch_custom_object_rmw(ctx.fns, loaded, args, output, ctx.resp_version);
  match step {
    CustomObjStep::Done => CustomObjOutcome::Done,
    CustomObjStep::Degrade => CustomObjOutcome::Degrade,
    CustomObjStep::Mutate(mutation) => {
      // 落笔前终态复验（判定核与 run_sync_rmw 同一单点）：Present 装载期望
      // 信封域仍在，Missing 新建期望全域确认不存在
      let loaded = existed.then_some(KeyTag::ObjectEnvelope);
      if !obj_save_recheck_sync(store, key, loaded).unwrap_or(false) {
        output.truncate(base);
        return CustomObjOutcome::Degrade;
      }
      // 落笔三态统一裁决（删空与回写同口径）：成功→Done / 写面弃写→Degrade /
      // IO 失败→GENERIC 错误帧
      let stored = match mutation {
        CustomObjMutation::Delete => store.try_delete_sync(key).map(|r| r.is_ok()),
        CustomObjMutation::Save(payload) => {
          obj_save_custom_notified(store, key, ctx.wire_tag(), &payload)
        }
      };
      match stored {
        Ok(true) => CustomObjOutcome::Done,
        Ok(false) => CustomObjOutcome::Degrade,
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          CustomObjOutcome::Done
        }
      }
    }
  }
}

/// 异步执行一条自定义对象命令（冷键降级后执行，与会话快路径同一
/// [`CustomArgs`] 拆分与同一臂分型）；`cmd_refs` 为完整参数域
/// [key..., 命令入参...]，Err = 存储 IO 失败由调用方写
/// RESP_ERR_SLOW_PATH_STORAGE
pub(crate) async fn custom_object_slow<D: Device>(
  storage: &StorageSession<'_, D>,
  tag: CustomObjectType,
  meta: &CustomCommandMeta,
  cmd_refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let ctx = CustomObjCtx {
    tag,
    fns: &meta.fns,
    resp_version: storage.resp_version,
  };
  // 键与入参的布局判定与会话快路径同一单点（KeyScope::split）；拆不出键
  // = 回放帧不成形，按慢路径既有口径回明确错误帧，绝不静默
  let Some(args) = meta.key_scope.split(cmd_refs) else {
    write_error_raw(output, RESP_ERR_ASYNC_REQUIRED);
    return Ok(());
  };
  match args {
    CustomArgs::Single { key, args } => match meta.command_type {
      CommandType::Read => custom_object_read_async(storage, &ctx, key, args, output).await,
      CommandType::ReadModifyWrite => {
        custom_object_rmw_async(storage, &ctx, key, args, output).await
      }
    },
    CustomArgs::Multi { keys, args } => {
      custom_object_multi_read_async(storage, &ctx, keys, args, output).await
    }
  }
}

/// 异步单键只读探测（异步重放段与多键循环共用）—— [`probe_custom_read_sync`] 的
/// 异步对位，决策核共用；闭包内借用切片直喂 reader（零堆分配），IO 错误如实上抛
/// （异步侧无更冷层可降级，故不产出 Degrade 由调用方按存储失败应答）
async fn probe_custom_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  out: &mut Vec<u8>,
) -> Result<ReadProbe, ()> {
  // 输出通道在闭包外唯一持有者是本函数的逐步落帧，借用穿不过 await 边界，
  // 以 RefCell 交出临时可变性（栈上单元格，零堆分配）
  let env = {
    let output_cell = RefCell::new(&mut *out);
    probe_tag_async(storage, key, KeyTag::ObjectEnvelope, |raw| {
      env_decode_object(raw, ctx.wire_tag(), |payload| {
        dispatch_custom_object_read(
          ctx.fns,
          Some(payload),
          args,
          *output_cell.borrow_mut(),
          ctx.resp_version,
        );
        Some(())
      })
    })
    .await
    .map_err(|_| ())?
  };
  // 核的装载族口径帧丢弃，出帧权在调用臂（同 [`probe_custom_read_sync`]）
  let mut sink = Vec::new();
  let loaded = match step_envelope(env, key, ctx.wire_tag(), &mut sink, ObjLoad::Present) {
    EnvStep::Done(loaded) => loaded,
    // 信封域确认缺失：反探 String 域
    EnvStep::StringDomain => read_string_domain_async(storage, key, &mut sink)
      .await
      .map_err(|_| ())?,
  };
  Ok(read_probe_of(loaded, ctx, args, out))
}

/// 异步 Read 臂：单键只读探测（[`try_custom_object_read_sync`] 的慢路径对位）
async fn custom_object_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  match probe_custom_read_async(storage, ctx, key, args, output).await? {
    ReadProbe::WrongType => write_error_raw(output, RESP_ERR_WRONG_TYPE),
    // 异步重放段已无更冷层可降级（存储失败以 Err 上抛），Degrade 不可达
    ReadProbe::Handled | ReadProbe::Degrade => {}
  }
  Ok(())
}

/// 异步多键读臂：数组头 + 逐键只读探测，非命中元素（类型不符/键缺失/
/// 已无更冷层可读的磁盘候选）回 nil；reader 错误帧同样禁入元素位（改 nil，
/// [`error_element_to_nil`] 同款）
async fn custom_object_multi_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  keys: &[&[u8]],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  output.write_resp_array_len(keys.len());
  for &key in keys {
    let el = output.len();
    match probe_custom_read_async(storage, ctx, key, args, output).await? {
      ReadProbe::Handled => error_element_to_nil(output, el, ctx.resp_version),
      ReadProbe::WrongType | ReadProbe::Degrade => output.write_resp_null_ver(ctx.resp_version),
    }
  }
  Ok(())
}

/// 异步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并回写或删空
///
/// [`try_custom_object_rmw_sync`] 的慢路径对位，同一套写回面保护（票
/// zcode-r22-wcustom 发现一）：装载前 [`wkv::BatchStoreSession::rmw_window`]
/// 让核等待臂取窗（run_async_rmw 同款，跨「装载 → 求值 → 写回」全程）；落笔前
/// [`obj_save_recheck_async`] 复验域归属（含候选读通让核与内存终判），复验不过
/// 弃写按存储忙信号交回客户端重试（本臂已是终态重放面，无更深降级通道）
async fn custom_object_rmw_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 本条应答起点（复验弃写经存储忙重试，按此回退本臂已写帧，绝不残留拼帧）
  let base = output.len();
  let _window = storage
    .batch
    .rmw_window(key)
    .await
    .map_err(|e| log::error!("custom_object_rmw_async rmw_window failed: {e:?}"))?;
  let loaded = obj_load_custom(storage, key, ctx.wire_tag(), output, |p| Some(p.to_vec()))
    .await
    .map_err(|_| ())?;
  let existed = matches!(loaded, ObjLoad::Present(_));
  let step = dispatch_custom_object_rmw(ctx.fns, loaded, args, output, ctx.resp_version);
  match step {
    CustomObjStep::Done | CustomObjStep::Degrade => Ok(()),
    CustomObjStep::Mutate(mutation) => {
      // 落笔前终态复验（判定核与 run_async_rmw 同一单点）：本窗口只挡 RMW 方，
      // 本臂「装载 → 求值 → 写回」跨读内核让核点，交叠面比同步臂更宽
      let loaded = existed.then_some(KeyTag::ObjectEnvelope);
      let unchanged = obj_save_recheck_async(storage, key, loaded)
        .await
        .map_err(|e| log::error!("custom_object_rmw_async recheck err: {e:?}"))?;
      if !unchanged {
        output.truncate(base);
        return Err(());
      }
      match mutation {
        CustomObjMutation::Delete => storage.delete_string(key).await.map(|_| ()).map_err(|_| ()),
        CustomObjMutation::Save(payload) => {
          match obj_save_custom_notified(&storage.batch, key, ctx.wire_tag(), &payload) {
            Ok(true) => Ok(()),
            Ok(false) => storage
              .upsert_tag(
                key,
                KeyTag::ObjectEnvelope,
                &obj_encode_custom(ctx.wire_tag(), &payload),
              )
              .await
              .map_err(|_| ()),
            Err(_) => Err(()),
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn dummy_fns() -> CustomObjectFns {
    CustomObjectFns {
      need_initial_update: |_args, output, _resp_version| {
        if _args.first().copied() == Some(b"bad") {
          output.extend_from_slice(b"-ERR bad\r\n");
          false
        } else {
          true
        }
      },
      updater: |payload, args, output, _resp_version| {
        if args.first().copied() == Some(b"fail") {
          output.extend_from_slice(b"-ERR fail\r\n");
          false
        } else {
          payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
          true
        }
      },
      reader: |payload, _args, output, _resp_version| {
        output.extend_from_slice(payload);
        true
      },
      not_found: |_args, output, resp_version| {
        output.write_resp_null_ver(resp_version);
      },
      is_empty: |payload| payload.is_empty(),
    }
  }

  #[test]
  fn test_dispatch_read() {
    let fns = dummy_fns();
    let mut out = Vec::new();

    // Missing -> NotFound
    dispatch_custom_object_read(&fns, None, &[], &mut out, 2);
    assert_eq!(out, b"$-1\r\n");

    // Present -> Reader (借用切片零拷贝)
    out.clear();
    dispatch_custom_object_read(&fns, Some(b"hello"), &[], &mut out, 2);
    assert_eq!(out, b"hello");
  }

  #[test]
  fn test_dispatch_rmw() {
    let fns = dummy_fns();
    let mut out = Vec::new();

    // Degrade
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Degrade, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Degrade);

    // WrongType
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::WrongType, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);

    // Missing + need_initial_update returns false
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"bad"], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);
    assert_eq!(out, b"-ERR bad\r\n");

    // Missing + need_initial_update ok + updater returns false
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"fail"], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);
    assert_eq!(out, b"-ERR fail\r\n");

    // Missing + need_initial_update ok + updater ok + empty payload -> Done (no ghost tombstone)
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);

    // Missing + need_initial_update ok + updater ok + non-empty payload -> Mutate(Save)
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"v1"], &mut out, 2);
    assert_eq!(
      step,
      CustomObjStep::Mutate(CustomObjMutation::Save(b"v1".to_vec()))
    );

    // Present + updater ok + non-empty payload -> Mutate(Save)
    out.clear();
    let step = dispatch_custom_object_rmw(
      &fns,
      ObjLoad::Present(b"p0".to_vec()),
      &[b"v1"],
      &mut out,
      2,
    );
    assert_eq!(
      step,
      CustomObjStep::Mutate(CustomObjMutation::Save(b"p0v1".to_vec()))
    );

    // Present + emptied payload -> Mutate(Delete) (strict empty deletion)
    let mut empty_fns = dummy_fns();
    empty_fns.updater = |payload, _args, _out, _resp_version| {
      payload.clear();
      true
    };
    let step = dispatch_custom_object_rmw(
      &empty_fns,
      ObjLoad::Present(b"old".to_vec()),
      &[],
      &mut out,
      2,
    );
    assert_eq!(step, CustomObjStep::Mutate(CustomObjMutation::Delete));
  }

  /// 同步 RMW 臂写回面保护确定性回归（票 zcode-r22-wcustom 发现一，对标 C#
  /// RMWMethods.cs 四钩子全在记录 X 锁内求值）：
  /// - uncontended：无并发时复验放行写回（绝非「一律降级」开关）
  /// - window contention：他者持窗即降级且求值绝不执行（RMW 方互斥封堵丢更新）
  /// - concurrent delete：updater 窗内会合真并发 DEL，复验判异弃写降级，
  ///   键不复活、无 String/信封双域并存
  mod rmw_guard {
    use std::{
      sync::{Arc, OnceLock, mpsc},
      thread,
    };

    use parking_lot::Mutex;
    use tempfile::tempdir;
    use wdev::SegmentedDevice;
    use wkv::{StoreConfig, WedbStore};

    use super::*;

    /// 小预算独立测试库（与 wtest_base::open_test_store 同参内联：16MB 预算
    /// 显式收窄；lib 单测二进制禁链接 wtest_base——其 ctor 会预装全局日志器，
    /// 污染 logging::tests 的首次安装前提，见票 zcode-r22-wcustom 回归记录）
    fn open_store(tag: &str) -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
      let dir = tempdir().expect("tempdir");
      let device =
        Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).expect("测试设备打开"));
      let store = Arc::new(
        WedbStore::open(StoreConfig::auto_with_budget(16 << 20), device).expect("测试存储打开"),
      );
      (dir, store)
    }

    /// 确定性会合闸门（`CustomObjectFns` 字段为 fn 指针禁捕获，经 static
    /// 通道注入「装载在手、写回未落」的窗内会合点；本子模块专用）
    struct Gate {
      held_tx: mpsc::SyncSender<()>,
      held_rx: Mutex<mpsc::Receiver<()>>,
      acked_tx: mpsc::SyncSender<()>,
      acked_rx: Mutex<mpsc::Receiver<()>>,
    }

    static GATE: OnceLock<Gate> = OnceLock::new();

    fn gate() -> &'static Gate {
      GATE.get_or_init(|| {
        let (held_tx, held_rx) = mpsc::sync_channel(0);
        let (acked_tx, acked_rx) = mpsc::sync_channel(0);
        Gate {
          held_tx,
          held_rx: Mutex::new(held_rx),
          acked_tx,
          acked_rx: Mutex::new(acked_rx),
        }
      })
    }

    /// updater 窗内会合执行体：宣告持窗装载、等对面命令完整回执后放行写回
    ///（rmw_writeback_revalidate 同款确定性交叠构造，不赌调度器）
    fn gate_updater(payload: &mut Vec<u8>, args: &[&[u8]], _out: &mut Vec<u8>, _rv: u8) -> bool {
      payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
      let g = gate();
      g.held_tx.send(()).expect("持窗宣告送达");
      g.acked_rx.lock().recv().expect("对面命令已回执");
      true
    }

    type CustomUpdater = fn(&mut Vec<u8>, &[&[u8]], &mut Vec<u8>, RespVersion) -> bool;

    /// rmw_guard 桩执行体装配：除 updater 外四钩子恒为中性桩
    /// （`CustomObjectFns` 字段为 fn 指针禁捕获闭包，会合面经 static 闸门注入）
    fn stub_fns(updater: CustomUpdater) -> CustomObjectFns {
      CustomObjectFns {
        need_initial_update: |_, _, _| true,
        updater,
        reader: |_, _, _, _| true,
        not_found: |_, _, _| (),
        is_empty: |p| p.is_empty(),
      }
    }

    /// 同步 RMW 臂直调装配
    fn rmw_sync(
      batch: &BatchStoreSession<'_, SegmentedDevice>,
      fns: &CustomObjectFns,
      key: &[u8],
      arg: &[u8],
    ) -> CustomObjOutcome {
      let ctx = CustomObjCtx {
        tag: CustomObjectType::Json,
        fns,
        resp_version: 2,
      };
      let args = [arg];
      try_custom_object_rmw_sync(batch, &ctx, key, &args, &mut Vec::new())
    }

    /// 无会合点执行体：updater 追加入参即求写回（正向对照与窗争用用）
    fn plain_fns() -> CustomObjectFns {
      stub_fns(|payload, args, _out, _rv| {
        payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
        true
      })
    }

    /// 装载复核：全域存活探针（Missing = 键不复活且无 String/信封双域并存）
    fn load_probe(batch: &BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> ObjLoad<()> {
      obj_load_custom_sync(
        batch,
        key,
        CustomObjectType::Json.as_u8(),
        &mut Vec::new(),
        |_| Some(()),
      )
    }

    /// 载荷读出复核（Present 值直取）
    fn load_value(batch: &BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> Option<Vec<u8>> {
      match obj_load_custom_sync(
        batch,
        key,
        CustomObjectType::Json.as_u8(),
        &mut Vec::new(),
        |p| Some(p.to_vec()),
      ) {
        ObjLoad::Present(p) => Some(p),
        other => panic!("预期信封存活，实得 {other:?}"),
      }
    }

    #[test]
    fn uncontended_rmw_writes_back() {
      let (_dir, store) = open_store("custom-rmw-guard-ctl.db");
      let sess = store.new_session().unwrap();
      let batch = sess.enter_batch();
      let key = b"custom:guard:ctl";
      let outcome = rmw_sync(&batch, &plain_fns(), key, b"v1");
      assert_eq!(outcome, CustomObjOutcome::Done);
      assert_eq!(load_value(&batch, key).as_deref(), Some(b"v1".as_slice()));
    }

    #[test]
    fn window_contention_degrades_without_evaluating() {
      let (_dir, store) = open_store("custom-rmw-guard-window.db");
      let sess = store.new_session().unwrap();
      let batch = sess.enter_batch();
      let key = b"custom:guard:window";
      assert_eq!(
        rmw_sync(&batch, &plain_fns(), key, b"v0"),
        CustomObjOutcome::Done
      );
      // 他者持窗（同键 RMW 方）：预算内不得闩即降级，求值与写回绝不执行，
      // 后到者交异步重放重新装载求值（丢更新面封堵）
      let rival = batch.try_rmw_window(key).expect("他者窗应可取");
      assert_eq!(
        rmw_sync(&batch, &plain_fns(), key, b"v1"),
        CustomObjOutcome::Degrade
      );
      assert_eq!(
        load_value(&batch, key).as_deref(),
        Some(b"v0".as_slice()),
        "不得闩即降级，求值与写回绝不执行"
      );
      drop(rival);
      // 释窗重放：写回生效，同键两写不丢失
      assert_eq!(
        rmw_sync(&batch, &plain_fns(), key, b"v1"),
        CustomObjOutcome::Done
      );
      assert_eq!(load_value(&batch, key).as_deref(), Some(b"v0v1".as_slice()));
    }

    /// updater 窗内会合执行体已由 [`gate_updater`] 经 [`stub_fns`] 注入
    #[test]
    fn concurrent_delete_recheck_discards_stale_write() {
      let (_dir, store) = open_store("custom-rmw-guard-del.db");
      let sess = store.new_session().unwrap();
      let batch = sess.enter_batch();
      let key: &[u8] = b"custom:guard:del";
      assert_eq!(
        rmw_sync(&batch, &plain_fns(), key, b"v0"),
        CustomObjOutcome::Done
      );

      let store2 = store.clone();
      let key2 = key.to_vec();
      // 对面写者独立连接（生产 thread-per-core 形态）：DEL 取物理记录键桶闩、
      // 不取本窗，窗口期内可完整落地
      let watcher = thread::spawn(move || {
        let g = gate();
        g.held_rx.lock().recv().expect("RMW 持窗装载宣告");
        let s = store2.new_session().expect("对面会话");
        let b = s.enter_batch();
        let deleted = b
          .try_delete_sync(&key2)
          .expect("DEL 存储面")
          .expect("DEL 判定面");
        assert!(deleted, "对面 DEL 应真实删除信封键");
        g.acked_tx.send(()).expect("DEL 回执送达");
      });
      let outcome = rmw_sync(&batch, &stub_fns(gate_updater), key, b"v1");
      watcher.join().expect("对面线程无 panic");
      assert_eq!(outcome, CustomObjOutcome::Degrade, "复验判异应弃写降级");
      assert!(
        matches!(load_probe(&batch, key), ObjLoad::Missing),
        "键复活或 String/信封双域并存（盲写已 ACK 删除的键）"
      );
    }
  }
}

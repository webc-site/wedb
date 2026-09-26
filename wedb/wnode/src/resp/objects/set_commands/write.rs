//! 集合写命令与集合运算实现（SADD, SREM, SPOP, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE）

use wbase::map::HashSet;
use wcol::set::{
  set_object::{SetObject, SetOperation},
  set_object_impl::NO_COUNT,
};
use wdev::Device;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
};

use super::{Rmw, SetLoad, parse_set_pop_args, run_operate, set_load_sync, set_save_or_gc};
use crate::{
  resp::{
    objects::object_store_utils::{
      RespRmwDone, SyncStoreWindow, obj_writeback_recheck_sync, store_writeback_clear_ttl,
      try_sync_rmw_window_pair,
    },
    resp_server_session::RespServerSession,
    vector::vector_manager::VectorManager,
  },
  storage::session::common::ttl_sync::registry_alive,
};

impl RespServerSession {
  /// SADD key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetAdd
  pub fn set_add<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SADD");

    set_rmw_count_or_bail!(self, store, SetOperation::Sadd, parse_state, output)
  }

  /// SREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRemove
  pub fn set_remove<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SREM");

    set_rmw_count_or_bail!(self, store, SetOperation::Srem, parse_state, output)
  }

  /// SPOP key \[count\]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetPop
  ///
  /// 弹空后整键回收
  pub fn set_pop<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, count_parameter)) = parse_set_pop_args(parse_state, output) else {
      return Ok(true);
    };

    // C# countParameter == 0 → 空集合（WriteEmptySet 版本分派，不触达后端）
    if count_parameter == 0 {
      cs::write_set_len(output, 0, self.resp_protocol_version);
      return Ok(true);
    }

    // 装载型写臂双保护·同步档（票 load-type-rmw-window）：装载前取 rmw 窗
    //（run_sync_rmw 同锁源），落笔前复验域归属
    set_windowed_load!(store, key, output, mut obj, {
      // C# NOTFOUND: parseState.Count == 2 (有 count) → WriteEmptySet（版本分派），否则 WriteNull ($-1\r\n)
      if count_parameter != NO_COUNT {
        cs::write_set_len(output, 0, self.resp_protocol_version);
      } else {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      return Ok(true);
    });
    let mut obj_out = run_operate(
      &mut obj,
      SetOperation::Spop,
      &[],
      count_parameter,
      0,
      self.resp_protocol_version,
      output,
    );
    if !obj_writeback_recheck_sync(store, key, true) {
      obj_out.reset();
      return Ok(false);
    }
    match set_save_or_gc(store, key, &obj) {
      Ok(true) => {}
      // Degrade/存储错误：回退挂载点，慢路径整体重放或错误帧独占应答
      Ok(false) => {
        obj_out.reset();
        return Ok(false);
      }
      Err(_) => {
        obj_out.reset();
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    Ok(true)
  }

  /// SMOVE source destination member
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetMove
  /// （存储侧语义对标 SetOps.SetMove）
  pub fn set_move<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "SMOVE");

    let source_key = parse_state[0];
    let destination_key = parse_state[1];
    let member = parse_state[2];

    // 双键装载型写臂双保护·同步档：键组桶升序单机制双窗取闩（全仓多键臂
    // 唯一取闩序，票 zcode-r135c-lockorder 案一），任一窗未取到整体走既有
    // Ok(false) 异步重放通道
    let Some(_windows) = try_sync_rmw_window_pair(store, source_key, destination_key) else {
      return Ok(false);
    };
    let mut src = set_load_or_bail!(store, source_key, output, {
      // C# NOTFOUND → :0
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    });

    // If the keys are the same, no operation is performed.
    if source_key == destination_key {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    }

    // dst 登记表第四域探针（C# 统一存储 GET dst 对向量记录回 WRONGTYPE，
    // ObjectStore/ReadMethods.cs:15-22 ValueIsObject=false 臂；SetOps.cs:298-304
    // 一手注释 "Validate the destination type before removing from the source
    // so that a WRONGTYPE destination does not lose the member"）。rust 向量
    // 索引驻 VectorManager 登记表、三域恒 Missing，不探即建空集先搬后写——
    // 登记键名下值域集合幽灵 + 源真实失员。判据单源 registry_alive（与派发门 /
    // EXISTS 第四态同源），置于 src take 之前严格复刻 C# 先拒后搬。
    if registry_alive(vector, store.session_prefix().as_slice(), destination_key) {
      output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      return Ok(true);
    }

    // dst 装载态（Missing 新建域须保持缺席才可落笔，既存信封域须保持归属不变）
    let mut dst_existed = false;
    let mut dst = match set_load_sync(store, destination_key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => return Ok(true),
      SetLoad::Missing => SetObject::new(),
      SetLoad::Present(o) => {
        dst_existed = true;
        o
      }
    };

    let Some(item) = src.set.take(member) else {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    };
    src.update_size(member, false);

    // C# SetMove → SetAdd 条件记账：目标已含成员时不重复添加、不虚增堆记账
    if dst.set.insert(item) {
      dst.update_size(member, true);
    }

    // 写回序先目标后源（对标 C# SetMove 两键事务的原子性语义，本仓无事务
    // 打包机制，以倒序加幂等重放补偿）：目标写回失败（升阶门降级 / IO 错误）
    // 时源零变异零写入，慢路径重放自完整初态整体执行；目标成功而源失败时，
    // 重放装载到源仍含 member、目标已含 member 的状态，take 成功且 insert
    // 不重复记账，双写收敛；最坏部分失败态由「member 丢失」改善为「member
    // 双份可重试收敛」
    if !obj_writeback_recheck_sync(store, destination_key, dst_existed) {
      return Ok(false);
    }
    match set_save_or_gc(store, destination_key, &dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
    if !obj_writeback_recheck_sync(store, source_key, true) {
      return Ok(false);
    }
    match set_save_or_gc(store, source_key, &src) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    output.extend_from_slice(cs::RESP_RETURN_VAL_1);
    Ok(true)
  }

  fn set_combine_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    cmd_name: &str,
    combine: impl FnOnce(&[SetObject]) -> HashSet<Vec<u8>>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, cmd_name);

    let dst = parse_state[0];
    // 装载型写臂先窗后装·同步档（票 wnode-set-store-selfref-load-outside-window，
    // 对位 SPOP/SMOVE 臂）：目标键 rmw 窗前移至源装载之前取得、句柄下传收尾单点
    // 复用（同键同单窗禁双取），自指形（dst ∈ srcs）装载自然落窗内取新态，
    // 杜绝「装载 → 开窗」间隙对面已确认写被陈旧快照覆写；begin 未取到窗与
    // 异构域拒写沿既有 Ok(false) 异步重放通道（combine_store 出口统一拆柄）
    let window = SyncStoreWindow::begin(store, dst);
    let objs = load_many_or_bail!(store, &parse_state[1..], output);

    // 折叠核出裸集，STORE 入口单点 from_members 装配计账（读臂零计账，
    // 对位 C# SetOps 三 STORE 臂 newSetObject 组装期 foreach Add+UpdateSize）
    let result = SetObject::from_members(combine(&objs));
    combine_store(dst, &result, window, store, output)
  }

  /// SINTERSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectStore
  pub fn set_intersect_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine_store(parse_state, store, output, "SINTERSTORE", intersect_sets)
  }

  /// SUNIONSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnionStore
  pub fn set_union_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine_store(parse_state, store, output, "SUNIONSTORE", union_sets)
  }

  /// SDIFFSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiffStore
  pub fn set_diff_store<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.set_combine_store(parse_state, store, output, "SDIFFSTORE", diff_sets)
  }
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 「缺失」含判死异构键（string 影子/RI 到期未清退，域读恒先过 wkv 域内 TTL
/// 门折叠 NotFound）——对 C# ObjectStore Reader「判型先于到期」门序为刻意
/// 采序，见 deviations.md §150，严禁按 C# 回改
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
pub(super) fn load_many(
  store: &wkv::BatchStoreSession<impl Device>,
  keys: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<Option<Vec<SetObject>>, ()> {
  let mut objs = Vec::with_capacity(keys.len());
  for key in keys {
    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(None),
      SetLoad::WrongType => return Err(()),
      SetLoad::Missing => objs.push(SetObject::new()),
      SetLoad::Present(o) => objs.push(o),
    }
  }
  Ok(Some(objs))
}

/// 集合求交（首集复制后逐集收缩；缺失键视为空集 → 空结果）
///
/// 对应 libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersect 算法的本地集合求交
///
/// 折叠核为裸集生产者（对位 C# 私有 SetIntersect 出 `out HashSet<byte[]>`）：
/// 读臂直消费出帧/取基数零计账；STORE 臂入口经 [`SetObject::from_members`]
/// 单点装配逐成员计账（C# SetIntersectStore 的 `foreach Set.Add + UpdateSize`
/// 同口径），杜绝零记账落盘旁路
pub(super) fn intersect_sets(objs: &[SetObject]) -> HashSet<Vec<u8>> {
  let Some(first) = objs.first() else {
    return HashSet::default();
  };
  let mut members = first.set.clone();

  for obj in &objs[1..] {
    // intersection of anything with empty set is empty set
    if members.is_empty() {
      break;
    }
    members.retain(|m| obj.contains(m.as_slice()));
  }
  members
}

/// 集合求并（逐集并入，extend 单次插入免逐元素判重分支）
///
/// 裸集生产者（对位 C# 私有 SetUnion 出 `out HashSet<byte[]>`），计账唯
/// STORE 臂入口的 [`SetObject::from_members`] 单点（C# SetUnionStore 同口径）
pub(super) fn union_sets(objs: &[SetObject]) -> HashSet<Vec<u8>> {
  let mut members: HashSet<Vec<u8>> = HashSet::default();
  for obj in objs {
    members.extend(obj.set.iter().cloned());
  }
  members
}

/// 集合求差（首集减去其余各集）
///
/// 对应 libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff 算法的本地集合求差
///
/// 裸集生产者（对位 C# 私有 SetDiff 出 `out HashSet<byte[]>`），计账唯
/// STORE 臂入口的 [`SetObject::from_members`] 单点（C# SetDiffStore 同口径）
pub(super) fn diff_sets(objs: &[SetObject]) -> HashSet<Vec<u8>> {
  let Some(first) = objs.first() else {
    return HashSet::default();
  };
  let mut members = first.set.clone();

  for obj in &objs[1..] {
    members.retain(|m| !obj.contains(m.as_slice()));
  }
  members
}

/// SINTER/SUNION/SDIFF 的 *STORE 公共收尾：空结果回收目标键，否则写回并回基数
///
/// 目标键为 SET 语义（清既有 key 级 TTL，对标 C# SetOps 的 SET 收尾），信封域
/// upsert 默认保留 TTL，非空结果写回成功后随写显式清退；空结果走 set_save_or_gc
/// 删空臂（try_delete_sync 级联清 TTL，对齐 C# EXPIRE key 0）
///
/// 序纪律（票 zcode-r122c-setstore1）：**写回先行、清退随后**，与冷臂
/// `obj_save_clear_ttl`（storage_session「先信封后清」）同序同机制——清退落于
/// [`store_writeback_clear_ttl`] 单点、信封写回 Ok(true) 之后的同一持窗临界区，
/// 写回失败/降级臂天然零清退即回错误帧/重放，失败即原态（旧「清退先于写回」序
/// 在写故障窗抹 TTL 后命令以 -ERR 终结，键被失败命令复活永不过期，禁复犯）。
///
/// 目标键 rmw 窗句柄由调用方装载前预取传入（票 wnode-set-store-selfref-load-
/// outside-window，装载型写臂先窗后装纪律；同键同单窗禁双取，本臂不再开窗）；
/// `None` = begin 未取到窗或异构域拒写，沿既有 `Ok(false)` 异步重放通道。
fn combine_store<'a, D: Device>(
  dst: &[u8],
  result: &SetObject,
  window: Option<SyncStoreWindow<'_, 'a, '_, D>>,
  store: &wkv::BatchStoreSession<'a, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  // STORE 覆写族目标键双保护·同步档：持目标键 rmw 窗跨「信封写回 → TTL 清退」
  // 全程（挡同键 RMW 写臂交错），装载态取开窗时刻存活域（先窗后装，窗跨源装载），
  // 落笔前复验域归属，对面 DEL/SET 交叠即拒写走既有降级/重试通道，杜绝双域并存盲写
  let Some(window) = window else {
    return Ok(false);
  };
  if !window.recheck() {
    return Ok(false);
  }
  match set_save_or_gc(store, dst, result) {
    Ok(true) => {
      // 写回闭环后同临界区尾笔清退（空结果的 TTL 已随删空臂整键回收级联清退，
      // 无需再清）；尾笔残留经单点告警并 fail-loud 错误帧（冷臂同款口径）
      if !result.is_empty() && !store_writeback_clear_ttl(store, dst) {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
      output.write_resp_int(result.set.len() as i64);
    }
    Ok(false) => return Ok(false),
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  Ok(true)
}

//! 集合写命令与集合运算实现（SADD, SREM, SPOP, SMOVE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE）

use wcol::set::{
  set_object::{SetObject, SetOperation},
  set_object_impl::NO_COUNT,
};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{self as cs, RESP_ERR_GENERIC},
  ext::RespVecExt,
};

use super::{Rmw, SetLoad, parse_set_pop_args, run_operate, set_load_sync, set_save_or_gc};
use crate::{
  resp::{objects::object_store_utils::RespRmwDone, resp_server_session::RespServerSession},
  storage::session::common::ttl_sync::del_ttl_sync,
};

impl RespServerSession {
  /// SADD key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetAdd
  pub fn set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SADD");

    let key = parse_state[0];
    match self.set_rmw(
      store,
      key,
      SetOperation::Sadd,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// SREM key member [member ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetRemove
  pub fn set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SREM");

    let key = parse_state[0];
    match self.set_rmw(
      store,
      key,
      SetOperation::Srem,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      Rmw::Present(RespRmwDone {
        result1,
        payload_written,
      }) => {
        if !payload_written {
          output.write_resp_int(result1);
        }
      }
    }
    Ok(true)
  }

  /// SPOP key \[count\]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetPop
  ///
  /// 弹空后整键回收
  pub fn set_pop<'a, D: wdev::Device>(
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

    match set_load_sync(store, key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => {}
      // C# NOTFOUND: parseState.Count == 2 (有 count) → WriteEmptySet（版本分派），否则 WriteNull ($-1\r\n)
      SetLoad::Missing => {
        if count_parameter != NO_COUNT {
          cs::write_set_len(output, 0, self.resp_protocol_version);
        } else {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
      SetLoad::Present(mut obj) => {
        let mut obj_out = run_operate(
          &mut obj,
          SetOperation::Spop,
          &[],
          count_parameter,
          0,
          self.resp_protocol_version,
          output,
        );
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
      }
    }
    Ok(true)
  }

  /// SMOVE source destination member
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetMove
  /// （存储侧语义对标 SetOps.SetMove）
  pub fn set_move<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "SMOVE");

    let source_key = parse_state[0];
    let destination_key = parse_state[1];
    let member = parse_state[2];

    let mut src = match set_load_sync(store, source_key, output) {
      SetLoad::Degrade => return Ok(false),
      // C# NOTFOUND → :0
      SetLoad::Missing => {
        output.extend_from_slice(cs::RESP_RETURN_VAL_0);
        return Ok(true);
      }
      SetLoad::WrongType => return Ok(true),
      SetLoad::Present(o) => o,
    };

    // If the keys are the same, no operation is performed.
    if source_key == destination_key {
      output.extend_from_slice(cs::RESP_RETURN_VAL_0);
      return Ok(true);
    }

    let mut dst = match set_load_sync(store, destination_key, output) {
      SetLoad::Degrade => return Ok(false),
      SetLoad::WrongType => return Ok(true),
      SetLoad::Missing => SetObject::new(),
      SetLoad::Present(o) => o,
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
    match set_save_or_gc(store, destination_key, &dst) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
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

  /// SINTERSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetIntersectStore
  pub fn set_intersect_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SINTERSTORE");

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = intersect_sets(&objs);
    combine_store(dst, &result, store, output)
  }

  /// SUNIONSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetUnionStore
  pub fn set_union_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SUNIONSTORE");

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = union_sets(&objs);
    combine_store(dst, &result, store, output)
  }

  /// SDIFFSTORE destination key [key ...]
  ///
  /// libs/server/Resp/Objects/SetCommands.cs:SetDiffStore
  pub fn set_diff_store<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2.., output, "SDIFFSTORE");

    let dst = parse_state[0];
    let objs = match load_many(store, &parse_state[1..], output) {
      Ok(Some(objs)) => objs,
      Ok(None) => return Ok(false),
      Err(()) => return Ok(true),
    };

    let result = diff_sets(&objs);
    combine_store(dst, &result, store, output)
  }
}

/// 多键装载（信封解码；缺失按空集合；WrongType 写错误行）
///
/// 返回 `Ok(None)` 表示磁盘候选须降级异步重放；`Err(())` 为错误行已写出
pub(super) fn load_many(
  store: &wkv::BatchStoreSession<impl wdev::Device>,
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
pub(super) fn intersect_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  result.set = first.set.clone();

  for obj in &objs[1..] {
    // intersection of anything with empty set is empty set
    if result.set.is_empty() {
      break;
    }
    result.set.retain(|m| obj.contains(m.as_slice()));
  }
  result
}

/// 集合求并（逐集并入，extend 单次插入免逐元素判重分支）
pub(super) fn union_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  for obj in objs {
    result.set.extend(obj.set.iter().cloned());
  }
  result
}

/// 集合求差（首集减去其余各集）
///
/// 对应 libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff 算法的本地集合求差
pub(super) fn diff_sets(objs: &[SetObject]) -> SetObject {
  let mut result = SetObject::new();
  let Some(first) = objs.first() else {
    return result;
  };
  result.set = first.set.clone();

  for obj in &objs[1..] {
    result.set.retain(|m| !obj.contains(m.as_slice()));
  }
  result
}

/// SINTER/SUNION/SDIFF 的 *STORE 公共收尾：空结果回收目标键，否则写回并回基数
///
/// 目标键为 SET 语义（清既有 key 级 TTL，对标 C# SetOps 的 SET 收尾），信封域
/// upsert 默认保留 TTL，非空写回前显式清退；空结果走 set_save_or_gc 删空臂
///（try_delete_sync 级联清 TTL，对齐 C# EXPIRE key 0）
fn combine_store<'a, D: wdev::Device>(
  dst: &[u8],
  result: &SetObject,
  store: &wkv::BatchStoreSession<'a, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  if !result.is_empty() {
    match del_ttl_sync(store, dst) {
      Ok(true) => {}
      // 环形页翻转：整体降级
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  match set_save_or_gc(store, dst, result) {
    Ok(true) => output.write_resp_int(result.set.len() as i64),
    Ok(false) => return Ok(false),
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  Ok(true)
}

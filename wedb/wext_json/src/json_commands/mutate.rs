use core::str;

use sonic_rs::{JsonValueMutTrait, JsonValueTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::{
  JsonCommands,
  common::{mutate_json_target, path_or_root},
};
use crate::{
  error::ERR_NUMBER_NOT_VALID_FLOAT,
  json_object::GarnetJsonObject,
  json_path::{JsonPath, val_from_f64},
};

/// 数值变异的结果节点形态
///
/// C# JSON 模块仅注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
/// JSON.NUMINCRBY / JSON.NUMMULTBY 为 wedb 侧 RedisJSON 兼容扩展，无 C# 同名对位。
///
/// 入参节点为整数且结果无小数位时落整数节点，否则落浮点节点：整值浮点
///（如 2.0）乘加后仍须保持浮点形态，故整数性由入参节点判定而非结果值判定
fn num_node(new_val: f64, from_integer: bool) -> Value {
  if from_integer && new_val.fract() == 0.0 {
    // 非负结果经 u64 落节点：> i64::MAX 的大正整数乘加后 as i64 会饱和
    return if new_val >= 0.0 {
      Value::from(new_val as u64)
    } else {
      Value::from(new_val as i64)
    };
  }
  val_from_f64(new_val)
}

/// NUMINCRBY/NUMMULTBY 共用入参解析：args[0]=路径、args[1]=数值
///
/// 任一校验失败即写出对应错误帧并回 None（调用方回 false，不回写 payload）；
/// `arity_err` 由调用点经 wrong_num_args! 编译期展开传入
fn parse_num_args<'a>(
  arity_err: &'static str,
  args: &[&'a [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'a str, f64)> {
  if args.len() != 2 {
    output.write_resp_error(arity_err);
    return None;
  }
  let Ok(path_str) = str::from_utf8(args[0]) else {
    output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  };
  let Ok(num) = str::from_utf8(args[1]).map(|s| s.parse::<f64>()) else {
    output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
    return None;
  };
  let Ok(num) = num else {
    output.write_resp_error(ERR_NUMBER_NOT_VALID_FLOAT);
    return None;
  };
  Some((path_str, num))
}

/// 数值节点乘加共用段：按节点形态取值套 `op` 后落回节点
///
/// 非数值节点（字符串/布尔/对象/数组/null）不改值，应答位补 null 占位，
/// 保证结果数组与 JSONPath 匹配数等长
fn num_apply(target: &mut Value, results: &mut Vec<Value>, op: impl FnOnce(f64) -> f64) {
  // 数值节点按形态取值：大于 i64::MAX 的大正整数 as_i64 回 None，
  // 须经 as_u64 分支承接以保住整数形态（否则落浮点节点）
  let updated = if let Some(i) = target.as_i64() {
    *target = num_node(op(i as f64), true);
    true
  } else if let Some(u) = target.as_u64() {
    *target = num_node(op(u as f64), true);
    true
  } else if let Some(f) = target.as_f64() {
    *target = num_node(op(f), false);
    true
  } else {
    false
  };
  results.push(if updated {
    target.clone()
  } else {
    Value::from(())
  });
}

/// 变异类命令共用执行管线（头尾单源见 common::mutate_json_target）：
/// 逐匹配节点 `mutate` → 序列化回写 payload，应答为结果数组 JSON 串（bulk string）
///
/// 前置校验失败即写出对应空/错误帧并回 true，此时绝不回写 payload
fn json_mutate_to_array(
  payload: &mut Vec<u8>,
  path_str: &str,
  resp_version: u8,
  output: &mut Vec<u8>,
  mut mutate: impl FnMut(&mut Value, &mut Vec<Value>),
) -> bool {
  mutate_json_target(payload, path_str, output, resp_version, |root, path| {
    let mut results = Vec::new();
    path.mutate(root, &mut |target| mutate(target, &mut results));
    results
  })
}

impl JsonCommands {
  pub const JSON_DEL: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::json_del_need_initial_update,
    updater: Self::json_del_updater,
    reader: Self::reject_write_only,
    not_found: Self::json_del_not_found,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_NUMINCRBY: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_numincrby_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_NUMMULTBY: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_nummultby_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_TOGGLE: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_toggle_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_CLEAR: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_clear_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.DEL ----
  fn json_del_need_initial_update(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_int(0);
    false
  }

  fn json_del_not_found(_args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
    output.write_resp_int(0);
  }

  fn json_del_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    if payload.is_empty() {
      output.write_resp_int(0);
      return true;
    }

    let mut obj = match GarnetJsonObject::from_slice(payload) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_int(0);
        return true;
      }
    };

    let path = args.first().copied();
    let count = obj.del(path);
    payload.clear();
    let _ = obj.serialize_object(payload);
    output.write_resp_int(count as i64);
    true
  }

  // ---- JSON.NUMINCRBY ----
  fn json_numincrby_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let Some((path_str, inc)) = parse_num_args(wrong_num_args!("json.numincrby"), args, output)
    else {
      return false;
    };
    json_mutate_to_array(
      payload,
      path_str,
      resp_version,
      output,
      |target, results| {
        num_apply(target, results, |x| x + inc);
      },
    )
  }

  // ---- JSON.NUMMULTBY ----
  fn json_nummultby_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let Some((path_str, mult)) = parse_num_args(wrong_num_args!("json.nummultby"), args, output)
    else {
      return false;
    };
    json_mutate_to_array(
      payload,
      path_str,
      resp_version,
      output,
      |target, results| {
        num_apply(target, results, |x| x * mult);
      },
    )
  }

  // ---- JSON.TOGGLE ----
  fn json_toggle_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    // 应答槽位与 JSONPath 匹配 1:1 等长（RedisJSON 2.0+ 契约）：非布尔目标不改值、该位补 null
    json_mutate_to_array(
      payload,
      path_or_root(args),
      resp_version,
      output,
      |target, results| match target.as_bool() {
        Some(b) => {
          *target = Value::from(!b);
          results.push(Value::from(!b));
        }
        None => results.push(Value::from(())),
      },
    )
  }

  // ---- JSON.CLEAR ----
  fn json_clear_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    let path_str = path_or_root(args);
    let mut obj = match GarnetJsonObject::from_slice(payload) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_int(0);
        return true;
      }
    };

    let Some(root) = obj.root_node.as_mut() else {
      output.write_resp_int(0);
      return true;
    };

    let Ok(json_path) = JsonPath::parse(path_str) else {
      output.write_resp_int(0);
      return true;
    };

    let mut cleared_count = 0;
    json_path.mutate(root, &mut |target| {
      if let Some(arr) = target.as_array_mut() {
        if !arr.is_empty() {
          arr.clear();
          cleared_count += 1;
        }
      } else if let Some(o) = target.as_object_mut() {
        if !o.is_empty() {
          o.clear();
          cleared_count += 1;
        }
      } else if let Some(i) = target.as_i64() {
        if i != 0 {
          *target = Value::from(0);
          cleared_count += 1;
        }
      } else if let Some(f) = target.as_f64()
        && f != 0.0
      {
        *target = val_from_f64(0.0);
        cleared_count += 1;
      }
    });

    payload.clear();
    let _ = obj.serialize_object(payload);
    output.write_resp_int(cleared_count);
    true
  }
}

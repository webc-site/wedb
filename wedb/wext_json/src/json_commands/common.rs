use core::str;

use sonic_rs::Value;
use wresp::{
  cmd_strings::{RESP_ERR_COMMAND_READ_ONLY, RESP_ERR_COMMAND_WRITE_ONLY},
  ext::RespVecExt,
};

use super::JsonCommands;
use crate::{
  error::{ERR_INVALID_JSON_PATH, ERR_JSON_DECODE_FAILED},
  json_object::GarnetJsonObject,
  json_path::JsonPath,
};

/// Reader 侧统一头尾（单源）：payload 反序列化 → 取根节点 → 解析 JSONPath，
/// 载荷解码失败 corrupt fail-fast 写错误帧回 false（reader false = 错误已写中止，
/// 对齐 wcustom CustomObjectFns 契约与同通道 roaring ERR_DECODE 先例），
/// 缺根/路径非法按原口径回 null；成功则 `f` 计算应答值，统一序列化写 bulk string
pub(super) fn eval_json_target<R, F>(
  payload: &[u8],
  path_str: &str,
  output: &mut Vec<u8>,
  resp_version: u8,
  f: F,
) -> bool
where
  F: FnOnce(&Value, &JsonPath) -> R,
  R: sonic_rs::Serialize,
{
  let obj = match GarnetJsonObject::from_slice(payload) {
    Ok(o) => o,
    Err(_) => {
      output.write_resp_error(ERR_JSON_DECODE_FAILED);
      return false;
    }
  };

  let Some(root) = obj.root_node.as_ref() else {
    output.write_resp_null_ver(resp_version);
    return true;
  };

  let Ok(json_path) = JsonPath::parse(path_str) else {
    output.write_resp_null_ver(resp_version);
    return true;
  };

  let out_bytes = sonic_rs::to_vec(&f(root, &json_path)).unwrap_or_default();
  output.write_resp_bulk_string(&out_bytes);
  true
}

/// Updater 侧统一头尾（单源）：payload 反序列化 → 取可变异根 → 解析 JSONPath，
/// 载荷解码失败 corrupt fail-fast 写错误帧回 false（updater false = 放弃落库，
/// 杜绝损坏载荷 Save 重写回库与重复 AOF 入账）、缺根回 null、路径非法回
/// ERR_INVALID_JSON_PATH（均 payload 不动），
/// 成功则 `f` 变异根并返回应答值，回写序列化对象后写 bulk string
pub(super) fn mutate_json_target<R, F>(
  payload: &mut Vec<u8>,
  path_str: &str,
  output: &mut Vec<u8>,
  resp_version: u8,
  f: F,
) -> bool
where
  F: FnOnce(&mut Value, &JsonPath, &mut Vec<u8>) -> Option<R>,
  R: sonic_rs::Serialize,
{
  let mut obj = match GarnetJsonObject::from_slice(payload) {
    Ok(o) => o,
    Err(_) => {
      output.write_resp_error(ERR_JSON_DECODE_FAILED);
      return false;
    }
  };

  let Some(root) = obj.root_node.as_mut() else {
    output.write_resp_null_ver(resp_version);
    return true;
  };

  let Ok(json_path) = JsonPath::parse(path_str) else {
    output.write_resp_error(ERR_INVALID_JSON_PATH);
    return true;
  };

  let Some(result) = f(root, &json_path, output) else {
    return true;
  };
  payload.clear();
  let _ = obj.serialize_object(payload);
  let out_bytes = sonic_rs::to_vec(&result).unwrap_or_default();
  output.write_resp_bulk_string(&out_bytes);
  true
}

/// 变异类命令可选路径参数：缺省或非法 UTF-8 一律按根路径处理
pub(super) fn path_or_root<'a>(args: &[&'a [u8]]) -> &'a str {
  args
    .first()
    .and_then(|p| str::from_utf8(p).ok())
    .unwrap_or("$")
}

impl JsonCommands {
  // ---- 共用闸口 helper（各族命令常量组合钩子时复用，禁在族文件里各抄一份）----

  pub(super) fn payload_is_empty(payload: &[u8]) -> bool {
    payload.is_empty()
  }

  pub(super) fn not_found_null(_args: &[&[u8]], output: &mut Vec<u8>, resp_version: u8) {
    output.write_resp_null_ver(resp_version);
  }

  pub(super) fn reject_read_only_initial(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  pub(super) fn reject_read_only_update(
    _payload: &mut Vec<u8>,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  pub(super) fn reject_write_only(
    _payload: &[u8],
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    output.write_resp_error(RESP_ERR_COMMAND_WRITE_ONLY);
    false
  }

  pub(super) fn reject_write_missing(
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    output.write_resp_null_ver(resp_version);
    false
  }
}

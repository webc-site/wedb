use core::str;

use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

/// 路径入参 UTF-8 严格解析：非法编码写语法错误帧并回 None（本文件四条命令共用）
fn path_arg<'a>(arg: &'a [u8], output: &mut Vec<u8>) -> Option<&'a str> {
  match str::from_utf8(arg) {
    Ok(s) => Some(s),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      None
    }
  }
}

use super::{
  JsonCommands,
  common::{eval_json_target, mutate_json_target, path_or_root},
};
use crate::json_object::parse_dom;

/// ARRINDEX 可选区间入参：缺位取 default（start 缺省 0，stop 缺省 0 = 检索至末尾），
/// 非十进制整数文本回 None 由调用方报语法错误
fn range_arg(args: &[&[u8]], idx: usize, default: i64) -> Option<i64> {
  match args.get(idx) {
    Some(&b) => str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()),
    None => Some(default),
  }
}

/// ARRINDEX 区间端点折算：负数自末尾起算后钳入 [0, len]
/// （口径同 C# modules/GarnetJSON/JSONPath/ArraySliceFilter.cs 的 startIndex / stopIndex 钳制）
fn clamp_index(raw: i64, len: i64) -> i64 {
  if raw < 0 { len + raw } else { raw }.clamp(0, len)
}

impl JsonCommands {
  pub const JSON_ARRLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_arrlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRAPPEND: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrappend_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRPOP: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrpop_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRINDEX: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_arrindex_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRINSERT: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrinsert_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_ARRTRIM: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_arrtrim_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.ARRLEN ----
  fn json_arrlen_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    eval_json_target(
      payload,
      path_or_root(args),
      output,
      resp_version,
      |root, path| {
        path
          .evaluate(root)
          .into_iter()
          .map(|m| m.as_array().map(|a| a.len()))
          .collect::<Vec<Option<usize>>>()
      },
    )
  }

  // ---- JSON.ARRAPPEND ----
  fn json_arrappend_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 {
      output.write_resp_error(wrong_num_args!("json.arrappend"));
      return false;
    }
    let Some(path_str) = path_arg(args[0], output) else {
      return false;
    };

    let mut values_to_append = Vec::new();
    for &val_bytes in &args[1..] {
      let Ok(v) = parse_dom(val_bytes) else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      };
      values_to_append.push(v);
    }

    mutate_json_target(payload, path_str, output, resp_version, |root, path| {
      // 应答槽位与 JSONPath 匹配 1:1 等长（RedisJSON 2.0+ 契约）：
      // 非数组目标不改值、该位补 null 占位，供调用方按下标对齐匹配项
      let mut lens: Vec<Option<usize>> = Vec::new();
      path.mutate(root, &mut |target| {
        if let Some(arr) = target.as_array_mut() {
          for v in &values_to_append {
            arr.push(v.clone());
          }
          lens.push(Some(arr.len()));
        } else {
          lens.push(None);
        }
      });
      lens
    })
  }

  // ---- JSON.ARRPOP ----
  fn json_arrpop_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let (path_str, pop_idx) = if args.is_empty() {
      ("$", -1)
    } else if args.len() == 1 {
      (str::from_utf8(args[0]).unwrap_or("$"), -1)
    } else {
      let p = str::from_utf8(args[0]).unwrap_or("$");
      let idx = str::from_utf8(args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(-1);
      (p, idx)
    };

    mutate_json_target(payload, path_str, output, resp_version, |root, path| {
      let mut popped = Vec::new();
      path.mutate(root, &mut |target| {
        if let Some(arr) = target.as_array_mut() {
          if arr.is_empty() {
            popped.push(Value::from(()));
            return;
          }
          let actual_idx = if pop_idx < 0 {
            arr.len() as i64 + pop_idx
          } else {
            pop_idx
          };
          if actual_idx >= 0 && (actual_idx as usize) < arr.len() {
            let item = arr
              .get(actual_idx as usize)
              .cloned()
              .unwrap_or(Value::from(()));
            arr.remove(actual_idx as usize);
            popped.push(item);
          } else {
            popped.push(Value::from(()));
          }
        } else {
          // 非数组目标不改值，应答位补 null 占位保槽位与匹配 1:1
          popped.push(Value::from(()));
        }
      });
      popped
    })
  }

  // ---- JSON.ARRINDEX ----
  ///
  /// C# JSON 模块仅注册 JSON.SET / JSON.GET（见 modules/GarnetJSON/JsonModule.cs），
  /// JSON.ARRINDEX 为 wedb 侧 RedisJSON 兼容扩展，无 C# 同名对位；start/stop
  /// 的负数折算与越界钳制沿用 C# JSONPath/ArraySliceFilter.cs 的索引口径
  fn json_arrindex_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 2 || args.len() > 4 {
      output.write_resp_error(wrong_num_args!("json.arrindex"));
      return false;
    }
    let Some(path_str) = path_arg(args[0], output) else {
      return false;
    };
    let target_val = match parse_dom(args[1]) {
      Ok(v) => v,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      }
    };
    let Some(start) = range_arg(args, 2, 0) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };
    let Some(stop) = range_arg(args, 3, 0) else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };

    eval_json_target(payload, path_str, output, resp_version, |root, path| {
      let mut indices: Vec<Option<i64>> = Vec::new();
      for m in path.evaluate(root) {
        // 目标节点非数组：应答位回 null，不参与检索
        let Some(arr) = m.as_array() else {
          indices.push(None);
          continue;
        };
        let len = arr.len() as i64;
        let s = clamp_index(start, len);
        // stop = 0 是 RedisJSON 的「检索至末尾」约定，非空区间
        let e = if stop == 0 {
          len
        } else {
          clamp_index(stop, len)
        };

        let mut idx: i64 = -1;
        if s < e {
          for (i, item) in arr.iter().enumerate().take(e as usize).skip(s as usize) {
            if item == &target_val {
              idx = i as i64;
              break;
            }
          }
        }
        indices.push(Some(idx));
      }
      indices
    })
  }

  // ---- JSON.ARRINSERT ----
  fn json_arrinsert_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() < 3 {
      output.write_resp_error(wrong_num_args!("json.arrinsert"));
      return false;
    }
    let Some(path_str) = path_arg(args[0], output) else {
      return false;
    };
    let Some(insert_idx) = str::from_utf8(args[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };

    let mut values_to_insert = Vec::new();
    for &val_bytes in &args[2..] {
      let Ok(v) = parse_dom(val_bytes) else {
        output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
        return false;
      };
      values_to_insert.push(v);
    }

    mutate_json_target(payload, path_str, output, resp_version, |root, path| {
      let mut lens: Vec<Option<usize>> = Vec::new();
      path.mutate(root, &mut |target| {
        let mut new_len = None;
        if let Some(arr) = target.as_array_mut() {
          let actual_idx = if insert_idx < 0 {
            arr.len() as i64 + insert_idx
          } else {
            insert_idx
          };
          if actual_idx >= 0 && (actual_idx as usize) <= arr.len() {
            for (pos, v) in (actual_idx as usize..).zip(&values_to_insert) {
              arr.insert(pos, v.clone());
            }
            new_len = Some(arr.len());
          }
        }
        // 非数组或索引越界均不改值，该位补 null 占位保槽位与匹配 1:1
        lens.push(new_len);
      });
      lens
    })
  }

  // ---- JSON.ARRTRIM ----
  fn json_arrtrim_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.len() != 3 {
      output.write_resp_error(wrong_num_args!("json.arrtrim"));
      return false;
    }
    let Some(path_str) = path_arg(args[0], output) else {
      return false;
    };
    let Some(start_idx) = str::from_utf8(args[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };
    let Some(stop_idx) = str::from_utf8(args[2])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return false;
    };

    mutate_json_target(payload, path_str, output, resp_version, |root, path| {
      let mut lens: Vec<Option<usize>> = Vec::new();
      path.mutate(root, &mut |target| {
        if let Some(arr) = target.as_array_mut() {
          let len = arr.len() as i64;
          let s = if start_idx < 0 {
            len + start_idx
          } else {
            start_idx
          }
          .max(0);
          let e = if stop_idx < 0 {
            len + stop_idx
          } else {
            stop_idx
          }
          .min(len - 1);
          if s > e || s >= len {
            arr.clear();
          } else {
            let mut new_arr = Vec::new();
            for (idx, item) in arr.iter().enumerate() {
              if (idx as i64) >= s && (idx as i64) <= e {
                new_arr.push(item.clone());
              }
            }
            arr.clear();
            for v in new_arr {
              arr.push(v);
            }
          }
          lens.push(Some(arr.len()));
        } else {
          // 非数组目标不改值，该位补 null 占位保槽位与匹配 1:1
          lens.push(None);
        }
      });
      lens
    })
  }
}

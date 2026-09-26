use sonic_rs::JsonContainerTrait;
use wcustom::CustomObjectFns;
use wresp::ext::RespVecExt;

use super::{
  JsonCommands,
  common::{eval_json_target, path_or_root},
};
use crate::json_object::GarnetJsonObject;

impl JsonCommands {
  pub const JSON_TYPE: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_type_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_OBJKEYS: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_objkeys_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_OBJLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_objlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.TYPE ----
  fn json_type_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    let obj = match GarnetJsonObject::from_slice(payload) {
      Ok(o) => o,
      Err(_) => {
        output.write_resp_null_ver(resp_version);
        return true;
      }
    };

    let path = args.first().copied();
    match obj.type_of(path) {
      Some(types) => {
        if types.len() == 1 && (path.is_none() || path == Some(b"$") || path == Some(b"")) {
          output.write_resp_simple_string(types[0]);
        } else {
          output.write_resp_array_len(types.len());
          for t in types {
            output.write_resp_simple_string(t);
          }
        }
      }
      None => output.write_resp_null_ver(resp_version),
    }
    true
  }

  // ---- JSON.OBJKEYS ----
  fn json_objkeys_reader(
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
        // 应答槽位与 JSONPath 匹配 1:1 等长：非对象目标该位补 null 占位
        let mut key_lists: Vec<Option<Vec<String>>> = Vec::new();
        for m in path.evaluate(root) {
          if let Some(o) = m.as_object() {
            key_lists.push(Some(o.iter().map(|(k, _)| k.to_string()).collect()));
          } else {
            key_lists.push(None);
          }
        }
        key_lists
      },
    )
  }

  // ---- JSON.OBJLEN ----
  fn json_objlen_reader(
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
          .map(|m| m.as_object().map(|o| o.len()))
          .collect::<Vec<Option<usize>>>()
      },
    )
  }
}

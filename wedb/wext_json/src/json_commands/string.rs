use core::str;

use sonic_rs::{JsonValueTrait, Value};
use wcustom::CustomObjectFns;
use wresp::{cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, ext::RespVecExt, wrong_num_args};

use super::{
  JsonCommands,
  common::{eval_json_target, mutate_json_target, path_or_root},
};

impl JsonCommands {
  pub const JSON_STRAPPEND: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_write_missing,
    updater: Self::json_strappend_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  pub const JSON_STRLEN: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::json_strlen_reader,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  // ---- JSON.STRAPPEND ----
  fn json_strappend_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    resp_version: u8,
  ) -> bool {
    if args.is_empty() || args.len() > 2 {
      output.write_resp_error(wrong_num_args!("json.strappend"));
      return false;
    }
    let (path_str, append_val) = if args.len() == 2 {
      (str::from_utf8(args[0]).unwrap_or("$"), args[1])
    } else {
      ("$", args[0])
    };

    let append_str = match sonic_rs::from_slice::<&str>(append_val) {
      Ok(s) => s,
      Err(_) => match str::from_utf8(append_val) {
        Ok(s) => s,
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return false;
        }
      },
    };

    mutate_json_target(payload, path_str, output, resp_version, |root, path| {
      // 应答槽位与 JSONPath 匹配 1:1 等长（RedisJSON 2.0+ 契约）：非字符串目标不改值、该位补 null
      let mut lens: Vec<Option<usize>> = Vec::new();
      path.mutate(root, &mut |target| {
        if let Some(s) = target.as_str() {
          let mut new_s = String::with_capacity(s.len() + append_str.len());
          new_s.push_str(s);
          new_s.push_str(append_str);
          *target = Value::from(new_s.as_str());
          lens.push(Some(new_s.len()));
        } else {
          lens.push(None);
        }
      });
      lens
    })
  }

  // ---- JSON.STRLEN ----
  fn json_strlen_reader(
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
          .map(|m| m.as_str().map(|s| s.len()))
          .collect::<Vec<Option<usize>>>()
      },
    )
  }
}

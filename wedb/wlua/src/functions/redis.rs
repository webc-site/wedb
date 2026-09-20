//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! redis 命令族：redis.call / redis.log / redis.setresp / ACL 检查与
//! 会话派发（对标 LuaRunner.Functions.cs 命令处理分支）。

use log::Level;
use wresp::{
  catalog::{expand_for_acls, try_get_resp_command_info_by_name},
  command::RespCommand,
  resp_memory_writer::RespWriter,
};

use crate::{
  LuaState,
  api::ScriptingApi,
  cache::SessionScriptCache,
  options::LuaLoggingMode,
  runner::{HostShared, lua_wrapped_error_view, process_resp_response_view},
  strings::ConstantStrings,
};

/// redis.log 的合法级别数。
const LOG_LEVELS: [f64; 4] = [0.0, 1.0, 2.0, 3.0];

use super::LuaRunnerFunctions;

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:NoSessionResponse
  ///
  /// 无会话的 redis.call（基准/测试路径）：吞参返回 nil。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn no_session_response(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    state.clear_stack();
    state.push_nil();
    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:GarnetCall
  ///
  /// redis.call 统一入口：C# 按事务形态分注册两个 trampoline，分别走
  /// transactionalGarnetApi / basicGarnetApi；事务形态（GarnetCallWithTransaction）
  /// 依赖的 TransactionalContext 内核在 wedb 不存在，该模式选项整链删除
  /// （task/done/lua-txn-mode-drop-placeholder.md），rust 恒承 C# 默认非事务形态
  /// （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:GarnetCallNoTransaction，
  /// 走 basicGarnetApi），redis.call 经会话分发逐条直提交。
  pub fn garnet_call(state: &mut LuaState, host: &mut HostShared) -> i32 {
    if host.session.is_none() {
      // C# 构造时以 GarnetCallNoSession 注册的等价形态。
      return Self::no_session_response(state, host);
    }
    Self::process_command_from_scripting(state, host)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:SHA1Hex
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn sha1_hex(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count != 1 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_WRONG_NUMBER_OF_ARGS);
    }

    let bytes = match state.type_name(1) {
      Some("string") => state.known_string_to_slice(1).unwrap_or_default(),
      Some("number") => {
        state.push_value(1);
        if !state.try_number_to_string() {
          state.pop(1);
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        let bytes = state.known_string_to_slice(-1).unwrap_or_default();
        let digest = SessionScriptCache::get_script_digest(bytes);
        state.pop(1);
        state.push_buffer(digest.as_str().as_bytes());
        return 1;
      }
      _ => &[],
    };

    // SessionScriptCache.GetScriptDigest：SHA1 → 40 字符小写 hex（一处定义）。
    let digest = SessionScriptCache::get_script_digest(bytes);
    state.push_buffer(digest.as_str().as_bytes());

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Log
  pub fn log(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count < 2 {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_REDIS_LOG_REQUIRED);
    }

    if state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_FIRST_ARG_MUST_BE_NUMBER);
    }

    let Some(raw_level) = state.check_number(1) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_FIRST_ARG_MUST_BE_NUMBER);
    };
    if !LOG_LEVELS.contains(&raw_level) {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_INVALID_DEBUG_LEVEL);
    }

    if host.log_mode == LuaLoggingMode::Disable {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_LOGGING_DISABLED);
    }

    // When shipped as a service, allowing arbitrary writes to logs is dangerous
    // so we support disabling it (while not breaking existing scripts)
    if host.log_mode == LuaLoggingMode::Silent {
      return 0;
    }

    // Construct and log the equivalent message
    let mut message = String::new();
    for arg_ix in 2..=arg_count {
      let mut append_slice = |bytes: &[u8]| {
        if !message.is_empty() {
          message.push(' ');
        }
        message.push_str(&String::from_utf8_lossy(bytes));
      };

      match state.type_name(arg_ix) {
        Some("string") => {
          if let Some(bytes) = state.known_string_to_slice(arg_ix) {
            append_slice(bytes);
          }
        }
        Some("number") => {
          state.push_value(arg_ix);
          if state.try_number_to_string()
            && let Some(bytes) = state.known_string_to_slice(-1)
          {
            append_slice(bytes);
          }
          state.pop(1);
        }
        _ => {}
      }
    }

    let log_level = match raw_level as i64 {
      0 => Level::Debug,
      1 => Level::Info,
      2 => Level::Warn,
      // We validated this above, so really it's just 3 but the switch needs to be exhaustive
      _ => Level::Error,
    };

    log::log!(log_level, "redis.log: {message}");

    0
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:SetResp
  pub fn set_resp(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_REDIS_SETRESP_ARG);
    }

    // C# 形态：栈 1 位须为数值类型，且取值为 2 或 3。
    if state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_RESP_VERSION);
    }
    let Some(num) = state.check_number(1).filter(|n| *n == 2.0 || *n == 3.0) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_RESP_VERSION);
    };

    if let Some(session) = host.session.as_mut() {
      session.get().update_resp_protocol_version(num as u8);
    }

    0
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:AclCheckCommand
  ///
  /// 有效性判定与权限检查全部下沉会话侧单点：查名走
  /// [`wresp::catalog::try_get_resp_command_info_by_name`]（externalOnly:
  /// false，含子命令表），成帧后经 [`ScriptingApi::parse_resp_command_buffer`]
  /// 解出 RespCommand，再过 [`ScriptingApi::check_acl_permissions`] 位图门。
  pub fn acl_check_command(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count == 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::PLEASE_SPECIFY_REDIS_CALL);
    }

    if state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
    }

    let cmd_span = state.known_string_to_slice(1).unwrap_or_default();

    // C# :2827：命令有效性判定 = 目录单点查名（externalOnly: false，
    // includeSubCommands: true），未命中即无效命令。
    let Some(info) =
      try_get_resp_command_info_by_name(&String::from_utf8_lossy(cmd_span), false, true)
    else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_INVALID_COMMAND);
    };

    let provided_resp_arg_count = (lua_arg_count - 1).max(0) as usize;

    let Some(session) = host.session.as_mut() else {
      // runner 模式无会话 ACL 面：视为允许（对标无 RespServerSession 形态）。
      state.pop(lua_arg_count as usize);
      state.push_boolean(true);
      return 1;
    };
    let mut session = session.get();

    // C# :2834-2836 三判据
    let is_bit_op_parent = info.command == RespCommand::Bitop && provided_resp_arg_count == 0;
    let has_sub_commands = !info.sub_commands.is_empty();
    let provides_sub_command = has_sub_commands && provided_resp_arg_count >= 1;

    let success = if is_bit_op_parent {
      // BITOP is _weird_: 无参形态需逐个子命令检查权限（子命令展开集单点
      // expand_for_acls，短名映射对标 C# :2855-2859 的 constStrs）。
      let mut success = true;
      for &sub in expand_for_acls(RespCommand::Bitop) {
        let name = match sub {
          RespCommand::BitopAnd => ConstantStrings::AND,
          RespCommand::BitopOr => ConstantStrings::OR,
          RespCommand::BitopXor => ConstantStrings::XOR,
          RespCommand::BitopNot => ConstantStrings::NOT,
          RespCommand::BitopDiff => ConstantStrings::DIFF,
          // 展开集恒为上五者（C# :2861 default 抛异常的同位断言）
          _ => unreachable!("expand_for_acls(BITOP) 恒为五枚子命令"),
        };
        match Self::acl_check_sub_command(state, &mut session, &mut host.scratch, info.arity, name)
        {
          Ok(true) => {}
          Ok(false) => {
            success = false;
            break;
          }
          Err(err) => return lua_wrapped_error_view(state, 1, err),
        }
      }
      success
    } else if has_sub_commands && !provides_sub_command {
      // 父命令带子命令但未提供子命令：ACL 覆盖的是实际（子）命令，
      // 逐子命令全查、全通过才 true（C# :2887-2978）。
      let mut success = true;
      for sub in &info.sub_commands {
        // C# :2915：子命令名取 '|' 后段（目录名小写，解析侧统一大写）
        let name = sub.name.rsplit_once('|').map_or(sub.name, |(_, tail)| tail);
        match Self::acl_check_sub_command(
          state,
          &mut session,
          &mut host.scratch,
          sub.arity,
          name.as_bytes(),
        ) {
          Ok(true) => {}
          Ok(false) => {
            success = false;
            break;
          }
          Err(err) => return lua_wrapped_error_view(state, 1, err),
        }
      }
      success
    } else {
      match Self::frame_and_acl_check(
        state,
        &mut session,
        &mut host.scratch,
        provided_resp_arg_count,
        info.arity,
      ) {
        Ok(ok) => ok,
        Err(err) => return lua_wrapped_error_view(state, 1, err),
      }
    };

    // We're done with these, so free up the space
    state.pop(lua_arg_count as usize);

    state.push_boolean(success);
    1
  }

  /// 子命令名压栈 → 成帧检查 → 弹栈（C# :2843-2867 / :2900-2947 的压栈-成帧-
  /// 弹栈序；父命令名恒在栈位 1，子命令短名压至栈位 2 以 `provided = 1` 成帧）。
  fn acl_check_sub_command(
    state: &mut LuaState,
    session: &mut impl ScriptingApi,
    scratch: &mut Vec<u8>,
    arity: i32,
    sub_name: &[u8],
  ) -> Result<bool, &'static [u8]> {
    state.push_buffer(sub_name);
    let res = Self::frame_and_acl_check(state, session, scratch, 1, arity);
    // Remove the sub command
    state.pop(1);
    res
  }

  /// C# AclCheckCommand 局部 PrepareAndCheckRespRequest（:3004）与三臂消费段
  /// （:2874-2882 / :2954-2962 / :2988-2993）的合并体：按 `arity` 宽容计数
  /// 裁补参数后成帧，经会话解析单点解出 RespCommand，再过 ACL 位图门。
  /// Err 为 Lua 错误文案（ERR_BAD_ARG / ERR_INVALID_COMMAND）。
  fn frame_and_acl_check(
    state: &mut LuaState,
    session: &mut impl ScriptingApi,
    scratch: &mut Vec<u8>,
    provided_resp_arg_count: usize,
    arity: i32,
  ) -> Result<bool, &'static [u8]> {
    // C# :3016-3018：min = |arity| - 1；负 arity 上不封顶，正 arity 封 arity - 1
    let min_resp_arg_count = (arity.unsigned_abs() as usize).saturating_sub(1);
    let max_resp_arg_count = if arity < 0 {
      usize::MAX
    } else {
      (arity as usize).saturating_sub(1)
    };
    let actual_resp_arg_count =
      provided_resp_arg_count.clamp(min_resp_arg_count, max_resp_arg_count);

    if !Self::prepare_and_check_resp_request(
      state,
      scratch,
      provided_resp_arg_count,
      actual_resp_arg_count,
    ) {
      return Err(ConstantStrings::ERR_BAD_ARG);
    }
    match session.parse_resp_command_buffer(scratch) {
      Some(cmd) => Ok(session.check_acl_permissions(cmd)),
      None => Err(ConstantStrings::ERR_INVALID_COMMAND),
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:PrepareAndCheckRespRequest
  ///
  /// 以 Lua 栈参数拼装 RESP 请求并校验参数类型；string 参数编码承接 C# 局部
  /// 函数 PrepareString（在 garnet 中的相对路径:libs/server/Lua/
  /// LuaRunner.Functions.cs:PrepareString）；拼装单点收敛至
  /// wresp::resp_memory_writer::RespWriter（恒 RESP2：请求面无 RESP3 形态）。
  ///
  /// 成帧单点同时承接 redis.call 回落（actual = provided，C#
  /// ProcessCommandFromScripting :3264-3298）与 redis.acl_check_cmd 的 arity
  /// 宽容成帧（C# AclCheckCommand 局部 PrepareAndCheckRespRequest :3004）：
  /// `provided` 为栈上实参数，`actual` 为目标参数数，超出 `provided` 的缺位
  /// 补空参（C# WriteArgument(default)）。
  pub fn prepare_and_check_resp_request(
    state: &mut LuaState,
    scratch: &mut Vec<u8>,
    provided_resp_arg_count: usize,
    actual_resp_arg_count: usize,
  ) -> bool {
    scratch.clear();
    let cmd_span = state.known_string_to_slice(1).unwrap_or_default();
    let mut writer = RespWriter::new_ref(scratch);
    writer.write_array_length(actual_resp_arg_count + 1);
    writer.write_bulk_string(cmd_span);

    for i in 0..actual_resp_arg_count {
      if i >= provided_resp_arg_count {
        // 缺位参数补空串（C# :3053-3056 WriteArgument(default)）
        writer.write_bulk_string(&[]);
        continue;
      }
      let stack_ix = 2 + i as i32;
      match state.type_name(stack_ix) {
        Some("nil") => writer.write_resp2_null(),
        Some("string") => {
          let span = state.known_string_to_slice(stack_ix).unwrap_or_default();
          writer.write_bulk_string(span);
        }
        Some("number") => {
          if let Some(num) = state.check_number(stack_ix) {
            if num.fract() == 0.0 && num >= i64::MIN as f64 && num <= i64::MAX as f64 {
              writer.write_int64_as_bulk_string(num as i64);
            } else {
              state.push_value(stack_ix);
              if !state.try_number_to_string() {
                state.pop(1);
                return false;
              }
              if let Some(span) = state.known_string_to_slice(-1) {
                writer.write_bulk_string(span);
              }
              state.pop(1);
            }
          } else {
            return false;
          }
        }
        _ => return false,
      }
    }

    true
  }

  /// SET 快路径（C# 对位 LuaRunner.Functions.ProcessCommandFromScripting 的 SET 分支；见 process_command_from_scripting 总入口）。
  ///
  /// key/value 形参判定顺序 1:1 对标 C#：string → known_string_to_slice；
  /// number → try_number_to_string_at 就地转串（失败回 OUT_OF_MEMORY，同 C# OutOfMemory 档）；
  /// 其它类型 → ERR_BAD_ARG。
  fn try_fast_path_set(state: &mut LuaState, host: &mut HostShared, arg_count: i32) -> Option<i32> {
    if arg_count != 3 {
      return None;
    }
    let cmd = state.known_string_to_slice(1)?;
    if !cmd.eq_ignore_ascii_case(b"SET") {
      return None;
    }
    let Some(session) = host.session.as_mut() else {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::NO_SESSION_AVAILABLE,
      ));
    };
    let mut session = session.get();
    if !session.check_acl_permissions(RespCommand::Set) {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::ERR_NO_PERM,
      ));
    }

    // number 槽先就地转串（转换需 &mut state，期间不持切片），全部结束后再统一取串。
    for slot in [2, 3] {
      match state.type_name(slot) {
        Some("string") => {}
        Some("number") => {
          if !state.try_number_to_string_at(slot) {
            return Some(lua_wrapped_error_view(
              state,
              1,
              ConstantStrings::OUT_OF_MEMORY,
            ));
          }
        }
        _ => {
          return Some(lua_wrapped_error_view(
            state,
            1,
            ConstantStrings::ERR_BAD_ARG,
          ));
        }
      }
    }

    let Some(key) = state.known_string_to_slice(2) else {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::ERR_BAD_ARG,
      ));
    };
    let Some(value) = state.known_string_to_slice(3) else {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::ERR_BAD_ARG,
      ));
    };

    if let Err(err) = session.set(key, value) {
      state.clear_stack();
      return Some(lua_wrapped_error_view(state, 1, &err));
    }

    state.clear_stack();
    state.push_buffer(ConstantStrings::OK);
    Some(1)
  }

  /// GET 快路径（C# 对位 LuaRunner.Functions.ProcessCommandFromScripting 的 GET 分支；见 process_command_from_scripting 总入口）。
  ///
  /// key 形参判定顺序同 SET：string → number（就地转串，失败 OUT_OF_MEMORY）→ 其它 ERR_BAD_ARG。
  fn try_fast_path_get(state: &mut LuaState, host: &mut HostShared, arg_count: i32) -> Option<i32> {
    if arg_count != 2 {
      return None;
    }
    let cmd = state.known_string_to_slice(1)?;
    if !cmd.eq_ignore_ascii_case(b"GET") {
      return None;
    }
    let Some(session) = host.session.as_mut() else {
      // 与 SET 快路径同文案（C# 本路径无空会话形态，Rust 托管面自洽选择）
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::NO_SESSION_AVAILABLE,
      ));
    };
    let mut session = session.get();
    if !session.check_acl_permissions(RespCommand::Get) {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::ERR_NO_PERM,
      ));
    }

    // number 槽先就地转串（转换需 &mut state），再取切片。
    match state.type_name(2) {
      Some("string") => {}
      Some("number") => {
        if !state.try_number_to_string_at(2) {
          return Some(lua_wrapped_error_view(
            state,
            1,
            ConstantStrings::OUT_OF_MEMORY,
          ));
        }
      }
      _ => {
        return Some(lua_wrapped_error_view(
          state,
          1,
          ConstantStrings::ERR_BAD_ARG,
        ));
      }
    }

    let Some(key) = state.known_string_to_slice(2) else {
      return Some(lua_wrapped_error_view(
        state,
        1,
        ConstantStrings::ERR_BAD_ARG,
      ));
    };

    let res = session.get(key);
    state.clear_stack();
    match res {
      Ok(Some(value)) => {
        state.push_buffer(&value);
        Some(1)
      }
      Ok(None) => {
        // Redis is weird, but false instead of Nil is correct here
        state.push_boolean(false);
        Some(1)
      }
      Err(err) => Some(lua_wrapped_error_view(state, 1, &err)),
    }
  }

  fn dispatch_scripting_command_fallback(
    state: &mut LuaState,
    host: &mut HostShared,
    arg_count: i32,
  ) -> i32 {
    let provided_resp_arg_count = (arg_count - 1).max(0) as usize;
    if !Self::prepare_and_check_resp_request(
      state,
      &mut host.scratch,
      provided_resp_arg_count,
      provided_resp_arg_count,
    ) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
    }

    // Once the request is formatted, we can release all the args on the Lua stack
    state.pop(arg_count as usize);

    let Some(session) = host.session.as_mut() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::NO_SESSION_AVAILABLE);
    };
    // 响应字节落入 host.response（与原始会话对象无重叠）。
    let mut session = session.get();
    session.dispatch_resp(&host.scratch, &mut host.response);
    let resp_protocol_version = session.resp_protocol_version();

    let result = process_resp_response_view(state, resp_protocol_version, &host.response);

    host.response.clear();

    result
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
  pub fn process_command_from_scripting(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;

    if arg_count <= 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::PLEASE_SPECIFY_REDIS_CALL);
    }

    if state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
    }

    // We special-case a few performance-sensitive operations to directly invoke via the storage API
    if let Some(res) = Self::try_fast_path_set(state, host, arg_count) {
      return res;
    }
    if let Some(res) = Self::try_fast_path_get(state, host, arg_count) {
      return res;
    }

    // As fallback, we format a RESP request and dispatch it through the session.
    Self::dispatch_scripting_command_fallback(state, host, arg_count)
  }
}

#[cfg(test)]
mod tests {
  use crate::cache::SessionScriptCache;

  #[test]
  fn sha1_digest_matches() {
    // sha1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
    assert_eq!(
      SessionScriptCache::get_script_digest(b"").as_str(),
      "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
  }
}

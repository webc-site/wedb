//! Lua 复合族 → RESP 写出（map / set / array）与写出单项调度
//! （try_write_single_item 为全类型族写出唯一调度点）
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSingleItem
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteMap
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSet
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteTableToArray

use wresp::{
  cmd_strings::{write_map_len, write_set_len},
  ext::is_resp3,
  resp_memory_writer::RespWriter,
};

use super::RespOut;
use crate::{runner::LuaRunner, strings::ConstantStrings};

impl LuaRunner {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSingleItem
  ///
  /// 写出栈顶单项并弹栈；返回是否完整写出（Vec 无界，恒真），
  /// 遭遇不可序列化错误时置 `err`。
  pub(super) fn try_write_single_item(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let Some(ret_type) = runner.state.type_name(cur_top) else {
      *err = Some(ConstantStrings::UNEXPECTED_ERROR);
      return false;
    };
    let is_nullish = matches!(ret_type, "nil" | "userdata" | "function" | "thread");

    if is_nullish {
      return if is_resp3(resp.protocol_version) {
        Self::try_write_resp3_null(runner, resp, err)
      } else {
        Self::try_write_resp2_null(runner, resp, err)
      };
    }

    match ret_type {
      "number" => Self::try_write_number(runner, resp, err),
      "string" => Self::try_write_string(runner, resp, err),
      "boolean" => {
        // 四象限单点判定（对标 C# LuaRunner.cs:TryWriteSingleItem 布尔臂
        // 1704-1747 分支序：Redis 实际行为——脚本返回布尔恒 :1/nil，仅
        // 脚本 setresp(3) 且连接 RESP3 才回 RESP3 布尔帧）
        match (resp.script_version == 3, is_resp3(resp.protocol_version)) {
          // 脚本 3 + 连接 2：:1/:0
          (true, false) => Self::write_boolean_as_integer(runner, resp, err),
          // 脚本 2 + 连接 3：true 回 :1，false 回 RESP3 null
          (false, true) => {
            if runner.state.to_boolean(cur_top) {
              Self::write_boolean_as_integer(runner, resp, err)
            } else {
              Self::try_write_resp3_null(runner, resp, err)
            }
          }
          // 脚本 3 + 连接 3：RESP3 有专属布尔类型
          (true, true) => Self::try_write_resp3_boolean(runner, resp, err),
          // RESP2 booleans are weird: false = null (bulk nil), true = 1
          (false, false) => {
            if runner.state.to_boolean(cur_top) {
              Self::write_boolean_as_integer(runner, resp, err)
            } else {
              Self::try_write_resp2_null(runner, resp, err)
            }
          }
        }
      }
      "table" => {
        // Redis does not respect metatables, so RAW access is ok here

        if Self::probe_table_key(runner, cur_top, ConstantStrings::DOUBLE, "number") {
          let fit = if is_resp3(resp.protocol_version) {
            Self::try_write_double(runner, resp, err)
          } else {
            // Force double to string for RESP2
            if !runner.state.try_number_to_string() {
              *err = Some(ConstantStrings::OUT_OF_MEMORY);
              return false;
            }
            Self::try_write_string(runner, resp, err)
          };
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }

        if Self::probe_table_key(runner, cur_top, ConstantStrings::MAP, "table") {
          let fit = Self::try_write_map(runner, resp, err);
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }

        if Self::probe_table_key(runner, cur_top, ConstantStrings::SET, "table") {
          let fit = Self::try_write_set(runner, resp, err);
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }

        // If the key "ok" is in there, we need to short circuit
        if Self::probe_table_key(runner, cur_top, ConstantStrings::OK_LOWER, "string") {
          let fit = Self::try_write_string(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }

        // If the key "err" is in there, we need to short circuit
        if Self::probe_table_key(runner, cur_top, ConstantStrings::ERR, "string") {
          let fit = Self::try_write_error(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }

        // Map this table to an array
        Self::try_write_table_to_array(runner, resp, err)
      }
      _ => {
        // All types should have been handled
        *err = Some(ConstantStrings::UNEXPECTED_ERROR);
        false
      }
    }
  }

  /// 表键探测骨架单源（C# 各臂重复的「压键常量 → RawGet → 类型判定 →
  /// 未命中 Pop」四连）：压 `key` 取 `table_ix` 表，栈顶值类型为 `want` 即命中。
  /// 命中时探测值留在栈顶（由写出臂弹出后再配对 Pop 表），未命中就地弹除。
  fn probe_table_key(runner: &mut LuaRunner, table_ix: i32, key: &[u8], want: &str) -> bool {
    runner.state.push_buffer(key);
    runner.state.raw_get(table_ix);
    if runner.state.type_name(-1) == Some(want) {
      return true;
    }
    runner.state.pop(1);
    false
  }

  /// map/set 共用的 lua_next 计数趟：压 nil 作起始键，每轮弹出值只留键进入
  /// 下一轮，返回键值对数。趟毕栈形与内联写法逐操作一致（残留探测 nil 由
  /// 第二趟再压一键覆盖语义，不动）。
  fn count_table_pairs(runner: &mut LuaRunner) -> usize {
    let mut size = 0usize;
    runner.state.push_nil();
    while runner.state.lua_next() {
      size += 1;
      // Remove value, we don't need it
      runner.state.pop(1);
    }
    size
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteMap
  ///
  /// C# 缓冲受限发送器路径另有降级形态 TryWriteMapToArray
  /// （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteMapToArray，
  /// map 逐对写成 key/value 交替数组并检查发送缓冲容量）；
  /// Rust 输出为 Vec 无界缓冲，无需容量判定与数组降级，直写 map 形态承接。
  fn try_write_map(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    // 计数趟：压 nil 起始键，lua_next 逐对计数后弹值（见 count_table_pairs）
    let map_size = Self::count_table_pairs(runner);

    // Write the map header
    write_map_len(resp.buf, map_size, resp.protocol_version);

    // Write the values out by traversing the table again
    runner.state.push_nil();
    while runner.state.lua_next() {
      // Copy key to top of stack
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }

      // Write (and remove) value out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSet
  ///
  /// C# 缓冲受限发送器路径另有降级形态 TryWriteSetToArray
  /// （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSetToArray，
  /// 仅写键集为数组并检查发送缓冲容量）；
  /// Rust 输出为 Vec 无界缓冲，无需容量判定与数组降级，直写 set 形态承接。
  fn try_write_set(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let set_size = Self::count_table_pairs(runner);

    // Write the set header
    write_set_len(resp.buf, set_size, resp.protocol_version);

    runner.state.push_nil();
    while runner.state.lua_next() {
      // Remove the value, it's ignored
      runner.state.pop(1);

      // Make a copy of the key
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key copy out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteTableToArray
  fn try_write_table_to_array(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_top = runner.state.get_top() as i32;

    // Lua # operator - this MAY stop at nils (raw length)
    let max_len = runner.state.raw_len(table_top) as usize;

    // Find the TRUE length by scanning for nils
    let mut true_len = 0usize;
    while true_len < max_len {
      // 表下标经类型分支先行校验，raw_get_integer 恒命中。
      runner.state.raw_get_integer(table_top, true_len as i64 + 1);
      let is_nil = runner.state.type_name(-1) == Some("nil");
      runner.state.pop(1);

      if is_nil {
        break;
      }
      true_len += 1;
    }

    RespWriter::new_ref(resp.buf).write_array_length(true_len);

    for i in 1..=true_len {
      // Push item at index i onto the stack
      runner.state.raw_get_integer(table_top, i as i64);

      // Write the item out, removing it from the stack
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }
}

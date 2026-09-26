//! 哈希写命令实现（HSET, HSETNX, HMSET, HDEL, HINCRBY 族, HEXPIRE 族, HPERSIST）

use wcol::hash::hash_object::HashOperation;
use wdev::Device;
use wresp::{check_args::check_arg_count, cmd_strings as cs};

use super::Rmw;
use crate::resp::{
  objects::object_store_utils::{
    ElementHeaderKind, parse_elements_only_args, parse_expire_elements_args,
    write_rmw_aof_fail_frame, write_rmw_reply,
  },
  resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// HSET key field value [field value ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashSet（HSET 形态，回复新增字段数）
  pub fn hash_set<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hset)
  }

  /// C# HashSet 共用入口（HSETNX 命令分支；精确锚点见本文件 158 行）
  ///
  /// HSETNX 入口，调用 hash_set_by_command(HashOperation::Hsetnx)
  pub fn hash_set_nx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hsetnx)
  }

  /// HMSET key field value [field value ...]（废弃别名，回复 +OK）
  ///
  /// C# HashSet 共用入口（HMSET 命令分支；精确锚点见本文件 158 行）
  ///
  /// HMSET 入口，调用 hash_set_by_command(HashOperation::Hmset)
  pub fn hash_set_map<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.hash_set_by_command(parse_state, store, output, HashOperation::Hmset)
  }

  /// HSET/HSETNX/HMSET 公共体
  fn hash_set_by_command<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    op: HashOperation,
  ) -> wresp::Result<bool> {
    let cmd_name = match op {
      HashOperation::Hsetnx => "HSETNX",
      HashOperation::Hmset => "HMSET",
      _ => "HSET",
    };
    let valid = if op == HashOperation::Hsetnx {
      parse_state.len() == 3
    } else {
      parse_state.len() > 1 && parse_state.len() % 2 == 1
    };
    check_arg_count!(valid, output, cmd_name);

    let key = parse_state[0];
    let args = &parse_state[1..];
    match self.hash_rmw(store, key, op, args, (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // HMSET 回复 +OK；其余回新增字段数（仅 result1，无负载，整数补写臂转调单源）
      Rmw::Present(done) => {
        if op == HashOperation::Hmset {
          if !done.payload_written {
            output.extend_from_slice(cs::RESP_OK);
          }
        } else {
          write_rmw_reply(done, output);
        }
      }
      // AofFail：信封写已生效、增量条目入队失败，落错误帧拒绝本命令
      //（AofEnqueue 契约，禁 +OK/:1 假成功；不回滚内存、不转异步重放）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
    }
    Ok(true)
  }

  /// HDEL key field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashDelete
  pub fn hash_delete<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# HashDelete 仅 Count < 1（:395）：HDEL key 零字段合法，两态回 :0
    check_arg_count!(parse_state, 1.., output, "HDEL");
    let key = parse_state[0];
    match self.hash_rmw(
      store,
      key,
      HashOperation::Hdel,
      &parse_state[1..],
      (0, 0),
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出（补写臂转调单源）
      Rmw::Present(done) => write_rmw_reply(done, output),
      // AofFail：写已生效、入账失败，落错误帧拒绝（AofEnqueue 契约单点收尾）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
    }
    Ok(true)
  }

  /// HINCRBY key field increment / HINCRBYFLOAT key field increment
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashIncrement
  pub fn hash_increment<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_float: bool,
  ) -> wresp::Result<bool> {
    let name = if is_float { "HINCRBYFLOAT" } else { "HINCRBY" };
    check_arg_count!(parse_state, 3, output, name);

    let key = parse_state[0];
    let op = if is_float {
      HashOperation::Hincrbyfloat
    } else {
      HashOperation::Hincrby
    };
    match self.hash_rmw(store, key, op, &parse_state[1..], (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      // AofFail：计数写已生效、入账失败，落错误帧拒绝（HINCRBY/HINCRBYFLOAT
      // 非幂等，严禁转异步重放二次施加，AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      // WRONGTYPE 帧 / Present 负载与整数补写均由骨架与 run_operate 透写闭环
      _ => {}
    }
    Ok(true)
  }

  /// HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashExpire
  pub fn hash_expire<'a, D: Device>(
    &mut self,
    cmd_name: &'static str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_milliseconds: bool,
    is_timestamp: bool,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some(args) = parse_expire_elements_args(
      cmd_name,
      parse_state,
      ElementHeaderKind::Fields,
      is_milliseconds,
      is_timestamp,
      output,
    ) else {
      return Ok(true);
    };

    // C# NOTFOUND：numFields 个 -2 数组；键缺失时对象层不可达（rmw Missing 分支以
    // 空对象执行，逐字段回 -2 语义一致），应答全由骨架透写，仅降级臂转异步重放
    match self.hash_rmw(
      store,
      args.key,
      HashOperation::Hexpire,
      args.elements,
      args.args12,
      output,
    ) {
      Rmw::Degrade => return Ok(false),
      // AofFail：写已生效、入账失败，落错误帧拒绝（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      _ => {}
    }
    Ok(true)
  }

  /// HPERSIST key FIELDS numfields field [field ...]
  ///
  /// libs/server/Resp/Objects/HashCommands.cs:HashPersist
  ///
  /// C# HashPersist → RMWObjectStoreOperation
  /// （libs/server/Storage/Session/ObjectStore/HashOps.cs:HashPersist），
  /// 对象层 HashPersist 先 DeleteExpiredItems 物理剔除，走 rmw 骨架使
  /// 剔除与持久化移除一并落盘
  pub fn hash_persist<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导单源（快慢共用，失败帧已写出）
    let Some((key, fields)) =
      parse_elements_only_args("HPERSIST", parse_state, ElementHeaderKind::Fields, output)
    else {
      return Ok(true);
    };

    // C# NOTFOUND：对象层以空对象执行，逐字段 -2 数组（payload 经骨架透写），
    // WRONGTYPE 错误帧已落即闭环，仅降级臂转异步重放
    match self.hash_rmw(store, key, HashOperation::Hpersist, fields, (0, 0), output) {
      Rmw::Degrade => return Ok(false),
      // AofFail：写已生效、入账失败，落错误帧拒绝（AofEnqueue 契约）
      Rmw::AofFail => {
        write_rmw_aof_fail_frame(output);
        return Ok(true);
      }
      _ => {}
    }
    Ok(true)
  }
}

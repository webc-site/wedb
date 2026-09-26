//! 自定义对象命令（对标 libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd
//! 会话执行入口，经 libs/server/Custom/CustomRespCommands.cs 的 CustomObjCmd 实现体
//! 与 CustomCommandRef 会话侧投影）。C# 的 NetworkCustomTxn /
//! NetworkCustomProcedure / NetworkCustomRawStringCmd 三族随动态注册管理层按转写规范
//! 删除：其 RespCommand 值是模块/REGISTERCS 注册期由解析器回填的内部分派哨兵，rust
//! 解析器对扩展命令只回填 Customobjcmd（编译期静态清单），无路径产出其余三者；事务过程
//! 入口单点收敛到 RUNTXP（见 resp_server_session/txn.rs 与 txn_resp_commands.rs）。

use std::fmt::{self, Formatter};

use wcustom::{CommandType, CustomObjectFns, KeyScope};
use wdev::Device;
use wresp::{
  cmd_strings::{self as cs},
  command::RespCommand,
};
use wval::CustomObjectType;

use super::core::{RespServerSession, is_command_arity_valid_checked};

/// 自定义命令引用（C# currentCustomTransaction / CustomProcedure /
/// CustomRawStringCommand / CustomObjCmd 的会话侧投影）。扩展命令编译期
/// 静态接线：解析期经 resp::custom_objects 静态清单取全量执行面入槽，
/// 会话侧零克隆零回查。
#[derive(Clone)]
pub struct CustomCommandRef {
  /// 命令名（静态清单规范形，零分配）
  pub name: &'static str,
  /// 命令类型（Read / ReadModifyWrite）
  pub command_type: CommandType,
  /// 键作用域（单键 / 多键读；静态清单形态位，执行面按此分派）
  pub key_scope: KeyScope,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
  /// 对象信封类型标签（wval::CustomObjectType 分配单点，经 wcustom
  /// CustomObjectEntry 静态描述清单流转；parse→exec 全程保持枚举，
  /// 仅在信封编解码边界收窄为 u8 线域）
  pub object_tag: CustomObjectType,
  /// 静态执行体（编译期函数指针集）
  pub fns: CustomObjectFns,
}

impl fmt::Debug for CustomCommandRef {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("CustomCommandRef")
      .field("name", &self.name)
      .field("command_type", &self.command_type)
      .field("key_scope", &self.key_scope)
      .field("arity", &self.arity)
      .field("object_tag", &self.object_tag)
      .finish_non_exhaustive()
  }
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd
  ///
  /// 自定义对象命令执行入口（arity 校验 → 注册表解析 → 存储执行域分派）。
  /// 同步降级（磁盘候选）时保留 `current_custom_command` 槽供慢路径快照
  /// 命令名（C# currentCustomObjectCommand 的承接形态），由执行域
  /// `StoreGarnetApi::exec` 消费后清槽。
  pub fn network_custom_obj_cmd<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((_, custom)) = self.current_custom_command.take() else {
      return Ok(true);
    };
    if !is_command_arity_valid_checked(custom.arity, self.parse_state.count) {
      cs::abort_with_wrong_number_of_arguments(output, custom.name);
      self.command_error_written = true;
      return Ok(true);
    }
    // 键与命令入参由静态清单键作用域拆定（单键与多键读同一分派面，会话
    // 执行臂不再按命令名比串特判）；拆不出键 = 参数域不足，按 arity 同款口径报错
    let Some(args) = custom.key_scope.split(parse_state) else {
      cs::abort_with_wrong_number_of_arguments(output, custom.name);
      self.command_error_written = true;
      return Ok(true);
    };

    // 自定义对象执行体的 nil 帧随会话协议（版本在调用点裁决，执行体不自存状态）
    let resp_version = self.resp_protocol_version;

    // 信封类型标签 + 执行面均为编译期静态取用（解析期已入槽，零锁零克隆）
    #[cfg(any(feature = "roaring", feature = "json"))]
    let done = {
      use crate::resp::objects::custom_object_commands::{
        CustomObjOutcome, CustomObjectCall, try_custom_object_command,
      };
      match try_custom_object_command(
        store,
        CustomObjectCall {
          cmd_type: custom.command_type,
          args,
          tag: custom.object_tag,
          fns: &custom.fns,
          resp_version,
        },
        output,
      ) {
        CustomObjOutcome::Done => true,
        // 降级异步重放：命令槽回填供 exec 快照命令名，本次不残留输出
        CustomObjOutcome::Degrade => {
          self.current_custom_command = Some((RespCommand::Customobjcmd, custom));
          false
        }
      }
    };
    // 未配置扩展特性：解析面无静态清单可命中，本分支不可达兜底明确报错，
    // 绝不静默
    #[cfg(not(any(feature = "roaring", feature = "json")))]
    let done = {
      let _ = (custom, args, store);
      log::error!("自定义对象命令执行域未配置，被拒绝");
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      self.command_error_written = true;
      true
    };

    if done { Ok(true) } else { Ok(false) }
  }
}

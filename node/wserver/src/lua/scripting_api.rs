//! 脚本命令面：LuaRunner / LuaCommands 与会话层的对接契约
//! （对标 C# RespServerSession 上 basicGarnetApi / transactionalGarnetApi
//! 与 TryConsumeMessages + ScratchBufferNetworkSender 的组合面）。

use super::scratch_buffer_network_sender::ScratchBufferNetworkSender;

/// 会话命令面（redis.call 的落地出口）。
///
/// C# 的 ProcessCommandFromScripting 把 Lua 参数格式化为 RESP 请求后经
/// `RespServerSession.TryConsumeMessages` 处理，响应字节落入
/// `ScratchBufferNetworkSender`；SET/GET 走 IGarnetApi 特例直连。
/// RESP 域（并行转写域）后续以其会话类型实现本 trait 接线。
pub trait ScriptingApi {
  /// 分派 RESP 请求，响应字节写入 `sender`（对标 TryConsumeMessages）。
  fn dispatch_resp(&mut self, request: &[u8], sender: &mut ScratchBufferNetworkSender);

  /// GET 特例（对标 api.GET；命中返回值）。
  ///
  /// Err 为同步面无法闭环的降级形态（冷键需异步 I/O 等），上抛为脚本错误。
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str>;

  /// SET 特例（对标 api.SET）。
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str>;

  /// 当前 RESP 协议版本。
  fn resp_protocol_version(&self) -> u8;

  /// 更新 RESP 协议版本（redis.setresp / 脚本回 RESP2）。
  fn update_resp_protocol_version(&mut self, version: u8);

  /// ACL 权限检查（对标 CheckACLPermissions）。
  fn check_acl_permissions(&self, command: &str) -> bool;

  /// 事务模式切换（对标 SetTransactionMode；默认空操作）。
  fn set_transaction_mode(&mut self, enabled: bool) {
    let _ = enabled;
  }

  /// 事务开始（对标 BeginTransaction；默认空操作）。
  fn begin_transaction(&mut self) {}

  /// 事务结束（对标 EndTransaction；默认空操作）。
  fn end_transaction(&mut self) {}
}

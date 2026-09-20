//! 脚本命令面：LuaRunner / LuaCommands 与会话层的对接契约
//! （对标 C# RespServerSession 上 basicGarnetApi / transactionalGarnetApi
//! 与 TryConsumeMessages 的组合面；C# 因 Socket 流需 INetworkSender
//! 形状的 ScratchBufferNetworkSender 占位，Rust 侧 RESP 输出纯属内存
//! 缓冲，应答直收 `&mut Vec<u8>`，无虚设抽象）。

use wresp::command::RespCommand;

/// 会话命令面（redis.call 的落地出口）。
pub trait ScriptingApi {
  /// 分派 RESP 请求，响应字节追加写入 `response`（对标 TryConsumeMessages）。
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>);

  /// GET 特例（对标 api.GET；命中返回值）。
  ///
  /// 错误臂携带真实错误帧文本（'-' 头与 CRLF 已剥离）：redis.call 快路径
  /// 错误以脚本错误中止，文本与 fallback 臂的 RESP 错误帧透传一致。
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Vec<u8>>;

  /// SET 特例（对标 api.SET；错误臂同 GET）。
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Vec<u8>>;

  /// 当前 RESP 协议版本。
  fn resp_protocol_version(&self) -> u8;

  /// 更新 RESP 协议版本（redis.setresp / 脚本回 RESP2）。
  fn update_resp_protocol_version(&mut self, version: u8);

  /// 独立缓冲解析（校验用途；不写错误应答）——redis.acl_check_cmd 的
  /// 有效性判定经会话解析单点承接，None 即 C# INVALID 语义。
  fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand>;

  /// ACL 权限检查（对标 CheckACLPermissions 的 RespCommand 重载；
  /// 命令名解析归 parse_resp_command_buffer，本面只做位图门）。
  fn check_acl_permissions(&self, command: RespCommand) -> bool;
}

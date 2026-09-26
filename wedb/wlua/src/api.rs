//! 脚本命令面：LuaRunner / LuaCommands 与会话层的对接契约
//! （对标 C# RespServerSession 上 basicGarnetApi / transactionalGarnetApi
//! 与 TryConsumeMessages 的组合面；C# 因 Socket 流需 INetworkSender
//! 形状的 ScratchBufferNetworkSender 占位，Rust 侧 RESP 输出纯属内存
//! 缓冲，应答直收 `&mut Vec<u8>`，无虚设抽象）。

use wresp::command::RespCommand;

/// GET/SET 特例回包错误形态（对标会话侧 `wresp::read::ReplyError` 两态的
/// owned 投影：错误帧文本按 C# 语义对脚本不可见，故不携载荷；另含窗内
/// 改权收口的 rust 自有形态 `AclChanged`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptApiError {
  /// `-TEXT\r\n` 错误应答帧（存储面拒绝操作）。C# 存储 API 直连无错误帧
  /// 形态：GET 非 OK 一律折 false、SET 显式丢弃 status 恒答 +OK
  /// （LuaRunner.Functions.cs:3246-3259 / :3214-3216），快路径 Err 臂按此折回。
  ErrorReply,
  /// 应答不可解析（协议级损伤）。无 C# 对应态（直连 API 不产帧），
  /// 保持上抛 Lua 错误。
  Protocol,
  /// 脚本执行窗内并发 ACL 改权命中门链停车的确定性收口形态（deviations
  /// §159）：挂载代数落后且刷新臂窗口内不可达，本条 redis.call 零存储执行、
  /// 以 [`Self::ACL_CHANGED_TEXT`] 错误中断脚本。§97 折叠臂不适用（既非
  /// C# 非 OK 折 false 形亦非协议损伤形），两快路径 Err 臂一律上抛该错误。
  AclChanged,
}

impl ScriptApiError {
  /// 协议级错误的 Lua 错误文案（rust 侧自定：本形态无 C# 常量对位）。
  pub const PROTOCOL_TEXT: &[u8] = b"protocol error";
  /// 窗内改权收口的 Lua 错误文案单源（三消费面统一：fallback 经
  /// dispatch_resp 环尾错误帧、快路经 `AclChanged`、acl_check_cmd 经
  /// `acl_mount_stale` 预门，见 deviations §159）。
  pub const ACL_CHANGED_TEXT: &str =
    "ERR ACL configuration changed during script execution, please retry the script";
}

/// 会话命令面（redis.call 的落地出口）。
pub trait ScriptingApi {
  /// 分派 RESP 请求，响应字节追加写入 `response`（对标 TryConsumeMessages）。
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>);

  /// GET 特例（对标 api.GET；命中返回值，miss → `Ok(None)` 即 Lua false）。
  ///
  /// 错误臂只分形态不定文案：`ErrorReply` 由快路径折回 C# 非 OK 恒 false
  /// 语义，`Protocol` 与 `AclChanged` 上抛 Lua 错误（后者为窗内改权停车
  /// 收口帧，见 [`ScriptApiError::AclChanged`]）。
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError>;

  /// SET 特例（对标 api.SET；`ErrorReply` 按 C# 丢弃 status 恒 +OK 折叠，
  /// 错误臂形态同 GET）。
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), ScriptApiError>;

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

  /// 挂载句柄陈旧预门（无副作用现读，deviations §159）：true = 引擎 ACL
  /// 代数已在本脚本窗开启后推进，`check_acl_permissions` 的位图裁决来自
  /// 落后挂载、不可再作终审（C# 共享句柄逐条现读无此态），调用面须改以
  /// [`ScriptApiError::ACL_CHANGED_TEXT`] 错误收口。默认 false = 实现侧无
  /// 陈旧判定面（无会话/回放形态）。
  fn acl_mount_stale(&self) -> bool {
    let _ = self;
    false
  }

  /// 脚本挂起让渡标记（协程化挂起协议）：`dispatch_resp` 消费命中阻塞/
  /// 慢路径挂起体时由会话侧置位并让渡挂起体，redis.call 收尾读取——Some(tag)
  /// 即挂起协程（tag 压协程栈作让渡值，挂起体由外层 resume 循环按 tag 分派
  /// await 驱动）。读取即复位；None = 正常收尾。
  fn take_script_yield(&mut self) -> Option<i32> {
    let _ = self;
    None
  }
}

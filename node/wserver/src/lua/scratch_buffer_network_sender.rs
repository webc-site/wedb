//! 假网络发送器：读写在固定内存缓冲中完成
//! （对标 libs/server/Lua/ScratchBufferNetworkSender.cs:ScratchBufferNetworkSender）。
//!
//! C# 经 INetworkSender 把 redis.call 的响应字节写进 ScratchBufferBuilder
//! 的剩余空间（head/tail 裸指针视图）；Rust 侧以安全切片视图承接同语义：
//! [`ScratchBufferNetworkSender::spare_capacity_mut`] 写入 +
//! [`ScratchBufferNetworkSender::send_response`] 推进有效区。

/// 发送缓冲上限（对标 BufferSizeUtils.ServerBufferSize 默认档位）。
pub const SERVER_BUFFER_SIZE: usize = 16 * 1024;

/// 最大尺寸设置（C# MaxSizeSettings 的接收/发送档位）。
#[derive(Debug, Clone, Copy)]
pub struct MaxSizeSettings {
  /// 接收缓冲尺寸。
  pub receive_buffer_size: usize,
  /// 发送缓冲尺寸。
  pub send_buffer_size: usize,
  /// 最大发送页尺寸。
  pub max_send_size: usize,
}

impl Default for MaxSizeSettings {
  fn default() -> Self {
    Self {
      receive_buffer_size: 512 * 1024,
      send_buffer_size: 16 * 1024,
      max_send_size: 16 * 1024,
    }
  }
}

/// 假网络发送器：响应字节累积于内存缓冲。
pub struct ScratchBufferNetworkSender {
  /// 已写入的有效字节（有效区 = buffer[..offset]）。
  buffer: Vec<u8>,
  /// 有效区长度（C# ScratchBufferBuilder 的 offset 语义）。
  offset: usize,
  /// 最大尺寸设置。
  max_size_settings: MaxSizeSettings,
  /// 服务端缓冲容量。
  server_buffer_size: usize,
}

impl Default for ScratchBufferNetworkSender {
  fn default() -> Self {
    Self::new()
  }
}

impl ScratchBufferNetworkSender {
  /// Create a new dummy network sender with a simple in-memory buffer
  ///
  /// libs/server/Lua/ScratchBufferNetworkSender.cs:ScratchBufferNetworkSender
  pub fn new() -> Self {
    let max_size_settings = MaxSizeSettings::default();
    Self {
      buffer: Vec::with_capacity(SERVER_BUFFER_SIZE),
      offset: 0,
      server_buffer_size: SERVER_BUFFER_SIZE,
      max_size_settings,
    }
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:Reset
  ///
  /// 清空有效区（C# ScratchBufferBuilder.Reset）。
  pub fn reset(&mut self) {
    self.buffer.clear();
    self.offset = 0;
  }

  /// 服务端缓冲容量（构造时由 MaxSizeSettings 折算）。
  pub fn server_buffer_size(&self) -> usize {
    self.server_buffer_size
  }

  /// 最大尺寸设置。
  pub fn max_size_settings(&self) -> &MaxSizeSettings {
    &self.max_size_settings
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:GetResponse
  ///
  /// 取当前已写出的完整响应字节（ViewFullArgSlice）。
  pub fn get_response(&self) -> &[u8] {
    &self.buffer[..self.offset]
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:IsLocalConnection
  ///
  /// 假发送器恒为本机连接。
  pub fn is_local_connection(&self) -> bool {
    true
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:DisposeNetworkSender
  ///
  /// 内存缓冲无需清理（C# 空方法）。
  pub fn dispose_network_sender(&mut self, _wait_for_send_completion: bool) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:Enter
  ///
  /// 进入响应写区（C# 空方法）。
  pub fn enter(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:EnterAndGetResponseObject
  ///
  /// 剩余可写空间的安全视图（C# head..tail 裸指针区间的切片形态）。
  /// 写入后经 [`Self::send_response`] 提交推进有效区。
  pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
    self.ensure_capacity();
    &mut self.buffer[self.offset..self.server_buffer_size]
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:EnterAndGetResponseObject
  ///
  /// 剩余可写空间视图（C# head..tail 裸指针区间的安全形态）：
  /// 返回 (可写切片, 可写字节数)；写入后经 [`Self::send_response`] 提交。
  pub fn enter_and_get_response_object(&mut self) -> (&mut [u8], usize) {
    let spare = self.spare_capacity_mut();
    let tail = spare.len();
    (spare, tail)
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:GetResponseObjectHead
  ///
  /// 剩余空间首指针（C# head 裸指针形态；写入经 spare_capacity_mut 安全承接）。
  pub fn get_response_object_head(&mut self) -> *mut u8 {
    self.ensure_capacity();
    unsafe { self.buffer.as_mut_ptr().add(self.offset) }
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:GetResponseObjectTail
  ///
  /// 剩余空间尾指针。
  pub fn get_response_object_tail(&mut self) -> *const u8 {
    self.ensure_capacity();
    unsafe { self.buffer.as_ptr().add(self.server_buffer_size) }
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:Exit
  ///
  /// 退出响应写区（C# 空方法）。
  pub fn exit(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:ExitAndReturnResponseObject
  ///
  /// 交还响应对象（C# 空方法）。
  pub fn exit_and_return_response_object(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:GetResponseObject
  ///
  /// 取响应对象（C# 空方法）。
  pub fn get_response_object(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:ReturnResponseObject
  ///
  /// 归还响应对象（C# 空方法）。
  pub fn return_response_object(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:SendCallback
  ///
  /// 发送回调（C# 空方法）。
  pub fn send_callback(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:SendResponse
  ///
  /// 以 `offset`（相对有效区末尾的起始）+ `size` 推进有效区
  /// （C# ScratchBufferBuilder.MoveOffset(offset + size)）。
  pub fn send_response(&mut self, offset: usize, size: usize) -> bool {
    let advanced = self.offset.saturating_add(offset + size);
    self.offset = advanced.min(self.server_buffer_size);
    true
  }

  /// 直写响应字节（RespWriteUtils 写入面的安全承接，
  /// ScriptingApi 实现方把序列化结果直接落入发送缓冲）。
  pub fn write_response_bytes(&mut self, bytes: &[u8]) {
    self.buffer.extend_from_slice(bytes);
    self.offset = self.buffer.len().min(self.server_buffer_size);
  }

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:Throttle
  ///
  /// 背压节流（C# 空方法）。
  pub fn throttle(&mut self) {}

  /// libs/server/Lua/ScratchBufferNetworkSender.cs:TryClose
  ///
  /// 假发送器无连接可关。
  pub fn try_close(&mut self) -> bool {
    false
  }

  /// 容量保障（剩余空间视图的前提）。
  fn ensure_capacity(&mut self) {
    if self.buffer.len() < self.server_buffer_size {
      self.buffer.resize(self.server_buffer_size, 0);
    }
  }
}

/// RESP 请求拼装缓冲（对标 Garnet.common ScratchBufferBuilder 的命令拼装面）。
#[derive(Default)]
pub struct ScratchBufferBuilder {
  /// 已拼装字节。
  buffer: Vec<u8>,
}

impl ScratchBufferBuilder {
  /// libs/server/Lua/LuaRunner.Functions.cs:StartCommand（经 ScratchBufferBuilder）
  ///
  /// 以 RESP 数组形态起头：`*N\r\n`。
  pub fn start_command(&mut self, command: &[u8], arg_count: usize) {
    self.buffer.push(b'*');
    self.write_decimal(arg_count + 1);
    self.write_arg_inner(command);
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:WriteArgument（经 ScratchBufferBuilder）
  pub fn write_argument(&mut self, arg: &[u8]) {
    self.write_arg_inner(arg);
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:WriteNullArgument（经 ScratchBufferBuilder）
  pub fn write_null_argument(&mut self) {
    self.buffer.extend_from_slice(b"$-1\r\n");
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:ViewFullArgSlice
  pub fn view_full_arg_slice(&self) -> &[u8] {
    &self.buffer
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Reset
  pub fn reset(&mut self) {
    self.buffer.clear();
  }

  /// 原样追加字节（对标 C# ViewRemainingArgSlice + MoveOffset 的直写形态，
  /// cjson/cmsgpack/struct 编码层使用）。
  pub fn append(&mut self, bytes: &[u8]) {
    self.buffer.extend_from_slice(bytes);
  }

  /// 追加单字节。
  pub fn append_byte(&mut self, byte: u8) {
    self.buffer.push(byte);
  }

  /// `$len\r\n` + 字节 + `\r\n`。
  fn write_arg_inner(&mut self, arg: &[u8]) {
    self.buffer.push(b'$');
    self.write_decimal(arg.len());
    self.buffer.extend_from_slice(arg);
    self.buffer.extend_from_slice(b"\r\n");
  }

  /// 十进制正文（无前导零）。
  fn write_decimal(&mut self, value: usize) {
    let mut buffer = itoa::Buffer::new();
    self
      .buffer
      .extend_from_slice(buffer.format(value).as_bytes());
    self.buffer.extend_from_slice(b"\r\n");
  }
}

#[cfg(test)]
mod tests {
  use super::{ScratchBufferBuilder, ScratchBufferNetworkSender};

  #[test]
  fn sender_response_region() {
    let mut sender = ScratchBufferNetworkSender::new();
    assert!(sender.is_local_connection());
    assert!(!sender.try_close());

    // 剩余空间视图 → 写入 → SendResponse 推进有效区。
    sender.enter();
    sender.spare_capacity_mut()[..4].copy_from_slice(b"+OK\r");
    sender.exit_and_return_response_object();
    assert!(sender.send_response(0, 4));
    assert_eq!(sender.get_response(), b"+OK\r");
    sender.return_response_object();
    sender.exit();
    sender.get_response_object();
    sender.send_callback();
    sender.throttle();
    sender.dispose_network_sender(false);

    sender.reset();
    assert!(sender.get_response().is_empty());
  }

  #[test]
  fn sender_direct_write_clamps_to_buffer_size() {
    let mut sender = ScratchBufferNetworkSender::new();
    sender.write_response_bytes(b"+PONG\r\n");
    assert_eq!(sender.get_response(), b"+PONG\r\n");
    // 超量直写截断在服务端缓冲容量内（C# 缓冲上限语义）。
    let big = vec![b'a'; sender.server_buffer_size() + 16];
    sender.write_response_bytes(&big);
    assert_eq!(sender.get_response().len(), sender.server_buffer_size());
    sender.reset();
    assert!(sender.get_response().is_empty());
  }

  #[test]
  fn builder_formats_resp_array() {
    let mut builder = ScratchBufferBuilder::default();
    builder.start_command(b"SET", 2);
    builder.write_argument(b"k");
    builder.write_null_argument();
    assert_eq!(
      builder.view_full_arg_slice(),
      b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$-1\r\n"
    );
    builder.reset();
    assert_eq!(builder.view_full_arg_slice(), b"");
  }
}

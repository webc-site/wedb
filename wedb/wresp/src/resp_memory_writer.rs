//! RESP 协议写出器（对标 libs/common/RespMemoryWriter.cs 与 libs/common/RespWriteUtils.cs）
//!
//! 基于泛型单态化（ZST）实现编译期零成本抽象，彻底消除热路径运行时协议分支。

use core::{marker::PhantomData, ops::Deref};

use itoa::{Buffer as IntBuf, Integer};
use zmij::Buffer as FloatBuf;

use crate::ext::{MAX_ERROR_MSG_LEN, sanitize_error_bytes};

/// 双精度浮点格式化（对标 Garnet RespWriteUtils.TryFormat / NumUtils）：
/// - NaN → "nan"
/// - +∞ → "inf"
/// - -∞ → "-inf"
/// - 普通数值：经 zmij 零堆分配格式化，整数值去除末尾 ".0"
///
/// 【有意偏差登记】此处未 1:1 对齐 C# WriteDouble / TryFormat G 的 15 位逐位截断、
/// 大写 E 科学计数窗口与 -0.0 表现。
/// 保留理由：C# 的截断破坏了 double 最短往返特性（如 0.1+0.2 成 "0.300000000000000"），
/// 与 IEEE 存储值偏离；且与 real Redis 亦不一致，强行对齐无兼容性收益。
/// 连带后果：因 INCRBYFLOAT 落盘文本与应答同串，同一累加序列自首个不可精确表示值起，两侧数值路径会发散。
///
/// 【特值词形防呆锚（§80 在册，工单 zcode-r151c-hincrby 案一）】"nan"/"inf"/"-inf" 系在册刻意
/// 输出词形：§80 裁 ZSCAN 分值第一消费位（对 C# TryFormat G "Infinity"/"-Infinity" 逐字节分叉），
/// HINCRBYFLOAT 求和溢出为同族第二消费位（deviations §1 尾回指注）。严禁按 C# Infinity 词形回改
/// 本单源出帧形；亦严禁在本函数上游补设求和/结果复检门——真 Redis would-produce 拒改形双侧皆
/// 不复刻（双侧均无结果门系在册裁决方向，后席勿按 Redis 语料单侧补门致双臂发散）。
#[inline]
pub fn format_double(value: f64, buf: &mut FloatBuf) -> &str {
  if value.is_nan() {
    "nan"
  } else if value.is_infinite() {
    if value > 0.0 { "inf" } else { "-inf" }
  } else {
    let s = buf.format(value);
    s.strip_suffix(".0").unwrap_or(s)
  }
}

/// RESP 输出目标缓冲抽象
pub trait RespBuffer {
  /// 可变引用目标缓冲
  fn buf_mut(&mut self) -> &mut Vec<u8>;
  /// 不可变引用目标缓冲
  fn buf_ref(&self) -> &Vec<u8>;
}

impl RespBuffer for Vec<u8> {
  #[inline(always)]
  fn buf_mut(&mut self) -> &mut Vec<u8> {
    self
  }

  #[inline(always)]
  fn buf_ref(&self) -> &Vec<u8> {
    self
  }
}

impl RespBuffer for &mut Vec<u8> {
  #[inline(always)]
  fn buf_mut(&mut self) -> &mut Vec<u8> {
    self
  }

  #[inline(always)]
  fn buf_ref(&self) -> &Vec<u8> {
    self
  }
}

/// 单字节前缀长度格式化：`<prefix><len>\r\n`
#[inline(always)]
pub fn write_prefixed_len_to(out: &mut Vec<u8>, prefix: u8, len: usize) {
  let mut buf = IntBuf::new();
  let s = buf.format(len).as_bytes();
  out.reserve(1 + s.len() + 2);
  out.push(prefix);
  out.extend_from_slice(s);
  out.extend_from_slice(b"\r\n");
}

/// 批量串直接写入底层缓冲：`$<len>\r\n<item>\r\n`
#[inline(always)]
pub fn write_bulk_string_to(out: &mut Vec<u8>, item: &[u8]) {
  let mut buf = IntBuf::new();
  let len_str = buf.format(item.len()).as_bytes();
  out.reserve(1 + len_str.len() + 2 + item.len() + 2);
  out.push(b'$');
  out.extend_from_slice(len_str);
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(item);
  out.extend_from_slice(b"\r\n");
}

/// 错误帧唯一成帧实现：`-<prefix> <clean msg>\r\n`（`prefix` 为空时 `-<clean msg>\r\n`）
///
/// `msg` 先过净化单点 [`sanitize_error_bytes`]（CRLF 切断 + [`MAX_ERROR_MSG_LEN`]
/// 长度帽，按字节边界），因此 wresp 内除本函数外不得再出现第二处自拼 `-` 与
/// `\r\n` 的简单错误成帧：[`RespWriter::write_error_bytes`]、
/// [`RespWriter::write_error`]、[`RespWriter::write_error_with_prefix`]
/// 全部收敛到本函数，清洗随成帧自动生效，调用侧不再各自预处理（wresp/wcol/wlua
/// 的字节切片入参站点即由此单源清洗）。
///
/// C# 对位（无清洗层）：`RespMemoryWriter` 的 `WriteError` 直落 `RespWriteUtils`
/// 的 `TryWriteError` 裸拷贝，那里靠 XML 注释的 “mustn't contain CR/LF” 前置约定，
/// 取舍见 `ext.rs` [`sanitize_error_bytes`] 注释锚点。
#[inline]
fn write_error_frame_to(out: &mut Vec<u8>, prefix: &[u8], msg: &[u8]) {
  let clean = sanitize_error_bytes(msg, MAX_ERROR_MSG_LEN);
  out.reserve(1 + prefix.len() + usize::from(!prefix.is_empty()) + clean.len() + 2);
  out.push(b'-');
  out.extend_from_slice(prefix);
  if !prefix.is_empty() {
    out.push(b' ');
  }
  out.extend_from_slice(clean);
  out.extend_from_slice(b"\r\n");
}

/// RESP 协议静态特征抽象（用于编译期单态化分派）
pub trait RespProtocol: Copy + Default + Send + Sync + 'static {
  /// 协议版本数值（2 或 3）
  const VERSION: u8;

  /// 写 null 应答
  fn write_null(buf: &mut Vec<u8>);

  /// 写 null 数组应答
  fn write_null_array(buf: &mut Vec<u8>);

  /// 写 map 长度头
  fn write_map_len(buf: &mut Vec<u8>, len: usize);

  /// 写 set 长度头
  fn write_set_len(buf: &mut Vec<u8>, len: usize);

  /// 写 push 长度头
  fn write_push_len(buf: &mut Vec<u8>, len: usize);

  /// 写双精度浮点数
  fn write_double(buf: &mut Vec<u8>, value: f64);

  /// 写布尔值
  fn write_bool(buf: &mut Vec<u8>, value: bool);

  /// 写 verbatim string
  fn write_verbatim_string(buf: &mut Vec<u8>, message: &[u8], format: &[u8; 3]);

  /// 写空 set
  fn write_empty_set(buf: &mut Vec<u8>);
}

/// RESP2 零大小标记类型（ZST）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resp2;

/// RESP3 零大小标记类型（ZST）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resp3;

impl RespProtocol for Resp2 {
  const VERSION: u8 = 2;

  #[inline(always)]
  fn write_null(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"$-1\r\n");
  }

  #[inline(always)]
  fn write_null_array(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"*-1\r\n");
  }

  #[inline(always)]
  fn write_map_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'*', len.saturating_mul(2));
  }

  #[inline(always)]
  fn write_set_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'*', len);
  }

  #[inline(always)]
  fn write_push_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'*', len);
  }

  #[inline(always)]
  fn write_double(buf: &mut Vec<u8>, value: f64) {
    let mut fbuf = FloatBuf::new();
    let s = format_double(value, &mut fbuf);
    write_bulk_string_to(buf, s.as_bytes());
  }

  #[inline(always)]
  fn write_bool(buf: &mut Vec<u8>, value: bool) {
    buf.extend_from_slice(if value { b":1\r\n" } else { b":0\r\n" });
  }

  #[inline(always)]
  fn write_verbatim_string(buf: &mut Vec<u8>, message: &[u8], _format: &[u8; 3]) {
    write_bulk_string_to(buf, message);
  }

  #[inline(always)]
  fn write_empty_set(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"*0\r\n");
  }
}

impl RespProtocol for Resp3 {
  const VERSION: u8 = 3;

  #[inline(always)]
  fn write_null(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"_\r\n");
  }

  #[inline(always)]
  fn write_null_array(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"_\r\n");
  }

  #[inline(always)]
  fn write_map_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'%', len);
  }

  #[inline(always)]
  fn write_set_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'~', len);
  }

  #[inline(always)]
  fn write_push_len(buf: &mut Vec<u8>, len: usize) {
    write_prefixed_len_to(buf, b'>', len);
  }

  #[inline(always)]
  fn write_double(buf: &mut Vec<u8>, value: f64) {
    let mut fbuf = FloatBuf::new();
    let s = format_double(value, &mut fbuf);
    buf.reserve(1 + s.len() + 2);
    buf.push(b',');
    buf.extend_from_slice(s.as_bytes());
    buf.extend_from_slice(b"\r\n");
  }

  #[inline(always)]
  fn write_bool(buf: &mut Vec<u8>, value: bool) {
    buf.extend_from_slice(if value { b"#t\r\n" } else { b"#f\r\n" });
  }

  #[inline(always)]
  fn write_verbatim_string(buf: &mut Vec<u8>, message: &[u8], format: &[u8; 3]) {
    let total_len = message.len().saturating_add(4); // format (3) + ':' (1)
    let mut buf_int = IntBuf::new();
    let len_str = buf_int.format(total_len).as_bytes();
    buf.reserve(1 + len_str.len() + 2 + 3 + 1 + message.len() + 2);
    buf.push(b'=');
    buf.extend_from_slice(len_str);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(format);
    buf.push(b':');
    buf.extend_from_slice(message);
    buf.extend_from_slice(b"\r\n");
  }

  #[inline(always)]
  fn write_empty_set(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"~0\r\n");
  }
}

/// 纯编译期泛型 RESP 协议写出器（无任何运行时协议判断分支）
///
/// 支持 owned `Vec<u8>` 或 borrowed `&mut Vec<u8>`。
///
/// wresp 写出面单点：C# 侧 `libs/common/RespMemoryWriter.cs` 的写出原语、
/// `libs/server/Resp/RespServerSessionOutput.cs` 的会话输出分部（后者是
/// `RespServerSession` 的 partial class，并非独立工具类）在 rust 统一由本类型承接；
/// 协议字节实现见本文件 `RespProtocol`，`Vec<u8>` 会话门面见 `ext.rs` 的 `RespVecExt`，
/// 三者之外不再另立写出入口（尤其勿按 C# 会话文件名再造静态门面）。
#[derive(Debug, Clone)]
pub struct RespWriter<B = Vec<u8>, P: RespProtocol = Resp2> {
  /// 输出缓冲
  pub out: B,
  _phantom: PhantomData<P>,
}

/// 拥有独立堆缓冲的默认写出器（对标 C# RespMemoryWriter）
pub type RespMemoryWriter<P = Resp2> = RespWriter<Vec<u8>, P>;

impl Default for RespWriter<Vec<u8>, Resp2> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<P: RespProtocol> RespWriter<Vec<u8>, P> {
  /// 创建新的内存写出器（协议形态由类型参数指定，别名缺省 Resp2）
  #[inline]
  pub fn new() -> Self {
    Self {
      out: Vec::with_capacity(256),
      _phantom: PhantomData,
    }
  }

  /// 创建指定初始容量的内存写出器
  #[inline]
  pub fn with_capacity(capacity: usize) -> Self {
    Self {
      out: Vec::with_capacity(capacity),
      _phantom: PhantomData,
    }
  }

  /// 消费写出器，返回底层缓冲
  #[inline]
  pub fn into_inner(self) -> Vec<u8> {
    self.out
  }
}

impl<'a> RespWriter<&'a mut Vec<u8>, Resp2> {
  /// 基于借用缓冲创建默认 Resp2 写出器
  #[inline]
  pub fn new_ref(out: &'a mut Vec<u8>) -> Self {
    Self {
      out,
      _phantom: PhantomData,
    }
  }
}

impl<'a, P: RespProtocol> RespWriter<&'a mut Vec<u8>, P> {
  /// 基于借用缓冲与协议标记创建写出器
  #[inline]
  pub fn new_ref_p(out: &'a mut Vec<u8>) -> Self {
    Self {
      out,
      _phantom: PhantomData,
    }
  }
}

impl<B: RespBuffer, P: RespProtocol> RespWriter<B, P> {
  /// 缓冲可变借用
  #[inline(always)]
  pub fn buf_mut(&mut self) -> &mut Vec<u8> {
    self.out.buf_mut()
  }

  /// 缓冲不可变借用
  #[inline(always)]
  pub fn buf_ref(&self) -> &Vec<u8> {
    self.out.buf_ref()
  }

  /// 当前已写字节数
  #[inline]
  pub fn len(&self) -> usize {
    self.buf_ref().len()
  }

  /// 缓冲是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.buf_ref().is_empty()
  }

  /// 获取当前已写入有效字节切片
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.buf_ref().as_slice()
  }

  /// 清空缓冲
  #[inline]
  pub fn clear(&mut self) {
    self.buf_mut().clear();
  }

  /// 预留空间
  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.buf_mut().reserve(additional);
  }

  /// 直写切片字节
  #[inline]
  pub fn write_direct(&mut self, span: &[u8]) {
    self.buf_mut().extend_from_slice(span);
  }

  /// 直写 ASCII 字符切片
  #[inline]
  /// libs/common/RespMemoryWriter.cs:WriteAsciiDirect
  pub fn write_ascii_direct(&mut self, message: &str) {
    self.write_direct(message.as_bytes());
  }

  /// 写带单字节前缀的长度头：`<prefix><len>\r\n`
  #[inline]
  pub fn write_prefixed_len(&mut self, prefix: u8, len: usize) {
    write_prefixed_len_to(self.buf_mut(), prefix, len);
  }

  /// 写数组头 `*<len>\r\n`
  #[inline(always)]
  pub fn write_array_length(&mut self, len: usize) {
    self.write_prefixed_len(b'*', len);
  }

  /// 写 map 头：根据 P 静态分派（RESP3 `%<len>\r\n`，RESP2 `*<len*2>\r\n`）
  #[inline(always)]
  pub fn write_map_length(&mut self, len: usize) {
    P::write_map_len(self.buf_mut(), len);
  }

  /// 写 set 头：根据 P 静态分派（RESP3 `~<len>\r\n`，RESP2 `*<len>\r\n`）
  #[inline(always)]
  pub fn write_set_length(&mut self, len: usize) {
    P::write_set_len(self.buf_mut(), len);
  }

  /// 写 push 头：根据 P 静态分派（RESP3 `><len>\r\n`，RESP2 `*<len>\r\n`）
  #[inline(always)]
  /// libs/common/RespMemoryWriter.cs:WritePushLength
  pub fn write_push_length(&mut self, len: usize) {
    P::write_push_len(self.buf_mut(), len);
  }

  /// 写 bulk string `$<len>\r\n<item>\r\n`
  ///
  /// libs/server/Resp/RespServerSession.cs:WriteDirectLarge 的机制位合并承接：
  /// C# 大值超网络缓冲时按 dcurr/dend 游标分块直写并翻转 GetResponseObject
  /// （WriteDirectLarge/WriteDirectLargeRespString 族）；rust 会话输出为自扩容
  /// `Vec<u8>` 缓冲（reserve 一次补足 + 整段追加），任意大值一次写入，网络层
  /// 按消息边界整体冲刷，分块翻转机制位不存在的。
  #[inline(always)]
  pub fn write_bulk_string(&mut self, item: &[u8]) {
    write_bulk_string_to(self.buf_mut(), item);
  }

  /// 写 ASCII bulk string
  #[inline(always)]
  pub fn write_ascii_bulk_string(&mut self, chars: &str) {
    self.write_bulk_string(chars.as_bytes());
  }

  /// 写简单字节串 `+<bytes>\r\n`
  #[inline(always)]
  pub fn write_simple_string_bytes(&mut self, bytes: &[u8]) {
    let out = self.buf_mut();
    out.reserve(1 + bytes.len() + 2);
    out.push(b'+');
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
  }

  /// 写简单字符串 `+<str>\r\n`
  #[inline(always)]
  pub fn write_simple_string(&mut self, simple_string: &str) {
    self.write_simple_string_bytes(simple_string.as_bytes());
  }

  /// 写 i64 整数 `:<val>\r\n`
  #[inline(always)]
  pub fn write_int64(&mut self, value: i64) {
    let mut buf = IntBuf::new();
    let s = buf.format(value).as_bytes();
    let out = self.buf_mut();
    out.reserve(1 + s.len() + 2);
    out.push(b':');
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
  }

  /// 写 i32 整数 `:<val>\r\n`
  #[inline(always)]
  pub fn write_int32(&mut self, value: i32) {
    self.write_int64(i64::from(value));
  }

  /// 整数作为 bulk string 写出的唯一实现：itoa 栈上定长 → [`Self::write_bulk_string`]
  /// 单点成帧，零堆分配、无临时 `String`。
  ///
  /// C# 对位 libs/common/RespWriteUtils.cs:542 `TryWriteInt32AsBulkString` 与 :565
  /// `TryWriteInt64AsBulkString`（`NumUtils.CountDigits` 定长后就地写
  /// `$<len>\r\n<digits>\r\n`）。C# 只有带符号两型，rust 侧 u32/u64 计数与句柄字段
  /// 同走本出口（itoa 的 `Integer` 已 sealed，收发散格式化在编译期即被排除）。
  #[inline(always)]
  pub fn write_integer_as_bulk_string<I: Integer>(&mut self, value: I) {
    let mut buf = IntBuf::new();
    self.write_bulk_string(buf.format(value).as_bytes());
  }

  /// 整数作为 bulk string 写出
  #[inline(always)]
  pub fn write_int64_as_bulk_string(&mut self, value: i64) {
    self.write_integer_as_bulk_string(value);
  }

  /// 整数作为 bulk string 写出
  #[inline(always)]
  pub fn write_int32_as_bulk_string(&mut self, value: i32) {
    self.write_integer_as_bulk_string(value);
  }

  /// 整数作为数组项写出（对标 C# WriteArrayItem）
  #[inline(always)]
  pub fn write_array_item(&mut self, item: i64) {
    self.write_int64_as_bulk_string(item);
  }

  /// 直写整数字节：`:<bytes>\r\n`
  #[inline(always)]
  /// libs/common/RespMemoryWriter.cs:WriteIntegerFromBytes
  pub fn write_integer_from_bytes(&mut self, integer_bytes: &[u8]) {
    let out = self.buf_mut();
    out.reserve(1 + integer_bytes.len() + 2);
    out.push(b':');
    out.extend_from_slice(integer_bytes);
    out.extend_from_slice(b"\r\n");
  }

  /// 写 0 整数：`:0\r\n`
  #[inline(always)]
  pub fn write_zero(&mut self) {
    self.buf_mut().extend_from_slice(b":0\r\n");
  }

  /// 写 1 整数：`:1\r\n`
  #[inline(always)]
  pub fn write_one(&mut self) {
    self.buf_mut().extend_from_slice(b":1\r\n");
  }

  /// 写空数组 `*0\r\n`
  #[inline(always)]
  pub fn write_empty_array(&mut self) {
    self.buf_mut().extend_from_slice(b"*0\r\n");
  }

  /// 写空 set：根据 P 静态分派
  #[inline(always)]
  pub fn write_empty_set(&mut self) {
    P::write_empty_set(self.buf_mut());
  }

  /// 写 null 应答：根据 P 静态分派（RESP3 `_\r\n`，RESP2 `$-1\r\n`）
  #[inline(always)]
  pub fn write_null(&mut self) {
    P::write_null(self.buf_mut());
  }

  /// 写 null 数组应答：根据 P 静态分派（RESP3 `_\r\n`，RESP2 `*-1\r\n`）
  #[inline(always)]
  pub fn write_null_array(&mut self) {
    P::write_null_array(self.buf_mut());
  }

  /// 固定写 RESP2 null: `$-1\r\n`
  #[inline(always)]
  pub fn write_resp2_null(&mut self) {
    Resp2::write_null(self.buf_mut());
  }

  /// 固定写 RESP3 null: `_\r\n`
  #[inline(always)]
  pub fn write_resp3_null(&mut self) {
    Resp3::write_null(self.buf_mut());
  }

  /// 写布尔值：根据 P 静态分派（RESP3 `#t\r\n`/`#f\r\n`，RESP2 `:1\r\n`/`:0\r\n`）
  #[inline(always)]
  pub fn write_bool(&mut self, value: bool) {
    P::write_bool(self.buf_mut(), value);
  }

  /// 强制写 RESP3 布尔格式 `#t\r\n` / `#f\r\n`
  #[inline(always)]
  pub fn write_resp3_bool(&mut self, value: bool) {
    Resp3::write_bool(self.buf_mut(), value);
  }

  /// 写浮点数（根据 P 静态分派：RESP3 为 `,val\r\n`，RESP2 降级为 bulk string）
  ///
  /// 在 garnet 中的相对路径:libs/common/RespWriteUtils.cs:TryWriteDoubleNumeric
  #[inline(always)]
  pub fn write_double_numeric(&mut self, value: f64) {
    P::write_double(self.buf_mut(), value);
  }

  /// 强制以 bulk string 格式写双精度浮点数
  ///
  /// 在 garnet 中的相对路径:libs/common/RespWriteUtils.cs:TryWriteDoubleBulkString
  #[inline]
  /// libs/common/RespMemoryWriter.cs:WriteDoubleBulkString
  pub fn write_double_bulk_string(&mut self, value: f64) {
    let mut fbuf = FloatBuf::new();
    let s = format_double(value, &mut fbuf);
    self.write_bulk_string(s.as_bytes());
  }

  /// 直写错误字节切片：`-<error_bytes>\r\n`
  ///
  /// 字节切片入参的错误应答口（wcol/wlua 错误回显即由此入参）；净化已下沉到
  /// 成帧点 [`write_error_frame_to`]，本口不再裸写，调用侧无须预处理。
  #[inline]
  pub fn write_error_bytes(&mut self, error_bytes: &[u8]) {
    write_error_frame_to(self.buf_mut(), b"", error_bytes);
  }

  /// 写错误应答：`-<error_string>\r\n`（自动清洗 CRLF）
  #[inline]
  pub fn write_error(&mut self, error_string: &str) {
    self.write_error_bytes(error_string.as_bytes());
  }

  /// 带前缀写错误应答：`-<prefix> <msg>\r\n`
  ///
  /// 前缀为代码内常量（如 `ERR`），不参与清洗；`msg` 由成帧点净化。
  #[inline]
  pub fn write_error_with_prefix(&mut self, prefix: &str, msg: &str) {
    write_error_frame_to(self.buf_mut(), prefix.as_bytes(), msg.as_bytes());
  }

  /// 写 verbatim string：根据 P 静态分派（RESP3 `={len}\r\n{format}:{message}\r\n`，RESP2 bulk string）
  /// libs/common/RespMemoryWriter.cs:WriteVerbatimString
  #[inline(always)]
  pub fn write_large_verbatim_string(&mut self, message: &[u8], format: &[u8; 3]) {
    P::write_verbatim_string(self.buf_mut(), message, format);
  }
}

impl<B: RespBuffer, P: RespProtocol> Deref for RespWriter<B, P> {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.buf_ref().as_slice()
  }
}

impl<B: RespBuffer, P: RespProtocol> AsRef<[u8]> for RespWriter<B, P> {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.buf_ref().as_slice()
  }
}

use core::str;

use wresp::read::{
  try_read_as_span, try_read_error_as_span, try_read_ptr_with_signed_length_header,
  try_read_signed_length_header,
};

use crate::{Error, Result};

/// 嵌套集合深度上限（数组所在层数，顶层为 1）：字符串数组嵌套臂（`* ~ >`）为
/// rust 侧对 C# 原型（仅 `$`/`:` 元素臂、无递归）的 RESP3 扩展，自递归必须配
/// 此熔断——对端深嵌套帧超限返回 [`Error::UnexpectedToken`] 走既有断连收场，
/// 杜绝栈溢出整进程 abort（RESP 服务器实现通例为数百层封顶后掷协议错误）
const MAX_NEST_DEPTH: usize = 128;

/// libs/client/RespReadResponseUtils.cs:RespReadResponseUtils
///
/// 客户端应答解析唯一门面：单行与带长头语义薄委托 wresp::read（对位 C# 门面逐个
/// 转调 RespReadUtils），仅做 `Result<Option<T>>` 形态适配（None = 应答未到齐，游标
/// 完整回滚）；RESP2/3 行读与扩展元素臂（-/,/#/_ 等）经
/// [`RespReadResponseUtils::try_read_token_span`] / [`RespReadResponseUtils::try_read_token_line`]
/// 单点承担
pub struct RespReadResponseUtils;

impl RespReadResponseUtils {
  /// 回滚包装单点：内层读返回 `None`（应答未到齐）时游标整体回滚至入口，
  /// `Err`（协议违例）不回滚保持现游标（上层断连收场），成功则原样透出。
  /// 门面各读臂的「存 orig、失败回滚游标」形态只在此实现一次
  #[inline]
  fn rolled<'a, T>(
    ptr: &mut &'a [u8],
    read: impl FnOnce(&mut &'a [u8]) -> Result<Option<T>>,
  ) -> Result<Option<T>> {
    let orig = *ptr;
    match read(ptr)? {
      Some(v) => Ok(Some(v)),
      None => {
        *ptr = orig;
        Ok(None)
      }
    }
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadErrorAsString
  /// （wresp 底层同 C#：非 '-' 前导按未到齐返回 false 而非报错）
  #[inline]
  pub fn try_read_error_as_string(ptr: &mut &[u8]) -> Result<Option<String>> {
    Self::rolled(ptr, |ptr| {
      let mut span = &[][..];
      if !try_read_error_as_span(&mut span, ptr)? {
        return Ok(None);
      }
      Ok(Some(str::from_utf8(span)?.to_string()))
    })
  }

  /// RESP3 null: `_\r\n`
  #[inline]
  pub fn try_read_null(ptr: &mut &[u8]) -> Result<Option<()>> {
    Ok(Self::try_read_token_span(ptr, b'_')?.map(|_| ()))
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadStringWithLengthHeader
  ///
  /// libs/common/RespReadUtils.cs:TryReadStringWithLengthHeader（C# 客户端门面
  /// 即转调本 common 件同名）
  /// （null bulk → Some(None)；严格 UTF-8 校验保持现网行为）
  pub fn try_read_string_with_length_header(ptr: &mut &[u8]) -> Result<Option<Option<String>>> {
    match Self::try_read_byte_slice_with_length_header(ptr)? {
      None => Ok(None),
      Some(None) => Ok(Some(None)),
      Some(Some(bytes)) => Ok(Some(Some(str::from_utf8(bytes)?.to_string()))),
    }
  }

  /// 零拷贝读取单个 bulk string 字节切片引用（基于借用视图消除 Vec 堆分配）；
  /// wresp::read::try_read_ptr_with_signed_length_header 薄委托（null 容忍逐字对位）
  pub fn try_read_byte_slice_with_length_header<'a>(
    ptr: &mut &'a [u8],
  ) -> Result<Option<Option<&'a [u8]>>> {
    Self::rolled(ptr, |ptr| {
      let mut slice = None;
      Ok(try_read_ptr_with_signed_length_header(&mut slice, ptr)?.then_some(slice))
    })
  }

  /// libs/client/RespReadResponseUtils.cs:TryReadStringArrayWithLengthHeader
  ///
  /// libs/common/RespReadUtils.cs:TryReadStringArrayWithLengthHeader（C# 客户端
  /// 门面即转调本 common 件同名）
  ///
  /// 元素臂扩至 RESP3（- 错误行 / , 浮点 / # 布尔 / _ null / ~ > 嵌套集合），
  /// wresp::read 同名件不容这些形态，保留门面本地组合；嵌套臂经 `depth`
  /// 层数自计受 [`MAX_NEST_DEPTH`] 熔断（骨架单点），对端深嵌套帧拒收断连
  pub fn try_read_string_array_with_length_header(
    ptr: &mut &[u8],
    depth: usize,
  ) -> Result<Option<Option<Vec<String>>>> {
    Self::read_array_with(ptr, depth, |ptr| {
      match ptr[0] {
        // 内层返回 None 即应答未到齐：整体按不完整处理，由骨架回滚游标
        b'$' => Ok(Self::try_read_string_with_length_header(ptr)?.map(|s| s.unwrap_or_default())),
        // 简单串/整数/错误行/RESP3 浮点与布尔共用行读取路径
        b'+' | b':' | b'-' | b',' | b'#' => {
          Ok(Self::try_read_token_line(ptr, ptr[0])?.map(str::to_string))
        }
        b'*' | b'~' | b'>' => Ok(
          Self::try_read_string_array_with_length_header(ptr, depth + 1)?
            .map(|a| a.map(|a| a.join(", ")).unwrap_or_default()),
        ),
        // RESP3 null: _\r\n
        b'_' => Ok(Self::try_read_token_span(ptr, b'_')?.map(|_| String::new())),
        b => Err(Self::unexpected_token(b)),
      }
    })
  }

  /// 二进制安全的 RESP 数组零拷贝解析：元素为字节切片引用
  ///
  /// 服务面：Str/Bytes 标量应答形的数组臂取首元素与集群复制帧（CLUSTER
  /// APPENDLOG 载荷）还原。元素臂对位 C# 客户端门面
  /// RespReadResponseUtils.TryReadStringArrayWithLengthHeader（注意是 client 件
  /// 非 common 件：common 件同名走 TryReadUnsignedLengthHeader 不容 `$-1`），
  /// 并对齐 [`Self::try_read_string_array_with_length_header`] 的 RESP3 扩展形态：
  /// - `$` 含 null bulk（`$-1` → 空切片：对位客户端门面 TryReadStringWithLengthHeader
  ///   置 null 返回 true，完整帧消费不判半包——flatten 成 None 会被骨架误判
  ///   元素未到齐，null 元素帧成永久半包死等）
  /// - `+ : - , #` 行读借用行体零拷贝（C# else 臂按整数行读，rust 扩 RESP3 行集）
  /// - `_` RESP3 null → 空切片
  /// - `* ~ >` 嵌套集合仅消费帧、元素记空切片（C# MemoryPool 重载无嵌套臂，
  ///   字符串重载为 Join 臂；此臂为 rust 侧 RESP3 扩展偏差，经 `depth` 受
  ///   [`MAX_NEST_DEPTH`] 熔断）
  /// - 其余按非预期标记 Err 断连
  pub fn try_read_byte_slice_array_with_length_header<'a>(
    ptr: &mut &'a [u8],
    depth: usize,
  ) -> Result<Option<Option<Vec<&'a [u8]>>>> {
    Self::read_array_with(ptr, depth, |ptr| match ptr[0] {
      b'$' => Ok(Self::try_read_byte_slice_with_length_header(ptr)?.map(|b| b.unwrap_or_default())),
      // 简单串/整数/错误行/RESP3 浮点与布尔共用行读取路径，借用行体零拷贝
      b'+' | b':' | b'-' | b',' | b'#' => Ok(Self::try_read_token_span(ptr, ptr[0])?),
      // RESP3 null: _\r\n → 空切片
      b'_' => Ok(Self::try_read_token_span(ptr, b'_')?.map(|_| &[][..])),
      // 嵌套集合仅消费帧：内容不保留（元素记空切片），内层 None 即元素未到齐
      b'*' | b'~' | b'>' => {
        Ok(Self::try_read_byte_slice_array_with_length_header(ptr, depth + 1)?.map(|_| &[][..]))
      }
      b => Err(Self::unexpected_token(b)),
    })
  }

  /// RESP 数组头解析单点：`*` / `~` / `>` 三 sigil + 有符号长度头 + null 数组
  /// 判定（两条数组读臂共用一份 sigil 集与负长度语义；None = 应答未到齐，
  /// 游标停在数组头之前）
  fn parse_array_header(ptr: &mut &[u8]) -> Result<Option<ArrayHead>> {
    let Some((&token, _)) = ptr.split_first() else {
      return Ok(None);
    };
    if !matches!(token, b'*' | b'~' | b'>') {
      return Err(Self::unexpected_token(token));
    }
    let Some(len) = read_length_header(ptr, token)? else {
      return Ok(None);
    };
    if len < 0 {
      return Ok(Some(ArrayHead::Null));
    }
    Ok(Some(ArrayHead::Items(len as usize)))
  }

  /// 数组应答读骨架单点：深度熔断 → 头解析 → 逐元素读 → 任一步未到齐整体回滚至数组头前
  ///（回滚经 [`Self::rolled`]，与预分配截断策略单点，两条数组读臂共用）。
  /// `read_elem` 返回 None = 该元素未到齐（子读自滚至自身边界，骨架再整体回滚）。
  /// `depth` 为当前数组所在层数（顶层 1），超 [`MAX_NEST_DEPTH`] 判非预期标记
  /// 走既有断连收场——嵌套递归深度计数单点，禁止盲目增大调用栈
  fn read_array_with<'a, E>(
    ptr: &mut &'a [u8],
    depth: usize,
    mut read_elem: impl FnMut(&mut &'a [u8]) -> Result<Option<E>>,
  ) -> Result<Option<Option<Vec<E>>>> {
    // 深度熔断：嵌套集合元素臂自递归，对端深嵌套帧（每层最小入帧 *1\r\n 仅
    // 4 字节）超限即协议错误，杜绝无界递归栈溢出整进程 abort
    if depth > MAX_NEST_DEPTH {
      let token = ptr.first().copied().unwrap_or(b'*');
      return Err(Self::unexpected_token(token));
    }
    Self::rolled(ptr, |ptr| {
      let len = match Self::parse_array_header(ptr)? {
        None => return Ok(None),
        Some(ArrayHead::Null) => return Ok(Some(None)), // null array
        Some(ArrayHead::Items(len)) => len,
      };
      // 预分配按上限截断：对端声称超长数组头（如 `*2000000000\r\n`）时
      // 按元素数精确预分配会立即触发容量溢出/分配失败进程中止（C# 为可捕获
      // 的 OOM）；截断为增量扩容，合法大数组仅损失对数级扩容拷贝
      let mut res = Vec::with_capacity(len.min(64));
      for _ in 0..len {
        if ptr.is_empty() {
          return Ok(None);
        }
        let Some(item) = read_elem(ptr)? else {
          return Ok(None);
        };
        res.push(item);
      }
      Ok(Some(Some(res)))
    })
  }

  /// 读取 `<token><正文>\r\n` 一行并返回正文字节切片借用（零拷贝）；应答未到齐
  /// 返回 None。RESP 行读单点：wresp::read::try_read_as_span 的带标记校验包装
  #[inline]
  pub(crate) fn try_read_token_span<'a>(ptr: &mut &'a [u8], token: u8) -> Result<Option<&'a [u8]>> {
    let Some((&first, rest)) = ptr.split_first() else {
      return Ok(None);
    };
    if first != token {
      return Err(Self::unexpected_token(first));
    }
    let mut span = &[][..];
    let mut temp = rest;
    if !try_read_as_span(&mut span, &mut temp)? {
      return Ok(None);
    }
    *ptr = temp;
    Ok(Some(span))
  }

  /// 读取 `<token><正文>\r\n` 一行并返回正文字符串借用（零拷贝）；应答未到齐
  /// 返回 None
  #[inline]
  pub(crate) fn try_read_token_line<'a>(ptr: &mut &'a [u8], token: u8) -> Result<Option<&'a str>> {
    let Some(span) = Self::try_read_token_span(ptr, token)? else {
      return Ok(None);
    };
    Ok(Some(str::from_utf8(span)?))
  }

  /// 非预期协议标记错误（统一错误构造单点，对位 RespParsingException.ThrowUnexpectedToken）
  #[inline]
  pub(crate) fn unexpected_token(b: u8) -> Error {
    Error::UnexpectedToken(b as char)
  }
}

/// RESP 数组头解析产物（sigil 已校验、长度已就绪）
#[derive(Clone, Copy)]
enum ArrayHead {
  /// null 数组（负长度，如 `*-1\r\n`）
  Null,
  /// 元素数（非负长度头真值）
  Items(usize),
}

/// 解析 `<sigil><len>\r\n` 有符号长度头为 isize（wresp::read::try_read_signed_length_header
/// 适配：false 即未到齐且游标未动）；数组头 sigil 动态（* / ~ / >）故不走
/// wresp 的定 sigil 组合件
#[inline]
fn read_length_header(ptr: &mut &[u8], token: u8) -> Result<Option<isize>> {
  let mut len = 0;
  Ok(try_read_signed_length_header(&mut len, ptr, token)?.then_some(len as isize))
}

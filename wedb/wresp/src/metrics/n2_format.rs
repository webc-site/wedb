//! .NET "N2" 定点格式化单点（千分位分组 + 固定两位小数）
//!
//! C# 侧三处指标输出零手写格式化，全部走 BCL 的
//! `double.ToString("N2", CultureInfo.InvariantCulture)`：
//! libs/client/GarnetClientMetrics.cs:35-41（客户端延迟百分位）、
//! libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:80-86（服务端延迟百分位）、
//! libs/server/Metrics/Info/GarnetInfoMetrics.cs:203（garnet_hit_rate）。
//! rust std 无带千分位的定点格式化（`{:.2}` 不分隔），故本文件是全仓唯一手写实现，
//! wmetric 服务端延迟百分位与 INFO 命中率两面同源，舍入口径不再分叉。
//!
//! 自研依据: INFO 数值格式化（zmij/itoa 面，C# 对应 InfoManager 格式化）

use itoa::Buffer;

/// .NET "N2"（invariant）：写入 `out`，对 double 本体取 `round`（半值远离零）。
///
/// 舍入以二进制值为准，不做十进制推挤：原 wmetric 侧 `(abs*100 + 0.5000000001)
/// .floor()` 的 epsilon 写法会把 1.005 这类十进制中点（其 double 值为
/// 1.004999…989）推成 "1.01"，与收敛前 wconn 客户端侧的 `round` 落 "1.00"
/// 分叉；可精确表示的中点（0.125）两写法一致。收敛后以本函数为准。
///
/// 非有限值按 BCL `NumberFormatInfo` 字面量输出 `NaN`/`Infinity`/`-Infinity`
/// （rust `{:.2}` 的 `nan`/`inf` 与之不同源）。有限值域要求
/// `|v| × 100 < u64::MAX`：调用面为直方图微秒（上界 100s = 1e8 μs）与命中率
/// （0..100），距该上界 9 个数量级。
pub fn fmt_n2_into(v: f64, out: &mut String) {
  if !v.is_finite() {
    out.push_str(if v.is_nan() {
      "NaN"
    } else if v.is_sign_negative() {
      "-Infinity"
    } else {
      "Infinity"
    });
    return;
  }
  // -0.0 亦带号，同 BCL（输出 "-0.00"）
  if v.is_sign_negative() {
    out.push('-');
  }
  let cents = (v.abs() * 100.0).round() as u64;
  let frac = cents % 100;

  let mut buf = Buffer::new();
  let digits = buf.format(cents / 100);
  let len = digits.len();
  for (i, b) in digits.bytes().enumerate() {
    // 从高位起每 3 位插一个千分位逗号
    if i > 0 && (len - i).is_multiple_of(3) {
      out.push(',');
    }
    out.push(b as char);
  }
  out.push('.');
  out.push((b'0' + (frac / 10) as u8) as char);
  out.push((b'0' + (frac % 10) as u8) as char);
}

/// 同上，返回新串（调用面为 `MetricsItem` 取值这类一次性字符串槽位）。
pub fn fmt_n2(v: f64) -> String {
  let mut s = String::with_capacity(16);
  fmt_n2_into(v, &mut s);
  s
}

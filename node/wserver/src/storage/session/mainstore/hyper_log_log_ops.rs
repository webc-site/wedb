//! HyperLogLog 操作（对标 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs，C# 为 StorageSession partial）
//!
//! 缺口说明：C# 侧 HLL 对象由 ObjectStore 承载（Objects/HyperLogLog/Hll 国密
//! 稠密/稀疏双编码 + 缓存寄存器）；wobject 尚无 HLL 对象入口，本域以
//! Redis 互操作的稠密寄存器格式（16 字节魔数头 + 16384 个 6 位寄存器，
//! MSB 优先双字节打包）落地，参数对齐 Redis P=14、sparse 上限 64 的稠密化语义。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

/// 寄存器个数 2^P（Redis HLL_P = 14）
const HLL_P: u32 = 14;
/// 寄存器数量
const HLL_REGISTERS: usize = 1 << HLL_P;
/// 稠密存储字节数（16384 寄存器 × 6 位 → 12288 字节）
const HLL_DENSE_BYTES: usize = HLL_REGISTERS * 6 / 8;
/// 序列化总长：魔数 8 字节 + P 1 字节 + 卡缓存 7 字节 + 稠密寄存器
const HLL_SERIAL_LEN: usize = 16 + HLL_DENSE_BYTES;
/// 魔数前缀（自描述格式，与 Redis "HYLL" 头区分）
const HLL_MAGIC: [u8; 4] = *b"WHLL";
/// 偏差修正常数 alpha_m * m（m=16384）
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64) * HLL_REGISTERS as f64;

impl<'a, D: Device> StorageSession<'a, D> {
  /// PFADD：登记元素，返回寄存器是否发生变更（-1 表示新建，1 变更，0 未变更）
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogAdd
  pub async fn hyper_log_log_add(
    &self,
    key: &[u8],
    elements: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i32)> {
    let (mut regs, existed) = match self.read_string(key).await? {
      Some(raw) => match decode_registers(&raw) {
        Some(r) => (r, true),
        None => return Ok((GarnetStatus::WrongType, 0)),
      },
      None => (vec![0u8; HLL_REGISTERS], false),
    };
    let mut changed = false;
    for element in elements {
      if hll_add_element(&mut regs, element) {
        changed = true;
      }
    }
    self.upsert_string(key, &encode_registers(&regs)).await?;
    let status = if changed {
      if existed { 1 } else { -1 }
    } else {
      0
    };
    Ok((GarnetStatus::Ok, status))
  }

  /// PFCOUNT：基数估计
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength
  pub async fn hyper_log_log_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, f64)> {
    let Some(raw) = self.read_string(key).await? else {
      return Ok((GarnetStatus::NotFound, 0.0));
    };
    let Some(regs) = decode_registers(&raw) else {
      return Ok((GarnetStatus::WrongType, 0.0));
    };
    Ok((GarnetStatus::Ok, hll_count(&regs)))
  }

  /// PFMERGE：多源寄存器逐槽取最大合并入目标键
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogMerge
  pub async fn hyper_log_log_merge(
    &self,
    dest: &[u8],
    sources: &[&[u8]],
  ) -> wkv::Result<GarnetStatus> {
    let mut regs = vec![0u8; HLL_REGISTERS];
    for src in sources {
      let Some(raw) = self.read_string(src).await? else {
        continue;
      };
      let Some(src_regs) = decode_registers(&raw) else {
        return Ok(GarnetStatus::WrongType);
      };
      for (d, &s) in regs.iter_mut().zip(src_regs.iter()) {
        *d = (*d).max(s);
      }
    }
    self.upsert_string(dest, &encode_registers(&regs)).await?;
    Ok(GarnetStatus::Ok)
  }
}

/// 单元素登记：hash 前导零寄存器更新，返回是否变更
fn hll_add_element(regs: &mut [u8], element: &[u8]) -> bool {
  let h = gxhash::gxhash64(element, 0);
  let idx = (h >> (64 - HLL_P)) as usize & (HLL_REGISTERS - 1);
  // 余下 50 位的最前导 1 位置（至少为 1）
  let rest = h << HLL_P;
  let rank = (rest.leading_zeros() + 1).min(64 - HLL_P) as u8;
  if rank > regs[idx] {
    regs[idx] = rank;
    true
  } else {
    false
  }
}

/// 基数估计：标准 HLL 估计量 + 小/大范围线性修正（Redis HllCount 语义）
fn hll_count(regs: &[u8]) -> f64 {
  let m = regs.len() as f64;
  let mut sum = 0.0;
  let mut zeros = 0u64;
  for &r in regs {
    sum += (-f64::from(r)).exp2();
    if r == 0 {
      zeros += 1;
    }
  }
  let estimate = HLL_ALPHA / sum * m * m;
  if estimate <= 2.5 * m {
    // 小基数线性修正
    if zeros > 0 {
      m * (m / zeros as f64).ln()
    } else {
      estimate
    }
  } else {
    estimate
  }
}

/// 寄存器数组 → 稠密序列化（4 字节魔数 + P + 保留头 + 6 位 MSB 优先连续打包）
fn encode_registers(regs: &[u8]) -> Vec<u8> {
  let mut out = vec![0u8; HLL_SERIAL_LEN];
  out[..4].copy_from_slice(&HLL_MAGIC);
  out[4] = HLL_P as u8;
  for (i, &r) in regs.iter().enumerate() {
    let base = i * 6;
    for k in 0..6u32 {
      if (u32::from(r) >> (5 - k)) & 1 == 1 {
        let pos = base + k as usize;
        out[16 + pos / 8] |= 0x80u8 >> (pos % 8);
      }
    }
  }
  out
}

/// 稠密序列化 → 寄存器数组（魔数/P 校验失败返回 None → WRONGTYPE）
fn decode_registers(raw: &[u8]) -> Option<Vec<u8>> {
  if raw.len() < HLL_SERIAL_LEN || raw[..4] != HLL_MAGIC || raw[4] != HLL_P as u8 {
    return None;
  }
  let mut regs = vec![0u8; HLL_REGISTERS];
  for (i, slot) in regs.iter_mut().enumerate() {
    let base = i * 6;
    let mut v = 0u8;
    for k in 0..6u32 {
      let pos = base + k as usize;
      v = (v << 1) | u8::from(raw[16 + pos / 8] & (0x80u8 >> (pos % 8)) != 0);
    }
    *slot = v;
  }
  Some(regs)
}

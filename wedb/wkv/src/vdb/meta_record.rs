//! 自研依据: doc/zh/db.md KeyTag::DbMeta 0x0E 编解码
use wval::SessionPrefixBuf;

/// DbMeta 系统记录固定根域前缀（ns 0, db 0）：set_context / flush / swap
/// 持久化、GC 墓碑删除与点查装载读面共用的落位前缀（键载荷与记录值布局的
/// 单点编解码见 [`DbMetaRecord`]，doc/zh/db.md 1.4）
pub const ROOT_DBMETA_PREFIX: SessionPrefixBuf = SessionPrefixBuf::new(0, 0);

/// DbMeta 记录变体子类型字节（物理持久化协议标识）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VdbMetaSubType {
  NsMap = 0x01,
  DbMap = 0x02,
  GcDeadNs = 0x03,
  GcDeadDb = 0x04,
  NextId = 0x05,
  DbSwap = 0x06,
}

impl VdbMetaSubType {
  /// 从裸字节解析
  #[inline]
  pub fn from_u8(v: u8) -> Option<Self> {
    match v {
      0x01 => Some(Self::NsMap),
      0x02 => Some(Self::DbMap),
      0x03 => Some(Self::GcDeadNs),
      0x04 => Some(Self::GcDeadDb),
      0x05 => Some(Self::NextId),
      0x06 => Some(Self::DbSwap),
      _ => None,
    }
  }

  /// 转换为裸字节
  #[inline]
  pub fn as_u8(self) -> u8 {
    self as u8
  }
}

/// DbMeta 系统记录物理布局单点编解码（garnet 无对应，wedb 自研 DbMeta 布局）
///
/// 换号元数据六变体落固定根域前缀 (ns 0, db 0) + `KeyTag::DbMeta`（doc/zh/db.md
/// 1.4），键载荷与记录值的 subtype 字节、字段偏移、字节序、长度守卫**只出现在
/// 本类型的编解码实现内**——写侧（flush / swap / 首映射 / GC 墓碑删除 / 冷装载
/// 持久化）与读侧（启动重建、点查装载）一律经 [`Self::key`] / [`Self::value`] /
/// [`Self::decode`] / [`Self::dead_vid_of`] / [`Self::key_ns_map`] /
/// [`Self::key_db_map`]，杜绝各处手写 `[0u8; N]` 栈缓冲与逐段偏移拷贝/解析。
///
/// 定长大端而非 bitcode：映射记录以键**点查**装载，物理键必须字节稳定（与
/// `wval::ns_codec` 物理键域不适用 bitcode 的公理同口径）；值侧沿用
/// `wval::codec::I64Codec`「定长大端、不引入 bitcode」先例。C# 每库独立
/// Tsavorite 实例、清库为物理截断、重启走 checkpoint cookie 结构化反序列化
/// （garnet/libs/server/StoreWrapper.cs:FlushDatabase），无本机制对位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbMetaRecord {
  /// 命名空间映射 logic_ns → vns：键 `[0x01][logic_ns: 8B be]`，值 `[vns: 8B be]`
  NsMap { logic_ns: u64, vns: u64 },
  /// 库级映射 (vns, logic_db) → vdb：键 `[0x02][vns: 8B be][logic_db: 8B be]`，
  /// 值 `[vdb: 8B be]`
  DbMap { vns: u64, logic_db: u64, vdb: u64 },
  /// 命名空间退役墓碑：键 `[0x03][expired_at: 8B be][old_vns: 8B be]`，
  /// 值 `[tail_address: 8B be]`
  GcDeadNs {
    expired_at: i64,
    old_vns: u64,
    tail_address: u64,
  },
  /// 库级退役墓碑：键 `[0x04][expired_at: 8B be][vns: 8B be][old_vdb: 8B be]`，
  /// 值 `[tail_address: 8B be]`
  GcDeadDb {
    expired_at: i64,
    vns: u64,
    old_vdb: u64,
    tail_address: u64,
  },
  /// 全局分配水位（0x05，doc/zh/db.md 即时原子提交段）：值为下一可分配号，
  /// 换号批收尾写入使水位单调抬升，重启重建取 max(落盘值, 映射扫描号+1)，
  /// 任一映射/墓碑丢写最坏旧域泄漏，绝不旧号复用撞号
  NextId { next_virtual_id: u64 },
  /// SWAPDB 成对映射单记录（0x06，一次追加即真原子，杜绝两条 DB_MAP 撕裂
  /// 出双库同指向的持久中间态）：键
  /// `[0x06][vns: 8B be][logic_db1: 8B be][logic_db2: 8B be]`，值
  /// `[db1 新指向: 8B be][db2 新指向: 8B be]`
  DbSwap {
    vns: u64,
    logic_db1: u64,
    logic_db2: u64,
    swapped_db1: u64,
    swapped_db2: u64,
  },
}

/// DbMeta 键载荷定长缓冲（最大 25 字节 + 1 字节长度，支持 `Copy`）
///
/// 容量取六变体键载荷最大值，杜绝调用点各配 `[0u8; 9/17/25]` 裸数组与双分支
/// 共享缓冲的错位隐患；调用方经 [`Self::as_slice`] 取有效载荷切片。
#[derive(Debug, Clone, Copy)]
pub struct DbMetaKeyBuf {
  buf: [u8; DbMetaRecord::KEY_MAX_LEN],
  len: u8,
}

impl DbMetaKeyBuf {
  /// 获取键载荷有效只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    &self.buf[..self.len as usize]
  }
}

/// DbMeta 记录值定长缓冲（最大 16 字节 + 1 字节长度，支持 `Copy`）
///
/// 容量取六变体记录值最大值（0x06 双指向 16 字节，其余定长 8 字节），
/// 与 [`DbMetaKeyBuf`] 同形态杜绝调用点裸数组；调用方经 [`Self::as_slice`]
/// 取有效值切片。
#[derive(Debug, Clone, Copy)]
pub struct DbMetaValueBuf {
  buf: [u8; DbMetaRecord::VALUE_MAX_LEN],
  len: u8,
}

impl DbMetaValueBuf {
  /// 获取记录值有效只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    &self.buf[..self.len as usize]
  }
}

impl DbMetaRecord {
  /// 0x05 键侧固定标记（doc/zh/db.md 布局行 `[0x05][b"next_virtual_id"]`）
  const NEXT_ID_MARKER: &'static [u8] = b"next_virtual_id";

  /// 六变体键载荷字节数（布局单点：9 / 17 / 17 / 25 / 16 / 25）
  const KEY_NS_MAP_LEN: usize = 9;
  const KEY_DB_MAP_LEN: usize = 17;
  const KEY_GC_DEAD_NS_LEN: usize = 17;
  const KEY_GC_DEAD_DB_LEN: usize = 25;
  const KEY_NEXT_ID_LEN: usize = Self::NEXT_ID_MARKER.len() + 1;
  const KEY_DB_SWAP_LEN: usize = Self::KEY_GC_DEAD_DB_LEN;
  /// 键载荷最大长度（[`DbMetaKeyBuf`] 定长容量）
  pub const KEY_MAX_LEN: usize = Self::KEY_GC_DEAD_DB_LEN;
  /// 单值变体记录值定长字节数（8 字节大端 u64）
  const VALUE_LEN: usize = 8;
  /// 0x06 双指向记录值定长字节数
  const VALUE_DB_SWAP_LEN: usize = Self::VALUE_LEN * 2;
  /// 记录值最大长度（[`DbMetaValueBuf`] 定长容量）
  pub const VALUE_MAX_LEN: usize = Self::VALUE_DB_SWAP_LEN;

  /// 键载荷 = 子类型 + 键侧字段逐段定长大端；映射变体的值侧字段
  /// （vns / vdb）、墓碑变体的 tail_address、0x05 的下一可分配号与 0x06 的
  /// 两条新指向均不进键
  pub fn key(&self) -> DbMetaKeyBuf {
    let mut buf = [0u8; Self::KEY_MAX_LEN];
    let len = match self {
      Self::NsMap { logic_ns, .. } => {
        buf[0] = VdbMetaSubType::NsMap.as_u8();
        put_be64(&mut buf[1..9], *logic_ns);
        Self::KEY_NS_MAP_LEN
      }
      Self::DbMap { vns, logic_db, .. } => {
        buf[0] = VdbMetaSubType::DbMap.as_u8();
        put_be64(&mut buf[1..9], *vns);
        put_be64(&mut buf[9..17], *logic_db);
        Self::KEY_DB_MAP_LEN
      }
      Self::GcDeadNs {
        expired_at,
        old_vns,
        ..
      } => {
        buf[0] = VdbMetaSubType::GcDeadNs.as_u8();
        put_be_i64(&mut buf[1..9], *expired_at);
        put_be64(&mut buf[9..17], *old_vns);
        Self::KEY_GC_DEAD_NS_LEN
      }
      Self::GcDeadDb {
        expired_at,
        vns,
        old_vdb,
        ..
      } => {
        buf[0] = VdbMetaSubType::GcDeadDb.as_u8();
        put_be_i64(&mut buf[1..9], *expired_at);
        put_be64(&mut buf[9..17], *vns);
        put_be64(&mut buf[17..25], *old_vdb);
        Self::KEY_GC_DEAD_DB_LEN
      }
      Self::NextId { .. } => {
        buf[0] = VdbMetaSubType::NextId.as_u8();
        buf[1..Self::KEY_NEXT_ID_LEN].copy_from_slice(Self::NEXT_ID_MARKER);
        Self::KEY_NEXT_ID_LEN
      }
      Self::DbSwap {
        vns,
        logic_db1,
        logic_db2,
        ..
      } => {
        buf[0] = VdbMetaSubType::DbSwap.as_u8();
        put_be64(&mut buf[1..9], *vns);
        put_be64(&mut buf[9..17], *logic_db1);
        put_be64(&mut buf[17..25], *logic_db2);
        Self::KEY_DB_SWAP_LEN
      }
    };
    DbMetaKeyBuf {
      buf,
      len: len as u8,
    }
  }

  /// 记录值 = 定长大端：映射项存新虚拟号，GC 墓碑项存回收截断线
  /// tail_address，0x05 存下一可分配号，0x06 存两条新指向（写侧 value
  /// 口径的唯一表达）
  pub fn value(&self) -> DbMetaValueBuf {
    let mut buf = [0u8; Self::VALUE_MAX_LEN];
    let len = match self {
      Self::NsMap { vns, .. } => {
        put_be64(&mut buf, *vns);
        Self::VALUE_LEN
      }
      Self::DbMap { vdb, .. } => {
        put_be64(&mut buf, *vdb);
        Self::VALUE_LEN
      }
      Self::GcDeadNs { tail_address, .. } | Self::GcDeadDb { tail_address, .. } => {
        put_be64(&mut buf, *tail_address);
        Self::VALUE_LEN
      }
      Self::NextId { next_virtual_id } => {
        put_be64(&mut buf, *next_virtual_id);
        Self::VALUE_LEN
      }
      Self::DbSwap {
        swapped_db1,
        swapped_db2,
        ..
      } => {
        put_be64(&mut buf, *swapped_db1);
        put_be64(&mut buf[Self::VALUE_LEN..], *swapped_db2);
        Self::VALUE_DB_SWAP_LEN
      }
    };
    DbMetaValueBuf {
      buf,
      len: len as u8,
    }
  }

  /// 点查探测 NS_MAP 键载荷：仅有 logic_ns（vns 即查询目标）时的单点构造，
  /// 与 [`Self::key`] 的 NsMap 臂同布局
  pub fn key_ns_map(logic_ns: u64) -> DbMetaKeyBuf {
    Self::NsMap { logic_ns, vns: 0 }.key()
  }

  /// 点查探测 DB_MAP 键载荷：仅有 (vns, logic_db)（vdb 即查询目标）时的单点
  /// 构造，与 [`Self::key`] 的 DbMap 臂同布局
  pub fn key_db_map(vns: u64, logic_db: u64) -> DbMetaKeyBuf {
    Self::DbMap {
      vns,
      logic_db,
      vdb: 0,
    }
    .key()
  }

  /// GC 墓碑回收注销：从墓碑键载荷提取被回收的死亡虚拟号（GC 变体载荷末 8
  /// 字节恰为其 vid）；非 GC 载荷或长度不符为 None，不判死
  pub fn dead_vid_of(payload: &[u8]) -> Option<u64> {
    Some(Self::dead_tombstone_of(payload)?.0)
  }

  /// 墓碑注销键判别（DbMeta 镜像 StoreDelete 条目应用面）：返回
  /// `(死亡虚拟号, 是否命名空间级)`，非墓碑键 None
  ///
  /// 注销只需要键自足信息（值侧 tail_address 已随判死登记入账），角色由键
  /// 子类型甄别——命名空间级注销须连带释放废弃租户路由快照
  /// （[`VirtualDbManager`] 与 [`crate::gc`] 注销编排共用本判别）
  pub fn dead_tombstone_of(payload: &[u8]) -> Option<(u64, bool)> {
    match VdbMetaSubType::from_u8(payload.first().copied()?)? {
      VdbMetaSubType::GcDeadNs if payload.len() == Self::KEY_GC_DEAD_NS_LEN => {
        Some((get_be64(payload, payload.len() - Self::VALUE_LEN)?, true))
      }
      VdbMetaSubType::GcDeadDb if payload.len() == Self::KEY_GC_DEAD_DB_LEN => {
        Some((get_be64(payload, payload.len() - Self::VALUE_LEN)?, false))
      }
      _ => None,
    }
  }

  /// 重建解码：键载荷 + 定长记录值复原完整记录；未知子类型、长度错位一律
  /// None（调用方不得静默吞掉，须告警留痕）
  pub fn decode(payload: &[u8], value: &[u8]) -> Option<Self> {
    match VdbMetaSubType::from_u8(payload.first().copied()?)? {
      // 0x06 双指向值 16 字节，不走单值 8 字节前置强解（否则被当作长度
      // 错位静默吞进未知臂）
      VdbMetaSubType::DbSwap if payload.len() == Self::KEY_DB_SWAP_LEN => {
        if value.len() != Self::VALUE_DB_SWAP_LEN {
          return None;
        }
        Some(Self::DbSwap {
          vns: get_be64(payload, 1)?,
          logic_db1: get_be64(payload, 9)?,
          logic_db2: get_be64(payload, 17)?,
          swapped_db1: get_be64(value, 0)?,
          swapped_db2: get_be64(value, Self::VALUE_LEN)?,
        })
      }
      subtype => {
        let val = <[u8; Self::VALUE_LEN]>::try_from(value)
          .ok()
          .map(u64::from_be_bytes)?;
        match subtype {
          VdbMetaSubType::NsMap if payload.len() == Self::KEY_NS_MAP_LEN => Some(Self::NsMap {
            logic_ns: get_be64(payload, 1)?,
            vns: val,
          }),
          VdbMetaSubType::DbMap if payload.len() == Self::KEY_DB_MAP_LEN => Some(Self::DbMap {
            vns: get_be64(payload, 1)?,
            logic_db: get_be64(payload, 9)?,
            vdb: val,
          }),
          VdbMetaSubType::GcDeadNs if payload.len() == Self::KEY_GC_DEAD_NS_LEN => {
            Some(Self::GcDeadNs {
              expired_at: get_be_i64(payload, 1)?,
              old_vns: get_be64(payload, 9)?,
              tail_address: val,
            })
          }
          VdbMetaSubType::GcDeadDb if payload.len() == Self::KEY_GC_DEAD_DB_LEN => {
            Some(Self::GcDeadDb {
              expired_at: get_be_i64(payload, 1)?,
              vns: get_be64(payload, 9)?,
              old_vdb: get_be64(payload, 17)?,
              tail_address: val,
            })
          }
          // 0x05 固定标记不符即错位：不接受任意 16 字节载荷
          VdbMetaSubType::NextId
            if payload.len() == Self::KEY_NEXT_ID_LEN && &payload[1..] == Self::NEXT_ID_MARKER =>
          {
            Some(Self::NextId {
              next_virtual_id: val,
            })
          }
          _ => None,
        }
      }
    }
  }

  /// 点查记录值解码（8 字节大端 u64，长度必符）
  pub fn decode_value(bytes: &[u8]) -> Option<u64> {
    <[u8; Self::VALUE_LEN]>::try_from(bytes)
      .ok()
      .map(u64::from_be_bytes)
  }
}

/// 布局写原语：定长大端写入目标 8 字节窗
#[inline]
fn put_be64(dst: &mut [u8], v: u64) {
  dst[..8].copy_from_slice(&v.to_be_bytes());
}

#[inline]
fn put_be_i64(dst: &mut [u8], v: i64) {
  put_be64(dst, v as u64);
}

/// 布局读原语：定长大端读出偏移 8 字节窗，越界 None
#[inline]
fn get_be64(buf: &[u8], off: usize) -> Option<u64> {
  <[u8; 8]>::try_from(buf.get(off..off + 8)?)
    .ok()
    .map(u64::from_be_bytes)
}

#[inline]
fn get_be_i64(buf: &[u8], off: usize) -> Option<i64> {
  get_be64(buf, off).map(|v| v as i64)
}

#[cfg(test)]
mod tests {
  use super::*;

  /// DbMeta 布局固化：六变体 encode→decode 往返一致、点查键与写侧键同字节、
  /// 长度差一字节必失败、未知子类型必失败、墓碑注销口径（载荷末 8 字节）
  #[test]
  fn test_dbmeta_record_roundtrip() {
    let records = [
      DbMetaRecord::NsMap {
        logic_ns: 7,
        vns: 3,
      },
      DbMetaRecord::DbMap {
        vns: 5,
        logic_db: 2,
        vdb: 9,
      },
      DbMetaRecord::GcDeadNs {
        expired_at: -123_456_789,
        old_vns: 11,
        tail_address: u64::MAX,
      },
      DbMetaRecord::GcDeadDb {
        expired_at: 1_000_000_000,
        vns: 12,
        old_vdb: 13,
        tail_address: 42,
      },
      DbMetaRecord::NextId {
        next_virtual_id: 1_099_511_627_775,
      },
      DbMetaRecord::DbSwap {
        vns: 5,
        logic_db1: 2,
        logic_db2: 8,
        swapped_db1: 14,
        swapped_db2: 9,
      },
    ];
    let key_lens = [9usize, 17, 17, 25, 16, 25];
    for (rec, klen) in records.iter().zip(key_lens) {
      let key = rec.key();
      assert_eq!(key.as_slice().len(), klen, "键载荷长度固化于单点");
      let back = DbMetaRecord::decode(key.as_slice(), rec.value().as_slice())
        .expect("encode→decode 往返必复原（含负 expired_at 与 u64::MAX）");
      assert_eq!(rec, &back);
      if rec.value().as_slice().len() == 8 {
        assert_eq!(
          DbMetaRecord::decode_value(rec.value().as_slice()),
          Some(u64::from_be_bytes(
            <[u8; 8]>::try_from(rec.value().as_slice()).unwrap()
          ))
        );
      }
    }
    // 点查探测键与写侧键同字节：NS_MAP/DB_MAP 键与 vns/vdb 值侧字段无关；
    // 0x06 值侧两条新指向不进键
    assert_eq!(
      DbMetaRecord::key_ns_map(7).as_slice(),
      records[0].key().as_slice()
    );
    assert_eq!(
      DbMetaRecord::key_db_map(5, 2).as_slice(),
      records[1].key().as_slice()
    );
    // 长度差一字节必失败（截短与多尾各验一轮）
    for rec in &records {
      let key = rec.key();
      let k = key.as_slice();
      assert!(DbMetaRecord::decode(&k[..k.len() - 1], rec.value().as_slice()).is_none());
      let mut longer = k.to_vec();
      longer.push(0);
      assert!(DbMetaRecord::decode(&longer, rec.value().as_slice()).is_none());
      // 记录值非定长（8B 族截为 7B / 0x06 截为 15B）必失败
      assert!(
        DbMetaRecord::decode(
          k,
          &rec.value().as_slice()[..rec.value().as_slice().len() - 1]
        )
        .is_none()
      );
    }
    // 未知子类型必失败
    let bogus = [0x09u8; 9];
    assert!(DbMetaRecord::decode(&bogus, &[0u8; 8]).is_none());
    assert_eq!(DbMetaRecord::decode_value(&bogus), None);
    // 0x05 固定标记不符必失败（任意 16 字节载荷不误读为水位）
    let mut fake_marker = [0u8; 16];
    fake_marker[0] = 0x05;
    assert!(DbMetaRecord::decode(&fake_marker, &[0u8; 8]).is_none());
    // 0x06 键不得混读为 GcDeadDb（同为 25B 但子类型分派）
    assert!(
      DbMetaRecord::decode(
        records[5].key().as_slice(),
        &records[3].value().as_slice()[..8]
      )
      .is_none()
    );
    // 墓碑注销：GC 变体载荷末 8 字节即死亡 vid，映射变体不判死
    assert_eq!(
      DbMetaRecord::dead_vid_of(records[2].key().as_slice()),
      Some(11)
    );
    assert_eq!(
      DbMetaRecord::dead_vid_of(records[3].key().as_slice()),
      Some(13)
    );
    assert!(DbMetaRecord::dead_vid_of(records[0].key().as_slice()).is_none());
    assert!(DbMetaRecord::dead_vid_of(records[1].key().as_slice()).is_none());
    assert!(DbMetaRecord::dead_vid_of(&[]).is_none());
    // 前缀错位不误读：真 GC_DEAD_NS 键整体右移一字节仍为 17B，子类型签名
    // 已不在 offset 0，末 8 字节却是位移后的实况字节——只按长度放行的实现
    // 必从此处读出假死亡号并把活库判死
    let shifted = {
      let dead_key = records[2].key();
      let mut b = [0u8; DbMetaRecord::KEY_GC_DEAD_NS_LEN];
      b[1..].copy_from_slice(&dead_key.as_slice()[..DbMetaRecord::KEY_GC_DEAD_NS_LEN - 1]);
      b
    };
    assert!(DbMetaRecord::dead_vid_of(&shifted).is_none());
  }
}

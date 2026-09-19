use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{Acquire, Relaxed},
    },
  },
};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ms,
};
use wval::SessionPrefixBuf;

/// 根域虚拟号（ns 0 / db 0 共用的零号）：常驻 immortal，永不参与空闲析构与引用计数
pub const ROOT_VIRTUAL_ID: u64 = 0;

/// DbMeta 系统记录固定根域前缀（ns 0, db 0）：set_context / flush / swap
/// 持久化、GC 墓碑删除与点查装载读面共用的落位前缀（键载荷与记录值布局的
/// 单点编解码见 [`DbMetaRecord`]，doc/zh/db.md 1.4）
pub const ROOT_DBMETA_PREFIX: SessionPrefixBuf = SessionPrefixBuf::new(0, 0);

/// 逻辑数据库到虚拟数据库 ID 的槽位级路由表（每格 `Arc<ArcSwap<u64>>` 单元格）
///
/// papaya 并发字典承载 logic_db → 单元格指针，FLUSHDB / SWAPDB 换号只单格
/// 原子换指虚拟库 ID：成本恒定 O(1)，与租户在册库数无关，杜绝整表写时克隆
/// 的线性放大（doc/zh/db.md 1.3/1.4「单次 O(1) 原子替换、耗时小于 1 微秒」
/// 的内存换号段口径）。读面一律经本表单点访问器（[`Self::get`] 等），禁散点
/// 直取 cells，杜绝旁路绕过单元格语义。
#[derive(Debug)]
pub struct DbRoutingTable {
  cells: ConcurrentMap<u64, Arc<ArcSwap<u64>>>,
}

impl DbRoutingTable {
  pub fn new() -> Self {
    Self {
      cells: new_concurrent_map(),
    }
  }

  /// 单点读访问器：逻辑库当前指向的虚拟库 ID（未映射为 None）
  #[inline]
  pub fn get(&self, logic_db: u64) -> Option<u64> {
    self.cells.pin().get(&logic_db).map(|cell| **cell.load())
  }

  /// 单点在册判定：逻辑库是否已有映射单元格（冷库甄别只读原语）
  #[inline]
  pub fn contains(&self, logic_db: u64) -> bool {
    self.cells.pin().contains_key(&logic_db)
  }

  /// 取用逻辑库单元格（缺席则原子建格预置 `initial`，恒返回在册格）——
  /// 换号写面（flush / swap / 回灌装载）共用的单点结构入口，并发首访经
  /// papaya get_or_insert 无锁抢占仅一者胜出插入
  #[inline]
  pub fn cell_or_insert(&self, logic_db: u64, initial: u64) -> Arc<ArcSwap<u64>> {
    let pin = self.cells.pin();
    Arc::clone(pin.get_or_insert(logic_db, Arc::new(ArcSwap::from_pointee(initial))))
  }

  /// 幂等覆盖写映射（重建装载与点查回建单点：后写覆盖，无旧值语义）
  #[inline]
  pub fn set(&self, logic_db: u64, vdb: u64) {
    self.cell_or_insert(logic_db, vdb).store(Arc::new(vdb));
  }

  /// FLUSHDB 单格换号：原子换指新虚拟库号并返回换号前旧指向（None = 本
  /// 运行期首映射，库从无退役旧域）。新建号全局唯一，故 swap 返回值等于
  /// `new_vdb` 即本格为刚建格，无需 CAS 重试
  #[inline]
  pub fn swap_out(&self, logic_db: u64, new_vdb: u64) -> Option<u64> {
    let old = self
      .cell_or_insert(logic_db, new_vdb)
      .swap(Arc::new(new_vdb));
    (*old != new_vdb).then_some(*old)
  }

  /// 只读枚举全部在册映射 `(logic_db, vdb)`（库枚举 / INFO 统计面专用，
  /// O(在册库数)，不在换号与读写热路径上）
  pub fn snapshot(&self) -> Vec<(u64, u64)> {
    let pin = self.cells.pin();
    pin
      .iter()
      .map(|(logic_db, cell)| (*logic_db, **cell.load()))
      .collect()
  }
}

impl Default for DbRoutingTable {
  fn default() -> Self {
    Self::new()
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
  const SUBTYPE_NS_MAP: u8 = 0x01;
  const SUBTYPE_DB_MAP: u8 = 0x02;
  const SUBTYPE_GC_DEAD_NS: u8 = 0x03;
  const SUBTYPE_GC_DEAD_DB: u8 = 0x04;
  const SUBTYPE_NEXT_ID: u8 = 0x05;
  const SUBTYPE_DB_SWAP: u8 = 0x06;

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
        buf[0] = Self::SUBTYPE_NS_MAP;
        put_be64(&mut buf[1..9], *logic_ns);
        Self::KEY_NS_MAP_LEN
      }
      Self::DbMap { vns, logic_db, .. } => {
        buf[0] = Self::SUBTYPE_DB_MAP;
        put_be64(&mut buf[1..9], *vns);
        put_be64(&mut buf[9..17], *logic_db);
        Self::KEY_DB_MAP_LEN
      }
      Self::GcDeadNs {
        expired_at,
        old_vns,
        ..
      } => {
        buf[0] = Self::SUBTYPE_GC_DEAD_NS;
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
        buf[0] = Self::SUBTYPE_GC_DEAD_DB;
        put_be_i64(&mut buf[1..9], *expired_at);
        put_be64(&mut buf[9..17], *vns);
        put_be64(&mut buf[17..25], *old_vdb);
        Self::KEY_GC_DEAD_DB_LEN
      }
      Self::NextId { .. } => {
        buf[0] = Self::SUBTYPE_NEXT_ID;
        buf[1..Self::KEY_NEXT_ID_LEN].copy_from_slice(Self::NEXT_ID_MARKER);
        Self::KEY_NEXT_ID_LEN
      }
      Self::DbSwap {
        vns,
        logic_db1,
        logic_db2,
        ..
      } => {
        buf[0] = Self::SUBTYPE_DB_SWAP;
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
    match payload.first().copied()? {
      Self::SUBTYPE_GC_DEAD_NS if payload.len() == Self::KEY_GC_DEAD_NS_LEN => {
        Some((get_be64(payload, payload.len() - Self::VALUE_LEN)?, true))
      }
      Self::SUBTYPE_GC_DEAD_DB if payload.len() == Self::KEY_GC_DEAD_DB_LEN => {
        Some((get_be64(payload, payload.len() - Self::VALUE_LEN)?, false))
      }
      _ => None,
    }
  }

  /// 重建解码：键载荷 + 定长记录值复原完整记录；未知子类型、长度错位一律
  /// None（调用方不得静默吞掉，须告警留痕）
  pub fn decode(payload: &[u8], value: &[u8]) -> Option<Self> {
    match payload.first().copied()? {
      // 0x06 双指向值 16 字节，不走单值 8 字节前置强解（否则被当作长度
      // 错位静默吞进未知臂）
      Self::SUBTYPE_DB_SWAP if payload.len() == Self::KEY_DB_SWAP_LEN => {
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
          Self::SUBTYPE_NS_MAP if payload.len() == Self::KEY_NS_MAP_LEN => Some(Self::NsMap {
            logic_ns: get_be64(payload, 1)?,
            vns: val,
          }),
          Self::SUBTYPE_DB_MAP if payload.len() == Self::KEY_DB_MAP_LEN => Some(Self::DbMap {
            vns: get_be64(payload, 1)?,
            logic_db: get_be64(payload, 9)?,
            vdb: val,
          }),
          Self::SUBTYPE_GC_DEAD_NS if payload.len() == Self::KEY_GC_DEAD_NS_LEN => {
            Some(Self::GcDeadNs {
              expired_at: get_be_i64(payload, 1)?,
              old_vns: get_be64(payload, 9)?,
              tail_address: val,
            })
          }
          Self::SUBTYPE_GC_DEAD_DB if payload.len() == Self::KEY_GC_DEAD_DB_LEN => {
            Some(Self::GcDeadDb {
              expired_at: get_be_i64(payload, 1)?,
              vns: get_be64(payload, 9)?,
              old_vdb: get_be64(payload, 17)?,
              tail_address: val,
            })
          }
          // 0x05 固定标记不符即错位：不接受任意 16 字节载荷
          Self::SUBTYPE_NEXT_ID
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

/// 待回收死亡虚拟 ID 项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcDeadEntry {
  pub expired_at: i64,
  pub tail_address: u64,
  /// 库级换号时记录所属虚拟命名空间 ID (vns)；命名空间换号时为 None
  pub vns: Option<u64>,
}

/// 租户路由快照（槽位级单元格路由表 + 会话引用计数）
///
/// `refs` 为绑定到本租户的活跃会话数：绑定协议（[`VirtualDbManager::bind_route`]）
/// 保证持引用期间快照不被空闲析构摘除，同步读路径因此永远命中在册快照、
/// 绝不盲分配换号；引用归零后快照进入空闲析构候选，由 GC 轮次摘除释放，
/// 后续访问经磁盘 DbMeta 点查装载回建（doc/zh/db.md 冷租户条款）
pub struct TenantRouting {
  pub table: DbRoutingTable,
  /// 绑定会话数（0 = 空闲可析构）
  refs: AtomicU64,
  /// 本运行期权威全量标志：新建租户（ns 首次映射）置位——其路由表由本运行
  /// 期逐库持久化维护，表内缺库即真新库，可同步分配；磁盘装载回建的租户
  /// 恒为否（表仅部分装载，缺库须经点查甄别冷库与真新库）
  authoritative: AtomicBool,
}

impl TenantRouting {
  #[inline]
  pub fn new(table: DbRoutingTable) -> Arc<Self> {
    Arc::new(Self {
      table,
      refs: AtomicU64::new(0),
      authoritative: AtomicBool::new(false),
    })
  }

  #[inline]
  fn refs(&self) -> u64 {
    self.refs.load(Acquire)
  }
}

/// 死亡虚拟 ID 待回收账本（O(1) 点查判死 + 按到期时间排序的小根堆索引）
///
/// 全量历史墓碑留存底层磁盘（KeyTag::DbMeta），内存账本仅持有未回收项；
/// `map` 承载点查判死（Compaction 顺带丢弃与 keyspace 统计过滤），
/// `expiry_heap` 为按 `expired_at` 升序的惰性索引——sweep 只弹到期前缀，
/// 彻底消除按租户数放大的每轮全表遍历（doc/zh/db.md「内存仅加载近期
/// 即将到期的小根堆」）。堆条目不随删除同步移除，弹出时以 `map` 最新态
/// 校验失效；同 vid 无重用号空间，账本条目唯一
pub struct GcDeadLog {
  map: ConcurrentMap<u64, GcDeadEntry>,
  expiry_heap: Mutex<BinaryHeap<Reverse<(i64, u64)>>>,
}

impl GcDeadLog {
  #[inline]
  fn new() -> Self {
    Self {
      map: new_concurrent_map(),
      expiry_heap: Mutex::new(BinaryHeap::new()),
    }
  }

  /// 登记死亡项（map + 到期堆索引）
  #[inline]
  pub fn insert(&self, vid: u64, entry: GcDeadEntry) {
    self.map.pin().insert(vid, entry);
    self
      .expiry_heap
      .lock()
      .push(Reverse((entry.expired_at, vid)));
  }

  /// 摘除死亡项（重建期墓碑回放撤销；堆索引惰性失效，弹出时校验）
  #[inline]
  pub fn remove(&self, vid: &u64) {
    self.map.pin().remove(vid);
  }

  /// 点查死亡项
  #[inline]
  pub fn get(&self, vid: &u64) -> Option<GcDeadEntry> {
    self.map.pin().get(vid).copied()
  }

  /// 积压长度（高低水位熔断口径）
  #[inline]
  pub fn len(&self) -> usize {
    self.map.pin().len()
  }

  /// 积压是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 清空（超管物理截断重置）
  #[inline]
  pub fn clear(&self) {
    self.map.pin().clear();
    self.expiry_heap.lock().clear();
  }

  /// 弹出「已到期且日志截断线已越过生前写入点」的回收前缀
  ///
  /// 只沿小根堆弹出到期条目（`expired_at <= now`），未到期即停——单轮
  /// 扫描成本与到期前缀成正比，与账本总量无关。到期但 `begin_address`
  /// 尚未越过 `tail_address` 的条目暂不入回收集，待紧缩推进后下轮重弹
  /// （幂等）；堆中已被回收/撤销的陈旧索引条目直接丢弃。
  ///
  /// 锁面短临界批量解耦（doc/zh/db.md 换号零锁口径的 GC 侧对表）：锁内只
  /// 摘到期前缀入局部栈，账本双检、物理摘除与暂扣回插全在锁外执行——后台
  /// sweep 长循环不再阻塞换号路径的墓碑登记（[`Self::insert`]），杜绝并发
  /// FLUSHDB 在 sweep 窗口的等锁尖刺
  pub fn pop_reclaimable(&self, now: i64, begin_addr: u64) -> Vec<(u64, GcDeadEntry)> {
    // 锁内短临界：仅按堆序摘出到期前缀（纯堆操作，无账本访问）
    let mut due: Vec<(i64, u64)> = {
      let mut heap = self.expiry_heap.lock();
      let mut due = Vec::new();
      while let Some(Reverse((expired_at, vid))) = heap.peek().copied() {
        if expired_at > now {
          break;
        }
        heap.pop();
        due.push((expired_at, vid));
      }
      due
    };
    // 锁外甄别回收：账本双检、物理摘除与暂扣分类；暂扣项统一回插等紧缩推进
    let pin = self.map.pin();
    let mut reclaimed = Vec::new();
    let mut deferred = Vec::new();
    for (expired_at, vid) in due.drain(..) {
      match pin.get(&vid).copied() {
        // 陈旧索引（已回收 / 重建期墓碑撤销）：丢弃
        None => {}
        Some(entry) if entry.expired_at == expired_at => {
          if entry.tail_address <= begin_addr {
            pin.remove(&vid);
            reclaimed.push((vid, entry));
          } else {
            // 截断线未越界：暂扣，收集完后统一回插（等紧缩推进，下轮重弹）
            deferred.push(Reverse((expired_at, vid)));
          }
        }
        // 键存活但到期时间与索引不符（防御性丢弃陈旧索引）
        Some(_) => {}
      }
    }
    if !deferred.is_empty() {
      self.expiry_heap.lock().extend(deferred);
    }
    reclaimed
  }
}

/// 虚拟数据库管理器（papaya 无锁并发字典 + 槽位级 ArcSwap 单元格路由表）
///
/// 双层映射的内存形态按「冷租户与冷库 0 内存常驻」收敛为三类：
/// - `ns_map` / `active_vns`：逻辑命名空间映射标量（16 字节级，装载后常驻）；
/// - `db_routing`：租户路由快照（[`DbRoutingTable`] 槽位级单元格表），按需
///   装载、引用归零空闲析构，析构后访问经磁盘 DbMeta 点查装载回建；
/// - [`GcDeadLog`]：死亡号账本 + 近期到期小根堆，sweep 只弹到期前缀。
///
/// 启动期重建不再全量灌入映射（仅根域、死亡账本与分配水位），冷数据全留磁盘；
/// 同步路径（[`Self::get_or_create_ns`] / [`Self::get_or_create_db`]）退化为
/// 纯内存原语，冷装载由异步解析面单点承担
pub struct VirtualDbManager {
  /// 租户空间映射: logic_ns -> virtual_ns_id
  pub ns_map: ConcurrentMap<u64, u64>,

  /// 活跃虚空间映射 (反向索引 O(1) 判活): virtual_ns_id -> logic_ns
  pub active_vns: ConcurrentMap<u64, u64>,

  /// 库级路由快照表: virtual_ns_id -> Arc<TenantRouting>
  pub db_routing: ConcurrentMap<u64, Arc<TenantRouting>>,

  /// 全局下一分配 ID
  pub next_virtual_id: AtomicU64,

  /// 全局换号纪元版本（FLUSHDB / SWAPDB / FLUSHALL 自增，驱动 StoreSession 刷新本地缓存）
  pub generation: AtomicU64,

  /// 死亡待回收账本（点查判死 + 到期前缀弹出）
  pub gc_dead: GcDeadLog,

  /// 空闲析构期限堆：(期限毫秒, vns) 升序；引用归零时登记，GC 轮次弹出
  /// 到期前缀逐个摘除（惰性失效：弹出时复核引用与在册状态）
  idle_heap: Mutex<BinaryHeap<Reverse<(u64, u64)>>>,
}

impl Default for VirtualDbManager {
  fn default() -> Self {
    Self::new()
  }
}

impl VirtualDbManager {
  pub fn new() -> Self {
    let ns_map = new_concurrent_map();
    let active_vns = new_concurrent_map();
    let db_routing = new_concurrent_map();
    {
      let pin_ns = ns_map.pin();
      pin_ns.insert(0, 0);
      let pin_active = active_vns.pin();
      pin_active.insert(0, 0);
      let pin_db = db_routing.pin();
      let root_table = DbRoutingTable::new();
      root_table.set(0, ROOT_VIRTUAL_ID);
      pin_db.insert(0, TenantRouting::new(root_table));
    }
    let vdb = Self {
      ns_map,
      active_vns,
      db_routing,
      next_virtual_id: AtomicU64::new(1),
      generation: AtomicU64::new(1),
      gc_dead: GcDeadLog::new(),
      idle_heap: Mutex::new(BinaryHeap::new()),
    };
    // 根域快照 immortal（重建全量装载 + 永不空闲析构），权威全量
    vdb.mark_route_authoritative(ROOT_VIRTUAL_ID);
    vdb
  }

  /// 重置虚拟数据库映射（超管物理截断时重置为初始零号状态）
  pub fn reset(&self) {
    let pin_ns = self.ns_map.pin();
    pin_ns.clear();
    pin_ns.insert(0, 0);

    let pin_active = self.active_vns.pin();
    pin_active.clear();
    pin_active.insert(0, 0);

    let pin_db = self.db_routing.pin();
    pin_db.clear();
    let root_table = DbRoutingTable::new();
    root_table.set(0, ROOT_VIRTUAL_ID);
    pin_db.insert(0, TenantRouting::new(root_table));
    self.mark_route_authoritative(ROOT_VIRTUAL_ID);

    self.gc_dead.clear();
    self.idle_heap.lock().clear();

    self.next_virtual_id.store(1, Relaxed);
    self.bump_generation();
  }

  /// 插入或更新逻辑命名空间到虚拟命名空间的映射，同步维护活跃反向索引
  pub fn insert_ns_mapping(&self, logic_ns: u64, vns: u64) {
    let pin_ns = self.ns_map.pin();
    if let Some(&old_vns) = pin_ns.insert(logic_ns, vns)
      && old_vns != vns
    {
      self.active_vns.pin().remove(&old_vns);
    }
    self.active_vns.pin().insert(vns, logic_ns);
  }

  /// 推进换号版本代数
  #[inline]
  pub fn bump_generation(&self) {
    self.generation.fetch_add(1, Relaxed);
  }

  /// 获取新虚拟 ID
  #[inline]
  pub fn alloc_next_virtual_id(&self) -> u64 {
    self.next_virtual_id.fetch_add(1, Relaxed)
  }

  /// 单调抬升分配水位（DbMeta 镜像应用面：主库已用的号在本节点绝不再分配）
  ///
  /// 从库应用镜像映射/墓碑记录时折叠值侧新号与键侧死亡旧号（与重建收尾
  /// [`WedbStore::finish_vdb_rebuild`] 的 `fetch_max(max_vid + 1)` 同口径），
  /// 保证后续本地取号不与主库未来取号撞号
  #[inline]
  pub fn bump_watermark(&self, min_next: u64) {
    self.next_virtual_id.fetch_max(min_next, Relaxed);
  }

  /// 枚举本节点全部活跃逻辑库 `(namespace, db)`（集群库级分片面的唯一
  /// 库枚举取用：CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT 按库定槽聚合、
  /// 扩缩容整库搬迁枚举本节点持有库，均经此单点，不新增 db 元数据结构）
  pub fn list_logic_dbs(&self) -> Vec<(u64, u64)> {
    let ns_pin = self.ns_map.pin();
    let routing_pin = self.db_routing.pin();
    let mut out = Vec::new();
    for (logic_ns, vns) in ns_pin.iter() {
      let Some(routing) = routing_pin.get(vns) else {
        continue;
      };
      out.extend(
        routing
          .table
          .snapshot()
          .into_iter()
          .map(|(db, _)| (*logic_ns, db)),
      );
    }
    out
  }

  /// 纯内存原语：获取虚命名空间 ID，不存在则原子分配新 ID
  ///
  /// 仅限启动引导、内部非严格会话与解析面的「磁盘点查未命中后分配」语义；
  /// 用户会话的冷装载走 [`crate::store::WedbStore::resolve_context`] 先点查
  /// 磁盘 DbMeta，禁止在未点查前盲分配（冷租户一经盲分配即换新号，
  /// 旧域数据被判死丢失——正是本模块要消灭的缺陷）
  pub fn get_or_create_ns(&self, logic_ns: u64) -> (u64, bool) {
    let pin = self.ns_map.pin();
    if let Some(&vns) = pin.get(&logic_ns) {
      return (vns, false);
    }
    let new_vns = self.alloc_next_virtual_id();
    let actual = pin.get_or_insert(logic_ns, new_vns);
    let created = *actual == new_vns;
    if created {
      self.active_vns.pin().insert(new_vns, logic_ns);
    }
    (*actual, created)
  }

  /// 取租户路由快照（在册直返，缺席建空表回插）
  pub fn routing_for(&self, vns: u64) -> Arc<TenantRouting> {
    let pin_routing = self.db_routing.pin();
    if let Some(r) = pin_routing.get(&vns) {
      return Arc::clone(r);
    }
    pin_routing
      .get_or_insert(vns, TenantRouting::new(DbRoutingTable::new()))
      .clone()
  }

  /// 纯内存原语：获取虚数据库 ID，不存在则写入（语义约束同 [`Self::get_or_create_ns`]）
  pub fn get_or_create_db(&self, logic_ns: u64, logic_db: u64) -> (u64, bool) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let routing = self.routing_for(vns);
    if let Some(vdb) = routing.table.get(logic_db) {
      return (vdb, false);
    }

    // 慢路径：并发首访经单格 get_or_insert 无锁抢占，仅一者建格胜出；
    // 新建号全局唯一，格内值等于本号即建格者，否则复用并发胜出者
    let new_vdb = self.alloc_next_virtual_id();
    let current = **routing.table.cell_or_insert(logic_db, new_vdb).load();
    (current, current == new_vdb)
  }

  /// 获取当前逻辑库对应的虚拟空间与虚拟库 ID（纯内存原语，语义约束同
  /// [`Self::get_or_create_ns`]）
  #[inline]
  pub fn get_virtual_ids(&self, logic_ns: u64, logic_db: u64) -> (u64, u64) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let (vdb, _) = self.get_or_create_db(logic_ns, logic_db);
    (vns, vdb)
  }

  /// 同步只读判定 (ns, db) 是否为冷库（严格上下文免盲分配门）
  ///
  /// 冷库 = ns 标量在册（重建装载全部 ns 标量，磁盘有记录必在册，未在册即
  /// 全新租户）且库映射未装载且路由表非本运行期权威全量——冷库与真新库
  /// 无法同步甄别（磁盘权威须点查方知），严格会话遇之拒绝盲分配，交异步
  /// 解析点查磁盘：命中装载既有映射（绝不换号），未命中创建并持久化
  #[inline]
  pub fn is_cold_db(&self, ns: u64, db: u64) -> bool {
    let Some(&vns) = self.ns_map.pin().get(&ns) else {
      return false; // ns 未在册 = 全新租户，同步创建
    };
    match self.db_routing.pin().get(&vns) {
      None => true, // 冷租户：路由快照未装载
      Some(r) => !r.authoritative.load(Relaxed) && !r.table.contains(db),
    }
  }

  /// 标记租户路由表为本运行期权威全量（ns 首次映射时调用）：后续新库映射
  /// 可直接同步分配，无需点查甄别
  #[inline]
  pub fn mark_route_authoritative(&self, vns: u64) {
    self.routing_for(vns).authoritative.store(true, Relaxed);
  }

  /// 点查租户路由表中的逻辑库映射（严格上下文与统计过滤的同步只读原语）
  #[inline]
  pub fn route_vdb_of(&self, vns: u64, logic_db: u64) -> Option<u64> {
    self
      .db_routing
      .pin()
      .get(&vns)
      .and_then(|r| r.table.get(logic_db))
  }

  /// 点查逻辑命名空间的虚拟空间 ID（未在册即 None；语义约束同
  /// [`Self::get_or_create_ns`]——只读甄别，绝不盲分配）
  #[inline]
  pub fn vns_of_ns(&self, logic_ns: u64) -> Option<u64> {
    self.ns_map.pin().get(&logic_ns).copied()
  }

  /// 点查虚拟空间的逻辑命名空间（[`Self::insert_ns_mapping`] 维护的
  /// `active_vns` 逆向索引只读读端：重建期 0x01 记录装载全部在册租户，
  /// 未在册即本节点从无该空间的逻辑入口；根域 (0, 0) 恒在册）
  #[inline]
  pub fn logic_ns_of(&self, vns: u64) -> Option<u64> {
    self.active_vns.pin().get(&vns).copied()
  }

  /// 物理域 → 逻辑域反查单点（[`Self::get_virtual_ids`] 的逆运算，只读）
  ///
  /// 回放面唯一的域反查入口：AOF keyed 条目只带物理前缀 `[vns][vdb]`（入账侧
  /// 键一律是引擎物理键），而库级定槽按逻辑域现算（`doc/zh/db.md` 4.1
  /// 「同一个 DB 对应同一个槽位」），故重放向量/索引族须经本反查取回逻辑
  /// `(ns, db)` 再交 `slot_of`，与在线面逐值同值。
  ///
  /// 两腿取用口径：`vns → logic_ns` 走 [`Self::active_vns`] 逆表（重建全量装载，
  /// 恒在册）；`vdb → logic_db` 走该租户路由表内指向本号的库格（按需装载面，
  /// O(在册库数) 只读枚举，冷租户快照未装载即无格可查）。任一反查未在册即按
  /// 物理号原样返回——本节点对该域无逻辑入口，物理号即其唯一身份，与根域
  /// (0,0)→(0,0) 恒等形态同形；全程零分配、零落盘、绝不改动任何映射。
  pub fn logic_domain_of(&self, vns: u64, vdb: u64) -> (u64, u64) {
    let logic_ns = self.logic_ns_of(vns).unwrap_or(vns);
    let logic_db = self
      .db_routing
      .pin()
      .get(&vns)
      .and_then(|routing| {
        routing
          .table
          .snapshot()
          .into_iter()
          .find(|&(_, mapped)| mapped == vdb)
          .map(|(logic_db, _)| logic_db)
      })
      .unwrap_or(vdb);
    (logic_ns, logic_db)
  }

  /// 只读枚举在册租户库快照 `(虚拟库号, 逻辑库号)`（[`Self::list_logic_dbs`]
  /// 的单租户投影，C# `GetDatabasesSnapshot` 的在册库口径）
  ///
  /// 租户未在册或路由快照未装载（冷库）即空集：本入口不建快照（区别于
  /// [`Self::routing_for`] 的缺席建表回插）、不分配虚库号、不落 DbMeta，
  /// 只读命令（INFO KEYSPACE 统计面）因此绝不改写存储状态
  pub fn registered_dbs(&self, vns: u64) -> Vec<(u64, u64)> {
    let routing_pin = self.db_routing.pin();
    let Some(routing) = routing_pin.get(&vns) else {
      return Vec::new();
    };
    routing
      .table
      .snapshot()
      .into_iter()
      .map(|(logic_db, vdb)| (vdb, logic_db))
      .collect()
  }

  /// 向租户路由表写入映射（后写覆盖，单格幂等）
  ///
  /// 磁盘 DbMeta 为映射权威：重建期根域装载与点查装载回建共用本入口，
  /// 覆盖引导期占位（如根域 (0,0)→0 初值）与陈旧在册值；并发写路径
  /// （flush/swap）持久化先于本入口可见时以磁盘记录为准
  pub fn insert_db_mapping(&self, vns: u64, logic_db: u64, vdb: u64) {
    self.routing_for(vns).table.set(logic_db, vdb);
  }

  /// 绑定会话到租户路由快照（refs+1）
  ///
  /// 摘除-回插竞态协议：计数递增后复核快照仍在映射内（被摘除则回插或
  /// 换绑新者），保证返回时本会话持有一个在册快照的引用——空闲析构的
  /// 摘除判定以引用为准，绑定期间快照绝不被析构，同步读路径因此永远
  /// 命中在册表。根域常驻 immortal 免计数；快照缺席时无保护返回（调用方
  /// 先经解析装载回建快照，本分支仅为防御）
  pub fn bind_route(&self, vns: u64) {
    if vns == ROOT_VIRTUAL_ID {
      return;
    }
    let pin = self.db_routing.pin();
    let mut route = match pin.get(&vns) {
      Some(r) => Arc::clone(r),
      None => return,
    };
    loop {
      route.refs.fetch_add(1, Acquire);
      match pin.get(&vns) {
        Some(r) if Arc::ptr_eq(r, &route) => return,
        Some(r) => {
          // 摘除后他方回插了新快照：解绑旧者改绑新者
          route.refs.fetch_sub(1, Acquire);
          route = Arc::clone(r);
        }
        None => match pin.get_or_insert(vns, Arc::clone(&route)) {
          // 被摘除且回插窗口内无他方快照：回插本快照（与磁盘重载内容一致）
          r if Arc::ptr_eq(r, &route) => return,
          r => {
            route.refs.fetch_sub(1, Acquire);
            route = Arc::clone(r);
          }
        },
      }
    }
  }

  /// 解绑租户路由快照（refs-1；归零时按 `idle_ms` 登记空闲析构期限）
  pub fn unbind_route(&self, vns: u64, idle_ms: u64) {
    if vns == ROOT_VIRTUAL_ID {
      return;
    }
    let Some(route) = self.db_routing.pin().get(&vns).cloned() else {
      return;
    };
    if route.refs.fetch_sub(1, Acquire) == 1 {
      let deadline = now_ms().saturating_add(idle_ms);
      self.idle_heap.lock().push(Reverse((deadline, vns)));
    }
  }

  /// 弹出空闲析构期限已到期的候选 vns 前缀
  ///
  /// 惰性失效：引用已复归（重绑定）、快照已不在册或已入死亡账本的条目
  /// 直接丢弃；未到期前缀保留在堆内
  pub fn pop_idle_candidates(&self, now_ms: u64) -> Vec<u64> {
    let mut heap = self.idle_heap.lock();
    let mut due = Vec::new();
    while let Some(Reverse((deadline, vns))) = heap.peek().copied() {
      if deadline > now_ms {
        break;
      }
      heap.pop();
      let idle = self
        .db_routing
        .pin()
        .get(&vns)
        .is_some_and(|r| r.refs() == 0)
        && !self.is_dead_ns(vns);
      if idle {
        due.push(vns);
      }
    }
    due
  }

  /// 空闲析构单个租户路由快照：摘除后引用仍归零才真正释放
  ///
  /// 摘除与绑定并发时（绑定方在摘除后递增计数）按摘除-回插协议回插同一
  /// 快照，绑定方经复核换绑，绝无「绑定生效而快照缺席」状态；根域拒释
  pub fn evict_idle_route(&self, vns: u64) -> bool {
    if vns == ROOT_VIRTUAL_ID {
      return false;
    }
    let pin = self.db_routing.pin();
    let Some(route) = pin.remove(&vns) else {
      return false;
    };
    if route.refs() != 0 {
      pin.get_or_insert(vns, Arc::clone(route));
      return false;
    }
    true
  }

  /// FLUSHDB：清空库并换号，返回 (new_vdb, old_vdb_opt)
  ///
  /// 内存换号段真 O(1)：分配新虚拟库号后对目标逻辑库单元格单次
  /// [`DbRoutingTable::swap_out`] 原子换指，零整表克隆、零 CAS 重试循环，
  /// 成本与租户在册库数无关（doc/zh/db.md 1.3「单次 O(1) 原子替换」口径）；
  /// 换出的旧库号登记死亡账本交延时 GC 回收。并发 flush 链式退役：后换出者
  /// 拿到的旧指向即前次换入的新号，逐代判死，杜绝旧号复用撞号。
  pub fn flush_db(
    &self,
    logic_ns: u64,
    logic_db: u64,
    expired_at: i64,
    tail_address: u64,
  ) -> (u64, Option<u64>) {
    let (vns, _) = self.get_or_create_ns(logic_ns);
    let routing = self.routing_for(vns);

    let new_vdb = self.alloc_next_virtual_id();
    let old_vdb_opt = routing.table.swap_out(logic_db, new_vdb);

    if let Some(old_vdb) = old_vdb_opt {
      self.gc_dead.insert(
        old_vdb,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: Some(vns),
        },
      );
    }

    self.bump_generation();
    (new_vdb, old_vdb_opt)
  }

  /// FLUSHALL：清空命名空间下所有库，返回 (new_vns, old_vns_opt)
  pub fn flush_ns(&self, logic_ns: u64, expired_at: i64, tail_address: u64) -> (u64, Option<u64>) {
    let new_vns = self.alloc_next_virtual_id();
    let pin_ns = self.ns_map.pin();
    let old_vns = pin_ns.insert(logic_ns, new_vns).copied().unwrap_or(new_vns);

    let pin_active = self.active_vns.pin();
    if old_vns != new_vns {
      pin_active.remove(&old_vns);
    }
    pin_active.insert(new_vns, logic_ns);

    let old_vns_opt = if old_vns != new_vns {
      self.gc_dead.insert(
        old_vns,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: None,
        },
      );
      Some(old_vns)
    } else {
      None
    };

    // 换号后新空间为本运行期权威全量（新库映射逐个持久化，表内缺库即真新库）
    self.mark_route_authoritative(new_vns);

    self.bump_generation();
    (new_vns, old_vns_opt)
  }

  /// 零锁无等待极速判断物理域 (vns, vdb) 是否已过期死亡（供 Compaction
  /// 顺带丢弃）
  ///
  /// 与 [`Self::is_dead_domain`] 同口径按退役角色精确比对（vns 与 vdb 共用
  /// 全局号空间，裸 id 命中判定在根库退役窗口会把一切同号活域整批误判死亡），
  /// 叠加 `expired_at` 到期判定：紧缩只丢已越过回收延迟的死域记录
  #[inline]
  pub fn is_virtual_id_dead_and_expired(&self, vns: u64, vdb: u64, now: i64) -> bool {
    if self
      .gc_dead
      .get(&vdb)
      .is_some_and(|e| e.vns.is_some() && e.expired_at <= now)
    {
      return true;
    }
    self
      .gc_dead
      .get(&vns)
      .is_some_and(|e| e.vns.is_none() && e.expired_at <= now)
  }

  /// 按退役角色精确判定物理域 (vns, vdb) 是否已死亡废弃
  ///
  /// vns 与 vdb 共用同一全局号空间（[`Self::get_or_create_ns`]、
  /// [`Self::get_or_create_db`]、[`Self::flush_db`]、[`Self::flush_ns`] 同走
  /// [`Self::alloc_next_virtual_id`]），故裸 id 命中判定在根域边界失准：
  /// FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，任何 vns=0 的活域都被误判死亡。
  /// 本判定经 [`GcDeadEntry::vns`] 区分退役角色——vdb 键须为库级退役
  /// （[`Self::flush_db`] 落 vns = Some）、vns 键须为命名空间级退役
  /// （[`Self::flush_ns`] 落 vns = None），角色不符的裸 id 碰撞不判死
  #[inline]
  pub fn is_dead_domain(&self, vns: u64, vdb: u64) -> bool {
    if self.gc_dead.get(&vdb).is_some_and(|e| e.vns.is_some()) {
      return true;
    }
    self.is_dead_ns(vns)
  }

  /// 命名空间级退役判定（点查装载防复活：换号退役的旧 vns 不再装回）
  #[inline]
  pub fn is_dead_ns(&self, vns: u64) -> bool {
    self.gc_dead.get(&vns).is_some_and(|e| e.vns.is_none())
  }

  /// 检查虚拟命名空间是否仍为活跃映射（O(1) 判定）
  #[inline]
  pub fn is_active_vns(&self, vns: u64) -> bool {
    self.active_vns.pin().contains_key(&vns)
  }

  /// 检查物理 (vns, vdb) 是否匹配当前活跃的指定逻辑库
  #[inline]
  pub fn matches_logic_db(&self, vns: u64, vdb: u64, target_logic_db: u64) -> bool {
    if self.is_dead_domain(vns, vdb) || !self.is_active_vns(vns) {
      return false;
    }
    self.route_vdb_of(vns, target_logic_db) == Some(vdb)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_vdb_active_vns() {
    let vdb = VirtualDbManager::new();
    assert!(vdb.is_active_vns(0));
    assert!(!vdb.is_active_vns(1));

    let (vns1, created) = vdb.get_or_create_ns(100);
    assert!(created);
    assert!(vdb.is_active_vns(vns1));

    // FLUSHALL
    let (vns2, old_vns) = vdb.flush_ns(100, 1000, 0);
    assert_eq!(old_vns, Some(vns1));
    assert!(!vdb.is_active_vns(vns1));
    assert!(vdb.is_active_vns(vns2));

    // insert_ns_mapping
    vdb.insert_ns_mapping(200, 999);
    assert!(vdb.is_active_vns(999));

    // Reset
    vdb.reset();
    assert!(vdb.is_active_vns(0));
    assert!(!vdb.is_active_vns(vns2));
    assert!(!vdb.is_active_vns(999));
  }

  /// 退役角色精确判定：FLUSHDB(0, 0) 退役 vdb 0 后 gc_dead 含键 0，
  /// 同 vns=0 的活库不得判死（仅旧 vdb 判死）；FLUSHNS 退役 vns 后该空间全域才判死
  #[test]
  fn test_vdb_dead_domain_role() {
    let vdb = VirtualDbManager::new();
    // 根命名空间：db0 -> vdb0（初始零号）、db1 -> vdb1（新分配）
    assert_eq!(vdb.get_virtual_ids(0, 1), (0, 1));
    assert!(!vdb.is_dead_domain(0, 0));
    assert!(!vdb.is_dead_domain(0, 1));

    // 库级换号：退役 vdb 0，键 0 与在用 vns 0 同号碰撞
    let (new_vdb, old_vdb) = vdb.flush_db(0, 0, 1000, 0);
    assert_eq!((new_vdb, old_vdb), (2, Some(0)));
    assert!(vdb.is_dead_domain(0, 0), "退役旧 vdb 判死");
    assert!(!vdb.is_dead_domain(0, 1), "同空间活库不得因裸 id 碰撞判死");
    assert!(vdb.matches_logic_db(0, 1, 1), "活库路由仍可匹配");
    assert!(vdb.matches_logic_db(0, new_vdb, 0), "换号后新 vdb 承接 db0");
    assert!(!vdb.matches_logic_db(0, 0, 0), "旧 vdb 不再匹配");

    // 命名空间级换号：退役 vns 0，该空间全域判死
    let (new_vns, old_vns) = vdb.flush_ns(0, 2000, 0);
    assert_eq!((new_vns, old_vns), (3, Some(0)));
    assert!(!vdb.is_active_vns(0));
    assert!(vdb.is_dead_domain(0, 1), "退役空间的活域判死");
  }

  /// 紧缩过期判定与角色比对同口径：库级退役键（vns=Some）只死配 vdb 槽，
  /// 空间级退役键（vns=None）只死配 vns 槽；未到期不判死（裸 id 同号碰撞
  /// 与到期前缀两维同时封死）
  #[test]
  fn test_vdb_dead_expired_role_and_time() {
    let vdb = VirtualDbManager::new();
    assert_eq!(vdb.get_virtual_ids(0, 1), (0, 1));
    // FLUSHDB(0,0) 以 expired_at=1000 退役 vdb 0：键 0 与在用 vns 0 同号
    let (_, Some(old_vdb)) = vdb.flush_db(0, 0, 1000, 0) else {
      unreachable!();
    };
    assert_eq!(old_vdb, 0);
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 0, 999),
      "未到期不得判死"
    );
    assert!(
      vdb.is_virtual_id_dead_and_expired(0, 0, 1000),
      "到期判死退役 vdb 0"
    );
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 1, 1000),
      "同空间活 vdb 1 不得因 vns 槽裸 id 命中判死（键 0 为库级退役）"
    );
    // FLUSHNS(0) 以 expired_at=2000 退役 vns 0：新 vns 3 号域判死，
    // 活 vdb 1 不因空间级键 0 的 vdb 槽比对被牵连
    let (new_vns, _) = vdb.flush_ns(0, 2000, 0);
    assert!(new_vns > 0);
    assert!(
      !vdb.is_virtual_id_dead_and_expired(0, 1, 1999),
      "空间级退役未到期不判死"
    );
    assert!(
      vdb.is_virtual_id_dead_and_expired(0, 1, 2000),
      "vns 0 空间级退役到期，该空间全域判死"
    );
  }

  /// 绑定协议：绑定期间空闲析构被回插拦截且映射不丢，解绑登记期限后可析构
  #[test]
  fn test_vdb_route_ref_eviction() {
    let vdb = VirtualDbManager::new();
    let (vns, _) = vdb.get_or_create_ns(7);
    vdb.get_or_create_db(7, 1);
    assert!(vdb.db_routing.pin().get(&vns).is_some());

    // 未绑定：即可析构（此形态对应未装载校验的防御面）
    assert!(vdb.evict_idle_route(vns), "空闲快照应被析构");
    assert!(vdb.db_routing.pin().get(&vns).is_none());

    // 析构后点查装载回建并绑定：绑定期间摘除被回插拦截
    vdb.insert_db_mapping(vns, 1, 1);
    vdb.bind_route(vns);
    assert!(!vdb.evict_idle_route(vns), "绑定期间不得析构");
    assert!(vdb.db_routing.pin().get(&vns).is_some());
    assert_eq!(vdb.route_vdb_of(vns, 1), Some(1), "绑定期间映射不丢");

    // 解绑归零登记期限，弹出候选后可析构
    vdb.unbind_route(vns, 0);
    assert!(vdb.pop_idle_candidates(0).is_empty(), "未到期不弹");
    assert_eq!(vdb.pop_idle_candidates(u64::MAX), vec![vns]);
    assert!(vdb.evict_idle_route(vns));

    // 重绑定使候选失效（惰性丢弃）
    vdb.bind_route(vns);
    vdb.unbind_route(vns, 0);
    vdb.bind_route(vns);
    assert!(
      vdb.pop_idle_candidates(u64::MAX).is_empty(),
      "引用复归候选失效"
    );
    vdb.unbind_route(vns, 0);

    // 根域永不参与计数与析构
    vdb.bind_route(ROOT_VIRTUAL_ID);
    vdb.unbind_route(ROOT_VIRTUAL_ID, 0);
    assert!(!vdb.pop_idle_candidates(u64::MAX).contains(&0));
    assert!(!vdb.evict_idle_route(ROOT_VIRTUAL_ID), "根域拒释");
    assert!(vdb.db_routing.pin().get(&0).is_some(), "根域快照常驻");
  }

  /// 死亡账本小根堆：sweep 只弹到期前缀，未到期与截断线未越界条目保留
  #[test]
  fn test_gc_dead_expiry_heap_prefix() {
    let log = GcDeadLog::new();
    let entry = |expired_at: i64, tail: u64| GcDeadEntry {
      expired_at,
      tail_address: tail,
      vns: None,
    };
    log.insert(1, entry(100, 0));
    log.insert(2, entry(200, 0));
    log.insert(3, entry(300, 0));
    log.insert(4, entry(250, 999)); // 到期但截断线未越界

    // 弹到 200 为止：250/300 未到期保留；100/200 已回收摘除
    let got = log.pop_reclaimable(200, 0);
    assert_eq!(got, vec![(1, entry(100, 0)), (2, entry(200, 0))]);
    assert!(log.get(&1).is_none() && log.get(&2).is_none());
    assert_eq!(log.get(&3).map(|e| e.expired_at), Some(300));

    // 同一时刻重弹：无重复回收，账本不动
    assert!(log.pop_reclaimable(200, 0).is_empty());

    // 截断线越界后 250 先回收，300 到期随后回收（堆序）
    let got = log.pop_reclaimable(300, 999);
    assert_eq!(got, vec![(4, entry(250, 999)), (3, entry(300, 0))]);
    assert_eq!(log.len(), 0);
  }

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

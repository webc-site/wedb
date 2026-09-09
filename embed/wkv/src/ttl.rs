use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use wbase::time::now_ms;
use wdev::Device;
use wval::{KeyTag, NamespaceDbCodec, TTL_VAL_LEN, TaggedKeyBuf, TtlCodec};

use crate::{error::Result, session::StoreSession};

/// TTL 记录值定长字节数（8 字节大端 u64 绝对毫秒时间戳，无需 bitcode）
pub const TTL_VALUE_LEN: usize = TTL_VAL_LEN;

/// 过期写选项（Redis EXPIRE NX/XX/GT/LT，风格对齐 wedb_hash::ExpireOpt）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TtlOpt {
  /// 仅当未设置过期时设置 (NX)
  pub nx: bool,
  /// 仅当已设置过期时设置 (XX)
  pub xx: bool,
  /// 仅当新过期时间大于当前过期时间时设置 (GT)
  pub gt: bool,
  /// 仅当新过期时间小于当前过期时间时设置 (LT)
  pub lt: bool,
}

impl TtlOpt {
  /// 无附加条件（普通 EXPIRE）
  pub const NONE: Self = Self {
    nx: false,
    xx: false,
    gt: false,
    lt: false,
  };
}

/// TTL 记录值解码（8 字节大端 u64，非法长度按无 TTL 容错处理）
#[inline(always)]
pub const fn ttl_val(v: &[u8]) -> Option<u64> {
  TtlCodec::decode(v)
}

/// mget 批量回调专用的同步 TTL 三态探针结果
pub enum TtlProbe {
  /// 放行：无 TTL 记录或未到期
  Pass,
  /// 已到期：先回调 None，批量读闭环后再物理清除
  Due,
  /// TTL 记录存在磁盘候选，需异步裁决（罕见冷路径，也是内存探针异常的降级出口）
  Deferred,
}

/// purge 链物理写镜像抑制守卫（RAII save/restore，panic/早退路径 Drop 兜底）
///
/// 进入时以 `swap` 把会话身份写入 [`WedbStore::purge_suppress`] 抑制槽，Drop 时
/// 无条件恢复进入前的旧值：嵌套 purge（已被"先删 TTL 记录"递归防护杜绝，此处
/// 纵深防御）与 unwind 路径均不会让标志残留——普通"置位 + Drop 清零"在嵌套场景
/// 会提前解抑制，save/restore 语义天然免疫
struct PurgeNotifyGuard<'a> {
  slot: &'a AtomicUsize,
  prev: usize,
}

impl<'a> PurgeNotifyGuard<'a> {
  /// 进入抑制窗口：`token` 为会话身份（&StoreSession 裸地址）
  #[inline]
  fn enter(slot: &'a AtomicUsize, token: usize) -> Self {
    let prev = slot.swap(token, Relaxed);
    Self { slot, prev }
  }
}

impl Drop for PurgeNotifyGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.slot.store(self.prev, Relaxed);
  }
}

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属 key 级 TTL 记录物理键 (KeyTag::Ttl)
  ///
  /// 镜像 `session_string_key`/`session_meta_key`：`[NsVarint] + [DbVarint] + 0x09 + [用户键]`
  #[inline(always)]
  pub fn ttl_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::Ttl, user_key)
  }

  /// 静态辅助：构造指定会话前缀的 TTL 记录物理键
  #[inline(always)]
  pub fn ttl_key_with_prefix(prefix: &[u8], user_key: &[u8]) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_with_session_prefix(prefix, KeyTag::Ttl, user_key)
  }

  /// 快速探测 TTL 记录标签是否存在于哈希索引（纯内存单次哈希探针，无记录 I/O）
  #[inline(always)]
  pub(crate) fn has_ttl_key(&self, ttl_k: &TaggedKeyBuf) -> Result<bool> {
    let _guard = self.participant.enter();
    self.has_ttl_key_unprotected(ttl_k)
  }

  /// 在已有纪元保护下快速探测 TTL 记录标签是否存在（彻底绕过 enter() 原子开销）
  #[inline(always)]
  pub(crate) fn has_ttl_key_unprotected(&self, ttl_k: &TaggedKeyBuf) -> Result<bool> {
    Ok(self.store.index.find_tag(ttl_k).is_some())
  }

  /// 快速探测用户键的 TTL 记录标签是否存在于哈希索引（纯内存单次哈希探针，无记录 I/O）
  #[inline]
  pub fn has_ttl_tag(&self, user_key: &[u8]) -> Result<bool> {
    let ttl_k = self.ttl_key(user_key);
    self.has_ttl_key(&ttl_k)
  }

  /// 在已有纪元保护下快速探测用户键的 TTL 记录标签（批处理上下文专用，零原子开销）
  #[inline(always)]
  pub fn has_ttl_tag_unprotected(&self, user_key: &[u8]) -> Result<bool> {
    let ttl_k = self.ttl_key(user_key);
    self.has_ttl_key_unprotected(&ttl_k)
  }

  /// 读取 TTL 记录的绝对过期毫秒时间戳（None = 无 TTL；记录值非法长度亦按无 TTL 容错）
  pub async fn ttl_of(&self, user_key: &[u8]) -> Result<Option<u64>> {
    let ttl_k = self.ttl_key(user_key);
    Ok(self.read_raw_with(&ttl_k, ttl_val).await?.flatten())
  }

  /// 写入 TTL 记录（定长 8B：可变区原位改写优先，失败降级 RCU 盲插）
  ///
  /// 取舍：原位改写零追加、零哈希表 CAS，是 EXPIRE 反复续期的主路径；记录已落盘
  /// 或复活槽位不适配时，upsert 写路径自身含链内复活/盲插兜底，无需在此重复处理。
  /// 原位改写走 raw unprotected 内核，本异步入口须自带纪元保护（对标 C# Tsavorite：
  /// 一切索引/日志访问都必须在 epoch 保护下执行，否则并发驱逐可在改写窗口内回收页内存）
  pub async fn put_ttl(&self, user_key: &[u8], expire_at_ms: u64) -> Result<()> {
    let bytes = TtlCodec::encode(expire_at_ms);
    let ttl_k = self.ttl_key(user_key);
    let in_place = {
      let _guard = self.participant.enter();
      self
        .try_modify_raw_in_place_unprotected(&ttl_k, |slot| {
          slot.copy_from_slice(&bytes);
          Some(())
        })?
        .is_some()
    };
    if !in_place {
      self.upsert_raw(&ttl_k, &bytes).await?;
    }
    Ok(())
  }

  /// 删除 TTL 记录（先单次哈希探针初筛：绝大多数无 TTL 键零额外写入与零索引污染）
  pub(crate) async fn del_ttl(&self, user_key: &[u8]) -> Result<()> {
    let ttl_k = self.ttl_key(user_key);
    if self.has_ttl_key(&ttl_k)? {
      self.delete_raw(&ttl_k).await?;
    }
    Ok(())
  }

  /// 物理清除已过期键（数据 + TTL 记录，走统一 DEL 路径保证索引/墓碑/WAL 钩子一致）
  ///
  /// 约定：必须先删 TTL 记录再调 delete，保证 delete 内部 load_meta 的 TTL 守卫
  /// 探测不到记录而不触发二次清除，天然杜绝递归（抑制挂点不改变该顺序）。
  ///
  /// AOF 单条化（对标 Garnet `RespInputFlags.Deterministic` 携带绝对过期时间的
  /// 单条确定性逻辑条目语义）：purge 端口在场时，链内物理写（TTL 记录墓碑 +
  /// 数据墓碑两条）的写监听镜像经会话级抑制槽精确跳过（仅本会话，其他会话并发
  /// 写不受影响），链成功闭环后触发一次端口，携带
  /// `(ns, db, 用户键, expire_at_ms)`；端口未注册时保持现状两条物理条目
  /// （嵌入式无 AOF 场景行为不变）。链中途失败（`?` 早退）守卫即时解抑制且
  /// 不触发端口——清除未闭环不得宣告过期。
  pub(crate) async fn purge_expired(&self, user_key: &[u8], expire_at_ms: u64) -> Result<()> {
    let _suppress = self
      .store
      .ttl_purge_listener()
      .map(|_| PurgeNotifyGuard::enter(&self.store.purge_suppress, self as *const Self as usize));
    self.del_ttl(user_key).await?;
    self.delete(user_key).await?;
    // 先解抑制再触发端口：端口实现体内的写镜像不受抑制窗口影响
    drop(_suppress);
    if let Some(listener) = self.store.ttl_purge_listener() {
      listener(self.namespace(), self.active_db(), user_key, expire_at_ms);
    }
    Ok(())
  }

  /// 同步内存 TTL 三态探针（mget_each 批量回调内使用，不触达磁盘，不传播错误）
  pub fn probe_ttl(&self, user_key: &[u8], now: u64) -> TtlProbe {
    let ttl_k = self.ttl_key(user_key);
    let Ok(res) = self.try_read_raw_in_memory(&ttl_k, ttl_val) else {
      // 内存探针异常：降级异步裁决（check_expired 内含完整磁盘路径与错误传播）
      return TtlProbe::Deferred;
    };
    match res {
      None => TtlProbe::Deferred,
      Some(None) => TtlProbe::Pass,
      Some(Some(v)) => match v {
        Some(exp) if exp <= now => TtlProbe::Due,
        _ => TtlProbe::Pass,
      },
    }
  }

  /// 惰性过期检查：key 已过期则物理删除（数据 + TTL 记录）并返回 true
  ///
  /// 读入口统一接线（read/contains_key/load_meta 等），内部仅经 raw 路径探测 TTL 记录，
  /// 绝不回调这些入口，无递归。约定调用方"先确认 key 存在再探测"（TTL 记录不存在即无
  /// TTL，快路径仅一次哈希探针零额外 I/O）；对不存在键调用亦安全（返回 false）
  pub async fn check_expired(&self, user_key: &[u8]) -> Result<bool> {
    let Some(exp) = self.ttl_of(user_key).await? else {
      return Ok(false);
    };
    if exp > now_ms() {
      return Ok(false);
    }
    // 装箱打破 async 递归布局环（purge_expired → delete → load_meta → check_expired），
    // 仅在真正过期的冷路径付出一次堆分配
    Box::pin(self.purge_expired(user_key, exp)).await?;
    Ok(true)
  }

  /// 设置 key 绝对过期毫秒时间戳 (EXPIREAT / PEXPIREAT 语义，相对时长由调用方换算)
  ///
  /// 返回码（对齐 Redis 7.4）：-2 key 不存在；0 NX/XX/GT/LT 条件不满足；
  /// 1 成功；2 已过期并立即物理删除。
  ///
  /// 融合遍历（原 3 次压至 2 次）：1 次[`Self::contains_key_ignore_ttl`]裸数据存活
  /// 判定（剥离 TTL 探测）+ 1 次 TTL 记录读取，后者同时服务「TTL 记录存在且已到期
  /// = 键已过期 = 视同不存在」的存活修正与 NX/XX/GT/LT 条件判定。语义不变式：
  /// - 判序对齐 Redis 与 wedb_hash::hexpire：先键存活、再选项校验、最后过去时间戳
  ///   删除，条件不满足时绝不误删 key；
  /// - TTL 记录不存在不代表键不存在（键可能从未设 TTL），存活判定始终以数据记录为
  ///   准，孤儿 TTL 记录（数据已亡）绝不误判存活；
  /// - TTL 记录已到期的键先 `purge_expired`（先删 TTL 记录再删数据）再返回 -2，
  ///   与 contains_key 的惰性过期口径一致；
  /// - 持本键独占桶锁串行化读改写窗口（对齐 hexpire 先例），锁内记录遍历由 3 压至 2
  pub async fn expire_at(&self, user_key: &[u8], expire_at_ms: u64, opt: TtlOpt) -> Result<i32> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    // 遍历 1/2：裸数据存活判定（无 TTL 探测，本键 TTL 裁决统一收敛到遍历 2）
    if !self.contains_key_ignore_ttl(user_key).await? {
      return Ok(-2);
    }
    // 遍历 2/2：单次 TTL 记录读取，同时完成「已过期」存活修正与 NX/XX/GT/LT 判定
    match self.ttl_of(user_key).await? {
      // TTL 记录已到期：惰性过期即视同不存在，物理清除（先删 TTL 再删数据）后 -2
      Some(c) if c <= now_ms() => {
        self.purge_expired(user_key, c).await?;
        Ok(-2)
      }
      // 已设未到期 TTL：NX 禁设、GT/LT 按当前值比较（多项同设须全部满足才放行）
      Some(c) if opt.nx || (opt.gt && expire_at_ms <= c) || (opt.lt && expire_at_ms >= c) => Ok(0),
      Some(_) => self.expire_at_apply(user_key, expire_at_ms).await,
      // 从未设 TTL：XX/GT 无当前值可比，一律不满足
      None if opt.xx || opt.gt => Ok(0),
      None => self.expire_at_apply(user_key, expire_at_ms).await,
    }
  }

  /// expire_at 尾段公共体：条件全部通过后写 TTL，或过去时间戳立即物理删除
  async fn expire_at_apply(&self, user_key: &[u8], expire_at_ms: u64) -> Result<i32> {
    // 过去时间戳：立即物理删除（数据 + TTL 记录，先删 TTL 再删数据）
    if expire_at_ms <= now_ms() {
      self.purge_expired(user_key, expire_at_ms).await?;
      return Ok(2);
    }
    self.put_ttl(user_key, expire_at_ms).await?;
    Ok(1)
  }

  /// 移除 key 的过期时间 (PERSIST)。返回 1=移除成功；0=key 不存在或未设 TTL
  ///
  /// 融合遍历（原 contains_key 内嵌惰性过期裁决 + 独立 ttl_of 共 3 次压至 2 次）：
  /// 1 次[`Self::contains_key_ignore_ttl`]裸数据存活判定 + 1 次 TTL 记录读取，
  /// 后者同时完成「已到期视同不存在」存活修正与有无 TTL 判定；语义不变式与
  /// [`Self::expire_at`]一致（已到期键先 purge_expired 再返回 0，先删 TTL 再删数据）
  pub async fn persist(&self, user_key: &[u8]) -> Result<i32> {
    if !self.contains_key_ignore_ttl(user_key).await? {
      return Ok(0);
    }
    match self.ttl_of(user_key).await? {
      // 从未设 TTL：无可移除
      None => Ok(0),
      // TTL 已到期：键视同不存在，惰性物理清除后返回 0（对齐原 contains_key 口径）
      Some(c) if c <= now_ms() => {
        self.purge_expired(user_key, c).await?;
        Ok(0)
      }
      Some(_) => {
        self.del_ttl(user_key).await?;
        Ok(1)
      }
    }
  }

  /// 查询 key 剩余过期毫秒数 (TTL / PTTL 语义)。-2 无 key；-1 无 TTL；否则 >0
  pub async fn pttl_ms(&self, user_key: &[u8]) -> Result<i64> {
    if !self.contains_key(user_key).await? {
      return Ok(-2);
    }
    match self.ttl_of(user_key).await? {
      None => Ok(-1),
      Some(exp) => Ok(exp.saturating_sub(now_ms()) as i64),
    }
  }

  /// 查询 key 绝对过期毫秒时间戳 (EXPIRETIME / PEXPIRETIME 语义)。-2 无 key；-1 无 TTL
  pub async fn expiretime_ms(&self, user_key: &[u8]) -> Result<i64> {
    if !self.contains_key(user_key).await? {
      return Ok(-2);
    }
    match self.ttl_of(user_key).await? {
      None => Ok(-1),
      // 防御性钳位：极端未来时间戳折叠为 i64 上界，避免 u64→i64 静默翻负
      Some(exp) => Ok(exp.min(i64::MAX as u64) as i64),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::panic::{AssertUnwindSafe, catch_unwind};

  use super::*;

  /// 抑制守卫 RAII 语义：正常退出与 panic unwind 路径均恢复进入前旧值（标志零残留）
  #[test]
  fn purge_notify_guard_restores_on_drop_and_unwind() {
    let slot = AtomicUsize::new(0);

    // 正常作用域退出：恢复 save/restore 链上的旧值
    {
      let _g = PurgeNotifyGuard::enter(&slot, 0x1234);
      assert_eq!(slot.load(Relaxed), 0x1234);
    }
    assert_eq!(slot.load(Relaxed), 0);

    // 嵌套：内层退出恢复外层窗口，而非直接清零
    let outer = PurgeNotifyGuard::enter(&slot, 0xAAAA);
    {
      let _inner = PurgeNotifyGuard::enter(&slot, 0xBBBB);
      assert_eq!(slot.load(Relaxed), 0xBBBB);
    }
    assert_eq!(slot.load(Relaxed), 0xAAAA);
    drop(outer);
    assert_eq!(slot.load(Relaxed), 0);

    // panic unwind：Drop 兜底，抑制标志不残留
    let _ = catch_unwind(AssertUnwindSafe(|| {
      let _g = PurgeNotifyGuard::enter(&slot, 0x5678);
      assert_eq!(slot.load(Relaxed), 0x5678);
      panic!("unwind through guard");
    }));
    assert_eq!(slot.load(Relaxed), 0, "panic 路径不得残留抑制标志");
  }
}

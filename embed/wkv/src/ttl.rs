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
  /// 或复活槽位不适配时，upsert 写路径自身含链内复活/盲插兜底，无需在此重复处理
  pub async fn put_ttl(&self, user_key: &[u8], expire_at_ms: u64) -> Result<()> {
    let bytes = TtlCodec::encode(expire_at_ms);
    let ttl_k = self.ttl_key(user_key);
    let in_place = self
      .try_modify_raw_in_place_unprotected(&ttl_k, |slot| {
        slot.copy_from_slice(&bytes);
        Some(())
      })?
      .is_some();
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
  /// 探测不到记录而不触发二次清除，天然杜绝递归
  pub(crate) async fn purge_expired(&self, user_key: &[u8]) -> Result<()> {
    self.del_ttl(user_key).await?;
    self.delete(user_key).await?;
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
    Box::pin(self.purge_expired(user_key)).await?;
    Ok(true)
  }

  /// 设置 key 绝对过期毫秒时间戳 (EXPIREAT / PEXPIREAT 语义，相对时长由调用方换算)
  ///
  /// 返回码（对齐 Redis 7.4）：-2 key 不存在；0 NX/XX/GT/LT 条件不满足；
  /// 1 成功；2 已过期并立即物理删除。
  /// 判序对齐 Redis 与 wedb_hash::hexpire：先键存活、再选项校验、最后过去时间戳删除，
  /// 条件不满足时绝不误删 key。持本键独占桶锁串行化读改写窗口（对齐 hexpire 先例）
  pub async fn expire_at(&self, user_key: &[u8], expire_at_ms: u64, opt: TtlOpt) -> Result<i32> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    // 键存活判定（惰性过期在 contains_key 内闭环，已过期即视同不存在）
    if !self.contains_key(user_key).await? {
      return Ok(-2);
    }
    let curr = self.ttl_of(user_key).await?;
    if let Some(c) = curr {
      if opt.nx {
        return Ok(0);
      }
      if opt.gt && expire_at_ms <= c {
        return Ok(0);
      }
      if opt.lt && expire_at_ms >= c {
        return Ok(0);
      }
    } else if opt.xx || opt.gt {
      return Ok(0);
    }
    // 过去时间戳：条件已通过，立即物理删除（数据 + TTL 记录）
    if expire_at_ms <= now_ms() {
      self.purge_expired(user_key).await?;
      return Ok(2);
    }
    self.put_ttl(user_key, expire_at_ms).await?;
    Ok(1)
  }

  /// 移除 key 的过期时间 (PERSIST)。返回 1=移除成功；0=key 不存在或未设 TTL
  pub async fn persist(&self, user_key: &[u8]) -> Result<i32> {
    if !self.contains_key(user_key).await? || self.ttl_of(user_key).await?.is_none() {
      return Ok(0);
    }
    self.del_ttl(user_key).await?;
    Ok(1)
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

//! SWAPDB 跨库交换数据面内核
//!
//! 在 garnet 中的相对路径:libs/server/Databases/MultiDatabaseManager.cs:TrySwapDatabases
//!
//! C# 两库为独立 Tsavorite 实例，SWAPDB 仅交换 GarnetDatabase 容器指针
//! （store + objectStore + 检查点 + lastSaveData 随容器走），字符串/对象/
//! TTL/AOF 全量零丢失。rust 为共享存储 + db 前缀模型，必须真实搬移数据：
//! 全 tag 收集两库快照（String / ObjectEnvelope / Meta 域 + 随键 TTL/ETag
//! 旁路记录）→ 双库完整删除 → 交叉重写并重放旁路记录。全程物理写经
//! `StoreSession` 写端口，AOF 写监听端口自动镜像（StoreUpsert /
//! ObjectStoreUpsert / StoreDelete / PEXPIREAT / PERSIST / SETWITHETAG），
//! `--recover` 与副本重放后视图与主端收敛——绝不静默截断 AOF。
//!
//! RangeIndex（Meta 域 + wbftree 树文件）按「元记录交叉重写 + 树文件保留」
//! 搬移：树文件按用户键哈希全局命名、注册表键不含 db 前缀（与 C# 全局单例
//! RangeIndexManager 一致），两库交换键集合后各自的树文件引用保持有效，
//! key_id 全局唯一随元记录原值迁移无冲突。元记录重写走 raw 端口，不触
//! RangeIndex 创建/删除监听（RI 的 AOF 面现状即以 db=0 固定键记录
//! RICREATE/RIDROP，与库域无交集，保持口径）。
//!
//! 非原子性边界：交换为「收集 → 删除 → 重插」三段，中途存储错误会留下
//! 部分搬移态且无法回滚（对齐 C# 在活跃会话 > 1 时直接拒绝换库的防御：
//! 调用方须以会话门控保证交换期间无并发写，见 wnode 命令面）。

use gxhash::HashMap as GxHashMap;
use wdev::Device;
use wval::{KeyTag, MetaValue, NamespaceDbCodec};

use crate::{error::Result, session::StoreSession};

/// 单库交换快照：全 tag 用户域 + 随键旁路记录
struct DbSwapSnapshot {
  /// 字符串键值（KeyTag::String 域）
  strings: Vec<(Vec<u8>, Vec<u8>)>,
  /// 对象信封记录（KeyTag::ObjectEnvelope 域；值 = `[1B 类型][bitcode 载荷]`，
  /// 整值即 Hash/Set/ZSet/List 对象全量）
  envelopes: Vec<(Vec<u8>, Vec<u8>)>,
  /// Meta 元记录原值（KeyTag::Meta 域；RangeIndex 为 MetaValue + RangeIndexStub）
  metas: Vec<(Vec<u8>, Vec<u8>)>,
  /// 随键 TTL（绝对 .NET Ticks）
  ttls: Vec<(Vec<u8>, i64)>,
  /// 随键 ETag
  etags: Vec<(Vec<u8>, i64)>,
}

/// 同键多版本聚合桶：地址升序扫描后遇覆盖先遇（最新版本胜出），
/// `None` = 最新态为墓碑（键已亡，不入交换清单）
type LatestVal = GxHashMap<Vec<u8>, Option<Vec<u8>>>;

impl<D: Device> StoreSession<D> {
  /// 交换两库全部用户域数据（SWAPDB 数据面内核，共享存储前缀模型）
  ///
  /// C# TrySwapDatabases 的等价数据面：库 1 数据落库 2，反之亦然；随键
  /// TTL/ETag 严格跟随；对象信封整值搬移（含 Hash/Set/ZSet/List 紧凑对象与
  /// RangeIndex 元记录）；树文件按用户键全局命名不随库迁移。任一存储操作
  /// 失败返回 Err（C# 失败口径，调用方回错）。
  ///
  /// 调用约定：上层须已拦截 `db_id1 == db_id2`（同库短路 +OK）并完成活跃
  /// 会话门控（> 1 拒绝）；交换期间调用方须保证无并发写。
  pub async fn swap_databases(&self, db_id1: i64, db_id2: i64) -> Result<()> {
    if db_id1 == db_id2 {
      return Ok(());
    }
    let ns = self.namespace();
    let snap1 = self.collect_swap_snapshot(ns, db_id1).await?;
    let snap2 = self.collect_swap_snapshot(ns, db_id2).await?;
    self.purge_swap_source(ns, db_id1, &snap1).await?;
    self.purge_swap_source(ns, db_id2, &snap2).await?;
    // 交叉重写：库 1 快照落库 2，库 2 快照落库 1（C# 交换容器指针的等价数据面）
    self.restore_swap_target(ns, db_id2, &snap1).await?;
    self.restore_swap_target(ns, db_id1, &snap2).await?;
    Ok(())
  }

  /// 收集指定库全 tag 快照（单次日志扫描分域聚合 + 逐键旁路记录探测）
  async fn collect_swap_snapshot(&self, ns: u64, db_id: i64) -> Result<DbSwapSnapshot> {
    let db = db_id.max(0) as u64;
    let mut strings = LatestVal::default();
    let mut envelopes = LatestVal::default();
    let mut metas = LatestVal::default();
    self
      .store
      .hlog()
      .scan(
        self.store.begin_address(),
        self.store.tail_address(),
        |_addr, rec| {
          if let Ok((rec_ns, rec_db, tag, user_key)) =
            NamespaceDbCodec::decode_tagged_key(rec.key())
            && rec_ns == ns
            && rec_db == db
            && tag.is_user_visible()
          {
            let bucket = match tag {
              KeyTag::String => &mut strings,
              KeyTag::ObjectEnvelope => &mut envelopes,
              KeyTag::Meta => &mut metas,
              // is_user_visible 封闭上述三域，穷尽防御
              _ => return Ok(true),
            };
            bucket.insert(
              user_key.to_vec(),
              (!rec.is_tombstone()).then(|| rec.value().to_vec()),
            );
          }
          Ok(true)
        },
      )
      .await?;
    self.set_context(ns, db);
    let mut ttls = Vec::new();
    let mut etags = Vec::new();
    // 旁路记录逐键探测（在目标库前缀上下文下；TTL/ETag 记录键 = 前缀 + 旁路 tag + 用户键）
    let mut key_set: Vec<&[u8]> = strings
      .keys()
      .chain(envelopes.keys())
      .chain(metas.keys())
      .map(|k| k.as_slice())
      .collect();
    key_set.sort_unstable();
    for key in key_set {
      if let Some(ticks) = self.ttl_of(key).await? {
        ttls.push((key.to_vec(), ticks));
      }
      if let Some(etag) = self.etag_of(key).await? {
        etags.push((key.to_vec(), etag));
      }
    }
    Ok(DbSwapSnapshot {
      strings: latest_alive(strings),
      envelopes: latest_alive(envelopes),
      metas: latest_alive(metas),
      ttls,
      etags,
    })
  }

  /// 清空库的全部用户域（交换源清侧）
  ///
  /// String/Envelope 键走完整删除（[`StoreSession::delete`]：随键 TTL/ETag
  /// 清理 + 双域墓碑，物理写经写端口入 AOF）；Meta 键保留树文件仅写元记录
  /// 墓碑 + 旁路清理（树文件由交叉重写后的对方库继续引用）。
  async fn purge_swap_source(&self, ns: u64, db_id: i64, snap: &DbSwapSnapshot) -> Result<()> {
    self.set_context(ns, db_id.max(0) as u64);
    for (key, _) in snap.strings.iter().chain(snap.envelopes.iter()) {
      self.delete(key).await?;
    }
    for (key, val) in &snap.metas {
      // 版本栅栏先判死（历史打平子键立即逻辑失效），再写元记录墓碑
      if let Ok(meta) = MetaValue::from_slice(val) {
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, false);
      }
      let meta_k = self.session_meta_key(key);
      self.delete_raw(&meta_k).await?;
      self.del_ttl(key).await?;
      self.del_etag(key).await?;
    }
    Ok(())
  }

  /// 将快照重写入库（交换目标落侧；物理写经写端口自动镜像 AOF）
  async fn restore_swap_target(&self, ns: u64, db_id: i64, snap: &DbSwapSnapshot) -> Result<()> {
    self.set_context(ns, db_id.max(0) as u64);
    for (key, val) in &snap.strings {
      self.upsert(key, val).await?;
    }
    for (key, val) in &snap.envelopes {
      self.upsert_tag(key, KeyTag::ObjectEnvelope, val).await?;
      // 信封整值写通知（wnode 异步段对象写漏斗的等价端口触发：入队
      // ObjectStoreUpsert 全量条目，副本/恢复重放直写信封域）
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      self.store.notify_envelope_upsert(env_k.as_slice(), val);
    }
    for (key, val) in &snap.metas {
      if let Ok(meta) = MetaValue::from_slice(val) {
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, true);
      }
      let meta_k = self.session_meta_key(key);
      self.upsert_raw(&meta_k, val).await?;
    }
    // 值先于旁路记录落位（重放端 PEXPIREAT/SETWITHETAG 依赖值条目在场）
    for (key, ticks) in &snap.ttls {
      self.put_ttl(key, *ticks).await?;
    }
    for (key, etag) in &snap.etags {
      self.put_etag(key, *etag).await?;
    }
    Ok(())
  }
}

/// 聚合桶物化：剔除墓碑态（None）键，仅保留存活键值
fn latest_alive(bucket: LatestVal) -> Vec<(Vec<u8>, Vec<u8>)> {
  bucket
    .into_iter()
    .filter_map(|(k, v)| v.map(|val| (k, val)))
    .collect()
}

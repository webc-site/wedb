use std::sync::Arc;

use wdev::Device;
use whasher::fast_hash;
use wval::{KeyTag, SessionPrefixBuf};

use super::{migration::drain_guard_ok, range_index_blocking};
use crate::{
  error::{Error, Result},
  session::StoreSession,
  store::StoreEvent,
};

/// 排空前守卫判据载体（删空臂专用）：本条命令装载的元记录身份与期望计数，
/// 经 `tombstone_meta_guarded` 在条带独占写锁内复核，判据并入
/// [`drain_guard_ok`] 单点（key_id + expect_size 两元，禁第二判据函数）
pub(crate) struct DrainGuard {
  /// 本条命令独占锁内刷新后的元记录 key_id：并发重建（key_id 漂移）判否，
  /// 杜绝误杀他人在册新记录
  pub(crate) key_id: u64,
  /// 期望计数：删空臂传 `Some(0)`（窗内并发 SET 复活 size >= 1 即判否）；
  /// 缺席 = 纯 key_id 判据（迁移三消费点同形）
  pub(crate) expect_size: Option<u64>,
}

impl<D: Device> StoreSession<D> {
  /// 树态键排空回收单点：双物理域原子墓碑（信封域 + 元记录）+ BfTree 树实例注销 +
  /// 换号旁表回收 + RangeIndexDrop AOF 入账；删键臂（keep_ttl=false）级联清理信封、
  /// 元记录与随键 TTL/ETag 旁路记录，旁路清退由 `keep_ttl` 分流（口径详见
  /// `drain_and_delete_collection_meta` 文档：删键臂 false 清 TTL/ETag
  /// 杜绝孤儿，降阶迁移臂 true 只墓碑元记录、不碰 TTL/ETag 旁路，对标 C#
  /// 对象记录重写原样前移 HasExpiration/HasETag、零发 TTL 事件）。
  ///
  /// 失败分级（[`Error::Swapped`] 错误面，供分层臂 WATCH 栅栏判别置脏去留）：
  /// 信封墓碑 / 元记录墓碑失败 = 键未消亡、树内容零变更，原样上抛；元记录
  /// 墓碑已落盘后的失败（del_ttl/del_etag/树注销/AOF 入账）= 键已死、树物理面已变更，
  /// 包装分级上抛保持置脏真实推进。
  ///
  /// 删键臂（keep_ttl=false）连带对信封域写幂等墓碑，两域回收一处收口：
  /// 升阶是非原子三步写（先发流块、再落元记录、后删信封），崩溃、AOF 截断
  /// 部分回放或信封删除 IO 失败都会留下「信封旧快照 + Meta 存根 + 树」双态
  /// 残留；双态期命令路由 Meta 优先读写无误，但排空臂若只清 Meta 域，删空后
  /// 命令回落信封域探测（`contains_key` / `read_tag_with` 双域臂），已删空集合
  /// 以升阶时刻的完整旧数据幽灵复活。C# 集合恒驻对象域单一物理域，删记录即
  /// 连尾随字段同亡、绝无第二域残留（
  /// C# 对象存储 RMWMethods 的 InPlaceUpdaterWorker 的 HasRemoveKey →
  /// ExpireAndStop 与 DeleteMethods 删除臂）；本仓分层态键消亡必须两域齐清，
  /// 落实 SKILL.md「严格删空生命周期与原子墓碑，杜绝幽灵空元记录」。墓碑先于
  /// 元记录落笔：命令窗口内回落域先死、路由域后死，杜绝中途幽灵读。迁移臂
  /// （keep_ttl=true）绝不触碰信封域：键全程存活，降阶臂随后 obj_save 写回新
  /// 信封，墓碑即自相残杀；分层重灌臂经 promote 直接换树、不再经本函数（先建
  /// 后拆，见 promote_collection_to_bftree）。纯 RangeIndex 键
  /// 无信封记录，delete_raw 哈希探针落空零写零入账（幂等），与 DEL 复合臂
  /// [`crate::session::StoreSession::delete`] 的双域墓碑共用 delete_raw 同一
  /// 原语，不新增第二条信封删除路径。复制面同口径：本函数末位入账的
  /// RangeIndexDrop 镜像为 StoreDelete(Meta) 条目，副本回放臂只以
  /// keep_ttl=true 形排空 Meta 域（见 wnode `AofProcessor::store_delete`），
  /// 信封域与 TTL/ETag 旁路的消亡各由主端逐域镜像条目承接，跨域级联一律禁止
  ///——级联会把懒降阶刚落盘的信封连同随键 TTL 在副本上抹成整键消失。
  ///
  /// 本形为守卫缺席门面（`drain_and_delete_index` 唯一内核的无守卫投影，
  /// 既有调用面零改动）：元记录墓碑无条件落笔
  pub async fn handle_bftree_drain_and_delete(&self, key: &[u8], keep_ttl: bool) -> Result<()> {
    self
      .drain_and_delete_index(key, keep_ttl, None, None)
      .await
      .map(|_| ())
  }

  /// [`Self::handle_bftree_drain_and_delete`] 的守卫门面（range_index_del
  /// 删空臂专用）：元记录墓碑经 `tombstone_meta_guarded` 在条带独占写锁内
  /// 「重读复核 + 落笔」原子完成。返回 false = 守卫判否未排空（键仍存活，
  /// 调用方按已生效成功收尾：不报错不回滚，物理面零墓碑零树销毁）。
  /// `pinned` = 调用方链首域钉三元组（del 臂多 await 发布链专用，见
  /// `range_index_del` 链首注）：排空面全部物理键构造与事件域一律消费钉定
  /// 值，禁逐点重解析；缺席（None）维持会话现解析（既有调用面形态）
  pub(super) async fn handle_bftree_drain_and_delete_guarded(
    &self,
    key: &[u8],
    keep_ttl: bool,
    guard: DrainGuard,
    pinned: Option<(u64, u64, SessionPrefixBuf)>,
  ) -> Result<bool> {
    self
      .drain_and_delete_index(key, keep_ttl, Some(guard), pinned)
      .await
  }

  /// 排空回收唯一内核（两门面单点收口，禁第三编排）：
  /// 信封墓碑 → Meta 域墓碑（守卫臂锁内复核判否即中止）→ 随键 TTL/ETag
  /// 清理 → 树注销（自取同条带写锁，调用方不得持锁进入）→ 换号旁表注销 →
  /// RangeIndexDrop 入账。守卫判否（`Ok(false)`）在信封墓碑之后返回——
  /// 纯 RangeIndex 键信封恒缺席，该墓碑幂等零写，判否臂物理面等价零变更。
  /// `pinned` 语义见 [`Self::handle_bftree_drain_and_delete_guarded`]：内核
  /// 单点解域（None 即会话现解析一次，Some 即钉定值直用），下方全取点同源
  async fn drain_and_delete_index(
    &self,
    key: &[u8],
    keep_ttl: bool,
    guard: Option<DrainGuard>,
    pinned: Option<(u64, u64, SessionPrefixBuf)>,
  ) -> Result<bool> {
    let (vns, vdb, prefix) = pinned.unwrap_or_else(|| {
      let (vns, vdb) = self.virtual_domain();
      (vns, vdb, SessionPrefixBuf::new(vns, vdb))
    });
    // 删键臂信封域幂等墓碑（回落域先于路由域消亡，落域 = 单点解出域）
    if !keep_ttl {
      let env_k = Self::session_tag_key_with_prefix(prefix.as_slice(), KeyTag::ObjectEnvelope, key);
      self.delete_raw(&env_k).await?;
    }
    if !self
      .drain_and_delete_collection_meta(key, keep_ttl, guard.as_ref(), prefix.as_slice())
      .await?
    {
      return Ok(false);
    }
    // 树身份键 = 物理 Meta 键（与上方元记录墓碑同键，落域 = 单点解出域），
    // 跨库同名键的树注册与数据文件按物理域隔离。树注销/文件删除失败时元记录
    // 墓碑已落盘——键路由域已死、树物理面已变更，经 [`Error::Swapped`] 分级
    // 上抛（分层臂 WATCH 判据据此保持置脏，推进真实反映变更）；孤儿树文件
    // 由 migration-tmp 启动清扫与惰性恢复收敛
    let mgr = Arc::clone(&self.store.range_index);
    let del_key = Self::session_tag_key_with_prefix(prefix.as_slice(), KeyTag::Meta, key);
    let _ = range_index_blocking(move || {
      mgr
        .delete_index(&del_key)
        .map_err(|e| Error::swapped(e.into()))
    })
    .await??;
    // 删除面唯一收敛点的旁表逆操作：树已销毁，同步注销换号回收登记。
    // 钉定链直调三参内核（登记域 = 单点解出域，与树身份域、元记录域同源），
    // 禁链中重解析——换代窗内现解析扳向新代即注销脱靶、旧域旁表条目漏清
    self.store.unregister_bftree_key(vns, vdb, key);
    // 事件域 = 单点解出域（与本键记录前缀同源），入账键方与记录落域一致。
    // AOF 入账失败按 error.rs AofEnqueue 契约上抛拒绝本命令：墓碑与树注销
    // 均已落盘、键已消亡，经 Swapped 分级保持分层臂 WATCH 判据真实推进
    self
      .store
      .emit_event(
        self.aof_session_id,
        StoreEvent::RangeIndexDrop {
          ns: vns,
          db: vdb,
          key,
        },
      )
      .map_err(Error::swapped)?;
    Ok(true)
  }

  /// 删空臂排空前守卫：条带独占写锁内「重读元记录复核 + 墓碑落笔」原子完成
  ///
  /// 竞态窗闭合（票 wkv-ri-del-drain-empty-arm-resurrect-race-acked-write-loss）：
  /// range_index_del 删空臂 `drop(tree)` 放锁后的排空多 await 窗内，并发
  /// range_index_set 可完整提交 ACK（load meta 仍 live → 取锁 →
  /// refresh_tiered_meta 放行 → ri_set → inc_size → save 覆写），随后墓碑
  /// 无条件落下即湮灭已 ACK 写；反向交错（墓碑先落、SET 的 save 后至）则以
  /// 同一 key_id 复活 live 元记录指向已销毁树的幽灵半键（EXISTS=1 而读恒
  /// NotFound）。C# 无此窗：同键写删经 TsavoriteKV RMW 记录 X 锁全程串行
  /// （TsavoriteKV.cs RMW InPlaceUpdater 前置记录锁），删除判定与后续写入
  /// 天然序化。本守卫对位同一互斥语义：SET 写臂的「锁内 refresh 重读 →
  /// 树写 → save 覆写」临界区与「复核 + 墓碑」临界区共用同一把条带写锁
  /// （树身份键 = 物理 Meta 键，与元记录同键同条带），互斥即窗闭合：
  /// - SET 先持锁提交（save size >= 1 落盘放锁）→ 复核见计数漂移判否，
  ///   排空中止，收敛为「字段删后又有写」的正常串行终态；
  /// - 守卫先持锁落墓碑 → SET 后续取锁，锁内 refresh 重读命中墓碑
  ///   （记录缺失 / 非 live）终态拒绝 NotFound，绝无穿透复活。
  ///
  /// 取锁纪律：经 [`Self::try_lock_tree_write`] 有界 try+让核档
  /// （TREE_LATCH_YIELD_BUDGET 预算耗尽沿 MigrationBusy 存储忙漏斗上抛，
  /// 绝不异步上下文无界停车）；持锁跨 read_raw/delete_raw 两个设备 await
  /// 系 TreeWriteGuard 锁内 refresh/save 的既有形态（双前提见
  /// `range_index_blocking` 文档：锁随本核任务收放，他人有界 try 取锁）。
  /// 判据并入 [`drain_guard_ok`] 单点扩形（key_id 相符 + expect_size 两元）。
  ///
  /// 返回 true = 复核通过且墓碑已锁内落笔（调用方接排空余序）；false =
  /// 判否（并发复活 size >= 1 / key_id 漂移 / 记录缺席），物理面零变更
  pub(crate) async fn tombstone_meta_guarded(
    &self,
    meta_k: &[u8],
    guard: &DrainGuard,
  ) -> Result<bool> {
    let key_hash = fast_hash(meta_k);
    let _xlock = self.try_lock_tree_xlock(key_hash).await?;
    let bytes = self.read_raw(meta_k).await?;
    if !drain_guard_ok(bytes.as_deref(), guard.key_id, guard.expect_size) {
      return Ok(false);
    }
    self.delete_raw(meta_k).await?;
    Ok(true)
  }
}

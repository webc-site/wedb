use std::{
  fs,
  path::{Path, PathBuf},
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub};
use wdev::Device;
use whasher::fast_hash;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, MetaValue};

use super::{
  RangeIndexError, discard_snapshot_file, range_index_blocking, rebind_stub, wait_tree_checkpoint,
};
use crate::{error::Result, ri::RiTreeOps, session::StoreSession, store::StoreEvent};

impl<D: Device> StoreSession<D> {
  /// 发布迁移或分块重组后的 RangeIndex 存储底座（对应 PublishMigratedIndex 的文件换入与存根落盘存储逻辑）
  ///
  /// obj_type 决定发布后元记录的集合判别：用户 RI 迁移流为 RangeIndex；集合就地
  /// 升阶经 AOF 数据通道回放时携原集合类型（Hash/Set/List/SortedSet），副本据此
  /// 重建 MetaValue，TYPE/HLEN 等命令与源侧一致（不再硬编码 RangeIndex）。
  ///
  /// `next_expiry` 为发布后元记录的成员 TTL 水位（随首块流块 ReplayInput.arg2
  /// 从源侧透传；`i64::MAX` = 无成员挂 TTL）。副本若落 `MetaValue::new` 的
  /// i64::MAX 假水位，分层集合计数门 `now <= next_expiry` 恒真，到期成员永不
  /// 出账、计数虚高，主从 HLEN/ZCARD 应答发散（升阶头注同款警告，见
  /// [`promote_collection_to_bftree`](crate::range_index)）。纯用户 RI 迁移流
  /// 无成员 TTL，传 i64::MAX 即原语义。
  ///
  /// 锁纪律：条带写锁在阻塞任务内部获取与释放，严禁持同步锁跨 await（与 rename_range_index 一致）。
  /// 存在性判定 → 快照文件原子换入与树恢复注册（阻塞任务持条带写锁原子完成）→ 存根落盘。
  pub async fn publish_migrated_range_index(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
    obj_type: GarnetObjectType,
    next_expiry: i64,
  ) -> StdResult<(), RangeIndexError> {
    if self.range_index_exists(key).await? && !replace {
      return Err(RangeIndexError::AlreadyExists);
    }

    // 文件换入 + 旧树释放 + 恢复 + 注册表发布。
    // 快照解析属重操作，卸载阻塞线程，并在阻塞任务内部获取条带写锁保证发布原子性。
    // 树身份键 = 物理 Meta 键（会话域内派生单点），跨库同名键的发布防重门与
    // 数据文件按物理域隔离
    let id_key = self.session_meta_key(key);
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = id_key.clone();
    let pub_src = temp_path.to_path_buf();
    let tree = range_index_blocking(move || {
      let key_hash = fast_hash(&pub_key);
      let _xlock = mgr.acquire_exclusive_for_delete(key_hash);
      mgr.publish_tree_from_snapshot_locked(&pub_key, &pub_src, replace)
    })
    .await?
    .map_err(RangeIndexError::from)?;

    // 发布树纳入换号回收旁表（迁移接收/重组发布与 RI.CREATE 同一回收语义）
    self.register_bftree_key(key);

    let mut stub = RangeIndexStub::decode(stub_bytes)?;
    rebind_stub(&mut stub, &tree);

    // 存根落盘（条带锁窗口之外：锁已随阻塞任务收束，严禁持同步锁跨 await；
    // 落盘工序列 save_bftree_meta_stub 门面单点，meta_k 自持供下方回滚复核）。
    // 崩溃最坏结果为旧存根 + 新数据文件（同键迁移语义下内容一致，惰性恢复可正常
    // 打开），不存在「存根在而文件失」的不可恢复态
    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let count = tree.ri_count_by_scan()? as u64;
    let meta = MetaValue::new_with_expiry(key_id, obj_type, count, next_expiry);

    self.save_bftree_meta_stub(key, &meta, &stub).await?;

    // 写后持锁复核（补偿存根落盘逃逸条带锁窗口与 C# PublishMigratedIndex 全程持
    // RangeIndex X 锁的原子发布契约差距）：换树注册 → 存根落盘的间隙内，同键
    // DELETE 可插入（锁内摘除注册表条目 + 删数据文件 + 元记录墓碑），本条后写的
    // 存根将复活指向已删文件的幻影键。复核发现条目已被并发摘除即回滚墓碑本条
    // 元记录；条目仍在但树已被并发再发布换替时不回滚——文件谱系已归新发布所有，
    // 其自身存根落盘收敛最终态（is_live 只以注册表为准，陈旧 stub.tree_handle
    // 运行时无害，见 range_index_metrics 注释）
    let mgr2 = Arc::clone(&self.store.range_index);
    let chk_hash = fast_hash(&id_key);
    let chk_key_id = RangeIndexManager::key_id_of(&id_key);
    let entry_alive = range_index_blocking(move || {
      let _xlock = mgr2.acquire_exclusive_for_delete(chk_hash);
      mgr2.live_indexes().pin().get(&chk_key_id).is_some()
    })
    .await?;
    if !entry_alive {
      // 盲回滚会误杀并发 re-publish（replace）刚落盘的新存根（不同 key_id，其自身
      // 存根落盘收敛最终态）：仅当 meta_k 最新记录仍是本条写入（key_id 相符）才写
      // 回滚墓碑——对标 C# PublishMigratedIndex 的 RICREATE RMW 以 status.Record.Created
      // 区分「新建成功 / 竞态被覆盖」并据此决定注册与否
      // (libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:216-227)。
      // 最新记录已被并发墓碑覆盖或已被新存根换替时，本条写入已不可见，无需回滚
      // （判据走 drain_guard_ok 单点，与段一复核/段五守卫同一处定义）
      let rollable = match self.read_raw(&meta_k).await {
        Ok(bytes) => drain_guard_ok(bytes.as_deref(), key_id),
        Err(e) => return Err(RangeIndexError::from(e)),
      };
      if rollable {
        self
          .delete_raw(&meta_k)
          .await
          .map_err(|e| RangeIndexError::Internal(format!("发布回滚失败（并发删除竞争中）: {e}")))?;
        return Err(RangeIndexError::Internal(
          "发布树在存根落盘窗口内被并发删除，已回滚元记录".to_string(),
        ));
      }
      return Err(RangeIndexError::Internal(
        "发布树在存根落盘窗口内被并发删除，元记录已被并发写覆盖，无需回滚".to_string(),
      ));
    }

    Ok(())
  }

  /// RENAME 迁移 RangeIndex（对标 C# RENAME 复制存根后索引持续可用语义）
  ///
  /// 本实现数据文件按"键名哈希前缀"命名，无法别名共享：一物两用（同一份段一
  /// 快照既作建树源又作 dst 覆写源，[`promote_collection_to_bftree`]
  /// (`super::promote_collection_to_bftree`) 同款写序不变量，禁手工 remove_file
  /// 直写 dst 数据路径的换入形）：
  /// - 段一（封堵并发写）：登记**非持久内存迁移 claim**（RangeIndexManager
  ///   migrating 注册表，old/new 双键成对——dst 侧仅存活记录在场（有被销毁域）
  ///   才登记，RENAME 到新键无 dst 记录则免，双键 key_id 均纯按键哈希派生；
  ///   RENAME 同键由解析期既有判定拒绝；各失败臂与成功臂按文末 claim 释放
  ///   配对表收尾——release 按 key_id 判等移除、不校验持有者，仅限 try_claim
  ///   成功后的持有者调用，未持有即调用会误删并发 RENAME 的同源键 claim。
  ///   RI.SET/RI.GET 等经 load_range_index_stub、DEL 排空与判活经 load_meta
  ///   两入口 claim 命中按键暂时不可见拒绝（RI 面 None 即终态 NotFound、无穿透
  ///   路径）；分层四族经 load_collection_stub 与 refresh_tiered_meta、RI.CREATE
  ///   经 ops 层早退三处 claim 命中显式回 MigrationBusy 锁忙/重试错（禁「视同
  ///   不存在」穿透——穿透会让分层写臂在 dst 信封域物化重建对象、RI.CREATE 走
  ///   重建路径，换一种已 ACK 丢失形）；字符串写面（SET 族覆写清退）经 wkv 写
  ///   内核两臂（try_upsert_tag_sync_unprotected_with_prefix / upsert_tag）内嵌
  ///   Meta 域清退前的 migration_claimed 判定收口：同步臂命中借既有
  ///   Ok(Err(u64::MAX)) 降级、异步闭环臂同位显式回 MigrationBusy（RESP 层
  ///   ri_write_gate 不另设第二判点——claim 在册键必有存活元记录，写原语单点
  ///   即全覆盖，禁 RESP 命令臂散补第二套判定），封堵「快照后写入不进新副本、
  ///   随旧键排空销毁」「dst 并发写 ACK 后随 dst 旧树换入销毁 + AOF 与流块交错
  ///   致主从发散」与「SET 覆写清退在册树后已 ACK 值随段四 meta 换域遮蔽丢失」
  ///   三形丢失写），随后旧树
  ///   惰性恢复 → 旧键条带写锁 + 防重入快照 claim 下整树 CPR 快照至
  ///   migration-tmp（不触 dst 数据路径；条带锁保证快照一致性）。claim 不删
  ///   元记录、不落持久墓碑：崩溃/掉电随进程内存消散、日志回放恢复旧键原态
  ///   （promote「先发流后毁旧态」不变量回正——持久墓碑改由段五排空单点在
  ///   流块已入账 + 新键 meta 已落之后落笔，杜绝旧形「墓碑后、流块前」崩溃窗
  ///   的整键丢失）；claim 先于条带锁：已过元记录门禁、滞留条带锁获取前的在途
  ///   写者残余一个派发微窗，除此窗口外并发写一律被拒而非丢失；
  /// - 段二：emit StoreEvent::RangeIndexStream（file_path = 段一快照，sink
  ///   同步分块灌 AOF 读毕返回，杜绝重复的第二次整树快照）；
  /// - 段三（dst 清退下沉至 publish 前紧邻）：dst String/信封残留经幂等墓碑
  ///   清退（缺席零写零入账；dst 若为分层键不预清退，旧树由换入内核在 dst
  ///   条带写锁内排空摘除承接）+ WATCH 版本栅栏恰一次推进（本内核单点，窗口
  ///   覆盖 dst 清退 + 段四裸原语全程，调用方不再显式补推），随后 dst 条带写锁
  ///   内 [`publish_tree_from_snapshot_locked`](wbftree::RangeIndexManager)
  ///   以 replace=true rename 原子换入；
  /// - 段四：落 dst 元记录（透传 old_meta 的 collection_type/next_expiry），
  ///   新键纳入换号回收旁表；
  /// - 段五（成功臂）：排空前守卫复核（旧键元记录仍为本条迁移在册 key_id，无
  ///   live 并发重建记录混入）→ 旧键排空回收单点
  ///   [`handle_bftree_drain_and_delete`](StoreSession::handle_bftree_drain_and_delete)
  ///   （信封域幂等墓碑 + 元记录持久墓碑 + 随键 TTL 清理 + 树销毁注销 +
  ///   RangeIndexDrop 入账，副本据此清场旧键）→ 释放迁移 claim（双键成对）。
  ///
  /// 逐段失败语义（错误一律上抛令 RENAME 报错，禁静默；补偿入账与 claim 释放
  /// 不遮蔽主错误）：
  ///
  /// | 失败段 | dst 状态 | 旧键状态 | 崩溃维（该窗口掉电恢复） | 副本维 |
  /// |---|---|---|---|---|
  /// | 段一（claim/快照） | 原态完整（未触） | 原态完整（claim 释放，元记录与树全程未触），重试对齐 | claim 随进程消散，元记录在册，回放恢复旧键原态，无孤儿无丢失 | 未触（未 emit） |
  /// | 段二（emit） | 原态完整（未触） | 同段一 | 流块未入账 → 回放无新键、旧键原态；流块已入账而后续段未落 → 主端旧键存活、副本新键（主从重复窗，重试/DEL 旧键收敛） | 流块未入账即未触 |
  /// | 段三（dst 清退/换入） | String/信封残留已清退（幂等墓碑，不可逆）；树/meta/TTL/ETag 原态 | 同段一 | 主端旧键存活、dst 树成孤儿文件（meta 缺失不可见）；副本已发布新键 → 主从重复窗 | 补偿 RangeIndexDrop{new_key} 摘除幻影树与元记录 |
  /// | 段四（dst meta） | 「新树 + 旧 meta」滞后态（promote 同构：键仍可读，惰性恢复按数据文件收敛、计数校正兜底），主端重试对齐 | 同段一 | 主端「新键 + 旧键」双活残留（重试对齐 / DEL 清旧键） | 同段三补偿 |
  /// | 段五（旧键排空） | 已完整换入 | 持久墓碑态 + 树在册残留，错误面可见，重试经防御性早退交由调用方清理 | 墓碑时点在流块已入账 + 新键 meta 已落之后 → 恢复至多旧键存活/主从重复，绝无整键丢失 | RangeIndexDrop{old_key}（排空单点既有入账）清场旧键 |
  ///
  /// claim 释放配对表（逐臂审计锚；「成对」指持有几释放几，非盲删双键——
  /// release 按 key_id 判等移除、不校验持有者，仅限本调用 try_claim 成功后的
  /// 持有者调用）：
  ///
  /// | 臂 | 持有 claim | 释放口 |
  /// |---|---|---|
  /// | 前置早退（旧键元记录缺失/定长不足/解码败/检查点屏障败） | 无 | 无（claim 未登记，无泄可谈） |
  /// | dst 存活性采样读失败 | 无（尚未 try_claim） | 无——`?` 直接上抛，禁调 release（会误删并发 RENAME 持有的同源键 claim） |
  /// | try_claim(old) 失败 | 无（try 未成功） | 无——直接上抛 |
  /// | try_claim(new) 失败 | old | 单侧释放 old（new 系并发持有者注册，成对闭包会误删他人 claim） |
  /// | 段一快照失败 / 段一复核失败 | old+new | release_claims |
  /// | 段二 emit 失败 | old+new | release_claims |
  /// | 段三 dst 清退失败 / 段三换入失败 | old+new | release_claims |
  /// | 段四 dst meta 落盘失败 | old+new | release_claims |
  /// | 段五守卫读失败 / 判否 / 排空失败 | old+new | release_claims |
  /// | 段五成功 | old+new | release_claims |
  ///
  /// 锁形取舍（C# 对标注释）：UnifiedStoreOps.cs 的 RENAME 先对 old/new 双键取
  /// 排他锁，并发写被串行化到 RENAME 前后、永不落入被销毁域；仓内不持双键锁
  /// ——跨条带双锁引入锁序死锁面，且各段阻塞任务间跨 await 拆分后双锁不可持续
  /// ——以段一 old/new 双键 claim 登记承接同一互斥语义（try 失败即退不持钥等待，
  /// 无死锁面）。dst 覆盖原子性以「dst 条带锁内 rename 原子换入」
  /// 承接（存储面原子性优先于锁形对位），锁纪律：各条带写锁在阻塞任务内部
  /// 获取与释放，严禁持同步锁跨 await
  pub async fn rename_range_index(&self, old_key: &[u8], new_key: &[u8]) -> Result<()> {
    // 读取旧键存根（调用方已确认 RI 元记录存在且 size > 0；缺失或畸形则无索引
    // 可迁移，防御性直接返回，交由调用方常规清理旧键）。claim 方案下元记录全程
    // 存活，各失败臂无需按位回写
    let old_meta_k = self.session_meta_key(old_key);
    let Some(bytes) = self.read_raw(&old_meta_k).await? else {
      return Ok(());
    };
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(());
    }
    let mut stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    let old_meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;

    // 检查点屏障异步等待：避免与检查点外层屏障（跨 await 持有）并发写树，
    // 挂起让出 reactor（与 acquire_tree_read 口径一致）。树身份键 = 物理 Meta
    // 键（本函数上下文已持有 old/new 两键的物理键），claim 与注册表查找同判据，
    // 跨库同名键的迁移封堵按物理域隔离
    wait_tree_checkpoint(&self.store.range_index, &old_meta_k).await?;

    // 段一：登记非持久内存迁移 claim（old/new 双键成对，封堵并发写/排空，见
    // 函数头注）→ 旧树惰性恢复 → 旧键条带写锁 + 防重入 claim 下整树快照至
    // migration-tmp 临时文件（不触 dst 数据路径）。整树快照含 fsync、恢复含快照
    // 解析 + 环形缓冲分配，均属百毫秒级重操作，一律卸载阻塞线程保护 compio 核
    // （锁纪律：get_or_open_tree 的条带写锁自取自放后，快照段再自取旧键写锁——
    // 两段先后串行不嵌套）。
    //
    // dst 侧成对登记判据：「dst 有存活记录才 claim」——存活元记录在场才有被
    // 迁移销毁的旧树/旧域（RENAME 到新键的常态无 dst 记录，无被销毁域，无需
    // claim）；claim 表键为 128 位 key_id（按物理 Meta 键哈希派生，上下文已持有
    // 两键的物理键，无需二次编码）。活性采样（await）先于双键登记，两次
    // try_claim 之间零 await，杜绝「源键已封堵、dst 未封堵」的半登记窗；采样至
    // 登记的残余间隙与源键既有「派发微窗」同类（compio 单 worker 内至多一个
    // 在途写者）。
    // dst claim 失败 = 反向（dst→old）或同 dst 并发 RENAME 已持：try 失败即退，
    // 双键互斥无持钥等待，无死锁面。
    // 快照失败臂：删半写残件（闭包内）+ 双键成对释放 claim 后上抛，旧键原态完整
    let dst_meta_k = self.session_meta_key(new_key);
    // 采样读失败直接 `?` 上抛：此刻尚未 try_claim（下方），本调用未持有任何
    // claim——release 按 key_id 判等移除、不校验持有者，未持有即调用会误删并发
    // RENAME 持有的同源键 claim（其 claim_recheck 随即伪败，产生可用性伪失败链）。
    // 释放纪律：仅 try_claim 成功后的持有者可经 release_claims 收尾（见头注配对表）
    let dst_claimable = self.read_raw(&dst_meta_k).await?.is_some_and(|bytes| {
      bytes.len() >= META_VALUE_SIZE
        && MetaValue::from_slice(&bytes[..META_VALUE_SIZE]).is_ok_and(|m| m.is_live())
    });
    if !self.store.range_index.try_claim_migration(&old_meta_k) {
      return Err(RangeIndexError::Internal("旧键迁移已被并发 RENAME 持有".to_string()).into());
    }
    if dst_claimable && !self.store.range_index.try_claim_migration(&dst_meta_k) {
      // 仅释放本调用自持的 old：new 已被并发持有者注册，release 按 key_id 判等
      // 移除、不校验持有者，成对闭包在此处会误删他人 claim（其复核随即伪败）
      self.store.range_index.release_migration_claim(&old_meta_k);
      return Err(RangeIndexError::Internal("目标键迁移已被并发 RENAME 持有".to_string()).into());
    }
    // 双键 claim 成对释放单点（双键均已登记、本调用为两键唯一持有者）：其后所有
    // 失败臂与成功臂一律经本闭包收尾，杜绝单侧泄漏令键永久不可见（逐臂配对表
    // 见函数头注）
    let release_claims = || {
      self.store.range_index.release_migration_claim(&old_meta_k);
      self.store.range_index.release_migration_claim(&dst_meta_k);
    };
    let mgr = Arc::clone(&self.store.range_index);
    let old_key_owned = old_meta_k.clone();
    let restore_stub = stub;
    let snapshot_path =
      match range_index_blocking(move || -> StdResult<PathBuf, RangeIndexError> {
        let old_tree = match mgr.get_tree(&old_key_owned) {
          Some(t) => t,
          None => mgr.get_or_open_tree(&old_key_owned, &restore_stub)?,
        };
        let snap_path = mgr.derive_temp_migration_path();
        {
          let old_hash = fast_hash(&old_key_owned);
          let _xlock = mgr.acquire_exclusive_for_delete(old_hash);
          if let Err(e) = mgr.snapshot_tree_to_path_locked(&old_key_owned, &old_tree, &snap_path) {
            let _ = fs::remove_file(&snap_path);
            return Err(RangeIndexError::from(e));
          }
        }
        Ok(snap_path)
      })
      .await
      {
        Ok(Ok(path)) => path,
        Err(e) | Ok(Err(e)) => {
          release_claims();
          return Err(e.into());
        }
      };

    // 段一闭包返回后复核（纵深防御，墓碑方案的并发重建冷键窗在 claim 方案下的
    // 收口）：claim 仍持有，且旧键元记录仍为本条迁移的在册记录（key_id 相符，
    // 无并发重建/误删改写）。检出异常走失败臂：弃快照残件 + 释放 claim 上抛，
    // 旧键原态完整
    let claim_recheck = self.store.range_index.migration_claimed(&old_meta_k)
      && self
        .read_raw(&old_meta_k)
        .await
        .is_ok_and(|bytes| drain_guard_ok(bytes.as_deref(), old_meta.key_id));
    if !claim_recheck {
      discard_snapshot_file(&snapshot_path, "RENAME 迁移");
      release_claims();
      return Err(
        RangeIndexError::Internal(
          "段一快照窗口检出迁移 claim 失守或旧键元记录被并发改写，已回退".to_string(),
        )
        .into(),
      );
    }

    // 段二：数据通道事件先行于清退与换入（promote 写序不变量：先发流后换入落
    // meta，杜绝「副本见 meta 却缺数据」的空树幻影）。sink 同步读取段一快照
    // 分块灌入 AOF，副本据流块重建新键树域。入队失败即删快照残件、释放 claim
    // 后上抛令 RENAME 失败——此刻 dst 未触、旧键元记录与树全程原态，「失败即
    // 原态」全程成立；emit 静默缺条目为禁区，本地有树而副本无数据是发散
    stub.reset_flags();
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexStream {
      ns,
      db,
      key: new_key,
      obj_type: old_meta.collection_type,
      stub: stub.encode(),
      file_path: &snapshot_path,
      // RENAME 为覆写目标键形态（Redis RENAME 覆盖已存在 dst），副本须以换入
      // 形态重放，否则既有同名树回放会被 IndexExists 拦截
      replace: true,
      // 成员 TTL 水位透传（单一通道增载，随首块流块 ReplayInput.arg2 携载）：
      // 副本发布若落 i64::MAX 假水位，分层集合计数门恒真、到期成员永不出账，
      // 主从 HLEN/ZCARD 应答发散（升阶头注同款警告）
      next_expiry: old_meta.next_expiry,
    }) {
      discard_snapshot_file(&snapshot_path, "RENAME 迁移");
      release_claims();
      return Err(e);
    }

    // 段三：dst 残留清退下沉至换入前紧邻（段一/段二全程 dst 未触，失败即原态
    // 的窗口推至换入前最后一刻）。String/信封残留经幂等墓碑清退；dst 若为分层
    // 键不预清退——旧树由下方换入内核在 dst 条带写锁内排空摘除（先建后拆），
    // 预清退反而制造「先销毁再建」的键蒸发窗与回放序乱。清退触碰 dst 前恰一次
    // 推进 WATCH 版本栅栏（调用方 delete_string 清退臂的 bump 对位，缺席键
    // 墓碑同向计入；本推进即 wkv 内核单点，窗口覆盖 dst 清退 + 段四裸原语全程，
    // 调用方不再显式补推）
    self.bump_watch_version(new_key);
    // 清退两臂失败同表段三收尾（emit 已成功、副本已按流块发布新键，dst 尚未换
    // 入）：弃快照残件 + 补偿摘除副本幻影 + 双键成对释放 claim 后上抛——禁单
    // 侧释放令 dst claim 泄漏至重启（dst 已登记时四判点全拒、重试 RENAME 必败）
    let str_k = self.session_string_key(new_key);
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, new_key);
    let clear_res = async {
      self.delete_raw(&str_k).await?;
      self.delete_raw(&env_k).await
    }
    .await;
    if let Err(e) = clear_res {
      discard_snapshot_file(&snapshot_path, "RENAME 迁移");
      self.compensate_stream_drop(new_key);
      release_claims();
      return Err(e);
    }

    // dst 条带写锁内先建后拆原子换入（一物两用：段一同一份快照既建新键又覆
    // dst）。快照源校验前置于摘旧树（见发布内核），换入失败 dst 旧树必在位；
    // 删快照残件 + 补偿摘除副本幻影（emit 已成功，副本已按流块实时发布新键，
    // 主端回滚须同步摘除，杜绝主从长期发散、重启回放复现幻影）+ 释放 claim 后
    // 上抛令 RENAME 失败，禁静默；残件另有 migration-tmp 启动清扫兜底
    let mgr = Arc::clone(&self.store.range_index);
    let new_key_owned = dst_meta_k.clone();
    let pub_snap = snapshot_path.clone();
    let new_tree =
      match range_index_blocking(move || -> StdResult<Arc<BfTreeService>, RangeIndexError> {
        let new_hash = fast_hash(&new_key_owned);
        let _xlock = mgr.acquire_exclusive_for_delete(new_hash);
        mgr
          .publish_tree_from_snapshot_locked(&new_key_owned, &pub_snap, true)
          .map_err(RangeIndexError::from)
      })
      .await
      {
        Ok(Ok(tree)) => tree,
        Err(e) | Ok(Err(e)) => {
          discard_snapshot_file(&snapshot_path, "RENAME 迁移");
          self.compensate_stream_drop(new_key);
          release_claims();
          return Err(e.into());
        }
      };

    // 新键纳入换号回收旁表（FLUSHDB 后同名键再发布不被 IndexExists 拦截；旧键
    // 由段五排空回收单点注销）
    self.register_bftree_key(new_key);
    rebind_stub(&mut stub, &new_tree);

    // 段四：写入新键元数据记录（Meta + 新存根，落盘工序列 save_bftree_meta_stub
    // 门面单点）。集合类型与成员 TTL 水位透传旧元记录（RangeIndex 与升阶集合键
    // 同构迁移，副本侧 TYPE/HLEN 等命令与源侧一致）。失败臂与 promote 换入后
    // 元记录落盘失败同构：dst 呈「新树 + 旧 meta」滞后态（键仍可读，读臂惰性
    // 恢复按数据文件收敛、计数校正兜底 size 滞后），补偿摘除副本幻影 + 释放
    // 双键 claim（旧键元记录全程存活，主端重试自然对齐）后错误面上抛
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new_with_expiry(
      key_id,
      old_meta.collection_type,
      old_meta.size,
      old_meta.next_expiry,
    );
    if let Err(e) = self.save_bftree_meta_stub(new_key, &meta, &stub).await {
      self.compensate_stream_drop(new_key);
      release_claims();
      return Err(e);
    }

    // 段五（成功臂）：排空前守卫复核（旧键元记录仍为本条迁移在册 key_id，无
    // live 并发重建记录混入，判据见 drain_guard_ok）→ 旧键排空回收单点——
    // 信封域幂等墓碑（升阶崩溃残件兜底）+ 元记录持久墓碑（时点在流块已入账 +
    // 新键 meta 已落之后，崩溃窗恢复 promote 级「至多旧键存活/主从重复」承诺）+
    // 随键 TTL 清理 + 树销毁注销 + 换号旁表逆操作 + RangeIndexDrop 入账（副本
    // 据此清场旧键）。失败上抛令 RENAME 报错：dst 已完整换入，旧键呈存活残留
    //（claim 已释放、错误面可见），重试经防御性早退交由调用方常规清理（见头注
    // 失败语义表）
    let drain_guard = match self.read_raw(&old_meta_k).await {
      Ok(bytes) => drain_guard_ok(bytes.as_deref(), old_meta.key_id),
      Err(e) => {
        release_claims();
        return Err(e);
      }
    };
    if !drain_guard {
      release_claims();
      return Err(
        RangeIndexError::Internal(
          "段五排空前检出旧键元记录被并发重建或改写，保守中止排空".to_string(),
        )
        .into(),
      );
    }
    if let Err(e) = self.handle_bftree_drain_and_delete(old_key, false).await {
      release_claims();
      return Err(e);
    }
    release_claims();
    Ok(())
  }

  /// 流块补偿摘除单点：emit 成功后的各失败臂共用——副本已按流块实时发布新键
  /// （replace=true 覆写形态），主端回滚须同步摘除副本幻影树与元记录，杜绝主从
  /// 长期发散、重启回放复现幻影。与段五旧键回收同一 `StoreEvent::RangeIndexDrop`
  /// 事件原语（副本既有清场臂直接消费，零新机制）；补偿入队失败仅告警不遮蔽
  /// 主错误（错误面必须可见的主失败优先沿原臂上抛）
  pub(crate) fn compensate_stream_drop(&self, key: &[u8]) {
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self
      .store
      .emit_event(StoreEvent::RangeIndexDrop { ns, db, key })
    {
      log::error!("迁移失败臂副本补偿 RangeIndexDrop 入队失败: {e}");
    }
  }
}

/// 迁移/发布元记录复核判据（纯函数，单测覆盖，同文件三消费点唯一定义——
/// publish_migrated_range_index 回滚臂、RENAME 段一复核与段五守卫，禁新增
/// 第二判据函数）：记录在场且 key_id 相符 = 本条迁移的在册元记录；缺失 /
/// 定长不足 / 解码失败 / key_id 漂移（并发重建混入）一律判否。claim 方案下
/// 元记录全程存活，「在场」不再等价并发重建，判据收敛到 key_id
#[inline]
fn drain_guard_ok(bytes: Option<&[u8]>, our_key_id: u64) -> bool {
  bytes.is_some_and(|bytes| {
    bytes.len() >= META_VALUE_SIZE
      && MetaValue::from_slice(&bytes[..META_VALUE_SIZE]).is_ok_and(|m| m.key_id == our_key_id)
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 段五排空前守卫判据：同 key_id 在册记录放行；缺失 / 定长不足 / key_id
  /// 漂移（live 并发重建）一律保守中止
  #[test]
  fn drain_guard_judges_by_key_id() {
    let mut record = MetaValue::new(7, GarnetObjectType::RangeIndex, 3)
      .to_bytes()
      .to_vec();
    record.extend_from_slice(&[0u8; RANGE_INDEX_STUB_SIZE]);
    assert!(drain_guard_ok(Some(&record), 7), "在册 key_id 相符须放行");
    assert!(!drain_guard_ok(None, 7), "记录缺失须中止");
    assert!(!drain_guard_ok(Some(&record[..4]), 7), "定长不足须中止");
    let mut drifted = MetaValue::new(9, GarnetObjectType::RangeIndex, 3)
      .to_bytes()
      .to_vec();
    drifted.extend_from_slice(&record[META_VALUE_SIZE..]);
    assert!(
      !drain_guard_ok(Some(&drifted), 7),
      "并发重建 key_id 漂移须中止"
    );
  }
}

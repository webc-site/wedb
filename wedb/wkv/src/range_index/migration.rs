use std::{
  fs,
  io::ErrorKind,
  path::Path,
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{
  BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackendType,
};
use wdev::Device;
use whasher::fast_hash;
use wval::{GarnetObjectType, META_VALUE_SIZE, MetaValue};

use super::{
  RangeIndexError, encode_meta_stub_record, range_index_blocking, rebind_stub, wait_tree_checkpoint,
};
use crate::{error::Result, ri::RiTreeOps, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 发布迁移或分块重组后的 RangeIndex 存储底座（对应 PublishMigratedIndex 的文件换入与存根落盘存储逻辑）
  ///
  /// obj_type 决定发布后元记录的集合判别：用户 RI 迁移流为 RangeIndex；集合就地
  /// 升阶经 AOF 数据通道回放时携原集合类型（Hash/Set/List/SortedSet），副本据此
  /// 重建 MetaValue，TYPE/HLEN 等命令与源侧一致（不再硬编码 RangeIndex）。
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
  ) -> StdResult<(), RangeIndexError> {
    if self.range_index_exists(key).await? && !replace {
      return Err(RangeIndexError::AlreadyExists);
    }

    // 文件换入 + 旧树释放 + 恢复 + 注册表发布。
    // 快照解析属重操作，卸载阻塞线程，并在阻塞任务内部获取条带写锁保证发布原子性。
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = key.to_vec();
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

    // 存根落盘（条带锁窗口之外：锁已随阻塞任务收束，严禁持同步锁跨 await）。
    // 崩溃最坏结果为旧存根 + 新数据文件（同键迁移语义下内容一致，惰性恢复可正常
    // 打开），不存在「存根在而文件失」的不可恢复态
    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let count = tree.ri_count_by_scan()? as u64;
    let meta = MetaValue::new(key_id, obj_type, count);

    let val = encode_meta_stub_record(&meta, &stub);

    self.upsert_raw(&meta_k, &val).await?;

    // 写后持锁复核（补偿存根落盘逃逸条带锁窗口与 C# PublishMigratedIndex 全程持
    // RangeIndex X 锁的原子发布契约差距）：换树注册 → 存根落盘的间隙内，同键
    // DELETE 可插入（锁内摘除注册表条目 + 删数据文件 + 元记录墓碑），本条后写的
    // 存根将复活指向已删文件的幻影键。复核发现条目已被并发摘除即回滚墓碑本条
    // 元记录；条目仍在但树已被并发再发布换替时不回滚——文件谱系已归新发布所有，
    // 其自身存根落盘收敛最终态（is_live 只以注册表为准，陈旧 stub.tree_handle
    // 运行时无害，见 range_index_metrics 注释）
    let mgr2 = Arc::clone(&self.store.range_index);
    let chk_hash = fast_hash(key);
    let chk_key_id = RangeIndexManager::key_id_of(key);
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
      // 最新记录已被并发墓碑覆盖或已被新存根换替时，本条写入已不可见，无需回滚。
      let rollable = match self.read_raw(&meta_k).await {
        Ok(Some(bytes)) => {
          bytes.len() >= META_VALUE_SIZE
            && MetaValue::from_slice(&bytes[..META_VALUE_SIZE]).is_ok_and(|m| m.key_id == key_id)
        }
        Ok(None) => false,
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
  /// 本实现数据文件按"键名哈希前缀"命名，无法别名共享：在旧键条带写锁 +
  /// 防重入快照 claim 下将活动树 CPR 快照至新键数据文件路径（快照窗口内旧键
  /// 写入被条带锁阻塞，杜绝「快照后写入不进新副本」的丢失写；锁释放到调用方
  /// 删除旧键之间的残留窗口由调用方紧随的 delete 收口），再从新文件恢复独立
  /// 树实例并按新键注册到管理器，最后写入新键元数据记录。
  ///
  /// 锁纪律：旧键条带写锁在阻塞任务内部获取与释放，严禁持同步锁跨 await
  pub async fn rename_range_index(&self, old_key: &[u8], new_key: &[u8]) -> Result<()> {
    // 读取旧键存根（调用方已确认 RI 元记录存在且 size > 0；缺失或畸形则无索引可迁移，
    // 防御性直接返回，交由调用方常规清理旧键）
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
    // 挂起让出 reactor（与 acquire_tree_read 口径一致）
    wait_tree_checkpoint(&self.store.range_index, old_key).await?;

    // 旧树惰性恢复 → 新键数据文件预置 → 旧键条带写锁 + 防重入 claim 下整树快照
    // → 新树独立恢复，四步串行合并进单一阻塞任务：整树快照含 fsync、恢复含
    // 快照解析 + 环形缓冲分配，均属百毫秒级重操作，一律卸载阻塞线程保护 compio 核
    // (锁纪律：get_or_open_tree 的条带写锁自取自放后，快照段再自取旧键写锁——
    // 两段先后串行不嵌套；快照持锁窗口阻塞同条带旧键写入，杜绝「快照后写入
    // 不进新副本」的丢失写)
    let mgr = Arc::clone(&self.store.range_index);
    let old_key_owned = old_key.to_vec();
    let new_key_owned = new_key.to_vec();
    let restore_stub = stub;
    let new_tree =
      range_index_blocking(move || -> StdResult<Arc<BfTreeService>, RangeIndexError> {
        let old_tree = match mgr.get_tree(&old_key_owned) {
          Some(t) => t,
          None => mgr.get_or_open_tree(&old_key_owned, &restore_stub)?,
        };
        let new_path = mgr.data_file_path_for_key(&new_key_owned);
        if let Some(parent) = new_path.parent() {
          fs::create_dir_all(parent)?;
        }
        if let Err(e) = fs::remove_file(&new_path)
          && e.kind() != ErrorKind::NotFound
        {
          return Err(e.into());
        }
        let old_hash = fast_hash(&old_key_owned);
        let _xlock = mgr.acquire_exclusive_for_delete(old_hash);
        mgr.snapshot_tree_to_path_locked(&old_key_owned, &old_tree, &new_path)?;

        let backend = StorageBackendType::from_u8(restore_stub.storage_backend);
        BfTreeService::recover_from_cpr_snapshot(&new_path, true, backend)
          .map(Arc::new)
          .map_err(RangeIndexError::from)
      })
      .await??;

    rebind_stub(&mut stub, &new_tree);
    self.store.range_index.register_tree(new_key, new_tree);

    // 新键纳入换号回收旁表（旧键由调用方常规删除经 handle_bftree_drain_and_delete 注销）
    self.register_bftree_key(new_key);

    // 写入新键元数据记录（Meta + 新存根，定长纯栈编码）。集合类型与成员
    // TTL 水位透传旧元记录（RangeIndex 与升阶集合键同构迁移，副本侧
    // TYPE/HLEN 等命令与源侧一致）
    let new_meta_k = self.session_meta_key(new_key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new_with_expiry(
      key_id,
      old_meta.collection_type,
      old_meta.size,
      old_meta.next_expiry,
    );
    let val = encode_meta_stub_record(&meta, &stub);
    self.upsert_raw(&new_meta_k, &val).await?;
    Ok(())
  }
}

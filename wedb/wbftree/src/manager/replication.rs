//! 刷盘快照文件共享枚举与文件名解析、日志截断回收、检查点快照树文件复制枚举与检查点全量恢复
//! (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:OnTruncateImpl / RecoverAllTreesFromCheckpoint 与 libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexSnapshotReader.cs)

use std::{
  fs, io,
  path::{Path, PathBuf},
  str,
  sync::Arc,
};

use wbase::base32::{decode_u64, decode_u128, encode_u128};
use whasher::fast_hash;

use super::{RangeIndexManager, TREE_FILE_SUFFIX, TreeEntry};
use crate::{
  error::{Error, Result},
  service::file_has_cpr_magic,
};

impl RangeIndexManager {
  /// 解析刷盘快照文件名 `{hash_prefix}.{logical_address_b32}.flush.bftree`
  ///
  /// 严格校验：前缀 26 位 Base32、地址段恰好 13 位 Base32，安全解码出 128 位 key_id 与地址。
  /// 枚举器私有解码步：仅由 [`Self::flush_files`] 的循环调用 (C# 锚点见该法文档)，
  /// 消费方一律经枚举器取件，不再各自内联 read_dir + 解析样板
  #[inline]
  fn parse_flush_file_name(file_name: &str) -> Option<(u128, u64)> {
    let rest = file_name.strip_suffix(".flush.bftree")?;
    let (prefix, addr_str) = rest.rsplit_once('.')?;
    let key_id = decode_u128(prefix)?;
    let addr = decode_u64(addr_str)?;
    Some((key_id, addr))
  }

  /// 枚举 ri_log_root 下全部带地址刷盘快照件，产出 `(path, key_id, addr)`
  /// ——1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:EnumerateFlushFiles
  /// (C# 的私有共享迭代器：一处枚举、多路分发，消费方为截断清理与复制期文件枚举)
  ///
  /// 目录有效性预检、read_dir、`file_name().to_str()` 容错与
  /// [`Self::parse_flush_file_name`] 严格解码全在此一处承担，外来文件 (裸名刷盘件、
  /// `.data.bftree` 工作文件、`.recovering` 残留、子目录) 一律跳过：
  /// - 目录不存在按空集处理 (与 C# 预检同口径，不作 IO 失败)；目录存在但不可读上抛；
  /// - 单个目录项读取失败仅跳过该项 (对齐 C# 消费方整轮 catch 的容错枚举)；
  /// - 惰性逐项产出 (C# `IEnumerable` 同形)，消费方可在遍历途中删件。
  ///
  /// rust 侧两处消费方：[`Self::on_truncate`] (按地址阈值回收)、
  /// [`Self::remove_addr_flush_files`] (按 key_id 删全世代)。惰性恢复不做目录
  /// 枚举择优——刷盘件由 [`Self::pre_stage_and_register_pending`] 按存根源记录
  /// 地址单件预置 (1:1 对标 C# RestoreTree 只 File.Exists(workingPath))。
  /// C# 复制期枚举的 flush 地址窗分支在 rust 无恢复面消费者、不实现 (理由见
  /// js/check/ignore/libs/server/Resp/RangeIndex/RangeIndexManager.yml 与本 crate
  /// lib.rs 的「flush / truncate 与复制文件面接线现状」)，故本枚举器不覆盖该分支。
  pub(super) fn flush_files(&self) -> Result<FlushFiles> {
    let entries = if self.ri_log_root.exists() {
      Some(fs::read_dir(&self.ri_log_root)?)
    } else {
      None
    };
    Ok(FlushFiles { entries })
  }

  /// 日志截断清理：删除逻辑地址小于 new_begin_address 的历史刷盘快照文件 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:OnTruncateImpl)
  ///
  /// C# 局部函数 libs/server/Resp/RangeIndex/RangeIndexManager.cs:TryDelete (容错
  /// 单文件删除，失败仅告警不中断) 在此内联为循环内 `fs::remove_file` 忽略错误
  pub fn on_truncate(&self, new_begin_address: u64) -> Result<()> {
    for (path, _, addr) in self.flush_files()? {
      if addr < new_begin_address {
        let _ = fs::remove_file(path);
      }
    }

    Ok(())
  }

  /// 删除指定 128 位 key_id 的全部带地址刷盘快照文件 (旧世代工件清理，见
  /// lifecycle::create_bftree_internal 与 publish_tree_from_snapshot_locked)
  ///
  /// 换代后旧件对新世代已无恢复价值 (预置一律按存根源记录的精确地址单件取用)，
  /// 但盘上仍可能被旧世代的迟到引用命中 (并发 promote 携带的旧源地址)，旧件不清
  /// 即把新世代数据文件覆盖回旧世代快照；on_truncate 仅按日志地址滞后回收，
  /// 换代点即唯一即时收口 (刷盘件只有带地址一种命名，故全目录枚举即覆盖全部待清工件)
  pub(super) fn remove_addr_flush_files(&self, key_id: u128) {
    let Ok(files) = self.flush_files() else {
      return;
    };
    for (path, file_key_id, _) in files {
      if file_key_id == key_id {
        let _ = fs::remove_file(path);
      }
    }
  }

  /// 检查点快照树文件标准路径（目录无关形态辅助函数）
  ///
  /// {target_dir}/{token_b32}/rangeindex/{key_id_b32}.bftree——主端复制枚举
  /// 与副本接收落盘共用一处定义；与 [`Self::recover_all_trees_from_dir`] 的
  /// 候选目录 [`Self::token_snapshot_dir`] 前缀严格同构，副本落盘即收敛。
  /// 1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:CheckpointSnapshotPath
  /// (目标目录显式传参替代 C# 构造器暂存的 cpr 目录)
  #[inline]
  pub fn checkpoint_snapshot_path_in(target_dir: &Path, token: u128, key_id: u128) -> PathBuf {
    let b32 = encode_u128(key_id);
    Self::snapshot_file_path(target_dir, token, &b32)
  }

  /// 枚举检查点目录内指定 token 的快照树文件（对标
  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  /// RangeIndexSnapshotReader.cs:RangeIndexSnapshotReader 构造期枚举）
  ///
  /// 产出 (key_id, path) 序列：主端全量同步逐文件随检查点段流下发。token 子目录
  /// 不存在返回空集；文件名严格校验（26 位 Base32 前缀 + .bftree 后缀），
  /// 外来文件跳过
  pub fn enumerate_checkpoint_snapshots(
    target_dir: &Path,
    token: u128,
  ) -> Result<Vec<(u128, PathBuf)>> {
    let snapshot_dir = Self::token_snapshot_dir(target_dir, token);
    let mut result = Vec::new();
    if !snapshot_dir.exists() {
      return Ok(result);
    }
    for entry in fs::read_dir(&snapshot_dir)? {
      let entry = entry?;
      let file_name = entry.file_name();
      let Some(name) = file_name.to_str() else {
        continue;
      };
      // 快照文件名固定为 26 位 Base32 前缀，安全解码出 128 位 key_id，跳过外来文件
      if let Some(stem) = name.strip_suffix(TREE_FILE_SUFFIX)
        && let Some(key_id) = decode_u128(stem)
      {
        result.push((key_id, entry.path()));
      }
    }
    Ok(result)
  }

  /// 从指定目标目录全量恢复所有 BfTree 索引至 ri_log_root 并注册 pending 条目 (支持多候选路径容错)
  ///
  /// 与 C# RecoverAllTreesFromCheckpoint / RebuildFromSnapshotIfPending 语义对齐；
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetRecoveredCheckpointToken
  /// 的承接：C# 恢复期可变暂存令牌（供 RebuildFromSnapshotIfPending 判定快照
  /// 目录）由本函数显式 `checkpoint_token` 传参替代，无需管理器可变状态：
  /// 仅做文件预置 (fs::copy 覆盖 data.bftree) + 注册 tree=None 的 pending 条目，
  /// 引擎实例一律由首次访问的 get_or_open_tree 惰性恢复——急切 open 会为每棵树
  /// 分配完整环形缓冲区，RI 键规模大时启动内存与耗时不可控 (C# 同样不在此处开树)。
  ///
  /// 与 C# RecoverAllTreesFromCheckpoint 的差异：C# 恢复期由 OnRecoverySnapshotRead
  /// 逐 stub 触发 (持有原始 key，keyHash 精确派生，单文件失败可 log 后继续)；本实现
  /// 按目录枚举快照文件批量预置 (wedb 恢复流程无主日志逐 stub 回放，见 wkv
  /// store/cpr_host.rs `run_recovery_pass`)，单文件失败即整批上抛——静默跳过等于恢复后缺树运行，宁可启动
  /// 恢复显式失败。魔数损坏同样上抛（该 Token 判失败），回退链据此整链回退；
  /// 非普通文件属异常布局而非介质损坏，warn 留痕后跳过。
  ///
  /// 正确性要点：
  /// - 每个文件在 stem 派生的条带写锁内以 try_insert 注册：条目已存在 (pending
  ///   预置 / get_or_open_tree 已激活 / 前一轮恢复) 时不覆盖，杜绝双开同一数据
  ///   文件的引擎实例。恢复流程契约由上层保证在服务对外前单线程执行；与运行期
  ///   get_or_open_tree (fast_hash(key) 条带) 分属不同条带时，try_insert 的
  ///   失败即拒绝语义兜底不产生覆盖。
  /// - key_hash 由 stem (key_id Base32 编码) 派生而非 fast_hash(原始 key)：恢复期
  ///   只有文件名，原始 key 不可得。条带锁只要求「同一 key 的并发路径派生同值」
  ///   ——stem 是 key_id 的确定性函数，同一 key 恒定落同一条带，锁分段成立；
  ///   键路由由 key_id (字典键) 承担，key_hash 不参与数据寻址。
  pub fn recover_all_trees_from_dir(
    &self,
    target_dir: &Path,
    checkpoint_token: u128,
  ) -> Result<usize> {
    let token_dir = Self::token_snapshot_dir(target_dir, checkpoint_token);
    let mut candidate_dirs = Vec::with_capacity(4);
    candidate_dirs.push(token_dir);
    candidate_dirs.push(target_dir.join("rangeindex"));
    if target_dir != self.cpr_dir {
      candidate_dirs.push(self.checkpoint_snapshot_dir(checkpoint_token));
      candidate_dirs.push(self.cpr_dir.join("rangeindex"));
    }

    let mut staged_count = 0;
    for snapshot_dir in &candidate_dirs {
      if !snapshot_dir.exists() {
        continue;
      }
      if let Ok(entries) = fs::read_dir(snapshot_dir) {
        // 单次 pin 贯穿目录内全部文件注册 (papaya epoch guard 一次获取，免逐文件重入)
        let pin = self.live_indexes.pin();
        for entry in entries.flatten() {
          let name = entry.file_name();
          if let Some(name_str) = name.to_str()
            && let Some(stem) = name_str.strip_suffix(".bftree")
            // 快照文件名固定为 26 位 Base32 前缀，安全解码出 128 位 key_id，跳过外来文件
            && let Some(key_id) = decode_u128(stem)
          {
            // 已注册 (pending 预置 / 已激活 / 前一轮恢复) 则跳过，避免重复拷贝磁盘大文件
            if pin.contains_key(&key_id) {
              continue;
            }

            let path = entry.path();
            // 非普通文件（异常布局）：warn 留痕后跳过，不中断本轮候选
            if !path.is_file() {
              log::warn!(
                "RI 快照恢复跳过非普通文件: path={}, key_id={key_id}",
                path.display()
              );
              continue;
            }
            // 魔数不匹配 = 快照介质损坏：warn 留痕（路径 + key_id）后报错，
            // 使本检查点判失败——回退链（wcpr::recover_latest）据此整链回退
            // 至更早有效版本。静默跳过等于恢复后缺树运行，存根已注册 pending、
            // 首次访问才 Recovery 硬错且缺失根因无处可查，wcpr 回退链也永远
            // 感知不到 RI 面损坏 (C# 对位 RecoverAllTreesFromCheckpoint 失败
            // LogError 后继续，rust 以显式失败换整链回退，缺失根因留痕等价)
            if !file_has_cpr_magic(&path) {
              log::warn!(
                "RI 快照魔数不匹配，判本检查点失败: path={}, key_id={key_id}",
                path.display()
              );
              return Err(Error::Corrupted(format!(
                "RI 快照魔数不匹配（触发检查点回退）: path={}, key_id={key_id}",
                path.display()
              )));
            }

            let target_data_path = self.data_file_path(stem);
            // 无条件以检查点快照预置工作文件：文件名恒不同 ({stem}.bftree vs
            // {stem}.data.bftree)，`path != target` 恒真——工作文件可能仅有环形
            // 缓冲中未落盘的部分页，快照才是恢复点权威版本，存在也必须覆盖
            // (`path == target` 分支仅防御 fs::copy 自拷贝，按命名规则不可达)
            if !target_data_path.exists() || target_data_path != path {
              fs::copy(&path, &target_data_path)?;
            }

            // 持 stem 条带写锁注册，与并发恢复轮次串行化
            let key_hash = fast_hash(stem.as_bytes());
            let _stripe_lock = self.locks.write(key_hash);

            // 锁内复查：避免并发恢复轮次重入覆盖
            if pin.contains_key(&key_id) {
              continue;
            }
            // 仅注册 pending 条目 (tree=None)，引擎实例交给 get_or_open_tree 惰性恢复
            // (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RebuildFromSnapshotIfPending 只预置不开树)。
            // 前缀取 stem 解码出的 key_id 再规范编码：stem 本就是 key_id 的 Base32
            // 规范编码 (检查点文件名恒为小写)，round-trip 恒等且零堆分配 (对标 C#
            // 直接截取文件名前缀 name[..HashPrefixLength])
            let tree_entry = Arc::new(TreeEntry::new(None, key_hash, key_id));
            // try_insert：锁内 contains 与插入间唯一竞争方是 fast_hash(原始 key)
            // 条带的 get_or_open_tree (恢复期契约排除)，失败即拒绝兜底不覆盖
            if pin.try_insert(key_id, tree_entry).is_ok() {
              staged_count += 1;
            }
          }
        }
      }
      if staged_count > 0 {
        break;
      }
    }

    Ok(staged_count)
  }

  /// 恢复回退清场：删除本轮恢复已预置拷贝的树工作文件（静态清场，无需实例）
  ///
  /// 回退链（wcpr::recover_latest）某轮 from_recovered 中途失败时，
  /// [`Self::recover_all_trees_from_dir`] 已把快照文件拷贝为
  /// {ri_log_root}/{stem}.data.bftree 工作文件（本轮注册的 pending 条目随
  /// 失败实例析构自动摘除，磁盘工作文件则留存）——下一轮更早 Token 恢复时
  /// 快照不含该 key，残留工作文件不被覆盖，AOF 重放首次访问即混出代际混杂
  /// 视图。与 [`Self::recover_all_trees_from_dir`] 同一候选目录口径枚举快照
  /// stem，删除对应工作文件：多删无虞（下一轮恢复重拷，或 AOF 重放自空树
  /// 重建），漏删即混代，故不做 staged>0 的候选截断，候选目录全量枚举。
  ///
  /// 无 C# 对位（C# 按 stub 单文件粒度 continue，无整轮重试继承态）。
  pub fn discard_staged_data_files(
    ri_log_root: &Path,
    target_dir: &Path,
    cpr_dir: &Path,
    token: u128,
  ) -> usize {
    let mut candidate_dirs = Vec::with_capacity(4);
    candidate_dirs.push(Self::token_snapshot_dir(target_dir, token));
    candidate_dirs.push(target_dir.join("rangeindex"));
    if target_dir != cpr_dir {
      candidate_dirs.push(Self::token_snapshot_dir(cpr_dir, token));
      candidate_dirs.push(cpr_dir.join("rangeindex"));
    }

    let mut removed = 0;
    for snapshot_dir in &candidate_dirs {
      let Ok(entries) = fs::read_dir(snapshot_dir) else {
        continue;
      };
      for entry in entries.flatten() {
        let name = entry.file_name();
        if let Some(name_str) = name.to_str()
          && let Some(stem) = name_str.strip_suffix(TREE_FILE_SUFFIX)
          // 快照文件名固定为 26 位 Base32 前缀，外来文件不入清场集
          && decode_u128(stem).is_some()
        {
          // 目标本不存在（前轮已清或从未预置）属正常态；其余删除失败 warn
          // 留痕不计数——不可删除项对下一轮恢复同样不可覆盖，留给运行期
          // 清扫路径按既有语义处置
          let target = Self::data_file_path_in(ri_log_root, stem);
          match fs::remove_file(&target) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
              log::warn!("恢复回退清场删除失败: path={}, err={e}", target.display());
            }
            Err(_) => {}
          }
        }
      }
    }
    removed
  }
}

/// [`RangeIndexManager::flush_files`] 的产物：ri_log_root 下带地址刷盘件的惰性迭代器
/// (1:1 对标 C# 同一原语返回的 `IEnumerable` 惰性序列，路径与解码按项即时产出)
pub(super) struct FlushFiles {
  /// 目录枚举句柄；None = 目录不存在，恒产出空集 (C# 目录有效性预检的 yield break)
  entries: Option<fs::ReadDir>,
}

impl Iterator for FlushFiles {
  /// `(path, key_id, addr)`：刷盘件全路径 + 文件名严格解码出的 128 位键 ID 与逻辑地址
  type Item = (PathBuf, u128, u64);

  fn next(&mut self) -> Option<Self::Item> {
    let entries = self.entries.as_mut()?;
    for entry in entries.by_ref() {
      // 单项 IO 失败与解码不出的文件名 (裸名刷盘件、工作文件、外来文件) 一律跳过
      let Ok(entry) = entry else {
        continue;
      };
      let name = entry.file_name();
      if let Some((key_id, addr)) = name
        .to_str()
        .and_then(RangeIndexManager::parse_flush_file_name)
      {
        return Some((entry.path(), key_id, addr));
      }
    }
    None
  }
}

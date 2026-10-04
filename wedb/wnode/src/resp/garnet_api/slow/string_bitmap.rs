//! 基础命令族慢路径承接（字符串 / OBJECT / 位图 / 键管理 / ETag / MSETNX / GET）
//!
//! BasicCommands.cs 各命令同步函数体内 CompletePending 就地闭环的 rust 慢路径
//! 对偶：快路径环形页翻转 / RI 门 / 存在性探针降级至此，解析与快路径同一单源，
//! 应答形态逐字节一致

use wdev::Device;
use wresp::{
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, write_error_raw},
  command::RespCommand,
  ext::RespVecExt,
};
use wval::KeyTag;

use super::StoreGarnetApi;
use crate::{
  resp::{
    MsetnxResume,
    basic_commands::slow::{bitmap_slow, string_slow},
    basic_etag_commands::etag_slow,
    key_admin_commands::slow::key_admin_slow,
  },
  storage::session::{
    common::ttl_sync::{probe_alive_with_registry, probe_alive_with_registry_async},
    storage_session::StorageSession,
  },
};

impl<D: Device> StoreGarnetApi<D> {
  /// 字符串族 / OBJECT 慢路径承接执行段
  pub(super) async fn string_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(output, string_slow, storage, cmd, refs, self.vector_mgr());
    output
  }

  /// 位图族慢路径承接执行段（BitmapCommands.cs 同型：读改写回异步闭环，
  /// BITFIELD 子命令序列解析单源，BITOP 逐源折叠）
  pub(super) async fn bitmap_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(output, bitmap_slow, storage, cmd, refs, self.vector_mgr());
    output
  }

  /// 键管理族慢路径承接执行段（KeyAdminCommands.cs：TTL / 存在性 / 迁移
  /// 裁决降级至此，wkv 异步判定表与三域探针闭环；RENAME 的 RangeIndex /
  /// 升阶键整树快照迁移经 wkv rename_range_index 单点）
  pub(super) async fn key_admin_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let vector = self.vector_mgr();
    slow_arm!(output, key_admin_slow, storage, cmd, refs, vector);
    output
  }

  /// ETag 族慢路径承接执行段（BasicEtagCommands.cs：六命令快路径失闩 /
  /// 磁盘候选降级至此——写臂持本键读改写窗口重放（票 zcode-r32-rmwmatrix
  /// 立项二），读共同体异步闭环，应答形态与快路径逐字节一致）
  pub(super) async fn etag_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(output, etag_slow, storage, cmd, refs);
    output
  }

  /// MSETNX 慢路径承接执行段（C# MSET_Conditional 事务锁内全同步闭环的
  /// rust 对偶：前缀外提 + 判定与批量写入折叠单窗口 + 中途失败条件回滚，
  /// 全有或全无，应答与最终存储状态恒一致）
  pub(super) async fn msetnx_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    args: &[Vec<u8>],
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    // 尾参为快路径续跑模式标记（[`MsetnxResume::from_tail`] 逆解析）：
    // Continue（b"1"）= NX 判定已整体通过、已写键保持，跳过判定续写
    // 全部键回 :1；Rollback（b"r"）= 快路径回滚存在删除降级残留，持窗
    // 条件回滚收尾回 :0；Replay（b"0"）= 判定段降级，先三域异步裁决
    // 存活——任一存活整体不写回 :0（C# EXISTS 非 NOTFOUND 即存在），
    // 全不存活才写入
    let mode = MsetnxResume::from_tail(args.last().map(Vec::as_slice));
    // 剥离尾参后的键值对序列（快路径已校验 arity 非空且偶数）
    let pairs = &refs[..refs.len() - 1];
    if mode == MsetnxResume::Rollback {
      // 回滚收尾模式（票 zcode-r37-lockfix 发现 B）：持全键窗逐键条件
      // 删除——仅删内容即本命令所写的键（并发盲写 SET 抢覆写键的已
      // 确认写保留不删），async 删除闭环无降级残留，回 :0（全有或全
      // 无失败面：残留清完后该语义成立；存储错误留痕可见）
      let Ok(_windows) = storage
        .batch
        .rmw_window_sorted(pairs.as_chunks::<2>().0.iter().map(|c| c[0]))
        .await
      else {
        // 取闩预算耗尽（LockTimeout）按本臂存储错误同源应答
        bail_frame!(output);
      };
      let prefix = storage.batch.session_prefix();
      let prefix_slice = prefix.as_slice();
      for [key, val] in pairs.as_chunks::<2>().0 {
        if matches!(
          storage
            .read_tag_with_prefix(prefix_slice, key, KeyTag::String, |cur| cur == &val[..])
            .await,
          Ok(Some(true))
        ) && let Err(e) = storage.delete_string(key).await
        {
          log::error!("MSETNX 回滚收尾删除失败: {e:?}");
        }
      }
      output.write_resp_int(0);
      return output;
    }
    let resume = mode == MsetnxResume::Continue;
    // 全键读改写窗口（快路径 network_msetnx 同一窗口契约，票
    // zcode-r15-generic 发现一对标 C# MSET_Conditional 全键排他锁）：
    // 桶序取闩无循环等待面，闩内完成判定（含逐键 await 闭环）与批量写
    // 全序列——快慢两路径「判定与写入一体」同一窗口，删去原「逐键 await
    // 窗口内的并发写入属顺序未定义」的自认窗口
    let Ok(_windows) = storage
      .batch
      .rmw_window_sorted(pairs.as_chunks::<2>().0.iter().map(|c| c[0]))
      .await
    else {
      // 取闩预算耗尽（LockTimeout）按本臂存储错误同源应答
      bail_frame!(output);
    };
    // 循环前缀外提（transpile SKILL 工程准则）：判定与写入两段共用同一
    // 外提前缀，零逐键 ns/db 原子变量重读与 Varint 重算
    let prefix = storage.batch.session_prefix();
    let prefix_slice = prefix.as_slice();
    // 登记表句柄：会话侧取用（本档 string_slow 臂同形装配）
    let vector = self.vector_mgr();
    if !resume {
      for key in pairs.iter().step_by(2) {
        // 同步折叠探针先行（快路径 network_msetnx 判定段同款：三域
        // String / ObjectEnvelope / Meta + 向量登记表第四态，票
        // zcode-r161c-msetnx 案一——快臂判定段磁盘候选降级后，派发与
        // 慢臂落笔之间另有 await 宽窗，其间他核 VADD 新提交登记须在本
        // 位判存在，否则 :1 覆写值被值域门永久遮蔽）；Deferred（磁盘
        // 候选 / TTL 待裁决）才逐键异步折叠闭环（TTL 过期键经异步读
        // 惰性清除后视同缺失）。逐键 await 全程持全键窗口，判定与写入一体
        match probe_alive_with_registry(&storage.batch, prefix_slice, key, vector) {
          // 任一键存活：整体零写入回 :0（C# MSET_Conditional error 短路）
          Ok(Some(true)) => {
            output.write_resp_int(0);
            return output;
          }
          Ok(Some(false)) => {}
          Ok(None) => {
            match probe_alive_with_registry_async(storage, prefix_slice, key, vector).await {
              Ok(true) => {
                output.write_resp_int(0);
                return output;
              }
              Ok(false) => {}
              Err(_) => bail_frame!(output),
            }
          }
          Err(_) => bail_frame!(output),
        }
      }
    }
    // 写入段（全有或全无，与 C# 事务缓冲 + Commit 的可观测语义对齐）：
    // 先单次同步折叠（与快路径 network_msetnx 同一折叠入口，多数场景
    // 零 await 原子窗口闭环）；降级或存储错误时已写键保持，逐键异步兜底
    // 重放全量（重放值与已写键同值幂等；SET 语义自动清新键残留 TTL，
    // 补写模式重写快路径前缀键同值幂等）。任一键 Err 条件回滚涉及键
    //（票 zcode-r37-lockfix 发现 B：仅删内容即本命令所写的键，禁盲删吞
    // 并发已确认写；Continue 续写形态判定由快路径承担，回滚时点不重持
    // 「全键不存在」前提，条件化恰补此洞。删除自身失败留痕可见，错误
    // 同源属尽力极限）
    let fold = pairs.as_chunks::<2>().0.iter().map(|c| (c[0], c[1]));
    match storage.batch.try_upsert_batch_sync(fold) {
      Ok(Ok(())) => {}
      _ => {
        let mut failed = false;
        for [key, val] in pairs.as_chunks::<2>().0 {
          if storage.upsert_string(key, val).await.is_err() {
            failed = true;
            break;
          }
        }
        if failed {
          for [key, val] in pairs.as_chunks::<2>().0 {
            if matches!(
              storage
                .read_tag_with_prefix(prefix_slice, key, KeyTag::String, |cur| cur == &val[..])
                .await,
              Ok(Some(true))
            ) && let Err(e) = storage.delete_string(key).await
            {
              log::error!("MSETNX 回滚删除失败: {e:?}");
            }
          }
          bail_frame!(output);
        }
      }
    }
    output.write_resp_int(1);
    output
  }

  /// GET 慢路径承接执行段（同步段磁盘候选降级至此：read_user_batch_into 走
  /// wkv 批量读口取 String 域值，TTL 门与逐条读口同源、在批量口内裁决，
  /// 过期键视同不存在回 nil，冷命中键经磁盘冷读装载并惰性物理清除（对标 C#
  /// InternalRead 的 ReadCache/磁盘冷读异步完成路径与 Reader 内
  /// CheckExpiry）；多键时触发流水线冷读 Scatter-Gather (SG) 异步批量 I/O，
  /// 对标 C# NetworkGET_SG。批量口确认缺失的键经三域判型续探（信封 / Meta
  /// 域命中即集合对象键）改出 WRONGTYPE 错误帧且簿记静默——与快臂
  /// read_user_sync 三域折叠同源，修复冷对象键回 nil+notfound 的双臂分叉
  ///（对位 C# NetworkGET pending 收割后同一 Reader 判型，快慢无第二形态）
  pub(super) async fn get_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    if storage
      .read_user_batch_into(refs, &mut output)
      .await
      .is_err()
    {
      // 批量读口以 Err 中止时 output 残留首个磁盘候选之前已先行交付的部分帧
      //（wkv `session/raw/batch.rs:read_batch_raw_with` 次序契约：调用方须
      // 整体丢弃本批部分结果）：先彻底丢弃残帧，再按挂起键数逐键各补一条错
      // 误帧——本臂一次承接的是 SG 流水线合并的 N 条 GET（garnet_api/mod.rs
      // 的 sg_batched_keys 快照），每键各是一条独立命令，应答须 N 键 N 帧
      // 严格对齐（对标 C# NetworkGET_SG 逐键独立成帧，BasicCommands.cs:244；
      // 单键 GET 即退化为单帧）
      output.clear();
      for _ in refs {
        err_frame!(output);
      }
    }
    output
  }
}

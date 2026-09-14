//! 存储执行域命令分派面（会话核心 → 存储执行的注入点）
//!
//! 对标 libs/server/API/IGarnetApi.cs:IGarnetApi：C# 会话的分派链
//! ProcessBasicCommands → ProcessArrayCommands → ProcessOtherCommands 以
//! 泛型参数 `ref TGarnetApi storageApi` 贯穿存储 API，单机与集群同一
//! 执行路径（集群差异仅槽位门 CanServeSlot 在前）。rust 侧命令实现
//! （resp 命令文件）需要 [`wkv::BatchStoreSession`] 纪元守卫与
//! `D: Device` 泛型，无法驻留会话结构体，故以统一分派句柄注入承接：
//! 宿主构造会话后经 [`RespServerSession::set_garnet_api`] 注入，
//! 会话主循环在槽位门放行后经 [`GarnetApi::exec`] 进入存储执行域。

use std::{
  mem,
  path::PathBuf,
  ptr,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use wbase::{num::strict_i32, time::now_ms};
use wbitmap::BitmapOperation;
use wcol::sortedset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wdev::Device;
use wkv::{BatchStoreSession, StoreSession};
use wresp::{
  RespCommand,
  cmd_strings::{
    RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_SWAPDB_UNSUPPORTED, RESP_ERR_WRONG_TYPE, RESP_OK,
    write_error_raw,
  },
  command::is_vector_set_command,
};

use super::{
  basic_commands::{IncrCmd, ObjectSubCmd, parse_flush_options},
  key_admin_commands::{ExpireCmd, ExpireTimeCmd, TtlCmd},
  objects::{sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind},
  rangeindex::resp_server_session_range_index as ri_cmds,
  resp_server_session::RespServerSession,
  slow_path::{SlowFuture, SlowWait},
};
use crate::{
  resp::vector::{
    resp_server_session_vectors::RespServerSessionVectors, vector_manager::VectorManager,
  },
  storage::session::{common::db_admin_functions::swap_db_keys, storage_session::StorageSession},
};

/// 同步会话上下文无法闭环（磁盘冷读 / 环形页翻转 / 全库扫描）时的降级
/// 错误文案——命令层 `Ok(false)` 约定不残留输出，慢路径执行器
/// [`SlowWait`] 承接闭环；仅当宿主未挂慢路径执行器时兜底写明错误而非静默
const RESP_ERR_ASYNC_REQUIRED: &str = "ERR command requires asynchronous completion";

/// 慢路径异步扫描/清库 IO 失败的兜底错误文案（存储层 wkv::Error 统一
/// 降噪为此单行，杜绝把内部错误细节泄漏给客户端）
const RESP_ERR_SLOW_PATH_IO: &str = "ERR slow path storage error";

/// 检查点通道未装配（宿主未注入 [`CheckpointCtx`]）时的显式拒绝文案
const RESP_ERR_CHECKPOINT_UNWIRED: &str = "ERR checkpoint channel not configured";

/// SAVE / BGSAVE / LASTSAVE 检查点通道（对标 C# storeWrapper 检查点域句柄；
/// 服务器级共享，由 StorageSessionProvider 装配期注入）
#[derive(Clone)]
pub struct CheckpointCtx {
  /// 检查点目录（C# GetStoreCheckpointDirectory(0) 口径）
  pub dir: PathBuf,
  /// 最近成功检查点时刻（Unix 毫秒；0 = 尚无检查点，对齐 C#
  /// GarnetDatabase 构造期 `DateTimeOffset.FromUnixTimeSeconds(0)`）
  ///
  /// 刻意差异（对照 C#）：C# LastSaveTime 挂在每个 GarnetDatabase（每库
  /// 一个值）；rust 检查点通道全局单例，多库共享本单值
  pub last_save_ms: Arc<AtomicI64>,
}

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派面（C# IGarnetApi 的 rust 注入投影）
///
/// C# 以泛型静态分发（`where TGarnetApi: IGarnetApi`）；rust 以静态虚表
/// 分派句柄消除 `D: Device` 泛型对会话/消费者层的传染与动态分发开销
pub trait GarnetApiFace: Send + Sync {
  /// libs/server/API/IGarnetApi.cs:IGarnetApi
  ///
  /// 执行一条命令并写回应答到会话输出缓冲（C# ProcessBasicCommands /
  /// ProcessArrayCommands / ProcessOtherCommands 的 switch 承接）。
  /// 未接入分派表的命令按 C# ProcessAdminCommands 尾部兜底写
  /// `ERR unknown command`
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]);

  /// 慢路径异步执行（同步段返回 `Ok(false)` 的命令在异步域闭环）
  ///
  /// 产出该命令的完整应答字节（错误应答含在内），由 [`SlowWait`] 驱动。
  /// future 借用线程本地执行域（wdev 设备段句柄为 TLS `Rc` 表，compio 任务
  /// 线程绑定），跨 [`SlowWait`] 的 Send 边界由其安全论证承担
  fn exec_slow<'a>(
    &'a self,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
  ) -> impl Future<Output = Vec<u8>> + 'a;
}

pub(crate) type RawExecFn = unsafe fn(*const (), &mut RespServerSession, RespCommand, &[&[u8]]);
pub(crate) type RawExecParts = (*const (), RawExecFn);

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派句柄（静态函数指针封装，彻底消除动态分发与二次包装）
pub struct GarnetApi {
  ptr: *const (),
  exec: RawExecFn,
  exec_slow: unsafe fn(*const (), RespCommand, Vec<Vec<u8>>) -> SlowFuture,
  drop: unsafe fn(*const ()),
  clone: unsafe fn(*const ()) -> *const (),
}

unsafe impl Send for GarnetApi {}
unsafe impl Sync for GarnetApi {}

impl GarnetApi {
  /// 从实现了 [`GarnetApiFace`] 的实例构造句柄
  pub fn new<T: GarnetApiFace + 'static>(target: T) -> Self {
    Self::from_arc(Arc::new(target))
  }

  /// 从 `Arc<T>` 构造句柄
  pub fn from_arc<T: GarnetApiFace + 'static>(arc: Arc<T>) -> Self {
    unsafe fn exec_fn<T: GarnetApiFace>(
      ptr: *const (),
      session: &mut RespServerSession,
      cmd: RespCommand,
      args: &[&[u8]],
    ) {
      let t = unsafe { &*(ptr as *const T) };
      t.exec(session, cmd, args);
    }
    unsafe fn exec_slow_fn<T: GarnetApiFace + 'static>(
      ptr: *const (),
      cmd: RespCommand,
      args: Vec<Vec<u8>>,
    ) -> SlowFuture {
      // future 持 Arc 克隆保 'static（Arc 归还虚表槽位，所有权不变）
      let arc = unsafe { Arc::from_raw(ptr as *const T) };
      let cloned = Arc::clone(&arc);
      let _ = Arc::into_raw(arc);
      SlowFuture::new(async move { cloned.exec_slow(cmd, args).await })
    }
    unsafe fn drop_fn<T>(ptr: *const ()) {
      unsafe {
        drop(Arc::from_raw(ptr as *const T));
      }
    }
    unsafe fn clone_fn<T>(ptr: *const ()) -> *const () {
      unsafe {
        let arc = Arc::from_raw(ptr as *const T);
        let cloned = Arc::clone(&arc);
        let _ = Arc::into_raw(arc);
        Arc::into_raw(cloned) as *const ()
      }
    }
    Self {
      ptr: Arc::into_raw(arc) as *const (),
      exec: exec_fn::<T>,
      exec_slow: exec_slow_fn::<T>,
      drop: drop_fn::<T>,
      clone: clone_fn::<T>,
    }
  }

  #[inline]
  pub fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    unsafe { (self.exec)(self.ptr, session, cmd, args) }
  }

  /// 提取底层指针与执行函数，供借用解耦分派（零 Arc::clone）
  #[inline]
  pub(crate) fn raw_exec_parts(&self) -> RawExecParts {
    (self.ptr, self.exec)
  }

  /// 慢路径分派（静态虚表直转，future 持 Arc 克隆保 'static，
  /// 由 [`SlowWait`] 持有驱动）
  #[inline]
  pub fn exec_slow(&self, cmd: RespCommand, args: Vec<Vec<u8>>) -> SlowFuture {
    unsafe { (self.exec_slow)(self.ptr, cmd, args) }
  }
}

impl Drop for GarnetApi {
  fn drop(&mut self) {
    if !self.ptr.is_null() {
      unsafe { (self.drop)(self.ptr) };
      self.ptr = ptr::null();
    }
  }
}

impl Clone for GarnetApi {
  fn clone(&self) -> Self {
    Self {
      ptr: unsafe { (self.clone)(self.ptr) },
      exec: self.exec,
      exec_slow: self.exec_slow,
      drop: self.drop,
      clone: self.clone,
    }
  }
}

impl<T: GarnetApiFace + 'static> From<Arc<T>> for GarnetApi {
  fn from(arc: Arc<T>) -> Self {
    Self::from_arc(arc)
  }
}

/// wkv 存储会话的 [`GarnetApi`] 实现（单机与集群共用执行域）
///
/// 对标 C# RespServerSession 构造时经 storeWrapper 创建的 storageSession
/// （每连接独立会话）；命令执行期进入批处理纪元（C# EnterUnsafe），整个
/// 消费批次内内存直读免逐操作 enter/exit
pub struct StoreGarnetApi<D: Device> {
  /// 底层存储会话
  session: StoreSession<D>,
  /// Vector Set 命令处理层（构造期缓存，杜绝每条向量命令克隆 `Arc<VectorManager>`）
  vector_session: Option<RespServerSessionVectors>,
  /// 检查点通道（未注入 = SAVE/BGSAVE 显式拒绝，LASTSAVE 回 0）
  checkpoint: Option<CheckpointCtx>,
}

impl<D: Device> StoreGarnetApi<D> {
  /// 在 garnet 中的相对路径:Storage/Session/StorageSession.cs:StorageSession
  ///
  /// 构造存储执行域（`session` 为每连接独立的 `store.new_session()` 产物）
  pub fn new(session: StoreSession<D>) -> Self {
    Self {
      session,
      vector_session: None,
      checkpoint: None,
    }
  }

  /// 关联向量集合管理器（构造期包装为命令处理层，单次 Arc 持有）
  pub fn with_vector_manager(mut self, vector_manager: Arc<VectorManager>) -> Self {
    self.vector_session = Some(RespServerSessionVectors::new(vector_manager));
    self
  }

  /// 关联检查点通道（SAVE/BGSAVE/LASTSAVE 经此闭环，对标 C#
  /// storeWrapper.TakeCheckpointAsync 的存储可达面）
  pub fn with_checkpoint_ctx(mut self, ctx: CheckpointCtx) -> Self {
    self.checkpoint = Some(ctx);
    self
  }
}

impl<D: Device + 'static> From<StoreGarnetApi<D>> for GarnetApi {
  fn from(store: StoreGarnetApi<D>) -> Self {
    Self::new(store)
  }
}

impl<D: Device> GarnetApiFace for StoreGarnetApi<D> {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    if is_vector_set_command(cmd)
      && let Some(vectors) = &self.vector_session
    {
      let resp3 = session.resp_protocol_version == 3;
      let reply = match cmd {
        RespCommand::Vadd => vectors.network_vadd_impl(args, resp3),
        RespCommand::Vsim => vectors.network_vsim_impl(args, resp3),
        RespCommand::Vemb => vectors.network_vemb(args),
        RespCommand::Vcard => vectors.network_vcard(args),
        RespCommand::Vdim => vectors.network_vdim(args),
        RespCommand::Vgetattr => vectors.network_vgetattr(args),
        RespCommand::Vinfo => vectors.network_vinfo(args),
        RespCommand::Vismember => vectors.network_vismember_impl(args, resp3),
        RespCommand::Vlinks => vectors.network_vlinks(args),
        RespCommand::Vrandmember => vectors.network_vrandmember(args),
        RespCommand::Vrem => vectors.network_vrem(args),
        RespCommand::Vsetattr => vectors.network_vsetattr_impl(args, resp3),
        _ => unreachable!(),
      };
      // 应答直写会话输出缓冲（self 借用 vectors 与 session.output 不相交，
      // 免 mem::take/换回的二次搬移）
      let output = &mut session.output;
      if resp3 {
        reply.encode_resp3(output);
      } else {
        reply.encode_resp2(output);
      }
      return;
    }
    let batch = self.session.enter_batch();
    let mut output = mem::take(&mut session.output);
    // 命令层约定：Ok(false) = 须异步闭环且本次不残留输出
    if dispatch(session, cmd, args, &batch, &mut output) == Ok(false) {
      // 慢路径分派（单次实现，多命令复用）：挂起 SlowWait 停止本批消费，
      // 网络泵 await 闭环后写回应答；参数快照脱离接收缓冲生命周期。
      // 句柄克隆保 Arc 存活，future 借用的执行域（self.session）在网络泵
      // await 期间有效（消费串行驱动，无并发进入）
      if let Some(api) = &session.garnet_api {
        session.pending_slow = Some(SlowWait::for_command(
          api,
          cmd,
          args.iter().map(|a| a.to_vec()).collect(),
        ));
      } else {
        // 执行域未挂载的装配缺口：写明错误，绝不静默吞命令
        write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
      }
    }
    session.output = output;
  }

  async fn exec_slow(&self, cmd: RespCommand, args: Vec<Vec<u8>>) -> Vec<u8> {
    use RespCommand as C;
    use wresp::RespVecExt;

    use super::array_commands::parse_scan_filter;
    use crate::{storage::session::storage_session::StorageSession, types::GarnetStatus};

    // 检查点族须在会话纪元保护区外发起（wcpr fail-fast 契约：批处理纪元
    // 守卫内自钉纪元会令排空屏障谓词永假），先于 batch 域闭环
    if matches!(
      cmd,
      RespCommand::Save | RespCommand::Bgsave | RespCommand::Lastsave
    ) {
      return self.checkpoint_command_slow(cmd).await;
    }

    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    // 慢路径执行域：独立批处理纪元 + 只读扫描会话（慢命令不登记/推进
    // WATCH，独立版本表与 GarnetDatabase 的共享面语义等价）
    let batch = self.session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    let mut output = Vec::new();
    match cmd {
      // ---- 全库扫描族（同步段仅校验，异步段承载实际扫描）
      C::Dbsize => match storage.db_size().await {
        Ok(n) => output.write_resp_int(n as i64),
        Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO),
      },
      C::Keys => {
        let pattern = refs.first().copied().unwrap_or(b"*");
        match storage.db_keys(pattern).await {
          Ok(keys) => {
            output.write_resp_array_len(keys.len());
            for key in &keys {
              output.write_resp_bulk_string(key);
            }
          }
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO),
        }
      }
      C::Scan => {
        let filter = match parse_scan_filter(&refs) {
          Ok(f) => f,
          Err(err) => {
            write_error_raw(&mut output, err);
            return output;
          }
        };
        // TYPE 参数出现时单页无上限（含未知类型，C# long.MaxValue 同口径）
        let count = if filter.type_given {
          usize::MAX
        } else {
          filter.count
        };
        match storage
          .scan_cursor(
            &filter.pattern,
            filter.all_keys,
            filter.cursor as u64,
            count,
            filter.type_filter,
          )
          .await
        {
          Ok((cursor, keys)) => {
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            RespServerSession::write_output_for_scan(cursor as i64, &key_refs, &mut output)
          }
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO),
        }
      }
      // ---- 清库族（BasicCommands.cs:ExecuteFlushDb：选项单源重解析 +
      // 逐键完整删除；单库模型 FLUSHALL 与 FLUSHDB 同径——C#
      // SingleDatabaseManager.FlushAllDatabases 即调 FlushDatabase）
      C::Flushdb | C::Flushall => {
        let Ok(opts) = parse_flush_options(&refs) else {
          write_error_raw(&mut output, RESP_ERR_GENERIC_SYNTAX_ERROR);
          return output;
        };
        let _ = opts.async_flush;
        if storage.delete_all_user_keys().await.is_err() {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO);
          return output;
        }
        // UNSAFETRUNCATELOG：物理截断历史段（C# ShiftBeginAddress truncateLog）
        if opts.unsafe_truncate_log && storage.batch.store.truncate().await.is_err() {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO);
          return output;
        }
        output.extend_from_slice(RESP_OK);
      }
      // ---- 跨库交换（ArrayCommands.cs:NetworkSWAPDB：整数解析同快路径
      // 单源；范围校验由快路径完成，args 为同一快照）
      C::Swapdb => {
        let (Some(idx1), Some(idx2)) = (
          refs.first().and_then(|a| strict_i32(a)),
          refs.get(1).and_then(|a| strict_i32(a)),
        ) else {
          write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
          return output;
        };
        if idx1 == idx2
          || swap_db_keys(
            &storage.batch.store,
            storage.batch.namespace(),
            idx1 as i64,
            idx2 as i64,
          )
          .await
        {
          output.extend_from_slice(RESP_OK);
        } else {
          // C# TrySwapDatabases 失败口径（搬移失咬合 / 存储错误）
          write_error_raw(&mut output, RESP_ERR_SWAPDB_UNSUPPORTED);
        }
      }
      // ---- 对象收集 `*` 全库族（AdminCommands.cs:NetworkHCOLLECT /
      // NetworkZCOLLECT：C# ObjectCollect 批扫 + 逐键 RMW 的慢路径承接；
      // 信封域键真回收，Meta 打平键过期字段由 wkv 版本栅栏惰性失效、
      // 无内存可回收，不入选清单）
      C::Hcollect | C::Zcollect => {
        if refs.first().copied() != Some(b"*") {
          // 快路径显式键清单已闭环，慢路径只承接 `*`（防御内部错序）
          write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
          return output;
        }
        let is_hash = matches!(cmd, C::Hcollect);
        let tag = if is_hash {
          wval::GarnetObjectType::Hash
        } else {
          wval::GarnetObjectType::SortedSet
        };
        let keys = match collect_envelope_keys(&storage, tag).await {
          Ok(keys) => keys,
          Err(_) => {
            write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO);
            return output;
          }
        };
        let mut wrong_type = false;
        for key in &keys {
          let res = if is_hash {
            storage.hash_collect(key).await
          } else {
            storage.sorted_set_collect(key).await
          };
          match res {
            Ok((GarnetStatus::WrongType, _)) => wrong_type = true,
            Ok(_) => {}
            Err(_) => {
              write_error_raw(&mut output, RESP_ERR_SLOW_PATH_IO);
              return output;
            }
          }
        }
        if wrong_type {
          write_error_raw(&mut output, RESP_ERR_WRONG_TYPE);
        } else {
          output.extend_from_slice(RESP_OK);
        }
      }
      // ---- RangeIndex 族（resp_server_session_range_index.rs：解析校验与
      // 执行一体在异步段闭环；ri 门取存储域共享范围索引管理器）
      C::Ricreate
      | C::Riset
      | C::Riget
      | C::Ridel
      | C::Riscan
      | C::Rirange
      | C::Riexists
      | C::Riconfig
      | C::Rimetrics => {
        let ri = Some(self.session.store.range_index.as_ref());
        // 解析校验 + 执行一体闭环；应答直写 output（错误映射在处理器内）
        let _ = match cmd {
          C::Ricreate => ri_cmds::network_ricreate(&refs, ri, &self.session, &mut output).await,
          C::Riset => ri_cmds::network_riset(&refs, ri, &self.session, &mut output).await,
          C::Riget => ri_cmds::network_riget(&refs, ri, &self.session, &mut output).await,
          C::Ridel => ri_cmds::network_ridel(&refs, ri, &self.session, &mut output).await,
          C::Riscan => ri_cmds::network_riscan(&refs, ri, &self.session, &mut output).await,
          C::Rirange => ri_cmds::network_rirange(&refs, ri, &self.session, &mut output).await,
          C::Riexists => ri_cmds::network_riexists(&refs, ri, &self.session, &mut output).await,
          C::Riconfig => ri_cmds::network_riconfig(&refs, ri, &self.session, &mut output).await,
          _ => ri_cmds::network_rimetrics(&refs, ri, &self.session, &mut output).await,
        };
      }
      // 未接入慢路径分派表的命令：写明错误，绝不静默
      _ => write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED),
    }
    output
  }
}

impl<D: Device> StoreGarnetApi<D> {
  /// 检查点族慢路径执行段（AdminCommands.cs:NetworkSAVE/NetworkBGSAVE/
  /// NetworkLASTSAVE；rust 映射为 WedbStore::create_checkpoint（wcpr 通道）；
  /// LASTSAVE 为纯读取）
  ///
  /// SAVE 同步等待检查点完成（C# NetworkSAVE：AsyncUtils.BlockingWait）；
  /// BGSAVE 对标 SingleDatabaseManager.cs:TakeCheckpointAsync(background=true)
  /// ——检查点后台任务承接（LastSaveTime 在任务完成时落定），命令即回
  /// "Background saving started"
  ///
  /// 独立方法承载以脱离调用方的批处理纪元保护区（wcpr 契约：
  /// CheckpointWhileEpochProtected fail-fast）
  async fn checkpoint_command_slow(&self, cmd: RespCommand) -> Vec<u8> {
    use RespCommand as C;
    use wresp::RespVecExt;

    let mut output = Vec::new();
    match cmd {
      C::Lastsave => {
        let secs = match &self.checkpoint {
          Some(ctx) => ctx.last_save_ms.load(Ordering::Acquire) / 1000,
          None => 0,
        };
        output.write_resp_int(secs);
      }
      C::Save | C::Bgsave => {
        let Some(ctx) = &self.checkpoint else {
          write_error_raw(&mut output, RESP_ERR_CHECKPOINT_UNWIRED);
          return output;
        };
        if matches!(cmd, C::Bgsave) {
          // C# background=true：TryPauseCheckpoints 通过后即返回 true，
          // 检查点任务后台跑完（ResumeCheckpoints / LastSaveTime 在其
          // finally/尾部）；rust 以 detach 的 compio 任务承接
          let store = Arc::clone(&self.session.store);
          let dir = ctx.dir.clone();
          let last_save_ms = Arc::clone(&ctx.last_save_ms);
          compio::runtime::spawn(async move {
            if let Err(e) = store
              .create_checkpoint(&dir, wcpr::CheckpointType::FoldOver)
              .await
            {
              log::warn!("background checkpoint failed: {e}");
              return;
            }
            last_save_ms.store(now_ms() as i64, Ordering::Release);
          })
          .detach();
          // C# BGSAVE 成功应答文案
          output.write_resp_simple_string("Background saving started");
        } else {
          match self
            .session
            .store
            .create_checkpoint(&ctx.dir, wcpr::CheckpointType::FoldOver)
            .await
          {
            Ok(_) => {
              ctx.last_save_ms.store(now_ms() as i64, Ordering::Release);
              output.extend_from_slice(RESP_OK);
            }
            // C# SAVE 失败回并发检查点错误
            Err(e) => {
              log::warn!("checkpoint failed: {e}");
              write_error_raw(&mut output, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
            }
          }
        }
      }
      _ => unreachable!("checkpoint_command_slow 仅承接检查点族"),
    }
    output
  }
}

/// HCOLLECT / ZCOLLECT `*` 全库收集的信封域键清单
///
/// 对标 C# ObjectCollect 的 DbScan(typeObject) 段：单趟 hlog 扫描收
/// 当前库指定类型标签的信封对象键（去重；墓碑、Meta 打平键不入——后者
/// 过期字段由 wkv 版本栅栏惰性失效，无内存可回收）
async fn collect_envelope_keys<'a, D: wdev::Device>(
  storage: &StorageSession<'a, D>,
  object_type: wval::GarnetObjectType,
) -> wkv::Result<Vec<Vec<u8>>> {
  use gxhash::HashSet as GxHashSet;

  let tag = object_type.as_u8();
  let prefix = storage.batch.session_prefix();
  let prefix_slice = prefix.as_slice();
  let mut keys: GxHashSet<Vec<u8>> = GxHashSet::default();
  storage
    .batch
    .store
    .hlog()
    .scan(
      storage.batch.store.begin_address(),
      storage.batch.store.tail_address(),
      |_addr, rec| {
        let key = rec.key();
        if let Some(rest) = key.strip_prefix(prefix_slice)
          && !rec.is_tombstone()
          && let Some((&key_tag, user_key)) = rest.split_first()
          && key_tag == wval::KeyTag::String.as_u8()
          && rec.value().first() == Some(&tag)
        {
          keys.insert(user_key.to_vec());
        }
        Ok(true)
      },
    )
    .await?;
  Ok(keys.into_iter().collect())
}

/// 分派表 · fast 段（C# ProcessBasicCommands + ProcessArrayCommands 的存储命令
/// switch：字符串族、多键数组族、键管理族与对象集合族；@fast 语义，不置位
/// containsSlowCommand）。未命中命令落入 [`dispatch_slow`]（C# 链式回退）
fn dispatch<D: Device>(
  session: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  batch: &BatchStoreSession<'_, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  use RespCommand as C;
  match cmd {
    // ---- 字符串族（ProcessBasicCommands switch 前段）
    C::Get => session.network_get(args, batch, output),
    C::Getex => session.network_getex(args, batch, output),
    C::Set => session.network_set(args, batch, output),
    C::Setex => session.network_setex(args, batch, output),
    C::Psetex => session.network_psetex(args, batch, output),
    C::Setnx => session.network_setnx(args, batch, output),
    C::Setexnx => session.network_setexnx(args, batch, output),
    C::Getset => session.network_getset(args, batch, output),
    C::Setrange => session.network_set_range(args, batch, output),
    C::Getrange | C::Substr => session.network_get_range(args, batch, output),
    C::Append => session.network_append(args, batch, output),
    C::Strlen => session.network_strlen(args, batch, output),
    C::Incr => session.network_increment(IncrCmd::Incr, args, batch, output),
    C::Decr => session.network_increment(IncrCmd::Decr, args, batch, output),
    C::Incrby => session.network_increment(IncrCmd::IncrBy, args, batch, output),
    C::Decrby => session.network_increment(IncrCmd::DecrBy, args, batch, output),
    C::Incrbyfloat => session.network_increment_by_float(args, batch, output),

    // ---- 多键数组族（ArrayCommands.cs:ProcessArrayCommands 前段）
    C::Del | C::Unlink => session.network_del(args, batch, output),
    C::Mget => session.network_mget(args, batch, output),
    C::Mset => session.network_mset(args, batch, output),
    C::Msetnx => session.network_msetnx(args, batch, output),

    // ---- 键管理族（KeyAdminCommands.cs）
    C::Exists => session.network_exists(args, batch, output),
    C::Expire => session.network_expire(ExpireCmd::Expire, args, batch, output),
    C::Pexpire => session.network_expire(ExpireCmd::Pexpire, args, batch, output),
    C::Expireat => session.network_expire(ExpireCmd::Expireat, args, batch, output),
    C::Pexpireat => session.network_expire(ExpireCmd::Pexpireat, args, batch, output),
    C::Persist => session.network_persist(args, batch, output),
    C::Ttl => session.network_ttl(TtlCmd::Ttl, args, batch, output),
    C::Pttl => session.network_ttl(TtlCmd::Pttl, args, batch, output),
    C::Expiretime => session.network_expiretime(ExpireTimeCmd::Expiretime, args, batch, output),
    C::Pexpiretime => session.network_expiretime(ExpireTimeCmd::Pexpiretime, args, batch, output),
    C::Getdel => session.network_getdel(args, batch, output),
    C::Rename => session.network_rename(args, batch, output),
    C::Renamenx => session.network_renamenx(args, batch, output),
    C::Dump => session.network_dump(args, batch, output),
    C::Restore => session.network_restore(args, batch, output),

    // ---- 库切换族（ArrayCommands.cs:NetworkSELECT / NetworkSWAPDB）
    C::Select => session.network_select(args, batch, output),
    C::Swapdb => session.network_swapdb(args, output),

    // ---- 哈希族（Objects/HashCommands.cs）
    C::Hset => session.hash_set(args, batch, output),
    C::Hsetnx => session.hash_set_nx(args, batch, output),
    C::Hmset => session.hash_set_map(args, batch, output),
    C::Hget => session.hash_get(args, batch, output),
    C::Hgetall => session.hash_get_all(args, batch, output),
    C::Hmget => session.hash_get_multiple(args, batch, output),
    C::Hlen => session.hash_length(args, batch, output),
    C::Hdel => session.hash_delete(args, batch, output),
    C::Hexists => session.hash_exists(args, batch, output),
    C::Hkeys => session.hash_keys(args, batch, output, true),
    C::Hvals => session.hash_vals(args, batch, output),
    C::Hrandfield => session.hash_random_field(args, batch, output),
    C::Hstrlen => session.hash_str_length(args, batch, output),
    C::Hincrby => session.hash_increment(args, batch, output, false),
    C::Hincrbyfloat => session.hash_increment(args, batch, output, true),
    C::Hexpire => session.hash_expire(args, batch, output, false, false),
    C::Hpexpire => session.hash_expire(args, batch, output, true, false),
    C::Hexpireat => session.hash_expire(args, batch, output, false, true),
    C::Hpexpireat => session.hash_expire(args, batch, output, true, true),
    C::Httl => session.hash_time_to_live(args, batch, output, false, false),
    C::Hpttl => session.hash_time_to_live(args, batch, output, true, false),
    C::Hexpiretime => session.hash_time_to_live(args, batch, output, false, true),
    C::Hpexpiretime => session.hash_time_to_live(args, batch, output, true, true),
    C::Hpersist => session.hash_persist(args, batch, output),
    C::Hscan => session.network_hscan(args, batch, output),

    // ---- 集合族（Objects/SetCommands.cs）
    C::Sadd => session.set_add(args, batch, output),
    C::Srem => session.set_remove(args, batch, output),
    C::Scard => session.set_length(args, batch, output),
    C::Smembers => session.set_members(args, batch, output),
    C::Sismember => session.set_is_member(args, batch, output),
    C::Smismember => session.set_multi_is_member(args, batch, output),
    C::Spop => session.set_pop(args, batch, output),
    C::Srandmember => session.set_random_member(args, batch, output),
    C::Smove => session.set_move(args, batch, output),
    C::Sinter => session.set_intersect(args, batch, output),
    C::Sinterstore => session.set_intersect_store(args, batch, output),
    C::Sintercard => session.set_intersect_length(args, batch, output),
    C::Sunion => session.set_union(args, batch, output),
    C::Sunionstore => session.set_union_store(args, batch, output),
    C::Sdiff => session.set_diff(args, batch, output),
    C::Sdiffstore => session.set_diff_store(args, batch, output),
    C::Sscan => session.network_sscan(args, batch, output),

    // ---- 有序集合族（Objects/SortedSetCommands.cs）
    C::Zadd => session.sorted_set_add(args, batch, output),
    C::Zscore => session.sorted_set_score(args, batch, output),
    C::Zrem => session.sorted_set_remove(args, batch, output),
    C::Zcard => session.sorted_set_length(args, batch, output),
    C::Zpopmin => session.sorted_set_pop(args, batch, output, true),
    C::Zpopmax => session.sorted_set_pop(args, batch, output, false),
    C::Zrange => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::NONE),
    C::Zrevrange => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::REVERSE),
    C::Zrangebylex => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::BY_LEX),
    C::Zrevrangebylex => session.sorted_set_range(
      args,
      batch,
      output,
      SortedSetRangeOpts::BY_LEX.union(SortedSetRangeOpts::REVERSE),
    ),
    C::Zrangebyscore => session.sorted_set_range(args, batch, output, SortedSetRangeOpts::BY_SCORE),
    C::Zrevrangebyscore => session.sorted_set_range(
      args,
      batch,
      output,
      SortedSetRangeOpts::BY_SCORE.union(SortedSetRangeOpts::REVERSE),
    ),
    C::Zrangestore => session.sorted_set_range_store(args, batch, output),
    C::Zmscore => session.sorted_set_scores(args, batch, output),
    C::Zmpop => session.sorted_set_m_pop(args, batch, output),
    C::Zcount => session.sorted_set_count(args, batch, output),
    C::Zlexcount => session.sorted_set_length_by_value(args, batch, output),
    C::Zincrby => session.sorted_set_increment(args, batch, output),
    C::Zrank => session.sorted_set_rank(args, batch, output, true),
    C::Zrevrank => session.sorted_set_rank(args, batch, output, false),
    C::Zremrangebyrank => {
      session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Rank)
    }
    C::Zremrangebyscore => {
      session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Score)
    }
    C::Zremrangebylex => session.sorted_set_remove_range(args, batch, output, RemoveRangeKind::Lex),
    C::Zrandmember => session.sorted_set_random_member(args, batch, output),
    C::Zdiff => session.sorted_set_difference(args, batch, output),
    C::Zdiffstore => session.sorted_set_difference_store(args, batch, output),
    C::Zinter => session.sorted_set_intersect(args, batch, output),
    C::Zintercard => session.sorted_set_intersect_length(args, batch, output),
    C::Zinterstore => session.sorted_set_intersect_store(args, batch, output),
    C::Zunion => session.sorted_set_union(args, batch, output),
    C::Zunionstore => session.sorted_set_union_store(args, batch, output),
    C::Bzpopmin => session.sorted_set_blocking_pop(args, batch, output, true),
    C::Bzpopmax => session.sorted_set_blocking_pop(args, batch, output, false),
    C::Bzmpop => session.sorted_set_blocking_m_pop(args, batch, output),
    C::Zexpire => session.sorted_set_expire(args, batch, output, false, false),
    C::Zpexpire => session.sorted_set_expire(args, batch, output, true, false),
    C::Zexpireat => session.sorted_set_expire(args, batch, output, false, true),
    C::Zpexpireat => session.sorted_set_expire(args, batch, output, true, true),
    C::Zttl => session.sorted_set_time_to_live(args, batch, output, false, false),
    C::Zpttl => session.sorted_set_time_to_live(args, batch, output, true, false),
    C::Zexpiretime => session.sorted_set_time_to_live(args, batch, output, false, true),
    C::Zpexpiretime => session.sorted_set_time_to_live(args, batch, output, true, true),
    C::Zpersist => session.sorted_set_persist(args, batch, output),
    C::Zscan => session.network_zscan(args, batch, output),

    // ---- 列表族（Objects/ListCommands.cs）
    C::Lpush => session.list_push(args, batch, output, true),
    C::Rpush => session.list_push(args, batch, output, false),
    C::Lpushx => session.list_push_x(args, batch, output, true),
    C::Rpushx => session.list_push_x(args, batch, output, false),
    C::Lpop => session.list_pop(args, batch, output, true),
    C::Rpop => session.list_pop(args, batch, output, false),
    C::Lpos => session.list_position(args, batch, output),
    C::Lmpop => session.list_pop_multiple(args, batch, output),
    C::Blpop => session.list_blocking_pop(args, batch, output, true),
    C::Brpop => session.list_blocking_pop(args, batch, output, false),
    C::Blmove => session.list_blocking_move(args, batch, output),
    C::Brpoplpush => session.list_blocking_pop_push(args, batch, output),
    C::Llen => session.list_length(args, batch, output),
    C::Ltrim => session.list_trim(args, batch, output),
    C::Lrange => session.list_range(args, batch, output),
    C::Lindex => session.list_index(args, batch, output),
    C::Linsert => session.list_insert(args, batch, output),
    C::Lrem => session.list_remove(args, batch, output),
    C::Lmove => session.list_move(args, batch, output),
    C::Rpoplpush => session.list_right_pop_left_push(args, batch, output),
    C::Lset => session.list_set(args, batch, output),
    C::Blmpop => session.list_blocking_pop_multiple(args, batch, output),

    // ---- 位图族（Bitmap/BitmapCommands.cs）
    C::Setbit => session.network_string_set_bit(args, batch, output),
    C::Getbit => session.network_string_get_bit(args, batch, output),
    C::Bitcount => session.network_string_bit_count(args, batch, output),
    C::Bitpos => session.network_string_bit_position(args, batch, output),
    C::BitopAnd => session.network_string_bit_operation(BitmapOperation::And, args, batch, output),
    C::BitopOr => session.network_string_bit_operation(BitmapOperation::Or, args, batch, output),
    C::BitopXor => session.network_string_bit_operation(BitmapOperation::Xor, args, batch, output),
    C::BitopNot => session.network_string_bit_operation(BitmapOperation::Not, args, batch, output),
    C::BitopDiff => {
      session.network_string_bit_operation(BitmapOperation::Diff, args, batch, output)
    }
    C::Bitfield => session.string_bit_field(args, batch, output),
    C::BitfieldRo => session.string_bit_field_read_only(args, batch, output),

    // ---- HyperLogLog 族（Objects/HyperLogLogCommands.cs）
    C::Pfadd => session.hyper_log_log_add(args, batch, output),
    C::Pfcount => session.hyper_log_log_length(args, batch, output),
    C::Pfmerge => session.hyper_log_log_merge(args, batch, output),

    // ---- 地理族（Objects/SortedSetGeoCommands.cs）
    C::Geoadd => session.geo_add(args, batch, output),
    C::Geodist => session.geo_commands(args, batch, output, SortedSetOperation::Geodist),
    C::Geohash => session.geo_commands(args, batch, output, SortedSetOperation::Geohash),
    C::Geopos => session.geo_commands(args, batch, output, SortedSetOperation::Geopos),
    C::Georadius => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadius)
    }
    C::GeoradiusRo => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadiusRo)
    }
    C::Georadiusbymember => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoRadiusByMember)
    }
    C::GeoradiusbymemberRo => session.geo_search_commands(
      args,
      batch,
      output,
      GeoSearchCommandKind::GeoRadiusByMemberRo,
    ),
    C::Geosearch => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoSearch)
    }
    C::Geosearchstore => {
      session.geo_search_commands(args, batch, output, GeoSearchCommandKind::GeoSearchStore)
    }

    // fast 段未命中 → 慢段（C# ProcessOtherCommands / ProcessAdminCommands 链式回退）
    _ => dispatch_slow(session, cmd, args, batch, output),
  }
}

/// 分派表 · slow 段（C# ProcessOtherCommands + ProcessAdminCommands 的存储命令
/// switch：TYPE/LCS/DBSIZE/KEYS/SCAN/清库族/CONFIG/对象收集/ETAG/OBJECT/
/// MEMORY/RI 族）。入口置位 containsSlowCommand（NET_RS 直方图分桶，C# 同款
/// 首行语义——本段皆 @slow，单点定义杜绝逐臂重复）
fn dispatch_slow<D: Device>(
  session: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  batch: &BatchStoreSession<'_, D>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  use RespCommand as C;

  session.contains_slow_command = true;
  match cmd {
    // ---- 慢命令族（ProcessOtherCommands：TYPE / LCS / DBSIZE / KEYS / SCAN）
    C::Type => session.network_type(args, batch, output),
    C::Lcs => session.network_lcs(args, batch, output),
    C::Dbsize => session.network_dbsize(args, output),
    C::Keys => session.network_keys(args, output),
    C::Scan => session.network_scan(args, output),
    C::Coscan => session.network_coscan(args, batch, output),

    // ---- 清库族（ProcessOtherCommands：选项校验同步承接，清库降级异步闭环）
    C::Flushdb => session.network_flushdb(args, output),
    C::Flushall => session.network_flushall(args, output),

    // ---- 会话族（无存储写，仅经统一注入面保持单一路径）
    C::Hello => session.network_hello(args, batch, output),

    // ---- 配置族（ServerConfig.cs:NetworkCONFIG_GET/SET/REWRITE，经
    // 会话共享 runtime_config 实例：CONFIG SET 即时全服务器生效）
    C::ConfigGet => session.network_config_get(args, output),
    C::ConfigSet => session.network_config_set(args, output),
    C::ConfigRewrite => session.network_config_rewrite(args, output),

    // ---- 对象收集族（AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT：
    // 显式键清单逐键 RMW；`*` 全库扫描降级异步闭环）
    C::Hcollect => session.network_hcollect(args, batch, output),
    C::Zcollect => session.network_zcollect(args, batch, output),

    // ---- COMMAND 族（ProcessOtherCommands：命令元数据自省，无存储面）
    C::CommandCount => session.network_command_count(args, batch, output),
    C::CommandDocs => session.network_command_docs(args, batch, output),
    C::CommandInfo => session.network_command_info(args, batch, output),
    C::CommandGetkeys => session.network_command_getkeys(args, batch, output),
    C::CommandGetkeysandflags => session.network_command_getkeysandflags(args, batch, output),

    // ---- MEMORY / OBJECT 族（ProcessOtherCommands：BasicCommands.cs）
    C::MemoryUsage => session.network_memory_usage(args, batch, output),
    C::ObjectHelp => session.network_objecthelp(args, batch, output),
    C::ObjectEncoding => session.network_object(ObjectSubCmd::Encoding, args, batch, output),
    C::ObjectFreq => session.network_object(ObjectSubCmd::Freq, args, batch, output),
    C::ObjectIdletime => session.network_object(ObjectSubCmd::Idletime, args, batch, output),
    C::ObjectRefcount => session.network_object(ObjectSubCmd::Refcount, args, batch, output),

    // ---- Etag 族（ProcessOtherCommands：BasicEtagCommands.cs）
    C::Getwithetag => session.network_getwithetag(args, batch, output),
    C::Getifnotmatch => session.network_getifnotmatch(args, batch, output),
    C::Setifmatch => session.network_setifmatch(args, batch, output),
    C::Setifgreater => session.network_setifgreater(args, batch, output),
    C::Setwithetag => session.network_setwithetag(args, batch, output),
    C::Delifgreater => session.network_delifgreater(args, batch, output),

    // ---- RangeIndex 族（ProcessOtherCommands：RespServerSessionRangeIndex.cs；
    // wkv 范围索引走 compio 异步存储路径，同步段直接降级 SlowWait 异步闭环）
    C::Ricreate
    | C::Riset
    | C::Riget
    | C::Ridel
    | C::Riscan
    | C::Rirange
    | C::Riexists
    | C::Riconfig
    | C::Rimetrics => Ok(false),

    // 未接入分派表的命令：按 C# ProcessAdminCommands 尾部兜底明确报错，
    // 绝不静默吞命令
    _ => {
      write_error_raw(output, RESP_ERR_GENERIC_UNK_CMD);
      Ok(true)
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::tempdir;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wresp::RespCommand;
  use wvector::Callbacks;

  use super::*;
  use crate::resp::{
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  };

  #[test]
  fn test_store_garnet_api_vector_dispatch() {
    let dir = tempdir().unwrap();
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();

    let v_session = Arc::new(store.new_session().unwrap());
    let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(v_session)));
    let vm = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      callbacks,
    ));
    let api = StoreGarnetApi::new(session).with_vector_manager(vm);

    let mut resp_session = RespServerSession::new(1, RespServerSessionOptions::default());

    // 1. 执行 VADD
    let v1 = [0u8; 8];
    api.exec(
      &mut resp_session,
      RespCommand::Vadd,
      &[b"mykey", b"FP32", &v1, b"ele1"],
    );
    assert_eq!(resp_session.output, b":1\r\n");

    // 2. 执行 VCARD
    resp_session.output.clear();
    api.exec(&mut resp_session, RespCommand::Vcard, &[b"mykey"]);
    assert_eq!(resp_session.output, b":1\r\n");

    // 3. 执行 VDIM
    resp_session.output.clear();
    api.exec(&mut resp_session, RespCommand::Vdim, &[b"mykey"]);
    assert_eq!(resp_session.output, b":2\r\n");
  }

  #[test]
  fn test_etag_family_dispatch_via_store() {
    // Etag 族接入分派表（C# ProcessOtherCommands 段）：SETWITHETAG →
    // GETWITHETAG → DELIFGREATER 全链路真存储读写
    let dir = tempdir().unwrap();
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("e.db")).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    let api = StoreGarnetApi::new(session);
    let mut s = RespServerSession::new(1, RespServerSessionOptions::default());

    // SETWITHETAG key val → +OK
    api.exec(&mut s, RespCommand::Setwithetag, &[b"k", b"v1"]);
    assert_eq!(s.output, b"+OK\r\n");
    s.output.clear();

    // GETWITHETAG key → [val, etag] 双元素数组
    api.exec(&mut s, RespCommand::Getwithetag, &[b"k"]);
    assert_eq!(s.output, b"*2\r\n$2\r\nv1\r\n:0\r\n");
    s.output.clear();

    // DELIFGREATER key 5 → 删除计数 1
    api.exec(&mut s, RespCommand::Delifgreater, &[b"k", b"5"]);
    assert_eq!(s.output, b":1\r\n");
  }

  #[test]
  fn test_ri_commands_suspend_to_slow_path() {
    // RI 族经 dispatch_slow 返回 Ok(false)：同步段挂起 SlowWait 停止本批
    // 消费，网络泵 await 闭环（compio 异步存储路径的统一承接形态）
    let dir = tempdir().unwrap();
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("ri.db")).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let _session = store.new_session().unwrap();
    let api: GarnetApi = StoreGarnetApi::new(store.new_session().unwrap()).into();
    let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
    s.set_garnet_api(api.clone());

    api.exec(&mut s, RespCommand::Riget, &[b"idx", b"f"]);
    // 应答不残留（本次无输出），挂起慢路径执行体由网络泵驱动
    assert!(s.output.is_empty());
    assert!(s.pending_slow.is_some());
  }
}

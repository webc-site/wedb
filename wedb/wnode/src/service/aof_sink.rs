use super::*;

/// 空参数命令镜像条目的参数区预置（TTL/ETag/过期清理三臂参数恒空）
const EMPTY_ARGS: &[&[u8]] = &[];

/// 物理键编码（栈上分配避免堆分配，TaggedKeyBuf 最多内联 62 字节）
///
/// 唯一入账键编码器：ns/db 取事件携带的会话真值（杜绝伪造 0,0 致跨租界
/// 落错库），tag 按记录实际驻留域选择——TTL/ETag/字符串删为 String 域，
/// RI 族（含就地升阶流）为 Meta 域，与 `doc/zh/db.md` 刚性前缀同构。
#[inline]
fn physical_key(ns: u64, db: u64, tag: KeyTag, user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(ns, db, tag, user_key)
}

impl AofSinkContext {
  /// 原始整值/墓碑条目入队统一出口（失败上抛拒绝该命令：主存写入已生效，AOF
  /// 缺条目即主从发散，对标 C# 会话转 GarnetException 失败回客户端）
  ///
  /// `(version, session_id)` 元组经 [`AofWriteContext::new`] 单点落地：数据条目
  /// 帧头携产生会话 id，与事务标记同键（对标 C# `Session.ID` 数据/标记同键）。
  #[inline]
  fn raw(
    &self,
    op: AofEntryType,
    ver: i64,
    sid: i32,
    key: &[u8],
    value: &[u8],
  ) -> wkv::Result<i64> {
    self
      .aof
      .enqueue_raw(op, (ver, sid), key, value, &EMPTY_REPLAY_INPUT_BYTES)
      .map_err(|e| Error::AofEnqueue(e.to_string()))
  }

  /// 切片零分配/低分配命令镜像条目入队（value 恒空，`(version, session_id)`
  /// 元组同 [`Self::raw`]）
  #[inline]
  fn cmd<T: AsRef<[u8]>>(
    &self,
    op: AofEntryType,
    ver: i64,
    sid: i32,
    key: &[u8],
    input: &ReplayInputSlice<'_, T>,
  ) -> wkv::Result<i64> {
    self
      .aof
      .enqueue_slices(op, (ver, sid), key, &[], input)
      .map_err(|e| Error::AofEnqueue(e.to_string()))
  }
}

pub(super) fn on_aof_store_event(
  ctx: &AofSinkContext,
  ver: i64,
  sid: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write {
      key,
      val,
      tombstone,
    } => {
      let tag = NamespaceDbCodec::decode_tag(key);
      if tag == Some(KeyTag::ObjectEnvelope) {
        // 信封墓碑照常入队；非墓碑整值写由 envelope upsert 承接
        if !tombstone {
          return Ok(());
        }
        ctx.raw(AofEntryType::StoreDelete, ver, sid, key, val)?;
        return Ok(());
      }
      // ACL 用户规则旁路标签（0x0D）：与 String 域同为整值写/墓碑删，
      // 经 StoreUpsert/StoreDelete 条目镜像（从库 AOF 回放据此重建用户表）
      //
      // DbMeta 系统元数据（0x0E）：映射体系镜像通道（doc/zh/db.md「物理日志
      // 复制与 Checkpoint 直接镜像主库的 KeyTag::DbMeta 与数据记录；从库完全
      // 继承主库的映射体系，不进行本地二次映射」）——主库全部换号批/首映射/
      // SWAPDB 记录与 GC 墓碑注销经此镜像，从库回放面交
      // WedbStore::apply_dbmeta_record / apply_dbmeta_tombstone 应用，换号虚号
      // 主从同源；条目即完整记录（键载荷 + 定长值），无需第二套映射同步机制
      if tag != Some(KeyTag::String) && tag != Some(KeyTag::Acl) && tag != Some(KeyTag::DbMeta) {
        return Ok(());
      }
      let op = if tombstone {
        AofEntryType::StoreDelete
      } else {
        AofEntryType::StoreUpsert
      };
      ctx.raw(op, ver, sid, key, val)?;
    }
    StoreEvent::TtlWrite {
      ns,
      db,
      key,
      expire_at,
    } => {
      // arg1 携主端线性化后的绝对 .NET Ticks 原值（对标 C# WriteLogRMW 的
      // ExpirationWithOption.Word 原样入队：libs/server/Storage/Functions/
      // UnifiedStore/PrivateMethods.cs:106-118，与 EtagWrite 携 etag 原值
      // 同型）。原 Unix 毫秒中转恒向下截断、重放端 TTL 系统性前移至多 1ms
      // 的损精分叉就此消灭；条目携原值 + 重放端 wkv `expire_at` 会话入口恒等
      // 裸写（粗化唯 EXPIRE 族命令边界单点）两事齐备后，主从/恢复后 PTTL
      // 与主端逐位一致对全部 TTL 族恒成立（旧会话入口头部粗化下 SET 族末位
      // 偏 ≤15 ticks 形已收口，见 doc/zh/deviations.md §143）
      let (cmd, arg1) = match expire_at {
        Some(ticks) => (RespCommand::Pexpireat, ticks),
        None => (RespCommand::Persist, 0),
      };
      let input = ReplayInputSlice::new(cmd, EMPTY_ARGS)
        .with_deterministic()
        .with_args_num(arg1, 0, 0);
      let pkey = physical_key(ns, db, KeyTag::String, key);
      ctx.cmd(AofEntryType::StoreRMW, ver, sid, &pkey, &input)?;
    }
    StoreEvent::EtagWrite { ns, db, key, etag } => {
      let input = ReplayInputSlice::new(RespCommand::Setwithetag, EMPTY_ARGS)
        .with_deterministic()
        .with_args_num(etag.unwrap_or(NO_ETAG), 0, 0);
      let pkey = physical_key(ns, db, KeyTag::String, key);
      ctx.cmd(AofEntryType::StoreRMW, ver, sid, &pkey, &input)?;
    }
    StoreEvent::ObjectRmw(notif) => {
      let input = ReplayInputSlice::new(RespCommand::None, notif.args)
        .with_deterministic()
        .with_sub_id(notif.op_code)
        .with_obj_type(notif.obj_type)
        .with_args_num(notif.arg1 as i64, notif.arg2 as i64, 0);
      ctx.cmd(AofEntryType::ObjectStoreRMW, ver, sid, notif.key, &input)?;
    }
    // 分层稳态写臂的确定性命令镜像单点（升阶流只镜像升阶时点内容，本分支
    // 承接其后 HSET 族 / SADD / ZADD 族 / LPUSH 族的树内原生写面）：入账
    // ObjectStoreRMW 条目、物理键取 Meta 域物化键（与升阶流同域）。条目形态
    // 与信封 ObjectRmw 同一 ReplayInput 布局（cmd=None + sub_id=操作码 +
    // obj_type + arg1/arg2 压缩字 + args 原文），重放端 object_store_rmw 先经
    // load_collection_stub 探分层态路由分层臂逐条重放收敛——镜像的是 RESP
    // 命令语义而非最终值。对标 RI.SET 每条字段写入的 RangeIndexWrite 逐条
    // 镜像先例（libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:
    // ReplicateRangeIndexSet）与 C# 对象域三 RMW 钩子全量 NeedAofLog 的
    // WriteLogRMW（libs/server/Storage/Functions/ObjectStore/
    // PrivateMethods.cs:WriteLogRMW）
    StoreEvent::TieredCollectionWrite(notif) => {
      let input = ReplayInputSlice::new(RespCommand::None, notif.args)
        .with_deterministic()
        .with_sub_id(notif.op_code)
        .with_obj_type(notif.obj_type)
        .with_args_num(notif.arg1 as i64, notif.arg2 as i64, 0);
      let pkey = physical_key(notif.ns, notif.db, KeyTag::Meta, notif.key);
      ctx.cmd(AofEntryType::ObjectStoreRMW, ver, sid, &pkey, &input)?;
    }
    StoreEvent::EnvelopeUpsert { key, val } => {
      ctx.raw(AofEntryType::ObjectStoreUpsert, ver, sid, key, val)?;
    }
    // RI.SET/RI.DEL 的 AOF 记录单点：C# 经 functionsState 显式调用
    // libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexSet
    // 与 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:ReplicateRangeIndexDel
    // 两对口，rust 由本 StoreEvent 通道一处承接
    StoreEvent::RangeIndexWrite {
      ns,
      db,
      key,
      field,
      val,
      delete,
    } => {
      let (cmd, args): (RespCommand, &[&[u8]]) = if delete {
        (RespCommand::Ridel, &[field])
      } else {
        (RespCommand::Riset, &[field, val])
      };
      let input = ReplayInputSlice::new(cmd, args).with_deterministic();
      let pkey = physical_key(ns, db, KeyTag::Meta, key);
      ctx.cmd(AofEntryType::StoreRMW, ver, sid, &pkey, &input)?;
    }
    StoreEvent::RangeIndexCreate {
      ns,
      db,
      key,
      backend,
      tuning,
    } => {
      let stub = RangeIndexStub::from_tuning(0, &tuning, *backend);
      let mut stub_bytes = [0u8; RANGE_INDEX_STUB_SIZE];
      if let Err(e) = stub.encode_into(&mut stub_bytes) {
        log::error!("RI.CREATE AOF 存根编码失败: {e}");
        return Ok(());
      }
      let stub_args = [&stub_bytes[..]];
      let input = ReplayInputSlice::new(RespCommand::Ricreate, &stub_args).with_deterministic();
      let pkey = physical_key(ns, db, KeyTag::Meta, key);
      ctx.cmd(AofEntryType::StoreRMW, ver, sid, &pkey, &input)?;
    }
    StoreEvent::RangeIndexDrop { ns, db, key } => {
      let pkey = physical_key(ns, db, KeyTag::Meta, key);
      ctx.raw(AofEntryType::StoreDelete, ver, sid, &pkey, &[])?;
    }
    StoreEvent::RangeIndexStream {
      ns,
      db,
      key,
      obj_type,
      stub,
      file_path,
      replace,
      next_expiry,
    } => {
      // 集合就地升阶 / 分层重灌 / RENAME 的树数据通道：复用 RI 迁移流，把快照
      // 文件分块灌入 RangeIndexStreamChunk。条目键取 Meta 域物化键（回放真值=
      // 物理键，KeyContextGuard 据此直设事件携带的虚拟域 (vns, vdb) 并以用户键
      // 重组发布），与其余 RI 臂同走物理键编码器 `physical_key(.., Meta, ..)`
      // 单一口径。发布判别类型 obj_type 随首块 ReplayInput 携带，副本据此重建
      // MetaValue；replace 形态位随流块 arg1 标志携载，副本据此对既有树换入
      // 重放（分层重灌/RENAME 覆写不换旧树即发布失败）；成员 TTL 水位
      // next_expiry 随首块 ReplayInput.arg2 携载（arg1 已被首/尾/replace 位占
      // 满），副本发布元记录据此落真水位，杜绝假 MAX 令副本到期成员永不出账。
      let meta_key = physical_key(ns, db, KeyTag::Meta, key);
      ctx
        .ri
        .replicate_range_index_stream(
          RangeIndexStreamArgs {
            key: &meta_key,
            obj_type,
            stub: &stub,
            file_path,
            ctx: AofWriteContext::new(ver, sid),
            // 分块大小恒为默认 256KB（rust AOF 记录为显式入队，无 C# RMW
            // 自动记日志需哨兵抑制的调小演练场景）
            chunk_size: DEFAULT_MIGRATION_CHUNK_SIZE,
            replace,
            next_expiry,
          },
          Some(&ctx.aof),
        )
        .map_err(|e| Error::AofEnqueue(e.to_string()))?;
    }
    StoreEvent::TtlPurge {
      ns,
      db,
      key,
      expire_at,
    } => {
      let input = ReplayInputSlice::new(RespCommand::Delifexpim, EMPTY_ARGS)
        .with_flags((RespInputFlags::DETERMINISTIC | RespInputFlags::EXPIRED).bits())
        .with_args_num(expire_at, 0, 0);
      let pkey = physical_key(ns, db, KeyTag::String, key);
      ctx.cmd(AofEntryType::StoreRMW, ver, sid, &pkey, &input)?;
    }
  }
  Ok(())
}

impl<D: Device> NodeService<D> {
  /// 组装节点服务（AOF 域由调用方装配后注入——单物理日志域唯一实例，
  /// 经 [`single_log_aof`] 工厂构造，与库管理面共享同一物理日志）
  ///
  /// 按运行态 GC 配置补启内置后台循环（幂等）：`gc.enabled`（默认禁用，
  /// 对标 C# ExpiredKeyDeletionScanFrequencySecs = -1）为真时拉起，全关
  /// 即 no-op——显式传配置或热更新打开的嵌入式形态在此承接；服务端形态
  /// 按槽位的启动期注册在 StorageSessionProvider::get_session 惰性段，
  /// 升主恢复点 resume_primary_tasks → start_gc 承接重拉
  pub fn new(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> crate::Result<Self>
  where
    D: 'static,
  {
    Self::assemble(store, aof)
  }

  /// 公共装配体：注册全部 AOF 写监听端口并拉起会话。
  fn assemble(store: SharedStore<D>, aof: Arc<GarnetAppendOnlyFile>) -> crate::Result<Self>
  where
    D: 'static,
  {
    // 各端口版本源快照（原子指针捕获消除循环引用与多余开销；对标 C# storeWrapper.store.CurrentVersion）
    // 复制面单例先造后共享：AofSinkContext 与本服务同持一个 Arc（C#
    // StoreWrapper.rangeIndexManager 单实例对位，杜绝第二实例分叉重组状态）
    let ri = Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      store.range_index(),
    )));
    let ctx = Arc::new(AofSinkContext {
      aof: Arc::clone(&aof),
      ri: Arc::clone(&ri),
    });
    let event_sink = StoreEventSink::new(ctx, on_aof_store_event);
    if !store.set_event_sink(event_sink) {
      log::warn!("存储事件处理器重复注册");
    }
    store.start_gc();
    let session = store.new_session()?;
    Ok(Self { session, aof, ri })
  }

  /// 存储引擎会话
  #[inline]
  pub fn session(&self) -> &StoreSession<D> {
    &self.session
  }

  /// 存储引擎句柄
  #[inline]
  pub fn store(&self) -> &SharedStore<D> {
    &self.session.store
  }

  /// AOF 句柄（GarnetLog 拓扑 + WaofSublog 磁盘承载；与库管理面共享同一
  /// 物理日志域）
  #[inline]
  pub fn aof(&self) -> &Arc<GarnetAppendOnlyFile> {
    &self.aof
  }

  /// 范围索引 AOF 复制面单例句柄（升阶分块在线实例；宿主装配链经此转交
  /// 提供者，停机链单点收口）
  #[inline]
  pub fn ri(&self) -> &Arc<RangeIndexManagerReplication> {
    &self.ri
  }

  /// 将已提交 AOF 流按序重放到指定目标引擎会话（支持异构/跨设备会话）
  ///
  /// 统一走 [`AofProcessor`]（唯一重放分发）：构造目标 [`ReplayTarget`]
  /// 与范围索引重放面后交 [`AofRecover::single_log_recover`] 从提交位点扫描
  /// 至尾部（`scan_single_async_with` 跨环形窗口与历史磁盘段）。重放期间目标端
  /// AOF 监听端口暂停（对齐 C# 重放会话 `recordToAof: false`），重放写入
  /// 不镜像回写目标端 AOF。
  ///
  /// 版本基线：逐记录动态读取目标存储当前版本（C# 恢复时
  /// `storeWrapper.store.CurrentVersion`——由 checkpoint 恢复流程设置；
  /// 重放中 `header.storeVersion < 当前版本` 的记录按
  /// `ShouldSkipRecord` 跳过，未从 checkpoint 恢复时为 0 = 全量重放）。
  /// 重放中检查点推进版本后即刻生效，绝不缓存构造期快照。
  pub async fn replay_into_session<D2: Device>(
    &self,
    target_session: &StoreSession<D2>,
  ) -> crate::Result<u64> {
    let _pause = target_session.store.pause_aof_listeners();
    // 重放趟自持执行域会话绑定（与 replay_database_aof 同口径：向量族条目
    // 应用臂须见当前执行域会话；守卫随本收口段起落）
    let _vector_domain = ActiveVectorSessionGuard::bind(target_session);
    let batch = target_session.enter_batch();
    let storage = StorageSession::new(batch);
    let mut processor = AofProcessor::new(Arc::clone(&self.aof));
    let ri_manager = Arc::new(RangeIndexManagerReplication::new(Arc::clone(
      target_session.store.range_index(),
    )));
    processor.set_range_index_manager(ri_manager);
    let target = ReplayTarget::new(&storage, &target_session.store);
    let replayed = AofRecover::single_log_recover(&processor, &self.aof, 0, 0, -1, &target).await?;
    Ok(replayed)
  }
}

impl NodeService<SegmentedDevice> {
  /// 兼容入口：以 waof 日志直接装配单物理日志域（内部经 [`single_log_aof`]
  /// 工厂构造权威 AOF，供测试及轻量级单物理日志场景快速接入）。
  pub fn with_wal(
    store: SharedStore<SegmentedDevice>,
    wal: Arc<WalLog<SegmentedDevice>>,
  ) -> crate::Result<Self> {
    Self::with_options(&RuntimeServerOptions::default(), store, wal)
  }

  /// 基于运行时服务选项与存储引擎组装单机服务（AOF 门面全量投影入口）
  pub fn with_options(
    options: &RuntimeServerOptions,
    store: SharedStore<SegmentedDevice>,
    wal: Arc<WalLog<SegmentedDevice>>,
  ) -> crate::Result<Self> {
    // 调用方实参投影的 RuntimeServerOptions 全量透传 AOF 门面（C# EnableAOF
    // 装配段：完整 serverOptions 直入 GarnetAppendOnlyFile），禁以合成参数
    // 二次窄化投影——背压预算、复制读超时、子日志/回放拓扑皆自此取值
    let aof = single_log_aof(wal, options)?;
    Self::assemble(store, aof)
  }
}

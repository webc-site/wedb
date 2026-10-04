use super::*;

impl<F> SessionProviderFace for StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync,
{
  type Consumer = RespSessionConsumer;

  /// libs/server/Providers/GarnetProvider.cs:GetSession
  ///
  /// 模板方法：公共装配流程 + 差异钩子（存储会话创建失败拒绝建连）
  ///
  /// libs/server/Resp/Vector/VectorManager.cs:Initialize 合并承接：C# 宿主
  /// 启动序列调用 Initialize（置初始化位 + 建元数据槽 + StartQuantizationTasks），
  /// rust 对位为下方向量后台链随首会话惰性拉起（量化协程按配额分摊 + 清理
  /// 协程托管；元数据槽由构造期 Default 承接）。
  fn get_session(
    &self,
    _wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    // 在线引擎单点取用（GC 配置投影、对象收集执行域绑定与会话创建共用同一
    // 句柄；装配体内无 await 无换装窗口，一次取用即恒定）
    let store = self.store();
    // 向量后台链随首会话惰性拉起且整体受 Vector Set 预览开关门控（对标 C#
    // VectorManager.Initialize 的 !IsEnabled 早退与 StoreWrapper.cs:1054
    // StartReplicaTasks 条件装配；C# 构造器无条件 fire 的三个空转清理协程
    // 在此形态下不拉起——命令面全臂 disabled 无人投递，等价省协程）
    if self.vector_manager.is_enabled() {
      // 量化消费者协程随首个 worker runtime 惰性拉起（C# 宿主启动序列
      // VectorManager.StartQuantizationTasks(QuantizationTaskCount)；
      // 逐 worker 分摊直至配额用尽，避免单 runtime 独扛全部量化负载。
      // 句柄在拉起点内就地 detach 交执行器持有——compio JoinHandle Drop
      // 即 cancel，此处不得持有/丢弃句柄）
      let quota = self
        .vector_manager
        .quantization_task_count
        .load(Ordering::Relaxed)
        .max(1);
      if self.quantization_started.fetch_add(1, Ordering::Relaxed) < quota {
        self.vector_manager.start_quantization_tasks(1);
      }

      // 向量清理两常驻协程随首个 worker runtime 惰性拉起一次并托管（对标 C#
      // VectorManager 构造器启动 RunCleanupTaskAsync/RunRequestCleanupTaskAsync；
      // 第三条 RunRequestDropTaskAsync 的唯一生产点是主存记录逐出触发器，rust 无该
      // 触发面不落地，见 vector_manager_cleanup 模块头；Rust 构造在 compio runtime 外，
      // 故与量化协程同处首会话拉起，spawn 与本调用同 runtime。JoinHandle 收进
      // CleanupRuntime 托管，停机时由 dispose_vector_cleanup 收敛释放）。
      self.vector_manager.ensure_cleanup_tasks_started();
    }

    self.try_start_commit_task();
    self.try_start_aof_size_limit_task();
    self.try_start_index_auto_grow_task();
    // 启动期过期键删除任务注册段（C# StoreWrapper.Start → StartPrimaryTasks
    // → TryStartExpiredKeyDeletionTask 对译：读 expired-key-deletion-scan-freq
    // 槽位，经唯一调停入口 apply_config_reconcile 走 CONFIG SET 同一分支——
    // 槽位即唯一启停真值源；副本角色不拉起对标 Start 按角色分派，升主恢复点
    // resume_primary_tasks → start_gc 承接重拉）。首个会话惰性触发，幂等。
    if !self.gc_scan_started.swap(true, Ordering::Relaxed) {
      // 紧缩旋钮启动投影（C# DoCompactionAsync 每轮 GetInt/GetEnum 现取
      // runtimeConfig 的同效前置：槽位初值一次性投影进 GcConfig，此后 CONFIG
      // SET 经调停消息增量同步，GcManager 每轮重读快照达成「每轮现取」语义；
      // 副本角色同样投影，升主恢复点即持最新实效值）
      let runtime_config = &self.runtime_config;
      store.update_gc_config(|c| {
        c.compaction_max_segments = runtime_config
          .get_int(ServerConfigType::CompactionMaxSegments)
          .max(0) as usize;
        c.compaction_type = runtime_config
          .get_enum(ServerConfigType::CompactionType)
          .unwrap_or(c.compaction_type);
      });
      if !self.primary_tasks.is_replica() {
        apply_config_reconcile(
          Some(&self.primary_tasks),
          &store,
          self.aof.as_ref(),
          None,
          ConfigReconcile::ExpiredKeyDeletionScan {
            scan_frequency_secs: i64::from(
              self
                .runtime_config
                .get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
            ),
          },
        );
      }
    }
    // 周期对象收集任务随首个会话惰性拉起（C# StoreWrapper.Start →
    // StartPrimaryTasks 的 ObjectCollectTask 注册段对译：执行域绑定装配
    // 终态引擎与配置后按 expired-object-collection-freq 槽位拉起，副本
    // 角色不拉起，升主恢复点 resume_primary_tasks 重拉）
    {
      self
        .primary_tasks
        .bind_object_collect_env(&store, Some(&self.runtime_config));
      self.primary_tasks.try_start_object_collect_task();
    }

    let mut session = store.new_session().ok()?;
    // AOF 归组会话 id 连接级固化：与 RESP 会话 id（`self.id as i32`，即
    // `session_id_counter` 自 1 起编、经 `decorate` 同值注入）同一取值源，令
    // 数据条目帧头 session_id 与事务标记面（`transaction_manager.rs` 的
    // `self.session_id` ← `resp` `session_id()`）同键，重放归组口据此命中组内
    // 数据（对标 C# `Session.ID` 数据/标记同键，`AofReplayCoordinator.cs:142`）。
    session = session.with_aof_session_id(network_sender_id as i32);
    // 副本一致读会话装配（C# RespServerSession.cs:298-300 建会话时按
    // EnableCluster + EnableAOF + MultiLogEnabled + appendOnlyFile 创建
    // consistentReadDBSession 的对位——rust 以 aof 的读一致性管理器在场为门，
    // 管理器仅 multi_log_enabled 拓扑才建）；角色动态性（C# EnforceConsistentRead
    // = enforceConsistentRead && clusterProvider.IsReplica()，StoreWrapper.cs:903-904）
    // 由 ReadSessionState pre 入口 role_gate 判定承接：主库/单机读路径零协议
    // 开销直通，晋升副本即时生效，快慢路径经连接级附着态自动共享
    if let Some(manager) = self
      .aof
      .as_ref()
      .and_then(|aof| aof.read_consistency_manager())
    {
      let state = ReadSessionState::attach(manager, Some(Arc::clone(&self.primary_tasks)));
      session = session.with_read_session_state(Some(Arc::new(state)));
    }
    // 会话指标共享句柄门控（C# StoreWrapper.trackStats =
    // MetricsSamplingFrequency > 0 方置 sessionMetrics，采样关闭会话与存储
    // 两侧均为 null；本处逐连接创建一个，与会话执行域共持同一对象）
    let session_metrics =
      (self.metrics_sampling_frequency_secs > 0).then(|| Arc::new(SessionMetricsHandle::default()));
    // 连接会话置严格上下文态：冷租户/冷库映射未装载时 set_context 拒绝
    // 盲分配，AUTH/HELLO/SELECT 据此挂起磁盘点查装载（冷租户 0 内存常驻条款；
    // 内部/重放/统计会话维持纯内存原语语义不受影响）
    session.set_strict_context(true);
    let checkpoint = CheckpointCtx::new(Arc::clone(&self.database_manager));
    // 集合更新唤醒：慢路径写回后唤醒阻塞观察者（C# itemBroker
    // HandleCollectionUpdate 的存储执行域可达面），move 闭包捕获经纪句柄注入
    let notify_broker = Arc::clone(&self.broker);
    // 慢路径阻塞等待面：阻塞族命令冷键装载未取到时在执行域内联等待
    //（C# BlockingWait 的 compio 投影），trait 对象剥离取件源泛型
    let wait_broker = Arc::clone(&self.broker);

    let api = StoreGarnetApi::new(session)
      .with_vector_manager(Arc::clone(&self.vector_manager))
      .with_checkpoint_ctx(checkpoint)
      .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
        notify_broker.handle_collection_update(domain, key);
      }) as CollectionNotify))
      .with_item_broker_wait(Some(wait_broker))
      // 会话指标共享句柄（对标 C# GarnetProvider/GarnetServer 装配链
      // trackStats 门控下创建 sessionMetrics 并同传 RespServerSession 与
      // storageSession：rust 以 provider 为单一创建点，执行域与会话共持；
      // 采样关闭 None 与 C# null 会话指标同形）
      .with_session_metrics(session_metrics.clone());
    let mut consumer = (self.decorate)(network_sender_id, api)?;
    consumer.attach_session_metrics(session_metrics);
    consumer.inject_dependencies(self.session_dependencies());
    Some(consumer)
  }

  /// 活跃消费者注册表（网络泵建连/注册、释放/注销的入口）
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }

  /// 复位存储复活化统计（trait 默认口的存储宿主实现，对位 C#
  /// storeWrapper.ResetRevivificationStats 的 databaseManager 直下；
  /// INFO RESETSTAT 的 reviv 臂装配侧唯一落点）
  fn reset_revivification_stats(&self) {
    self.database_manager.reset_revivification_stats();
  }

  /// AOF 门面（inherent `aof()` 转发；EnableAOF 门控——未点亮为 None）
  fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    self.aof.as_ref()
  }

  /// TLS 证书热加载共享句柄（inherent 装配字段转发；C#
  /// storeWrapper.serverOptions.TlsOptions 的会话侧可达面，None = 未装配 TLS）
  #[cfg(feature = "tls")]
  fn tls_config(&self) -> Option<Arc<ServerTlsConfig>> {
    self.tls_config.clone()
  }

  /// libs/server/StoreWrapper.cs:WaitForCommitAsync
  ///
  /// WAIT-FOR-COMMIT 档的存储侧等待口：`!EnableAOF` 直返 Ok(false)（C# 同款
  /// 门），否则经 `wait_for_commit_to_aof_async` 下达
  /// 全部活跃库 AOF 提交落盘（C# `databaseManager.WaitForCommitToAofAsync`
  /// 的接口面调用）。提交失败 Err 上浮（C# 异常沿 await 穿透至
  /// RespServerSession.Send 的 BlockingWait 抛出点）。
  /// 副本角色收口在子日志等待口唯一角色闸（WaofSublog::wait_for_commit_async
  /// 副本臂以 flush-only 内核自驱等待，严禁触达 commit_to 组提交帧），
  /// 本命令面读同一角色源，不设第二角色判定
  fn wait_for_commit_async(&self) -> impl Future<Output = waof::Result<bool>> {
    let aof_enabled = self.aof.is_some();
    let database_manager = self.database_manager.as_ref();
    async move {
      if !aof_enabled {
        return Ok(false);
      }
      database_manager
        .wait_for_commit_to_aof_async()
        .await
        .map(|_| true)
    }
  }

  /// 向量清理协程停机收敛（对标 C# `VectorManager.Dispose`）：转发
  /// [`VectorManager::dispose_cleanup`]，由宿主 `stop()` 在 coordinator 停
  /// 监听（Phase 1）后、worker join 前于主线程驱动——消费协程栖 worker
  /// 运行时，收敛窗口内运行时仍存活排空积压通道。
  fn dispose_vector_cleanup(&self) -> bool {
    self.vector_manager.dispose_cleanup()
  }

  /// pubsub 中枢停机收口（对标 C# InternalDispose 的
  /// `subscribeBroker?.Dispose()`，随 Provider.Dispose 排空后置于 join 之后）：
  /// 停机链经本门面单点取用 broker 同步收口（置 disposed + 清订阅表，rust 无
  /// 常驻消费任务、无等待面）；`--disable-pubsub` 形态（None）空操作
  fn dispose_pubsub(&self) {
    if let Some(b) = &self.pubsub {
      b.dispose();
    }
  }

  /// 集合项经纪停机收口（对标 C# StoreWrapper.Dispose 的
  /// `itemBroker?.Dispose()`）：经 SharedItemBroker 转发口下达——置取消、
  /// 解除全部等待观察者、唤醒主循环退出；紧随其后的 worker join 即排空
  /// 屏障。经纪为必装配单例（node_components），无 None 形态
  fn dispose_item_broker(&self) {
    self.broker.dispose();
  }

  /// Lua 超时看门狗停机收口（对标 C# `StoreWrapper.Dispose` 的
  /// `luaTimeoutManager?.Dispose()`：置停机位唤醒专属线程并 join）；
  /// 超时未启用形态（None）空操作。装配口见 [`assemble_lua_timeout`]，
  /// 宿主经 [`Self::with_lua_timeout`] 挂停机句柄
  fn dispose_lua_timeout(&self) {
    if let Some(manager) = &self.lua_timeout {
      manager.dispose();
    }
  }

  /// 范围索引停机收口（在 garnet 中的相对路径:libs/server/StoreWrapper.cs:Dispose
  /// 的 `rangeIndexManager?.Dispose()`：DisposeIncompleteStreamReassembly +
  /// 逐树 `Tree?.Dispose()` + `liveIndexes.Clear()`）。rust 拆两臂：
  /// 复制面臂清未完成流重组（AOF 装配在场才收），引擎臂释放当前在线引擎
  /// 全部在线树（幂等——wbftree manager 逐树 `take` 语义，与
  /// `WedbStore::drop` 的 dispose 兜底及 manager Drop 并存安全）；
  /// `store()` 现取在线引擎，副本检查点导入置换后仍收口新引擎
  fn dispose_range_index(&self) {
    if let Some(ri) = &self.ri {
      ri.dispose();
    }
    self.store().range_index().dispose();
  }
}

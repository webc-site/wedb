//! GarnetServer 生命周期域：start / stop / Drop / 停机等待与 worker 线程治理
//!
//! 对标 C# libs/host/GarnetServer.cs 的 Start / InternalDispose 生命周期面

#[cfg(unix)]
use super::accept::run_uds_accept_loop;
use super::{accept::run_tcp_accept_loop, *};

/// worker 线程 join 有界上界毫秒（P3 防御档，deviations §152 在册）：停机
/// 排空护栏（consumer_registry `DRAIN_TIMEOUT_MS`=5s）到期强收后，同线程
/// Runtime 析构兜底取消残余任务的量级余量。挂死向量绝大多数已由 §84
/// 护栏有界，本档仅覆盖「worker 线程内部同步死锁」残余面
const WORKER_JOIN_TIMEOUT_MS: u64 = 15_000;

/// 单个 worker 线程有界 join（std [`JoinHandle::join`] 无超时、不可打断的
/// 防御承接）：join 移交中介线程，通道 recv_timeout 有界收口。返回
/// `Some(true)`=正常退出、`Some(false)`=worker panic 退出、`None`=到期未收敛
/// （疑线程内死锁，句柄随中介线程迁出——进程收口已脱离挂死的防御取舍）。
///
/// C# GarnetServerBase 的 DisposeActiveHandlers 臂（同名排空件锚留
/// `consumer_registry.rs` 的 dispose_active_handlers 一处；C# 侧
/// 5s 滞留诊断留痕形的 join 面延伸；rust 到期 error 留痕并强行推进）
pub fn join_worker_bounded(handle: JoinHandle<()>, timeout: Duration) -> Option<bool> {
  let (tx, rx) = channel();
  thread::spawn(move || {
    let _ = tx.send(handle.join().is_ok());
  });
  rx.recv_timeout(timeout).ok()
}

/// worker 线程组逐个有界 join（[`GarnetServer::stop`] 与
/// [`GarnetServer::reclaim_workers`] 共用收口单点）：超时/panic 留痕后强推
/// 停机后续步骤（AOF 刷盘收尾、锁守护释放不被滞留 worker 绑架）
fn join_workers_bounded(handles: &mut Vec<JoinHandle<()>>) {
  for (idx, handle) in handles.drain(..).enumerate() {
    match join_worker_bounded(handle, Duration::from_millis(WORKER_JOIN_TIMEOUT_MS)) {
      Some(true) => {}
      Some(false) => warn!("Worker 线程 {idx} panic 退出（有界 join 收口）"),
      None => {
        log::error!(
          "Worker 线程 {idx} 未在 {}ms 内退出（疑线程内部死锁），强推停机后续收尾，残余交 Runtime 析构兜底（deviations §152）",
          WORKER_JOIN_TIMEOUT_MS
        );
      }
    }
  }
}

/// 接入 worker 线程启动骨架（TCP 首发核 / TCP 其余核 / UDS 三处共用）：
/// 新起 compio 运行时 → 运行时内驱动接入前奏（端点绑定，产出移交接入循环的
/// 资源与投递父线程的就绪值）→ 前奏成功即投递就绪值并把资源交给接入循环，
/// 前奏失败即投递错误后收线程；运行时构造失败按原语义投递
/// `io::Error::other(运行时错误 Display)` 文本
fn spawn_accept_worker<R, T, Pr, PrF, Mt, MtF>(
  name: String,
  prelude: Pr,
  main: Mt,
) -> io::Result<(JoinHandle<()>, RxOneshot<io::Result<T>>)>
where
  T: Send + 'static,
  Pr: FnOnce() -> PrF + Send + 'static,
  PrF: Future<Output = io::Result<(R, T)>>,
  Mt: FnOnce(R) -> MtF + Send + 'static,
  MtF: Future<Output = ()>,
{
  let (ready_tx, ready_rx) = oneshot();
  let handle = ThreadBuilder::new().name(name).spawn(move || {
    let rt = match Runtime::new() {
      Ok(rt) => rt,
      Err(e) => {
        ready_tx.send(Err(io::Error::other(e.to_string())));
        return;
      }
    };

    rt.block_on(async move {
      match prelude().await {
        Ok((res, ready)) => {
          ready_tx.send(Ok(ready));
          main(res).await;
        }
        Err(e) => ready_tx.send(Err(e)),
      }
    });
  })?;
  Ok((handle, ready_rx))
}

/// 接入 worker 就绪回执收取（线程失联即通道断开，兜底文本由调用方按原口径
/// 单点传入，禁各臂二写）
fn recv_ready<T>(ready_rx: RxOneshot<io::Result<T>>, gone: &str) -> io::Result<T> {
  match ready_rx.recv() {
    Ok(res) => res,
    Err(_) => Err(io::Error::other(gone)),
  }
}

impl<P: SessionProviderFace + 'static> GarnetServer<P> {
  /// worker 线程 spawn 前共享句柄单点快照（三接入线程闭包逐字段捕获清单的
  /// 唯一装配位；克隆次序与本方法字段序一致；UDS 侧绑定路径与权限位为循环
  /// 独参，不入本快照）
  fn capture(&self) -> AcceptContext<P> {
    AcceptContext {
      coordinator: self.shutdown_coordinator.clone(),
      id_gen: Arc::clone(&self.session_id_counter),
      provider: Arc::clone(&self.session_provider),
      pool: Arc::clone(&self.buffer_pool),
      #[cfg(feature = "tls")]
      tls_config: self.tls_config.clone(),
      #[cfg(feature = "tls")]
      tls_handshake_timeout: self.tls_handshake_timeout,
      #[cfg(feature = "tls")]
      tls_shutdown_timeout: self.tls_shutdown_timeout,
    }
  }

  /// start 失败臂回收单点：停 coordinator → 逐个有界 join 本端点已 spawn 的
  /// worker（次序与原各臂一致：stop 先于 join，杜绝幽灵 worker 存活；join
  /// 走 [`join_workers_bounded`] 防御档，失败收口臂绝不因滞留 worker 挂死）
  fn reclaim_workers(&self, mut handles: Vec<JoinHandle<()>>) {
    self.shutdown_coordinator.stop();
    join_workers_bounded(&mut handles);
  }

  /// 启动服务器（Thread-per-Core 多核 SO_REUSEPORT 并发监听）
  ///
  /// 返回 `io::Result` 维持本域（监听绑定/套接字失败的天然通道，对标 C#
  /// SocketException 同域外抛，不并入节点错误枚举）
  ///
  /// libs/host/GarnetServer.cs:Start
  /// libs/host/GarnetServer.cs:InitializeServer
  pub fn start(&self, worker_threads: Option<NonZeroUsize>) -> io::Result<()> {
    // TLS 证书定时刷新循环挂表单点（对标 C# GarnetTlsOptions 构造 selector
    // 即 Timer 挂表；rust 构造/start 皆可发生在 compio 运行时域外——
    // run_async 在室内驱动 start，wnode_test::start_server 等 harness 则在
    // 室外（本注释旧文「start 全程在调用方运行时域内执行」经现有 harness
    // 证否已校正）。室外无 executor 可挂，必须 warn 留痕，禁静默失效）
    #[cfg(feature = "tls")]
    if let Some(tls) = &self.tls_config
      && !tls.ensure_refresh_loop()
    {
      log::warn!("证书刷新循环未挂载（调用点无 compio 运行时域），刷新将滞后至下次承接臂");
    }
    let nthreads = worker_threads
      .unwrap_or_else(|| available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() }))
      .get();

    // 连接上限启动投影（PR #2157，C# GarnetServer.cs:289 把 opts
    // .NetworkConnectionLimit 装进 ConnectionLimit 后逐监听器 Register 的
    // rust 对位：注册表内共享原子单一真源，accept 容量门与 CONFIG SET
    // maxclients 调停同源读写）
    if let Some(registry) = self.session_provider.consumer_registry() {
      registry.set_connection_limit(self.network_connection_limit);
    }

    for endpoint in &self.endpoints {
      let res = match endpoint {
        ServerEndpoint::Tcp(addr) => self.start_tcp_workers(*addr, nthreads),
        ServerEndpoint::Unix(_path) => {
          #[cfg(unix)]
          {
            self.start_unix_worker(_path, self.unix_socket_perm)
          }
          #[cfg(not(unix))]
          {
            Err(io::Error::new(
              io::ErrorKind::Unsupported,
              "Unix domain socket not supported",
            ))
          }
        }
      };

      if let Err(e) = res {
        // 失败臂：对之前已成功启动的端点和 workers 进行全部清理回收（确保返回 Err 时零幽灵监听）
        self.reclaim_workers(self.worker_threads.lock().drain(..).collect());
        self.tcp_addrs.lock().clear();
        return Err(e);
      }
    }

    Ok(())
  }

  /// 启动多核 TCP Worker 线程（基于 SO_REUSEPORT 端口复用）
  fn start_tcp_workers(&self, target_addr: SocketAddr, nthreads: usize) -> io::Result<()> {
    let mut handles = Vec::with_capacity(nthreads);

    // Worker 0: 负责首发绑定（特别针对动态端口 :0）并广播实际地址
    let cap = self.capture();
    let (handle, ready_rx) = spawn_accept_worker(
      "wnode-worker-0".into(),
      move || async move {
        let listener = bind_reuseport(target_addr).await?;
        let actual_addr = listener.local_addr()?;
        Ok((listener, actual_addr))
      },
      move |listener| run_tcp_accept_loop(0, listener, cap),
    )?;

    handles.push(handle);

    // 等待 Worker 0 绑定成功
    let actual_addr = match recv_ready(ready_rx, "Worker-0 初始化失败") {
      Ok(addr) => addr,
      Err(e) => {
        self.reclaim_workers(handles);
        return Err(e);
      }
    };

    self.tcp_addrs.lock().push(actual_addr);

    info!(
      "启动 TCP 多核网络监听: {} ({} 核心 SO_REUSEPORT)",
      actual_addr, nthreads
    );

    // 启动其余 Worker 核心 (1..nthreads)
    for core_id in 1..nthreads {
      let cap = self.capture();
      let (handle, ready_rx) = match spawn_accept_worker(
        format!("wnode-worker-{core_id}"),
        move || async move { Ok((bind_reuseport(actual_addr).await?, ())) },
        move |listener| run_tcp_accept_loop(core_id, listener, cap),
      ) {
        Ok(pair) => pair,
        Err(e) => {
          self.tcp_addrs.lock().retain(|a| *a != actual_addr);
          self.reclaim_workers(handles);
          return Err(e);
        }
      };

      handles.push(handle);

      if let Err(e) = recv_ready(ready_rx, "Worker 初始化失败") {
        self.tcp_addrs.lock().retain(|a| *a != actual_addr);
        self.reclaim_workers(handles);
        return Err(e);
      }
    }

    self.worker_threads.lock().extend(handles);
    Ok(())
  }

  /// 启动 Unix 域套接字监听 Worker
  #[cfg(unix)]
  fn start_unix_worker(&self, path: &Path, perm: Option<u32>) -> io::Result<()> {
    // 绑定路径与循环本端点回退路径各持一份（同值，分别迁入绑定前奏与接入循环）
    let bind_path = path.to_path_buf();
    let loop_path = path.to_path_buf();
    let ctx = self.capture();
    let (handle, ready_rx) = spawn_accept_worker(
      "wnode-uds".into(),
      move || async move {
        let bound = UdsGuard::bind(&bind_path, perm).await?;
        info!("启动 Unix 域套接字监听: {}", bind_path.display());
        Ok((bound, ()))
      },
      move |(listener, sock_guard)| run_uds_accept_loop(ctx, loop_path, listener, sock_guard),
    )?;

    if let Err(e) = recv_ready(ready_rx, "UDS Worker 初始化失败") {
      self.reclaim_workers(vec![handle]);
      return Err(e);
    }

    self.worker_threads.lock().push(handle);
    Ok(())
  }

  /// 优雅关停服务器（资源与网络收口）
  ///
  /// **语义边界声明**：`stop` 与 [`Drop`] 仅为连接与内存等资源收口，**不包含 AOF 尾部刷盘**。
  /// 嵌入宿主若需保证未提交帧安全落盘停机，必须调用 [`Self::dispose_async`] 或 [`Self::wait_for_shutdown`]。
  ///
  /// 三阶段时序对标 C# InternalDispose（servers\[i\].Close 先于一切通道收敛，
  /// subscribeBroker.Dispose 排在 Provider.Dispose 即排空之后）：coordinator.stop
  /// 停监听阻断新连接（Phase 1，必须最前置——收敛窗口内新连接仍可涌入提交
  /// VADD/VREM 而清理通道已闭，写任务静默丢弃成孤儿索引）→ AOF 背压闸门
  /// 放行（滞留追加方出口，置位幂等）→ 向量清理协程收敛（消费协程栖 worker
  /// 运行时，硬约束仅先于 join）→ 集合项经纪收口（解除全部阻塞等待者，
  /// C# itemBroker?.Dispose() 对位；主循环跑在 worker 运行时，紧随其后的
  /// join 即排空屏障）→ 有界 join worker 线程（各线程运行时内已完成活跃
  /// 连接排空，Phase 2）→ Lua 看门狗收口 → 范围索引收口（Provider.Dispose
  /// 级联尾项）→ pubsub 中枢收口（C# subscribeBroker?.Dispose() 排在
  /// Provider.Dispose 之后，即 join 排空后；rust 无常驻消费任务，置 disposed
  /// 与清订阅表为同步单步）→ 释放缓冲池；join 返回即排空结束，上层的集群
  /// dispose 与配置刷盘（Phase 3）在其后执行
  ///
  /// 闸门放行必须先于 join：同步 wait_slow 阻塞的是 worker 线程本体（poll 内
  /// 阻塞，连接任务强杀不可打断），滞留追加方若不在排空窗口前放行将吃满
  /// join 防御档上界（[`WORKER_JOIN_TIMEOUT_MS`]，deviations §152）；C# 靠 WaitSlow
  /// 轮询 disposed 与 DrainActiveHandlers 超时兜底，无此前置约束
  ///
  /// AOF 语义边界：本方法止步于背压闸门放行（Phase 1/2 资源收口），不含
  /// AOF 尾刷——同步上下文无法直驱 async 设备 IO，环形缓冲未提交帧不入盘。
  /// 落盘停机（C# InternalDispose Phase 3 的 AppendOnlyFile.Dispose 对位）
  /// 须走 [`Self::wait_for_shutdown`] 或 [`Self::dispose_async`]，二者尾部
  /// 共用同一收口单点；裸 [`Self::drop`] 同此边界
  ///
  /// libs/host/GarnetServer.cs:InternalDispose
  pub fn stop(&self) {
    // Phase 1（C# InternalDispose 首步 servers[i].Close() 关监听端口阻断内核
    // backlog 新连接的对位）：协调器广播终止三接入循环，停机取消桥打断在途
    // accept 并 drop 监听套接字——必须先于一切通道收敛，否则收敛窗口内新连接
    // 仍可涌入提交 VADD/VREM，而清理通道已闭致写任务丢弃、孤儿索引泄漏
    self.shutdown_coordinator.stop();
    // AOF 背压闸门关停放行（C# AofBackpressure.Dispose：Release all stalled
    // appenders permanently (server shutdown)；无 AOF 形态经 trait 默认
    // None 跳过）
    if let Some(aof) = self.session_provider.aof()
      && let Some(bp) = aof.backpressure()
    {
      bp.dispose();
    }
    // 向量清理协程收敛（C# Provider.Dispose 级联 VectorManager.Dispose 逐通道
    // CompleteAndWaitForConsumerTask）：硬约束仅先于 join——消费协程栖各 worker
    // 线程 compio 运行时（thread-per-core，独立），主线程此处阻塞等待期间
    // worker 运行时经 dispose_active_handlers 排空臂持续存活并驱动协程消费
    // 退出；新连接已被上方 Phase 1 阻断，收敛期无在途写任务可丢
    if !self.session_provider.dispose_vector_cleanup() {
      log::warn!("向量清理协程未在超时内全部收敛，停机继续");
    }
    // 集合项经纪收口（C# StoreWrapper.Dispose 的 `itemBroker?.Dispose()`，
    // 本函数上方注释枚举 C# 序列所点名的一步）：置取消、解除全部等待观察者
    // （客户端收最终空应答，不依赖 terminate 广播兜底弃答）、投事件唤醒
    // 经纪主循环退出——主循环跑在 worker 运行时，紧随其后的 join 即天然
    // 排空屏障（对应 C# done.Wait() 的排空语义），避免经纪主循环任务与
    // 取件源会话（独立纪元参与者）随 worker 硬杀消亡于 enter_batch 纪元
    // 临界段内
    self.session_provider.dispose_item_broker();
    // Phase 2 收口：有界 join（案三防御档，deviations §152——std join 无超时
    // 不可打断，worker 线程内部同步死锁属 §84 排空护栏未覆的残余挂死面，
    // 到期告警强推后续收尾，绝不上抛挂死停机链）
    let mut handles = self.worker_threads.lock();
    join_workers_bounded(&mut handles);
    // Lua 超时看门狗停机收口（C# StoreWrapper.Dispose 的
    // `luaTimeoutManager?.Dispose()`：置停机位唤醒专属线程并 Join，时序在
    // rangeIndexManager.Dispose 之前——rust join 返回即连接排空完成，在途
    // 脚本已收敛，此刻收口不再需要看门狗；排空前收口会让 drain 中的死循环
    // 脚本失去抢占兜底）。看门狗为独立 OS 线程、不经 worker 运行时驱动，
    // 与 join 屏障无次序耦合；未装配超时形态默认口空操作
    self.session_provider.dispose_lua_timeout();
    // 范围索引停机收口（C# StoreWrapper.Dispose 的
    // rangeIndexManager?.Dispose()，Provider.Dispose 段第七步：时序在
    // clusterProvider/itemBroker/taskManager 之后、databaseManager 之前——
    // 对位即 Phase 2 连接排空（join）之后、引擎 store 兜底析构之前）。
    // 显式步落地后，嵌入式/复用进程形态 stop() 返回即释放全部在线树与
    // native 页缓存，不再依赖各 Arc 引用恰好归零；引擎侧幂等，与
    // WedbStore::drop 兜底并存安全。早于 join 则在途命令仍可触达在建树，
    // 故不可前移；未装配引擎经 trait 默认跳过
    self.session_provider.dispose_range_index();
    // pubsub 中枢收口（C# InternalDispose 的 subscribeBroker?.Dispose() 排在
    // Provider.Dispose 即排空之后，本次对标后移至 join 后）：rust 无 C# 的
    // TsavoriteLog 介质与后台消费循环（SubscribeBroker::dispose 即置 disposed
    // 并清三订阅表，同步单步、无 done.WaitOne 等待面），排空后统一释放，
    // 杜绝旧时序在 worker 排空臂注销订阅/写出在途 PUBLISH 前抢先清表；
    // 未装配 pubsub（--disable-pubsub）经 trait 默认跳过
    self.session_provider.dispose_pubsub();
    self.buffer_pool.purge();
  }

  /// 等待系统停机信号或协调器停机并优雅关机
  ///
  /// `quiet`（C# GarnetServer.cs:535 `if (!opts.QuietMode)`，选项源头
  /// Options.cs:363-364 QuietMode）置位时静默监听就绪横幅文本
  pub async fn wait_for_shutdown(&self, server_label: &str, quiet: bool) -> crate::Result<()> {
    #[cfg(feature = "tls")]
    let tls_info = if self.tls_config.is_some() {
      " (TLS 启用)"
    } else {
      ""
    };
    #[cfg(not(feature = "tls"))]
    let tls_info = "";
    if !quiet {
      log::info!(
        "{server_label} 网络监听就绪: {:?}{tls_info}",
        self.local_addrs()
      );
    }

    let coordinator = self.shutdown_coordinator.clone();
    let (tx, rx) = bounded_async::<&'static str>(1);
    let tx_sig = tx.clone();
    let sig_task = spawn(async move {
      let sig = wait_shutdown_signal().await.unwrap_or("信号监听失败");
      let _ = tx_sig.try_send(sig);
    });
    let coord_task = spawn(async move {
      coordinator.wait().await;
      let _ = tx.try_send("停机协调器");
    });

    let sig = rx.recv().await.unwrap_or("未知信号");
    drop(sig_task);
    drop(coord_task);

    log::info!("捕获停机信号 ({sig}), 正在优雅关机 {server_label}...");
    // 停机尾唯一收口单点（stop 排空 + AOF 尾刷合一）；C# InternalDispose
    // Phase 3 Provider.Dispose → AppendOnlyFile.Dispose 的对位动作经此落位
    self.dispose_async().await?;
    log::info!("{server_label} 安全退出完成");
    Ok(())
  }
}

/// 析构守卫
///
/// **语义边界声明**：Drop 仅同步调用 [`GarnetServer::stop`] 完成网络连接与内存资源收口，
/// **不执行 AOF 异步刷盘**（异步设备 IO 无法在同步 Drop 内安全阻塞驱动）。
/// 如需保证 AOF 尾部帧落盘，停机须显式调用 [`GarnetServer::dispose_async`] 或 [`GarnetServer::wait_for_shutdown`]。
impl<P: SessionProviderFace + 'static> Drop for GarnetServer<P> {
  /// 资源收口（转调 [`GarnetServer::stop`]）＝网络与线程域排空，不含 AOF
  /// 尾刷：环形缓冲未提交帧随停机弃置（async 设备 IO 进不了同步 Drop，
  /// compio poll 栈内严禁 block_on 兜底）。落盘停机须显式走
  /// [`GarnetServer::wait_for_shutdown`] 或 [`GarnetServer::dispose_async`]，
  /// 此处文档即契约，无隐式兜底
  fn drop(&mut self) {
    self.stop();
  }
}

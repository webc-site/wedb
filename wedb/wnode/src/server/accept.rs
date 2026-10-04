//! TCP / UDS 双接入循环与接入计量、预注册、退避分档共用件
//!
//! 对标 C# libs/server/Servers/GarnetServerTcp.cs 的 HandleNewConnection /
//! HandleAcceptError 族

use compio::net::TcpStream;
#[cfg(unix)]
use compio::net::UnixStream;

use super::*;

/// accept 资源压力退避的初值与封顶毫秒数（每接入循环协程栈上持有一份，
/// 与 C# 每监听器一字段同粒度，禁全局可变）
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:33-34
const INITIAL_ACCEPT_BACKOFF_MS: u64 = 100;
const MAX_ACCEPT_BACKOFF_MS: u64 = 5000;

/// accept 资源压力判定（对标 C# 档内 SocketError 在各平台的对应物）：
/// ENOMEM 用 std 稳定归一的 `OutOfMemory` kind；EMFILE/ENFILE/ENOBUFS 因 std 未将
/// 其归一为稳定 kind，以 cfg 编译期 raw errno 集合补判——热路径零字符串比较、零平台运行时分支
///
/// Linux: EMFILE=24, ENFILE=23, ENOBUFS=105
/// macOS/BSD: EMFILE=24, ENFILE=23, ENOBUFS=55
/// Windows: WSAEMFILE=10024, WSAENOBUFS=10055（对标 C# TooManyOpenSockets / NoBufferSpaceAvailable）
#[cfg(target_os = "linux")]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[24, 23, 105];
#[cfg(any(
  target_os = "macos",
  target_os = "ios",
  target_os = "freebsd",
  target_os = "openbsd",
  target_os = "netbsd",
  target_os = "dragonfly"
))]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[24, 23, 55];
#[cfg(windows)]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[10024, 10055];
#[cfg(not(any(
  target_os = "linux",
  target_os = "macos",
  target_os = "ios",
  target_os = "freebsd",
  target_os = "openbsd",
  target_os = "netbsd",
  target_os = "dragonfly",
  windows
)))]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[];

fn is_accept_resource_pressure(e: &io::Error) -> bool {
  e.kind() == io::ErrorKind::OutOfMemory
    || matches!(e.raw_os_error(), Some(errno) if ACCEPT_RESOURCE_ERRNOS.contains(&errno))
}

/// accept 失败分档收口：返回 false 表示接入循环应退出
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleAcceptError
///
/// 三档对标：一档致命（C# OperationAborted/Shutdown 静默停循环）对应 rust
/// 停机竞态 `coordinator.is_stopped()`；二档资源压力（C# TooManyOpenSockets /
/// NoBufferSpaceAvailable / ProcessLimit，即 unix 的 EMFILE/ENFILE/ENOBUFS/
/// ENOMEM，见 is_accept_resource_pressure）warning 带退避毫秒 + 指数退避
/// 封顶 5s，成功 accept 后由调用方复位（C#:234）；三档其余（C# default）
/// debug 即续。C# 档内 NetworkDown/SystemNotReady 为监听 socket 不可能经
/// accept 产出的形态（后者 Windows 专属），不纳入判定。
///
/// C# 的 `Thread.Sleep` 阻塞在此换成退避 sleep 与停机取消令牌竞速：本线程
/// reactor 绝不能被阻塞（单线程多协程），且 SIGTERM 最迟在下一次让渡点生效，
/// 不被拖到 5 秒。C#:197-204 的 throw 崩溃档不做——那组 NotSocket 一类是
/// IOCP/Windows 产物，rust 协程持有着 listener 本体，对应形态不可达，
/// panic 爆炸半径亦不等价，只保留 break/退避两种收口。
async fn handle_accept_error(
  target: impl fmt::Display,
  e: io::Error,
  coordinator: &ShutdownCoordinator,
  cancel_token: &CancelToken,
  backoff_ms: &mut u64,
) -> bool {
  // 一档：停机竞态（对标 C# return false）
  if coordinator.is_stopped() {
    return false;
  }
  if is_accept_resource_pressure(&e) {
    // 二档：资源压力，先按当前档位让渡再翻倍封顶（对标 C#:212-216）
    warn!("{target} 接收连接资源压力，退避 {backoff_ms}ms: {e}");
    let slept = time::sleep(Duration::from_millis(*backoff_ms))
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;
    *backoff_ms = (*backoff_ms * 2).min(MAX_ACCEPT_BACKOFF_MS);
    return slept.is_ok();
  }
  // 三档：瞬时错误，debug 续不退避（对标 C#:220-222）
  debug!("{target} 瞬时接收错误，继续: {e}");
  true
}

/// accept 成功接入判定结果：通过（携在途守卫；无注册表的哑桩形态为 None，
/// 语义同 limit=-1 不设门）/ 超限拒绝（调用方 continue，drop stream 关闭新连接）
enum Admission {
  Passed(Option<ConnectionGuard>),
  Rejected,
}

/// 因连接上限被拒时回写对端的 RESP 错误帧（PR #2157，C#
/// GarnetServerTcp.cs:MaxClientsReachedError——逐字节对齐 Redis 线上文本，
/// 既有客户端错误处理不变即适用）
static MAX_CLIENTS_REACHED_ERROR: &[u8] = b"-ERR max number of clients reached\r\n";

/// accept 成功后的接入计量与在途容量门收口（TCP/UDS 双循环共用；C#
/// GarnetServerTcp.cs:236-241 容量门 accept 成功即刻计量、:302-307 超限臂
/// 即刻关闭新连接）：received 计数前移（容量门前——TLS 握手失败、容量门
/// 拒绝、发字即断的短命连接全部进统计；C# :288 计数点在 TryAdd 后 rust
/// 前移一档，r14-conn.md:11「容量门拒绝除外」括注不采纳，维持现计数，
/// 裁决归属 deviations §101）→ 容量门；超限臂 rejected 计数（PR #2157 只在
/// 容量门分支计，装配期死套接字与 handler 构造失败不是拒绝）、disposed
/// 配对递增（rust received 前移后的配对点）并 debug 留痕。守卫由调用方
/// move 进连接任务随其结束（正常收尾/TLS 握手失败）Drop 归零
fn admit_connection(registry: &Option<Arc<ConsumerRegistry>>) -> Admission {
  let Some(registry) = registry.as_ref() else {
    return Admission::Passed(None);
  };
  registry.note_connection_received();
  match registry.try_acquire_connection() {
    Some(guard) => Admission::Passed(Some(guard)),
    None => {
      registry.note_connection_rejected();
      registry.note_connection_disposed();
      Admission::Rejected
    }
  }
}

/// accept 成功后的预注册收口（TCP/UDS 两循环共用；C# HandleNewConnection
/// 的 GarnetServerTcp.cs:256 `activeHandlers.TryAdd` 即刻注册、先于 :290
/// `handler.Start`）：条目即刻入 entries——TLS 握手与首字节读取前连接即
/// 处于 CLIENT LIST/KILL 治理面（view 动态字段由会话建立后每批镜像重导
/// 补全），终止哨兵同点挂接，握手与泵读共用同一取消令牌。返回该令牌供
/// TLS 握手臂复用（非 TLS 构建无握手位，令牌仅驻 handler 域）
fn preregister_consumer<C: MessageConsumerFace>(
  registry: &Option<Arc<ConsumerRegistry>>,
  handler: &mut NetworkHandler<C>,
  id: i64,
  remote_endpoint: String,
  local_endpoint: String,
) -> Option<CancelToken> {
  let registry = registry.as_ref()?;
  let entry = registry.register(id, remote_endpoint, local_endpoint);
  let token = CancelToken::new();
  spawn_kill_watcher(Arc::clone(&entry), token.clone());
  handler.attach_consumer(Arc::clone(registry), entry, token.clone());
  Some(token)
}

/// 接入循环停机取消桥（TCP/UDS 双循环共用）：协调器停机即刻取消本循环
/// 在途 accept 与退避 sleep，使接入臂于停机信号到达当刻收场
fn spawn_shutdown_bridge(coordinator: &ShutdownCoordinator, cancel_token: &CancelToken) {
  let watcher_cancel = cancel_token.clone();
  let coordinator = coordinator.clone();
  spawn(async move {
    coordinator.wait().await;
    watcher_cancel.cancel();
  })
  .detach();
}

/// 连接任务收尾单点（TCP/UDS 双循环共用）：在途守卫随本任务结束归零 →
/// 驱动连接泵 → 泵错 debug 留痕
///
/// 守卫先于 handler 释放的次序由形参/局部析构次序保证（局部先于形参 drop，
/// C# activeHandlerCount 的 decrement 挂点在 handler dispose），勿改形参次序
async fn serve_connection<P: SessionProviderFace + 'static>(
  target: impl fmt::Display,
  mut handler: NetworkHandler<P::Consumer>,
  stream: ConnectionStream,
  provider: Arc<P>,
  sender_id: u64,
  guard: Option<ConnectionGuard>,
) {
  let _in_flight = guard;
  if let Err(err) = handler.process_stream(stream, provider, sender_id).await {
    log::debug!("{target} 连接处理结束: {err}");
  }
}

/// TCP / UDS 接入循环的端点差异面（双循环共用骨架 [`run_accept_loop`] 的
/// 泛型承载，仅描述差异、不承载共用骨架）：accept 产物、容量门后的套接字
/// 配置、计量与日志目标名、连接任务装配（TCP 含 TLS 握手臂）与停机
/// Phase 1 关闭面
trait AcceptEndpoint: Sized {
  /// 单次 accept 进件（流本体 + 端点捕获要素）
  type Accepted;

  /// 单次 accept（骨架统一包停机取消竞速与 fail_fast）
  async fn accept(&self) -> io::Result<Self::Accepted>;

  /// 计量与日志目标名（`Worker-{core_id}` / `UDS`；借端点存活，零分配）
  fn target(&self) -> impl fmt::Display + Copy;

  /// 容量门通过后的套接字配置（TCP nodelay/保活；UDS 无操作）
  fn configure(&self, accepted: &Self::Accepted);

  /// 容量门拒绝后的对端告知与关闭（PR #2157，C# RejectConnection）：非 TLS
  /// 对端写出 [`MAX_CLIENTS_REACHED_ERROR`] 后优雅关闭——失败可诊断而非无解
  /// 复位；TLS 对端只关闭不写（对端只发了 ClientHello，明文错误帧是协议
  /// 违例，会以握手失败浮出、把运维指向证书而非容量，rejected_connections
  /// 度量即 TLS 形态的全部补救）。写为 detach 任务尽力而为、非阻塞：达限
  /// 意味着服务器已在连接压力下，阻塞写会串行化拒绝并放大过载；优雅关闭
  /// 由 Drop 收场（内核冲刷已排队字节后再 FIN）
  fn reject(&self, accepted: Self::Accepted, tls: bool);

  /// 停机取消退出的 debug 留痕（双端点文案形态各异，保持原样不归一）
  fn log_cancel_exit(&self);

  /// 连接任务装配：端点对捕获 → handler 构造 → 预注册 → 连接泵任务体
  ///（返回任务体由骨架统一 spawn detach；TCP 含 TLS 握手分派）
  fn launch<P: SessionProviderFace + 'static>(
    &self,
    ctx: &AcceptContext<P>,
    registry: &Option<Arc<ConsumerRegistry>>,
    accepted: Self::Accepted,
    sender_id: u64,
    guard: Option<ConnectionGuard>,
  ) -> impl Future<Output = ()> + 'static;

  /// 停机 Phase 1：关闭监听面（TCP 关监听套接字；UDS 加移除套接字文件）
  fn shutdown_phase1(self);
}

/// TCP / UDS 双接入循环共用骨架（端点差异经 [`AcceptEndpoint`] 承载）：
/// shutdown 取消桥、退避复位、接入计量容量门、sender_id 分配、连接任务
/// 装配拉起、停机 Phase 1/2 收口完全同构，仅端点捕获与 socket 配置分端
async fn run_accept_loop<P: SessionProviderFace + 'static, E: AcceptEndpoint>(
  ctx: AcceptContext<P>,
  endpoint: E,
) {
  let cancel_token = CancelToken::new();
  spawn_shutdown_bridge(&ctx.coordinator, &cancel_token);

  // 退避状态协程栈持有（C#:35 acceptBackoffMs，每监听器一份）
  let mut backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;

  while !ctx.coordinator.is_stopped() {
    let accept_res = endpoint
      .accept()
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;

    match accept_res {
      Ok(Ok(accepted)) => {
        // 成功接入复位退避（C# GarnetServerTcp.cs:234）
        backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
        let registry = ctx.provider.consumer_registry();
        // received 前移与在途容量门单源收口（C# 对位、计数口径与 deviations
        // §101 裁决归属见 admit_connection 文档）
        let guard = match admit_connection(&registry) {
          Admission::Passed(guard) => guard,
          #[cfg_attr(not(feature = "tls"), allow(unused_variables))]
          Admission::Rejected => {
            #[cfg(feature = "tls")]
            let tls = ctx.tls_config.is_some();
            #[cfg(not(feature = "tls"))]
            let tls = false;
            endpoint.reject(accepted, tls);
            continue;
          }
        };
        // 容量门通过后的套接字配置（TCP nodelay/保活，C#:249；UDS 无操作）
        endpoint.configure(&accepted);
        if ctx.coordinator.is_stopped() {
          break;
        }
        let sender_id = ctx.id_gen.fetch_add(1, Ordering::Relaxed);
        // 连接任务装配与拉起（端点对捕获 → handler 构造 → 预注册 → 泵驱动；
        // C# :256 TryAdd 先于 :290 handler.Start）
        let connection = endpoint.launch(&ctx, &registry, accepted, sender_id, guard);
        spawn(connection).detach();
      }
      Ok(Err(e)) => {
        // accept 失败分档（对标 C# HandleAcceptError 的一/二/三档）
        if !handle_accept_error(
          endpoint.target(),
          e,
          &ctx.coordinator,
          &cancel_token,
          &mut backoff_ms,
        )
        .await
        {
          break;
        }
      }
      Err(Cancelled) => {
        endpoint.log_cancel_exit();
        break;
      }
    }
  }

  // 停机 Phase 1：关闭监听面（TCP 即刻关闭监听套接字阻断新连接进内核
  // backlog；UDS 加移除套接字文件——对标 C# GarnetServer.cs:InternalDispose
  // Phase 1 → Close()）
  endpoint.shutdown_phase1();

  // 停机 Phase 2：本线程运行时析构前排空活跃连接（C#
  // GarnetServerBase.DisposeActiveHandlers），detach 连接任务须在本
  // block_on 尾部驱动至归零，否则随 Runtime 析构被截断
  if let Some(registry) = ctx.provider.consumer_registry() {
    registry.dispose_active_handlers().await;
  }
}

/// TCP worker 目标名展示投影（`Worker-{core_id}`；零分配 [`fmt::Display`]
/// 包装——`format_args!` 结果不能跨语句存活，以类型承载）
#[derive(Clone, Copy)]
struct WorkerTarget(usize);

impl fmt::Display for WorkerTarget {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Worker-{}", self.0)
  }
}

/// TCP 接入端点（`SO_REUSEPORT` 多核 worker 各持一份）
struct TcpEndpoint {
  /// 所属核心号（日志 `Worker-{core_id}` 与连接任务留痕）
  core_id: usize,
  /// 监听套接字
  listener: TcpListener,
}

/// TCP 单次 accept 进件
struct TcpAccepted {
  stream: TcpStream,
  client_addr: SocketAddr,
}

impl AcceptEndpoint for TcpEndpoint {
  type Accepted = TcpAccepted;

  async fn accept(&self) -> io::Result<TcpAccepted> {
    let (stream, client_addr) = self.listener.accept().await?;
    Ok(TcpAccepted {
      stream,
      client_addr,
    })
  }

  fn target(&self) -> impl fmt::Display + Copy {
    WorkerTarget(self.core_id)
  }

  fn configure(&self, accepted: &Self::Accepted) {
    // 接入侧装配 nodelay + 默认保活（C#:249 仅 NoDelay，保活为 rust 自有面）
    let _ = configure_socket(&accepted.stream);
  }

  fn reject(&self, accepted: Self::Accepted, tls: bool) {
    let TcpAccepted { mut stream, .. } = accepted;
    debug!(
      "Worker-{} 在途连接达上限，关闭新连接（TLS 对端不回写错误帧）",
      self.core_id
    );
    if tls {
      return; // 拒绝计数已是 TLS 形态的全部补救
    }
    spawn(async move {
      use compio::io::AsyncWriteExt;
      let _ = stream.write_all(MAX_CLIENTS_REACHED_ERROR.to_vec()).await;
    })
    .detach();
  }

  fn log_cancel_exit(&self) {
    debug!("Worker-{} 收到停机取消信号，退出接入循环", self.core_id);
  }

  fn launch<P: SessionProviderFace + 'static>(
    &self,
    ctx: &AcceptContext<P>,
    registry: &Option<Arc<ConsumerRegistry>>,
    accepted: Self::Accepted,
    sender_id: u64,
    guard: Option<ConnectionGuard>,
  ) -> impl Future<Output = ()> + 'static {
    let TcpAccepted {
      stream,
      client_addr,
    } = accepted;
    // 本地端点构造期捕获（C# TcpNetworkHandlerBase 构造期捕获
    // socket.LocalEndPoint 的 localEndpointName 对标；TLS 臂流型擦除
    // 不透出内层 TCP，取值必须前移到握手前）
    let local_endpoint = tcp_local_endpoint(&stream);
    let remote_endpoint = client_addr.to_string();
    // 回环判定 accept 侧当场折叠（C# TcpNetworkHandlerBase.cs:42-44 的
    // IPEndPoint 臂 IPAddress.IsLoopback：typed SocketAddr 直判，含
    // v4-mapped IPv6 解映射，见 wbase::endpoint::ip_is_loopback）——
    // 展示字符串自此仅供 CLIENT INFO/日志，判定面绝不字符串再解析
    let peer_source = PeerSource::Ip {
      loopback: ip_is_loopback(client_addr),
    };
    let mut handler = NetworkHandler::<P::Consumer>::new(
      remote_endpoint.clone(),
      peer_source,
      local_endpoint.clone(),
      Arc::clone(&ctx.pool),
    );
    // 收场尾帧超时界注入（TLS 收场治理面：close_notify 尾帧确定性上界
    // TLS_SHUTDOWN_TIMEOUT；明文臂 shutdown 即刻就绪，包裹零成本）
    #[cfg(feature = "tls")]
    handler.set_shutdown_timeout(ctx.tls_shutdown_timeout);
    // 预注册（C# :256 TryAdd 即刻注册，先于 :290 handler.Start——TLS
    // 握手与首字节读取前条目即入 entries，握手期/不发字节的空闲连接
    // 均可见可 KILL；view 动态字段由会话建立后每批镜像重导补全）。
    // 返回令牌供 TLS 握手臂复用（非 TLS 构建无握手位，令牌仅驻 handler，
    // 下划线绑定兼作两 feature 形态的零告警单点调用）
    let _kill_token = preregister_consumer(
      registry,
      &mut handler,
      sender_id as i64,
      remote_endpoint,
      local_endpoint,
    );
    let provider_clone = Arc::clone(&ctx.provider);

    #[cfg(feature = "tls")]
    let tls_acceptor = ctx.tls_config.as_ref().map(|c| c.acceptor().clone());
    #[cfg(feature = "tls")]
    let tls_handshake_timeout = ctx.tls_handshake_timeout;

    let core_id = self.core_id;
    async move {
      #[cfg(feature = "tls")]
      let connection_stream = if let Some(acceptor) = tls_acceptor {
        // TLS 握手挂 KILL/停机令牌与确定性超时双边界（预注册条目治理面
        // 延伸到握手期；Slowloris 慢速握手防护，见 TLS_HANDSHAKE_TIMEOUT）。
        // timeout 置于 with_cancel 内层：令牌触发时 fail_fast 即刻收场且
        // 驱动级撤销握手与睡眠两枚在途操作；超时触发时握手 Future 随
        // timeout 落值被 Drop，compio Submit 的 Drop 臂向驱动撤销挂起读。
        // 两条早退臂同走 RAII 收口：在途守卫先 Drop 回落容量、handler
        // Drop → dispose → 注销配对（次序同 serve_connection 收尾臂），
        // 半开套接字随 Drop 释放
        let handshake = time::timeout(tls_handshake_timeout, acceptor.accept(stream));
        let shaken = match &_kill_token {
          Some(token) => handshake.with_cancel(token.clone()).fail_fast().await,
          None => Ok(handshake.await),
        };
        match shaken {
          // 三层落值：with_cancel → timeout → 握手本体
          Ok(Ok(Ok(tls_stream))) => ConnectionStream::tls(tls_stream),
          Ok(Ok(Err(err))) => {
            log::debug!("Worker-{core_id} TLS 握手失败: {err}");
            drop(guard);
            return;
          }
          Ok(Err(_elapsed)) => {
            log::debug!("Worker-{core_id} TLS 握手超时 ({tls_handshake_timeout:?})，关闭半开连接");
            drop(guard);
            return;
          }
          Err(Cancelled) => {
            drop(guard);
            return;
          }
        }
      } else {
        ConnectionStream::Tcp(stream)
      };

      #[cfg(not(feature = "tls"))]
      let connection_stream = ConnectionStream::Tcp(stream);

      serve_connection(
        format_args!("Worker-{core_id}"),
        handler,
        connection_stream,
        provider_clone,
        sender_id,
        guard,
      )
      .await;
    }
  }

  fn shutdown_phase1(self) {
    // 停机 Phase 1：即刻关闭监听套接字，阻断新连接进内核 backlog
    // （对标 C# GarnetServer.cs:InternalDispose Phase 1 → GarnetServerTcp.Close()
    //  释放 listenSocket 端口；排空窗口内 TCP 握手不再完成，客户端 connect 立即失败）
    drop(self.listener);
  }
}

/// 驱动单个核心的 TCP 连接接入循环
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
///
/// accept 成功回调本体承接：复位退避 → 在途容量门 → socket 配置 → 建 handler
/// 并注册 → 拉起连接泵；容量门子步骤对位见
/// [`ConsumerRegistry::try_acquire_connection`]，accept 失败分档见
/// handle_accept_error（对标 C# HandleAcceptError 一/二/三档）
pub(super) async fn run_tcp_accept_loop<P: SessionProviderFace + 'static>(
  core_id: usize,
  listener: TcpListener,
  ctx: AcceptContext<P>,
) {
  run_accept_loop(ctx, TcpEndpoint { core_id, listener }).await;
}

/// UDS 接入端点（监听套接字 + 套接字文件守卫同寿命收口）
#[cfg(unix)]
struct UdsEndpoint {
  /// 监听绑定路径（local_endpoint 取值失败时的回退真源）
  path_buf: PathBuf,
  /// 监听套接字
  listener: UnixListener,
  /// 套接字文件守卫（drop 移除套接字文件）
  sock_guard: UdsGuard,
}

/// UDS 单次 accept 进件
#[cfg(unix)]
struct UdsAccepted {
  stream: UnixStream,
}

#[cfg(unix)]
impl AcceptEndpoint for UdsEndpoint {
  type Accepted = UdsAccepted;

  async fn accept(&self) -> io::Result<UdsAccepted> {
    let (stream, _) = self.listener.accept().await?;
    Ok(UdsAccepted { stream })
  }

  fn target(&self) -> impl fmt::Display + Copy {
    "UDS"
  }

  fn configure(&self, _accepted: &Self::Accepted) {}

  fn reject(&self, accepted: Self::Accepted, _tls: bool) {
    let UdsAccepted { mut stream } = accepted;
    debug!("UDS 在途连接达上限，关闭新连接");
    spawn(async move {
      use compio::io::AsyncWriteExt;
      let _ = stream.write_all(MAX_CLIENTS_REACHED_ERROR.to_vec()).await;
    })
    .detach();
  }

  fn log_cancel_exit(&self) {
    debug!("UDS 接收循环收到停机取消信号，退出");
  }

  fn launch<P: SessionProviderFace + 'static>(
    &self,
    ctx: &AcceptContext<P>,
    registry: &Option<Arc<ConsumerRegistry>>,
    accepted: Self::Accepted,
    sender_id: u64,
    guard: Option<ConnectionGuard>,
  ) -> impl Future<Output = ()> + 'static {
    let UdsAccepted { stream } = accepted;
    // 端点对 accept 侧捕获：
    // 对端 remote_endpoint 经 getpeername 取 as_pathname，未命名对端（如客户端直连）
    // 为空串，对标 C# TcpNetworkHandlerBase.cs remoteEndpoint?.ToString() ?? string.Empty
    // （.NET UnixDomainSocketEndPoint 对未命名对端 ToString 为空串）；
    // 本端 local_endpoint 为监听绑定路径（getsockname 取 as_pathname，失败回退到 path_buf）。
    let remote_endpoint = stream
      .peer_addr()
      .ok()
      .and_then(|addr| addr.as_pathname().map(|p| p.display().to_string()))
      .unwrap_or_default();
    // 来源类型折叠：UDS accept 臂本即知道连接类型（C# 对端为
    // UnixDomainSocketEndPoint 对象即恒本地，未命名对端空串展示
    // 不影响判定），TcpNetworkHandlerBase.cs:42-44 类型臂承接
    let peer_source = PeerSource::Unix;
    let local_endpoint = stream
      .local_addr()
      .ok()
      .and_then(|addr| addr.as_pathname().map(|p| p.display().to_string()))
      .unwrap_or_else(|| self.path_buf.display().to_string());
    let mut handler = NetworkHandler::<P::Consumer>::new(
      remote_endpoint.clone(),
      peer_source,
      local_endpoint.clone(),
      Arc::clone(&ctx.pool),
    );
    // 预注册 + 终止哨兵（TCP 循环同一收口：C# :256 TryAdd 先于
    // handler.Start，条目即刻入治理面；UDS 无 TLS 握手位）
    let _ = preregister_consumer(
      registry,
      &mut handler,
      sender_id as i64,
      remote_endpoint,
      local_endpoint,
    );
    serve_connection(
      "UDS",
      handler,
      ConnectionStream::Unix(stream),
      Arc::clone(&ctx.provider),
      sender_id,
      guard,
    )
  }

  fn shutdown_phase1(self) {
    // 停机 Phase 1：即刻关闭 UDS 监听套接字并移除套接字文件
    // （对标 C# GarnetServer.cs:InternalDispose Phase 1 → Close()；
    //  drop(listener) 关 fd，drop(sock_guard) 触发 UdsGuard::drop 移除套接字文件，
    //  排空窗口内新连接无法建立）
    drop(self.listener);
    drop(self.sock_guard);
  }
}

/// 驱动 Unix 域套接字连接接入循环
///
/// 与 TCP 循环共用 [`run_accept_loop`] 骨架（接入计量、失败分档、停机取消
/// 桥、连接收尾与 Phase 1/2 收口）；端点对取值与恒本地来源、监听套接字与
/// 套接字文件守卫的停机 Phase 1 关闭为本端点独有（UDS 无 TLS 握手位）
#[cfg(unix)]
pub(super) async fn run_uds_accept_loop<P: SessionProviderFace + 'static>(
  ctx: AcceptContext<P>,
  path_buf: PathBuf,
  listener: UnixListener,
  sock_guard: UdsGuard,
) {
  run_accept_loop(
    ctx,
    UdsEndpoint {
      path_buf,
      listener,
      sock_guard,
    },
  )
  .await;
}

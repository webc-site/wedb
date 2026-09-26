//! TLS 证书与鉴权配置（纯 Rust 实现，基于 rustls 与 compio-tls）
//!
//! 1:1 对标微软 Garnet libs/server/TLS/GarnetTlsOptions.cs、
//! IGarnetTlsOptions.cs 与 libs/server/TLS/ServerCertificateSelector.cs

use std::{
  io,
  path::{Path, PathBuf},
  sync::{Arc, LazyLock},
  time::Duration,
};

use arc_swap::ArcSwap;
use compio::{
  runtime::{Runtime, spawn},
  time,
};
use compio_tls::{
  TlsAcceptor, rustls,
  rustls::{
    DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    crypto::{self, WebPkiSupportedAlgorithms, ring},
    pki_types::{CertificateDer, PrivateKeyDer, UnixTime},
    server::{
      ClientHello, ResolvesServerCert, WebPkiClientVerifier,
      danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
  },
};
use parking_lot::Mutex;

use crate::{
  cert::{certified_key, load_certs, load_private_key},
  validity::Validity,
};

/// 证书刷新失败的退避重试间隔秒数
///
/// 在 garnet 中的相对路径: libs/server/TLS/ServerCertificateSelector.cs:certificateRefreshRetryInterval
const CERT_REFRESH_RETRY_SECS: u64 = 5;

/// TLS 服务端配置装配器
///
/// `Clone` 即共享：内部全 Arc（acceptor 的 ServerConfig、动态解析器、
/// 证书状态），克隆体与原件共用同一活跃证书——网络端点侧与 CONFIG SET
/// 会话侧各持一份克隆即 C# `storeWrapper.serverOptions.TlsOptions` 单实例
/// 共享的对位（UpdateCertFile 换证书全端点立即可见）
#[derive(Clone)]
pub struct ServerTlsConfig {
  inner: Arc<Inner>,
}

/// 共享内核：握手数据面（acceptor + 解析器）与控制面（证书路径 + 刷新循环）单点
struct Inner {
  acceptor: TlsAcceptor,
  resolver: Arc<DynamicCertResolver>,
  /// 当前证书与私钥路径、刷新循环代际与在挂票据（CONFIG SET 换路径与重启
  /// 刷新循环的状态单点；全字段同锁读写，去重无需额外原子）
  state: Mutex<CertState>,
  /// 定时刷新周期秒数（0 = 禁用；对标 C# GarnetTlsOptions.CertificateRefreshFrequency）
  refresh_freq_secs: u64,
}

/// [`CertState::running_epoch`] 的"无循环在跑"初值（代际自增永不触及的哨兵位）
const NOT_RUNNING: u64 = u64::MAX;

/// 证书文件来源状态（刷新循环按此路径重读）
struct CertState {
  cert_path: PathBuf,
  key_path: PathBuf,
  /// 刷新循环代际（换证书重启循环即自增；compio CancelToken 钉定单核
  /// 运行时不可跨线程共享，C# EndTimer 的跨核取消语义由代际失配自退承接：
  /// 旧循环每轮醒来核对代际，失配即自行退出——对标 C#
  /// UpdateCertFile → 旧 selector.EndTimer() + new Timer）
  refresh_epoch: u64,
  /// 已在某 compio 运行时拉起的刷新循环代际（[`Self::try_start_refresh_loop`]
  /// 幂等去重单点；循环未来跑即由 [`RefreshTicket`] 复位 [`NOT_RUNNING`]，
  /// 宿主运行时析构经任务 drop 同样复位——初值 [`NOT_RUNNING`] = 无循环在跑）
  running_epoch: u64,
}

/// 刷新循环在挂票据：循环未来跑（代际失配 return、panic 展开、宿主运行时
/// 析构致任务 drop——compio `Executor::drop` 清理在挂任务）析构时复位
/// [`CertState::running_epoch`] 哨兵，使下一次服务启动能重挂
struct RefreshTicket {
  inner: Arc<Inner>,
  my_epoch: u64,
}

impl Drop for RefreshTicket {
  fn drop(&mut self) {
    let mut state = self.inner.state.lock();
    // 仅回收自己那一代票据：换代后新循环已换发，不误伤在挂邻代
    if state.running_epoch == self.my_epoch {
      state.running_epoch = NOT_RUNNING;
    }
  }
}

impl ServerTlsConfig {
  /// 从 PEM 格式的证书与私钥文件加载配置（纯 Rust，零 C/OpenSSL 依赖）
  ///
  /// `client_cert_required` 对标 ClientCertificateRequired；`issuer_path` 对标
  /// IssuerCertificatePath（客户端证书校验根，None = 宽松模式见 [`client_verifier`]）；
  /// `refresh_freq_secs` 对标 CertificateRefreshFrequency（> 0 时定时重读循环
  /// 随服务启动臂拉起，构造本身不限运行时域）
  pub fn from_pem_files(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    client_cert_required: bool,
    issuer_path: Option<&Path>,
    refresh_freq_secs: u64,
  ) -> io::Result<Self> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let issuer = issuer_path.map(CaSource::Pem);

    Self::assemble(
      certified_key(certs, key)?,
      cert_path.to_path_buf(),
      key_path.to_path_buf(),
      client_cert_required,
      issuer,
      refresh_freq_secs,
    )
  }

  /// 从 DER 格式证书链与私钥构造（供测试或自签名证书直接内存装配）
  ///
  /// 只承载 C# 侧的证书装载面（把内存 DER 字节入装为证书/私钥），
  /// 认证选项装配单点在其 [`server_config`]，本入口不另挂锚点。
  ///
  /// 参数语义同 [`Self::from_pem_files`]，`issuer_ca` 为内存 DER 形态的签发者
  /// CA；`refresh_freq_secs` 仅文件来源形态可定时重读，内存 DER 无文件路径，
  /// 传非零值即编程错误（debug 断言），运行期按 0 处理（无刷新循环）
  pub fn from_der(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    client_cert_required: bool,
    issuer_ca: Option<Vec<CertificateDer<'static>>>,
    refresh_freq_secs: u64,
  ) -> io::Result<Self> {
    let issuer = issuer_ca.map(CaSource::Der);

    Self::assemble(
      certified_key(certs, key)?,
      PathBuf::new(),
      PathBuf::new(),
      client_cert_required,
      issuer,
      refresh_freq_secs,
    )
  }

  /// 装配收口：解析器 + 认证选项 + 刷新循环单点
  fn assemble(
    ck: CertifiedKey,
    cert_path: PathBuf,
    key_path: PathBuf,
    client_cert_required: bool,
    issuer: Option<CaSource<'_>>,
    refresh_freq_secs: u64,
  ) -> io::Result<Self> {
    let resolver = Arc::new(DynamicCertResolver(Arc::new(ArcSwap::from_pointee(ck))));
    let config = server_config(Arc::clone(&resolver), client_cert_required, issuer)?;
    let inner = Arc::new(Inner {
      acceptor: TlsAcceptor::from(Arc::new(config)),
      resolver,
      state: Mutex::new(CertState {
        cert_path,
        key_path,
        refresh_epoch: 0,
        running_epoch: NOT_RUNNING,
      }),
      refresh_freq_secs,
    });
    let this = Self { inner };
    // 周期 > 0 仅记代际意图后尝试拉起：compio spawn 钉死调用线程运行时域，
    // 构造发生在运行时内即挂表（生产 run_async 装配链），运行室外为无操作，
    // 由服务启动臂的 ensure_refresh_loop 承接
    if refresh_freq_secs > 0 {
      this.try_start_refresh_loop();
    }
    Ok(this)
  }

  /// 获取 compio-tls Acceptor
  #[inline]
  pub fn acceptor(&self) -> &TlsAcceptor {
    &self.inner.acceptor
  }

  /// 共享证书源句柄（与入站 acceptor 同一 ArcSwap 单源，零第二份证书态）
  ///
  /// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions
  ///（:185-189 出站 LocalCertificateSelectionCallback 闭包动态读
  /// serverCertificateSelector 实例字段——UpdateCertFile 换 selector 即
  /// 传播出站；rust 对位：出站 [`crate::client`] 解析器握手期对本句柄
  /// load_full 现取，热换装与周期刷新两方向一次生效）
  #[inline]
  pub fn cert_source(&self) -> Arc<ArcSwap<CertifiedKey>> {
    Arc::clone(&self.inner.resolver.0)
  }

  /// 在线重载证书文件（CONFIG SET cert-file-name / cert-password 的执行端）
  ///
  /// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile
  ///
  /// `cert_file_name` 缺席对位 C# "Cannot provide null cert-file-name."（只给
  /// password 不给 file 同落此臂——C# certFileName 为 null 同判）；私钥路径
  /// 缺席回落证书文件本体（C# PEM 单文件形态 CertPassword=null 对位）。
  /// 重载成功即原子换装活跃证书，新握手立即用新证书而已建立连接不受影响；
  /// 失败保留既有证书并返回错误（对位 C# GetServerCertificate catch 沿用旧
  /// sslServerCertificate）。刷新周期 > 0 时以新路径换代重启后台刷新循环
  ///（对标 C# UpdateCertFile 重建 selector：EndTimer + new Timer）
  pub fn update_cert_file(
    &self,
    cert_file_name: Option<&Path>,
    cert_password: Option<&Path>,
  ) -> io::Result<()> {
    let Some(cert_path) = cert_file_name else {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Cannot provide null cert-file-name.",
      ));
    };
    let key_path = cert_password.unwrap_or(cert_path);

    // 重载先行：失败即 Err 返回，既有证书原封不动（禁半态换装；IO 留锁外）
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let ck = certified_key(certs, key)?;

    // 换装-换代单临界区：store、路径落位、代际自增同守卫内原子落位，与
    // reload 的核对-换装临界区互斥（reload 先落位则随后被本段新证覆盖，
    // 后落位则代际核对失配丢弃）——回灌路径闭合，对位 C# :118 整体重建
    // TlsServerOptions 的换装即终态
    let refresh_on = self.inner.refresh_freq_secs > 0;
    {
      let mut state = self.inner.state.lock();
      self.inner.resolver.0.store(Arc::new(ck));
      state.cert_path = cert_path.to_path_buf();
      state.key_path = key_path.to_path_buf();
      if refresh_on {
        state.refresh_epoch += 1;
      }
    }
    // 循环拉起保持锁外：try_start_refresh_loop 自取同一把 state 锁
    //（parking_lot 非重入，锁内调用即死锁）；室外返回的 false 按既有语义
    // 丢弃——换代意图已落代际，循环由后续运行时的启动/换证臂承接
    if refresh_on {
      let _ = self.try_start_refresh_loop();
    }
    Ok(())
  }

  /// 服务启动臂幂等补拉起刷新循环（对标 C# selector 构造器 Timer 挂表；
  /// rust 构造可在 compio 运行室外发生，故循环拉起统一并入服务启动的
  /// 既有 executor 通道——禁第二运行时）
  ///
  /// 返回 false = 周期 > 0 但调用点不在 compio 运行时域内、循环未挂且当下
  /// 无人承接（降级观测面：调用方必须留 warn 痕迹，禁静默失效）
  pub fn ensure_refresh_loop(&self) -> bool {
    self.try_start_refresh_loop()
  }

  /// 拉起去重单点：周期 > 0、存在未承接代际且当前线程在 compio 运行时
  /// 内才 spawn（[`Runtime::try_current`] 探测，运行室外返回 false——
  /// 意图已记于代际，后续启动/换证臂自然承接）；循环未来跑经
  /// [`RefreshTicket`] 复位票据，二次 start 可重挂
  fn try_start_refresh_loop(&self) -> bool {
    if self.inner.refresh_freq_secs == 0 {
      return true;
    }
    if Runtime::try_current().is_none() {
      return false;
    }
    let my_epoch = {
      let mut state = self.inner.state.lock();
      let desired = state.refresh_epoch;
      if state.running_epoch == desired {
        return true;
      }
      state.running_epoch = desired;
      desired
    };
    let inner = Arc::clone(&self.inner);
    let ticket = RefreshTicket {
      inner: Arc::clone(&inner),
      my_epoch,
    };

    spawn(async move {
      // 在挂票据随循环未来跑析构（含宿主运行时 drop 任务清理）复位哨兵
      let _ticket = ticket;
      loop {
        sleep_secs(inner.refresh_freq_secs).await;
        if inner.state.lock().refresh_epoch != my_epoch {
          return;
        }
        // 失败退避链：错误即 5 秒后重试直至成功或换代退出
        while let Err(e) = inner.reload(my_epoch) {
          log::error!("TLS 证书定时重载失败，{CERT_REFRESH_RETRY_SECS}s 后重试: {e}");
          sleep_secs(CERT_REFRESH_RETRY_SECS).await;
          if inner.state.lock().refresh_epoch != my_epoch {
            return;
          }
        }
        if inner.state.lock().refresh_epoch != my_epoch {
          return;
        }
      }
    })
    .detach();
    true
  }
}

/// 秒级睡眠（刷新循环节拍原语）
async fn sleep_secs(secs: u64) {
  time::sleep(Duration::from_secs(secs)).await;
}

impl Inner {
  /// 从当前路径重读证书并原子换装（IO 段不持状态锁）
  ///
  /// 核对-换装单临界区：锁内比对 refresh_epoch 与 my_epoch，失配即丢弃本次
  /// 重读结果不 store（周期臂与退避臂同受保护）；匹配则同守卫内 store——
  /// 与 update_cert_file 的换装-换代临界区互斥，杜绝「核对通过后 update
  /// 全程完成再回灌旧证」的检查-换装间隙竞态。
  fn reload(&self, my_epoch: u64) -> io::Result<()> {
    let (cert_path, key_path) = {
      let state = self.state.lock();
      (state.cert_path.clone(), state.key_path.clone())
    };
    let certs = load_certs(&cert_path)?;
    let key = load_private_key(&key_path)?;
    let ck = certified_key(certs, key)?;
    let state = self.state.lock();
    if state.refresh_epoch != my_epoch {
      return Ok(());
    }
    self.resolver.0.store(Arc::new(ck));
    Ok(())
  }
}

/// 动态证书解析器：ArcSwap 无锁原子持有活跃证书链
///
/// 在 garnet 中的相对路径: libs/server/TLS/ServerCertificateSelector.cs:sslServerCertificate
///
/// C# 字段原子替换 + GetSslServerCertificate 读取面（经
/// ServerCertificateSelectionCallback 挂入握手）的 rustls 对位：
/// ResolvesServerCert::resolve，数据面握手零锁争用，读取仅一次 Arc 克隆
#[derive(Debug)]
struct DynamicCertResolver(Arc<ArcSwap<CertifiedKey>>);

impl ResolvesServerCert for DynamicCertResolver {
  fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
    Some(self.0.load_full())
  }
}

/// 签发者 CA 来源（PEM 文件或内存 DER，两构造入口收敛同一装配链）
enum CaSource<'a> {
  Pem(&'a Path),
  Der(Vec<CertificateDer<'static>>),
}

/// 服务端 ServerConfig 单点装配：客户端认证面三态收敛于此
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslServerAuthenticationOptions
///
/// C# `SslServerAuthenticationOptions { ClientCertificateRequired,
/// RemoteCertificateValidationCallback, ServerCertificateSelectionCallback }`
/// 的 rustls 对位：required=false → 不请求客户端证书（单向 TLS，行为零
/// 变化）；required=true → with_client_cert_verifier（校验器形态见
/// [`client_verifier`]）；证书动态选择面即动态解析器挂载（对位
/// ServerCertificateSelectionCallback 动态返回最新 sslServerCertificate）
fn server_config(
  resolver: Arc<DynamicCertResolver>,
  client_cert_required: bool,
  issuer: Option<CaSource<'_>>,
) -> io::Result<ServerConfig> {
  let builder = ServerConfig::builder();
  let builder = if client_cert_required {
    builder.with_client_cert_verifier(client_verifier(issuer)?)
  } else {
    builder.with_no_client_auth()
  };
  Ok(builder.with_cert_resolver(resolver))
}

/// 客户端证书校验器装配
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:ValidateClientCertificateCallback
///
/// - issuer 在位 → rustls 原生 [`WebPkiClientVerifier`]（根为 issuer PEM/DER
///   载入的 CA，对标 C# ValidateCertificateIssuer 的颁发者钉根臂）
/// - issuer 缺席 → [`AnyClientCert`] 宽松校验器（要求证书但不校验颁发者链，
///   对标 C# GetCertificateIssuer :273 告警语义，构造期 warn 一次）
fn client_verifier(issuer: Option<CaSource<'_>>) -> io::Result<Arc<dyn ClientCertVerifier>> {
  match issuer {
    Some(src) => WebPkiClientVerifier::builder(Arc::new(ca_roots(src)?))
      .build()
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())),
    None => {
      log::warn!(
        "tls_client_cert_required=true 且未提供 tls_issuer_cert：要求客户端证书但不校验颁发者链（GarnetTlsOptions.cs:273）"
      );
      Ok(Arc::new(AnyClientCert))
    }
  }
}

/// CA 信任根装载（空证书集快速失败，禁静默空根拒绝一切）
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetCertificateIssuer
fn ca_roots(src: CaSource<'_>) -> io::Result<RootCertStore> {
  let mut roots = RootCertStore::empty();
  let certs = match src {
    CaSource::Pem(path) => load_certs(path)?,
    CaSource::Der(certs) => certs,
  };
  for cert in certs {
    roots
      .add(cert)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
  }
  if roots.is_empty() {
    return Err(io::Error::new(
      io::ErrorKind::NotFound,
      "tls_issuer_cert 未提供任何可用的 CA 证书",
    ));
  }
  Ok(roots)
}

/// 宽松客户端证书校验器：不校验颁发者链，但实校验有效期窗口
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:ValidateCertificateIssuer
///（authority == null 臂：链策略 AllowUnknownCertificateAuthority，任意自签/未知
/// CA 证书放行；X509Chain.Build 的 NotTimeValid 时间面仍生效——本校验器的
/// [`Self::verify_client_cert`] 即对位该面，链信任构建豁免、时间窗不豁免）
///
/// 与 [`crate::client`] 的 NoVerify（出站远端恒真）对等形态：握手签名校验仍全量
/// 委托 rustls provider 实校验（.NET SslStream 同样只豁免证书链校验面），
/// 证书缺失仍按 client_auth_mandatory 拒绝——禁静默降级为不请求证书
#[derive(Debug)]
struct AnyClientCert;

/// 签名校验算法表（ring provider 单例，避免每次握手重建；与 [`crate::client`] 同源形态）
static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

impl ClientCertVerifier for AnyClientCert {
  /// 证书必选（对标 ClientCertificateRequired=true：缺失即握手失败）
  fn client_auth_mandatory(&self) -> bool {
    true
  }

  /// 无钉根 CA 即不发送 certificate_authorities 提示
  fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
    &[]
  }

  /// 链信任豁免面 + 有效期实校验面：颁发者链不构建（任意自签/未知 CA 放行，
  /// 对标 AllowUnknownCertificateAuthority），但 NotBefore/NotAfter 窗口外
  /// 拒绝（对标 X509Chain.Build 的 NotTimeValid 拒绝臂；DER 畸形同拒，
  /// fail-closed）——过期/未生效客户端证书放行即身份门失效
  fn verify_client_cert(
    &self,
    end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    now: UnixTime,
  ) -> Result<ClientCertVerified, rustls::Error> {
    Validity::from_der(end_entity)?.check(now)?;
    Ok(ClientCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    crypto::verify_tls12_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    crypto::verify_tls13_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    SIGNATURE_ALGS.supported_schemes()
  }
}

// r327 部分迁移甄别注报：本测试块 4 例全部直触私有面——`ServerTlsConfig.inner`
// 私有字段、`Inner` 私有结构的 state/resolver 字段与私有 reload、私有常量
// NOT_RUNNING（触达 ≥3 项），按「不为迁就搬运扩 pub」红线整块弃迁留地，一字不动。
#[cfg(test)]
mod tests {
  use std::{fs::write, thread};

  use compio::runtime::Runtime;
  use compio_tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

  use super::*;

  /// 内存自签装配（rcgen 产 DER，零外部文件依赖）
  fn der_config(client_cert_required: bool, freq_secs: u64) -> io::Result<ServerTlsConfig> {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书生成");
    ServerTlsConfig::from_der(
      vec![CertificateDer::from(ck.cert.der().to_vec())],
      PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        ck.signing_key.serialized_der().to_vec(),
      )),
      client_cert_required,
      None,
      freq_secs,
    )
  }

  /// 刷新代际与在挂票据：非零周期在运行时外只记意图不拉起（false）；
  /// 室内拉起即锁代际，换代自增使旧代失配自退；宿主运行时析构清理在挂
  /// 任务时票据复位哨兵；周期为 0 恒无循环
  #[test]
  fn refresh_loop_epoch_and_ticket_lifecycle() {
    // 室外：refresh_freq_secs > 0 意图记录后拉起失败（false），未标记在挂
    let outdoor = der_config(false, 1).expect("装配");
    assert!(
      !outdoor.try_start_refresh_loop(),
      "运行时外拉起必须报 false"
    );
    {
      let state = outdoor.inner.state.lock();
      assert_eq!(state.refresh_epoch, 0);
      assert_eq!(state.running_epoch, NOT_RUNNING, "未拉起不得标记在挂");
    }

    // 室内：拉起成功且锁代际；换代后旧代票据不再回收新代位
    let probe = {
      let rt = Runtime::new().expect("运行时");
      let probe = rt.block_on(async {
        let config = der_config(false, 1).expect("装配");
        assert!(config.try_start_refresh_loop(), "室内拉起必须成功");
        {
          let state = config.inner.state.lock();
          assert_eq!(state.refresh_epoch, 0);
          assert_eq!(state.running_epoch, 0, "拉起即锁代际");
        }
        // 换代：refresh_epoch 自增使 0 代循环失配自退，新代接管 running_epoch
        config.inner.state.lock().refresh_epoch += 1;
        assert!(config.try_start_refresh_loop(), "换代后再拉起");
        {
          let state = config.inner.state.lock();
          assert_eq!(state.refresh_epoch, 1);
          assert_eq!(state.running_epoch, 1);
        }
        config
      });
      // 克隆共享同一 Inner，宿主运行时随本块析构后经探针核销票据复位
      probe.clone()
    };

    // 周期 0：无条件 true 且无状态迁移
    let inert = der_config(false, 0).expect("装配");
    assert!(inert.try_start_refresh_loop());
    {
      let state = inert.inner.state.lock();
      assert_eq!(state.running_epoch, NOT_RUNNING, "周期 0 不得挂循环");
    }

    // 宿主运行时已析构（块退出）：在挂任务连票据一并清理，代际复位哨兵，
    // 下次服务启动可重挂
    {
      let state = probe.inner.state.lock();
      assert_eq!(state.running_epoch, NOT_RUNNING, "运行时析构必须复位票据");
    }
  }

  /// 换证在途重读回灌竞态防御：
  /// 当 refresh_epoch 发生推进（换证换代）时，在途旧代 reload 丢弃本次重读结果不 store，
  /// 活跃证书保持新换证书，不被旧重读覆盖回灌
  #[test]
  fn reload_race_epoch_mismatch_drops_stale_cert() {
    let dir = tempfile::tempdir().expect("临时目录");
    let cert_a_path = dir.path().join("cert_a.pem");
    let key_a_path = dir.path().join("key_a.pem");
    let cert_b_path = dir.path().join("cert_b.pem");
    let key_b_path = dir.path().join("key_b.pem");

    let cert_a = rcgen::generate_simple_self_signed(vec!["server_a.com".into()]).expect("自签 A");
    let cert_b = rcgen::generate_simple_self_signed(vec!["server_b.com".into()]).expect("自签 B");

    write(&cert_a_path, cert_a.cert.pem()).expect("写 A 证书");
    write(&key_a_path, cert_a.signing_key.serialize_pem()).expect("写 A 私钥");
    write(&cert_b_path, cert_b.cert.pem()).expect("写 B 证书");
    write(&key_b_path, cert_b.signing_key.serialize_pem()).expect("写 B 私钥");

    let config =
      ServerTlsConfig::from_pem_files(&cert_a_path, &key_a_path, false, None, 10).expect("装配 A");

    let a_cert_der = cert_a.cert.der().to_vec();
    let b_cert_der = cert_b.cert.der().to_vec();

    assert_eq!(
      config.inner.resolver.0.load().cert[0].as_ref(),
      a_cert_der.as_slice()
    );

    // 切换至证书 B（refresh_epoch 推进为 1）
    config
      .update_cert_file(Some(&cert_b_path), Some(&key_b_path))
      .expect("更新至 B 证书");

    assert_eq!(
      config.inner.resolver.0.load().cert[0].as_ref(),
      b_cert_der.as_slice()
    );

    // 旧代 my_epoch = 0 尝试 reload
    config.inner.reload(0).expect("旧代 reload 应静默返回 Ok");

    // 断言活跃证书仍然是 B，未被旧代回灌
    assert_eq!(
      config.inner.resolver.0.load().cert[0].as_ref(),
      b_cert_der.as_slice(),
      "活跃证书必须保持为新证书 B，决不可被旧代重载回灌"
    );

    // 新代 my_epoch = 1 执行 reload 正常换装
    config.inner.reload(1).expect("新代 reload 应成功");
    assert_eq!(
      config.inner.resolver.0.load().cert[0].as_ref(),
      b_cert_der.as_slice()
    );
  }

  /// 验证在途重读回灌竞态：
  /// 若 reload 在换证前已读取旧路径证书 A，在 store 前发生 update_cert_file 切换至证书 B（推进 refresh_epoch），
  /// reload 必须核对代际失配并丢弃旧证书 A，防止旧证书 A 覆盖新证书 B
  #[test]
  fn reload_in_flight_stale_cert_dropped_on_epoch_mismatch() {
    let dir = tempfile::tempdir().expect("临时目录");
    let cert_a_path = dir.path().join("cert_a.pem");
    let key_a_path = dir.path().join("key_a.pem");
    let cert_b_path = dir.path().join("cert_b.pem");
    let key_b_path = dir.path().join("key_b.pem");

    let cert_a = rcgen::generate_simple_self_signed(vec!["server_a.com".into()]).expect("自签 A");
    let cert_b = rcgen::generate_simple_self_signed(vec!["server_b.com".into()]).expect("自签 B");

    write(&cert_a_path, cert_a.cert.pem()).expect("写 A 证书");
    write(&key_a_path, cert_a.signing_key.serialize_pem()).expect("写 A 私钥");
    write(&cert_b_path, cert_b.cert.pem()).expect("写 B 证书");
    write(&key_b_path, cert_b.signing_key.serialize_pem()).expect("写 B 私钥");

    let config =
      ServerTlsConfig::from_pem_files(&cert_a_path, &key_a_path, false, None, 10).expect("装配 A");

    let b_cert_der = cert_b.cert.der().to_vec();

    // 换证至 B
    config
      .update_cert_file(Some(&cert_b_path), Some(&key_b_path))
      .expect("更新至 B 证书");

    // 假设在途重读拿到的是 cert_a_path（模拟在换证前克隆出的旧路径）
    {
      let mut state = config.inner.state.lock();
      state.cert_path = cert_a_path.clone();
      state.key_path = key_a_path.clone();
      // 保持 refresh_epoch = 1（代表新代已生效）
    }

    // 在途旧循环（my_epoch = 0）调用 reload：读取 cert_a，但因 my_epoch(0) != state.refresh_epoch(1) 丢弃
    config.inner.reload(0).expect("旧代 reload 应静默返回 Ok");

    // 活跃证书必须仍然是 B，决不被 A 覆盖
    assert_eq!(
      config.inner.resolver.0.load().cert[0].as_ref(),
      b_cert_der.as_slice(),
      "在途重载旧证书必须被丢弃，活跃证书必须保持为 B"
    );
  }

  /// 第三态交错用例（并发迭代式，不对生产 API 做分段暴露）：旧代 reload 的
  /// 核对-换装临界区与 update_cert_file 的换装-换代临界区收敛于同一把 state
  /// 锁后，两种落位序终态均为新证——reload 先落位则被 update 新证覆盖，
  /// update 先落位则 reload 代际核对失配丢弃。逐轮 spawn 匹配代 reload 与
  /// 主线程 update 并发赛跑，join 后断言活跃证书恒为当轮新证
  #[test]
  fn concurrent_reload_update_race_always_lands_new_cert() {
    let dir = tempfile::tempdir().expect("临时目录");
    let cert_a_path = dir.path().join("cert_a.pem");
    let key_a_path = dir.path().join("key_a.pem");
    let cert_b_path = dir.path().join("cert_b.pem");
    let key_b_path = dir.path().join("key_b.pem");

    let cert_a = rcgen::generate_simple_self_signed(vec!["server_a.com".into()]).expect("自签 A");
    let cert_b = rcgen::generate_simple_self_signed(vec!["server_b.com".into()]).expect("自签 B");

    write(&cert_a_path, cert_a.cert.pem()).expect("写 A 证书");
    write(&key_a_path, cert_a.signing_key.serialize_pem()).expect("写 A 私钥");
    write(&cert_b_path, cert_b.cert.pem()).expect("写 B 证书");
    write(&key_b_path, cert_b.signing_key.serialize_pem()).expect("写 B 私钥");

    let config =
      ServerTlsConfig::from_pem_files(&cert_a_path, &key_a_path, false, None, 10).expect("装配 A");

    let a_cert_der = cert_a.cert.der().to_vec();
    let b_cert_der = cert_b.cert.der().to_vec();

    // A/B 交替换装逐轮赛跑：每轮 spawn 的 reload 代际恰为轮次（update 每轮
    // 自增一次）；主线程 yield 让渡调度以提高 reload 先落位序占比，两种序
    // 同受 join 后终态断言约束
    for round in 0..64u64 {
      let to_b = round % 2 == 0;
      let (target_path, target_key, target_der) = if to_b {
        (&cert_b_path, &key_b_path, &b_cert_der)
      } else {
        (&cert_a_path, &key_a_path, &a_cert_der)
      };

      let racer = {
        let config = config.clone();
        thread::spawn(move || config.inner.reload(round))
      };
      thread::yield_now();
      config
        .update_cert_file(Some(target_path), Some(target_key))
        .expect("换装");
      racer.join().expect("赛跑线程").expect("reload");

      assert_eq!(
        config.inner.resolver.0.load().cert[0].as_ref(),
        target_der.as_slice(),
        "第 {round} 轮 join 后活跃证书必须为当轮新证，回灌即竞态复活"
      );
    }
    assert_eq!(
      config.inner.state.lock().refresh_epoch,
      64,
      "每轮换装必须恰好自增一次代际"
    );
  }
}

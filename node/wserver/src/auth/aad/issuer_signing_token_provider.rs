//! AAD 签发者签名密钥供给器（对标 libs/server/Auth/Aad/IssuerSigningTokenProvider.cs）
//!
//! 密钥获取走 [`DocumentFetch`] 注入的文档获取器（对标 C#
//! HttpDocumentRetriever）：`{authority}/common/.well-known/openid-configuration`
//! → jwks_uri → JWKS。本工作区暂无 HTTP 客户端依赖，部署侧以真实传输
//! 注入即可闭环。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread::{self, JoinHandle},
  time::{Duration, Instant},
};

use jsonwebtoken::jwk::JwkSet;
use parking_lot::RwLock;
use thiserror::Error;

/// OpenID 配置地址模板（对标 C# OpenIdConfigurationAddressFormat）
const OPENID_CONFIGURATION_ADDRESS_FORMAT: &str =
  "{authority}/common/.well-known/openid-configuration";

/// 日刷新间隔（对标 C# TimeSpan.FromDays(1)）
const REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// 文档获取器错误
#[derive(Debug, Error)]
pub enum AadError {
  /// 文档获取失败
  #[error("document fetch failed: {0}")]
  Fetch(String),
  /// 文档解析失败
  #[error("document parse failed: {0}")]
  Parse(String),
}

/// 文档获取器（GET 返回文档字节；对标 C# HttpDocumentRetriever）
pub trait DocumentFetch: Send + Sync {
  /// 获取给定 URL 的文档内容
  fn get(&self, url: &str) -> Result<Vec<u8>, AadError>;
}

/// 空操作获取器（测试装配用）
#[cfg(test)]
struct NoopFetch;

#[cfg(test)]
impl DocumentFetch for NoopFetch {
  fn get(&self, _url: &str) -> Result<Vec<u8>, AadError> {
    Err(AadError::Fetch("noop".into()))
  }
}

/// OpenID 配置文档（仅取 jwks_uri）
#[derive(serde::Deserialize)]
struct OpenIdConfiguration {
  jwks_uri: String,
}

/// AAD 签名密钥供给器
pub struct IssuerSigningTokenProvider {
  /// AAD 机构地址
  authority: String,
  /// 当前签名密钥集
  signing_tokens: RwLock<Arc<JwkSet>>,
  /// 日刷线程（停刷标记置位后自行退出）
  refresh: RwLock<Option<JoinHandle<()>>>,
  /// 停刷标记（Drop 置位）
  stopped: Arc<AtomicBool>,
  /// 文档获取器
  fetch: Arc<dyn DocumentFetch>,
}

impl IssuerSigningTokenProvider {
  /// 创建供给器：同步取一次密钥，并按日启动自动刷新线程
  ///
  ///
  /// libs/server/Auth/Aad/IssuerSigningTokenProvider.cs:Create
  pub fn create(authority: &str, fetch: Arc<dyn DocumentFetch>) -> Result<Arc<Self>, AadError> {
    if authority.is_empty() {
      return Err(AadError::Fetch("Authority cannot be null".into()));
    }
    let signing_tokens = Self::retrieve_signing_tokens(authority, fetch.as_ref())?;
    let provider = Arc::new(Self {
      authority: authority.to_string(),
      signing_tokens: RwLock::new(Arc::new(signing_tokens)),
      refresh: RwLock::new(None),
      stopped: Arc::new(AtomicBool::new(false)),
      fetch,
    });

    // 日刷线程（C# Timer(RefreshSigningTokens, 0, 1d) 的线程化承接）；
    // 线程创建失败仅失去自动刷新，不影响既有密钥集
    let for_thread = Arc::clone(&provider);
    let thread_stopped = Arc::clone(&provider.stopped);
    if let Ok(handle) = thread::Builder::new()
      .name("aad-signing-tokens".into())
      .spawn(move || {
        let mut last = Instant::now();
        while !thread_stopped.load(Ordering::Relaxed) {
          thread::sleep(Duration::from_secs(1));
          if last.elapsed() >= REFRESH_INTERVAL {
            last = Instant::now();
            for_thread.refresh_signing_tokens();
          }
        }
      })
    {
      *provider.refresh.write() = Some(handle);
    }
    Ok(provider)
  }

  /// 测试装配：直接注入密钥集（跳过网络取钥与刷新线程）
  #[cfg(test)]
  pub fn create_for_test(signing_tokens: JwkSet) -> Arc<Self> {
    Arc::new(Self {
      authority: String::new(),
      signing_tokens: RwLock::new(Arc::new(signing_tokens)),
      refresh: RwLock::new(None),
      stopped: Arc::new(AtomicBool::new(true)),
      fetch: Arc::new(NoopFetch),
    })
  }

  /// 当前签名密钥集快照
  pub fn signing_tokens(&self) -> Arc<JwkSet> {
    Arc::clone(&self.signing_tokens.read())
  }

  /// 刷新签名密钥（失败仅记日志，保留既有密钥集）
  ///
  /// libs/server/Auth/Aad/IssuerSigningTokenProvider.cs:RefreshSigningTokens
  pub fn refresh_signing_tokens(&self) {
    match Self::retrieve_signing_tokens(&self.authority, self.fetch.as_ref()) {
      Ok(tokens) => *self.signing_tokens.write() = Arc::new(tokens),
      Err(err) => {
        log::error!("Failed to retrieve issuer signing tokens: {err}");
      }
    }
  }

  /// 从 AAD 机构拉取签名密钥：OpenID 配置 → jwks_uri → JWKS
  ///
  /// libs/server/Auth/Aad/IssuerSigningTokenProvider.cs:RetrieveSigningTokens
  pub fn retrieve_signing_tokens(
    authority: &str,
    fetch: &dyn DocumentFetch,
  ) -> Result<JwkSet, AadError> {
    let config_url = OPENID_CONFIGURATION_ADDRESS_FORMAT.replace("{authority}", authority);
    let config_doc = fetch.get(&config_url)?;
    let config: OpenIdConfiguration =
      sonic_rs::from_slice(&config_doc).map_err(|e| AadError::Parse(e.to_string()))?;
    let jwks_doc = fetch.get(&config.jwks_uri)?;
    sonic_rs::from_slice(&jwks_doc).map_err(|e| AadError::Parse(e.to_string()))
  }
}

impl Drop for IssuerSigningTokenProvider {
  /// 对标 C# Dispose（停刷新线程并释放密钥集引用）
  ///
  /// libs/server/Auth/Aad/IssuerSigningTokenProvider.cs:Dispose
  fn drop(&mut self) {
    // 停刷标记置位，刷新线程随即退出并释放引用
    self.stopped.store(true, Ordering::Relaxed);
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::AtomicUsize;

  use super::*;

  /// 内存文档获取器：openid 配置 → jwks 两跳
  struct MemFetch {
    openid: Vec<u8>,
    jwks: Vec<u8>,
    hits: AtomicUsize,
  }

  impl DocumentFetch for MemFetch {
    fn get(&self, url: &str) -> Result<Vec<u8>, AadError> {
      self.hits.fetch_add(1, Ordering::Relaxed);
      if url.ends_with("/.well-known/openid-configuration") {
        Ok(self.openid.clone())
      } else if url == "https://keys.example/keys" {
        Ok(self.jwks.clone())
      } else {
        Err(AadError::Fetch(format!("unexpected url {url}")))
      }
    }
  }

  fn fixtures() -> (Vec<u8>, Vec<u8>) {
    let openid = br#"{"jwks_uri":"https://keys.example/keys"}"#.to_vec();
    let jwks = br#"{"keys":[{"kty":"RSA","kid":"k1","n":"xYuC","e":"AQAB"}]}"#.to_vec();
    (openid, jwks)
  }

  /// 对标 C# RetrieveSigningTokens：OpenID 配置 → jwks_uri → JWKS
  #[test]
  fn retrieve_signing_tokens_follows_openid_flow() {
    let (openid, jwks) = fixtures();
    let fetch = MemFetch {
      openid,
      jwks,
      hits: AtomicUsize::new(0),
    };
    let set =
      IssuerSigningTokenProvider::retrieve_signing_tokens("https://login.example", &fetch).unwrap();
    assert_eq!(set.keys.len(), 1);
    assert_eq!(set.keys[0].common.key_id.as_deref(), Some("k1"));
    assert_eq!(fetch.hits.load(Ordering::Relaxed), 2);
  }

  /// 解析失败错误传播
  #[test]
  fn retrieve_signing_tokens_parse_errors() {
    let fetch = MemFetch {
      openid: b"not json".to_vec(),
      jwks: Vec::new(),
      hits: AtomicUsize::new(0),
    };
    assert!(matches!(
      IssuerSigningTokenProvider::retrieve_signing_tokens("https://login.example", &fetch),
      Err(AadError::Parse(_))
    ));

    let fetch = MemFetch {
      openid: br#"{"jwks_uri":"https://keys.example/keys"}"#.to_vec(),
      jwks: b"{}".to_vec(),
      hits: AtomicUsize::new(0),
    };
    assert!(matches!(
      IssuerSigningTokenProvider::retrieve_signing_tokens("https://login.example", &fetch),
      Err(AadError::Parse(_))
    ));
  }

  /// authority 为空报错（对标 C# Create 的空检查）
  #[test]
  fn create_rejects_empty_authority() {
    let (openid, jwks) = fixtures();
    let fetch: Arc<MemFetch> = Arc::new(MemFetch {
      openid,
      jwks,
      hits: AtomicUsize::new(0),
    });
    assert!(IssuerSigningTokenProvider::create("", fetch).is_err());
  }

  /// Create 取钥一次；RefreshSigningTokens 换新（密钥集引用更替）
  #[test]
  fn create_and_refresh_swaps_keys() {
    let (openid, jwks) = fixtures();
    let fetch: Arc<MemFetch> = Arc::new(MemFetch {
      openid,
      jwks,
      hits: AtomicUsize::new(0),
    });
    let provider =
      IssuerSigningTokenProvider::create("https://login.example", fetch.clone()).unwrap();
    assert_eq!(fetch.hits.load(Ordering::Relaxed), 2); // create 仅取一次
    assert_eq!(provider.signing_tokens().keys.len(), 1);

    // 换钥后 refresh：密钥集更替为新实例
    let jwk = provider.signing_tokens();
    provider.refresh_signing_tokens();
    let jwk2 = provider.signing_tokens();
    assert!(!Arc::ptr_eq(&jwk, &jwk2));
    assert_eq!(jwk2.keys.len(), 1);
    assert_eq!(fetch.hits.load(Ordering::Relaxed), 4);

    // Drop 后停刷标记置位
    drop(provider);
  }
}

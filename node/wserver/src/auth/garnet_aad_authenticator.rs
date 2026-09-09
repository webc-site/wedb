//! AAD（Microsoft Entra）令牌认证器（对标 libs/server/Auth/GarnetAadAuthenticator.cs）
//!
//! 令牌验证：RS256 签名 + 签发者 / 受众（签名密钥来自
//! [`IssuerSigningTokenProvider`]）；授权决策：应用主体（appidacr）→
//! 应用白名单（appid）→（可选）用户名与 OID / 组声明匹配；有效窗口
//! （nbf..exp）随每次 IsAuthorized 重新校验。

use std::{collections::HashMap, sync::Arc};

use gxhash::HashSet;
use jsonwebtoken::{
  Algorithm, DecodingKey, Validation, decode, decode_header,
  errors::{Error as JwtError, ErrorKind, new_error},
};
use serde::Deserialize;

use super::{
  aad::issuer_signing_token_provider::IssuerSigningTokenProvider, ascii_sanitize,
  i_garnet_authenticator::IGarnetAuthenticator,
};

/// appidacr 声明（客户端认证方式：公开客户端 0 / 机密 1 / 证书 2）
const APP_ID_ACR_CLAIM: &str = "appidacr";
/// scope 声明
const SCOPE_CLAIM: &str = "http://schemas.microsoft.com/identity/claims/scope";
/// appid 声明
const APP_ID_CLAIM: &str = "appid";
/// 对象标识声明
const OID_CLAIM: &str = "http://schemas.microsoft.com/identity/claims/objectidentifier";
/// 组声明
const GROUPS_CLAIM: &str = "groups";

/// AAD 令牌声明面（仅提取认证决策所需字段）
#[derive(Deserialize, Default)]
struct AadClaims {
  #[serde(default)]
  appidacr: String,
  #[serde(default)]
  appid: String,
  #[serde(default, rename = "http://schemas.microsoft.com/identity/claims/scope")]
  scope: String,
  #[serde(
    default,
    rename = "http://schemas.microsoft.com/identity/claims/objectidentifier"
  )]
  oid: String,
  #[serde(default, deserialize_with = "string_or_vec")]
  groups: Vec<String>,
  #[serde(default)]
  nbf: Option<u64>,
  #[serde(default)]
  exp: Option<u64>,
}

/// 单串或数组统一折为 Vec（AAD groups 声明两种形态都出现）
fn string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
  D: serde::Deserializer<'de>,
{
  #[derive(Deserialize)]
  #[serde(untagged)]
  enum StrOrVec {
    Str(String),
    Vec(Vec<String>),
  }
  match StrOrVec::deserialize(deserializer) {
    Ok(StrOrVec::Str(s)) => Ok(s.split(',').map(str::to_string).collect()),
    Ok(StrOrVec::Vec(v)) => Ok(v),
    Err(_) => Ok(Vec::new()),
  }
}

/// AAD 令牌认证器
pub struct GarnetAadAuthenticator {
  /// 白名单应用 ID（小写规范，忽略大小写匹配）
  authorized_app_ids: HashSet<String>,
  /// 合法受众
  audiences: HashSet<String>,
  /// 合法签发者
  issuers: HashSet<String>,
  /// 签名密钥供给器
  signing_token_provider: Arc<IssuerSigningTokenProvider>,
  /// 是否校验用户名（OID / 组声明须匹配 AUTH 用户名）
  validate_username: bool,
  /// 当前时刻供给（.NET 刻度：自 0001-01-01 起的 100ns 单位）
  now: Box<dyn Fn() -> i64 + Send + Sync>,
  /// 令牌授权决策结果
  authorized: bool,
  /// 有效窗口（热路径比较用刻度投影）
  valid_from_ticks: i64,
  valid_to_ticks: i64,
}

/// 1970-01-01 与 0001-01-01 间的刻度差
const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;

/// unix 秒折 .NET 刻度
#[inline]
fn unix_seconds_to_ticks(seconds: u64) -> i64 {
  seconds as i64 * 10_000_000 + UNIX_EPOCH_TICKS
}

/// 默认时钟（coarsetime 粗粒度 UTC）
fn default_now_ticks() -> i64 {
  coarsetime::Clock::now_since_epoch().as_u64() as i64 / 100 + UNIX_EPOCH_TICKS
}

impl GarnetAadAuthenticator {
  /// 构造
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    authorized_app_ids: HashSet<String>,
    audiences: HashSet<String>,
    issuers: HashSet<String>,
    signing_token_provider: Arc<IssuerSigningTokenProvider>,
    validate_username: bool,
  ) -> Self {
    Self::with_clock(
      authorized_app_ids,
      audiences,
      issuers,
      signing_token_provider,
      validate_username,
      Box::new(default_now_ticks),
    )
  }

  /// 带自定义时钟构造（对标 C# TimeProvider 测试注入位）
  pub fn with_clock(
    authorized_app_ids: HashSet<String>,
    audiences: HashSet<String>,
    issuers: HashSet<String>,
    signing_token_provider: Arc<IssuerSigningTokenProvider>,
    validate_username: bool,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
  ) -> Self {
    Self {
      authorized_app_ids,
      audiences,
      issuers,
      signing_token_provider,
      validate_username,
      now,
      authorized: false,
      valid_from_ticks: i64::MAX,
      valid_to_ticks: i64::MIN,
    }
  }

  /// 验证并授权 AAD 令牌（口令位携带令牌，用户名位携带 OID / 组）
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:Authenticate
  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    match self.validate_token(password, username) {
      Ok((valid_from, valid_to, authorized)) => {
        self.valid_from_ticks = valid_from;
        self.valid_to_ticks = valid_to;
        self.authorized = authorized;
        log::info!(
          "Authentication successful. Token valid within window [{valid_from}, {valid_to}]"
        );
        self.is_authorized()
      }
      Err(err) => {
        // 验证失败：清空既有授权状态
        self.authorized = false;
        self.valid_from_ticks = i64::MAX;
        self.valid_to_ticks = i64::MIN;
        log::error!("Authentication failed: {err}");
        false
      }
    }
  }

  /// 令牌验证 + 授权决策；成功返回 (nbf 刻度, exp 刻度, 授权结果)
  fn validate_token(&self, token: &[u8], username: &[u8]) -> Result<(i64, i64, bool), JwtError> {
    let keys = self.signing_token_provider.signing_tokens();

    // 按头部 kid 匹配签名密钥后整体验证（签名 / 签发者 / 受众 / 生命期）
    let header = decode_header(token)?;
    let validation = self.build_validation()?;
    let mut claims = None;
    let mut matched_any_key = false;
    let mut last_err = new_error(ErrorKind::InvalidSignature);
    for jwk in keys.keys.iter() {
      // kid 存在则先按 kid 过滤
      if let Some(kid) = &header.kid
        && let Some(jwk_kid) = &jwk.common.key_id
        && kid != jwk_kid
      {
        continue;
      }
      let Ok(key) = DecodingKey::from_jwk(jwk) else {
        continue;
      };
      matched_any_key = true;
      match decode::<AadClaims>(token, &key, &validation) {
        Ok(data) => {
          claims = Some(data.claims);
          break;
        }
        Err(err) => last_err = err,
      }
    }
    // 无可用密钥或全部验证失败（保留最后一次真实错误）
    let claims = if matched_any_key {
      claims.ok_or(last_err)?
    } else {
      return Err(new_error(ErrorKind::InvalidKeyFormat));
    };

    // 有效窗口（缺省与 C# JwtSecurityToken.ValidFrom/ValidTo 缺省一致）
    let valid_from = claims
      .nbf
      .map(unix_seconds_to_ticks)
      .unwrap_or(UNIX_EPOCH_TICKS);
    let valid_to = claims.exp.map(unix_seconds_to_ticks).unwrap_or(i64::MAX);

    let authorized = self.is_identity_authorized(&claims, username);
    Ok((valid_from, valid_to, authorized))
  }

  /// 构建令牌验证参数（对标 C# TokenValidationParameters + 5 分钟钟偏）
  fn build_validation(&self) -> Result<Validation, JwtError> {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.leeway = 300;
    validation.validate_nbf = true;
    validation.set_audience(
      &self
        .audiences
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>(),
    );
    validation.set_issuer(&self.issuers.iter().map(String::as_str).collect::<Vec<_>>());
    Ok(validation)
  }

  /// 令牌声明是否满足授权（应用主体 + 应用白名单，可选用户名校验）
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:IsIdentityAuthorized
  fn is_identity_authorized(&self, claims: &AadClaims, username: &[u8]) -> bool {
    let claims_map = self.claims_map(claims);
    let is_valid =
      self.is_application_principal(&claims_map) && self.is_application_authorized(&claims_map);
    if !self.validate_username {
      is_valid
    } else {
      is_valid && self.is_user_name_authorized(&claims_map, username)
    }
  }

  /// 声明折叠为按名映射（多值以逗号连接，对标 C# GroupBy + Join）
  fn claims_map(&self, claims: &AadClaims) -> HashMap<&str, String> {
    let mut map = HashMap::new();
    map.insert(APP_ID_ACR_CLAIM, claims.appidacr.clone());
    map.insert(SCOPE_CLAIM, claims.scope.clone());
    map.insert(APP_ID_CLAIM, claims.appid.clone());
    map.insert(OID_CLAIM, claims.oid.clone());
    map.insert(GROUPS_CLAIM, claims.groups.join(","));
    map
  }

  /// appid 是否在白名单
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:IsApplicationAuthorized
  fn is_application_authorized(&self, claims: &HashMap<&str, String>) -> bool {
    claims.get(APP_ID_CLAIM).is_some_and(|app_id| {
      self
        .authorized_app_ids
        .iter()
        .any(|a| a.eq_ignore_ascii_case(app_id))
    })
  }

  /// 用户名是否获 OID 或组声明授权（先 OID 后全部组）
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:IsUserNameAuthorized
  fn is_user_name_authorized(&self, claims: &HashMap<&str, String>, username: &[u8]) -> bool {
    let user_name = ascii_sanitize(username);
    if let Some(oid) = claims.get(OID_CLAIM)
      && oid.eq_ignore_ascii_case(&user_name)
    {
      return true;
    }
    if let Some(groups) = claims.get(GROUPS_CLAIM) {
      // 多值声明以逗号连接，逐组比对
      if groups
        .split(',')
        .any(|g| g.eq_ignore_ascii_case(&user_name))
      {
        return true;
      }
    }
    false
  }

  /// 授权且当前时刻落在有效窗口内
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:IsAuthorized
  fn is_authorized(&self) -> bool {
    let now_ticks = (self.now)();
    self.authorized && now_ticks >= self.valid_from_ticks && now_ticks <= self.valid_to_ticks
  }

  /// 是否应用主体：appidacr > "0" 且无 delegated scope
  ///
  /// libs/server/Auth/GarnetAadAuthenticator.cs:IsApplicationPrincipal
  fn is_application_principal(&self, claims: &HashMap<&str, String>) -> bool {
    // appidacr 指示客户端认证方式：公开客户端 0，客户端机密 1，证书 2
    if let Some(app_id_acr) = claims.get(APP_ID_ACR_CLAIM)
      && !app_id_acr.is_empty()
      && app_id_acr.as_str() > "0"
    {
      return !claims
        .get(SCOPE_CLAIM)
        .is_some_and(|scope| !scope.trim().is_empty());
    }
    false
  }
}

impl IGarnetAuthenticator for GarnetAadAuthenticator {
  fn is_authenticated(&self) -> bool {
    self.is_authorized()
  }

  fn can_authenticate(&self) -> bool {
    true
  }

  fn has_acl_support(&self) -> bool {
    false
  }

  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    Self::authenticate(self, password, username)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
  };

  use gxhash::{HashSet, HashSetExt};
  use jsonwebtoken::{
    Algorithm, EncodingKey, Header, encode,
    jwk::{AlgorithmParameters, Jwk, JwkSet},
  };
  use serde::Serialize;

  use super::{super::aad::issuer_signing_token_provider::IssuerSigningTokenProvider, *};

  /// 测试签发者（公共标识，非机密）
  const ISSUER: &str = "https://sts.windows.net/975f013f-7f24-47e8-a7d3-abc4752bf346/";

  /// 测试专用 RSA 私钥（PEM，仅测试夹具）
  const TEST_RSA_PEM: &str = r###"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDFi4I2e5TRXGkV
MF6d3Hu42+k1XM8rA6XUMvZEB86VEMHe7xSyR7UoU/Aae7EJDa+SQ1skTp49h1In
LO5nW+aAUa8FAcz0kCchpgOz9qqC/aabFuFb42m+/z06N3CfZ3en8x+rNBWDofBc
rqdoTn59f2plQfJGf42Ld7Yy3XsGb/lOfBhshLM9wYOtYSWM8BhZK4SZNlS2AKO9
OL5JnENZtFsY7zfTGZ1LEMXdgEQUwEnN6om/KOzIwHdLjN3gRwWJ6AFFBtpAZHv4
4ujpYuJ8krNsUpuHrgAaxhxeuDBVGsdHCrqp81HC20L+Lpu6M2LjyvF/YHOs8NO1
mYv7H2LfAgMBAAECggEAAdJiWXCuU95cToiE3UPWFRBbu26nSNt4J6RvE2JXDDAW
LNSO0xriMTfRwQzFYMxlg1EBTpM2PMigs1SXJkQTGuyl7PZLsGQb8eCyeNhpHigk
smcPN2YgTyI5Ee5gKLgvKR3CFWjqweBrQELEFhKTP0KlKPKz8n5+pnvS8rxXXbad
scBgNMcqklqdOIkyxX9/QLxwrJGEscXOKY1av028o6iofCArFNQItUzQb4EyIyjG
C3EOr6+D2NcieYxTBqeM/5086+pFU8fiAY7lLs3dQH8S1ODiKki4ACsPAeAOgtJj
26RyK+H7WX0WO9h9EuGE83lhlRpMQ7eeM/NCyPbOcQKBgQDvACdNfHdORAL6++ir
x1cv8gwmsNhV4HjnVWQYZVYxyU/HkG72Z6qlBkUtegCwLK/NT6872HMxnQewqGJC
wVVIURRDwlMqHEdYJBjBdey8H6XO0z1y8i4zj3Pf7BXWrYfdLRtc91tviHYpNnfb
RV506dWf+gFWEejvelFyPmPAVQKBgQDTmIJiwumVIDWmXRbXgeIesHGeM1cxHVLn
NAWnegJStZ/7vGgxv9ezbPtQwa9WbK0zuAv7E7EECsbKE1NdCS7g/D4UyK1mwi6u
UXFmmqnM7HWQahp6Fz4cecK+gkdgDzf8C+nal+3EojyOdRFX2Y6wkBjJ8jFM3leO
AhLtwtX6YwKBgBJWDK3hMrYKecAPu5YSQRook84UG0TbZ+s3nepRO/+t1jUVW64D
3g+zH3ORvKPHieVSi7snrA7OYVyoToGYm5Zkwg1Ax2ifqsmUW2HcTkFQnEP16Oz8
r5a9FsLDNhm2Toolf8fuIlG4Qnh++UsVU13c4srXP2r/DMPyunTtXHHRAoGAFW2z
fqb4mtaZOFDjGE4aBTufNgNDCEFM/Qcir3RGufOU0RyifK9N++jEhsgmppz46Wzx
Ias72QnyWdKdNaXRbo19CAPOs8JkkdjDoD13hr5HpalpJwYljyKxmzN8YPkJlWfr
dey+8pPviAnJCNmKfs0JBaKWZY961HHXUubRXrsCgYEA7fSa1kVoA3xus7jQpl9z
b99qmGE/UdqP6/3ac14vF04/EumBvje7IAX6+QakPhW5xUHyMS/peY3xAvImsxF2
H+j5IdXsDowUFUJwKMn9z7vj/FvcTStKHF66nO4pg6Ur5jpRtz/cjFXNh4kPcRgv
eK9BrqY1JsNGGskNv57e0RI=
-----END PRIVATE KEY-----
"###;

  /// 可推进的假时钟（对标 C# FakeTimeProvider）
  struct FakeClock {
    ticks: AtomicI64,
  }

  /// 铸币参数（mint_token 成组入参）
  struct Mint<'a> {
    app_id_acr: &'a str,
    app_id: &'a str,
    oid: &'a str,
    groups: &'a [&'a str],
    nbf: i64,
    exp: i64,
    scope: Option<&'a str>,
    aud: &'a str,
  }

  /// 测试令牌声明面
  #[derive(Serialize)]
  struct MintedClaims<'a> {
    appidacr: &'a str,
    appid: &'a str,
    #[serde(rename = "http://schemas.microsoft.com/identity/claims/objectidentifier")]
    oid: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<Vec<&'a str>>,
    #[serde(
      rename = "http://schemas.microsoft.com/identity/claims/scope",
      skip_serializing_if = "Option::is_none"
    )]
    scope: Option<&'a str>,
    nbf: i64,
    exp: i64,
    iat: i64,
    iss: &'a str,
    aud: &'a str,
  }

  /// 签发测试令牌（RS256）
  fn mint_token(m: Mint<'_>) -> String {
    let claims = MintedClaims {
      appidacr: m.app_id_acr,
      appid: m.app_id,
      oid: m.oid,
      groups: (!m.groups.is_empty()).then(|| m.groups.to_vec()),
      scope: m.scope,
      nbf: m.nbf,
      exp: m.exp,
      iat: m.nbf,
      iss: ISSUER,
      aud: m.aud,
    };
    encode(
      &Header::new(Algorithm::RS256),
      &claims,
      &EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).unwrap(),
    )
    .unwrap()
  }

  /// 签名私钥（含派生公钥组件）
  fn encoding_key() -> EncodingKey {
    EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).unwrap()
  }

  /// 内存 JWKS 供给器（从签名私钥派生公钥 JWK，保证配对）
  fn provider() -> Arc<IssuerSigningTokenProvider> {
    let jwk = Jwk::from_encoding_key(&encoding_key(), Algorithm::RS256).unwrap();
    let jwks = JwkSet { keys: vec![jwk] };
    IssuerSigningTokenProvider::create_for_test(jwks)
  }

  /// 认证器装配（授权应用 / 受众 / 签发者 + 假时钟）
  fn build(clock: Arc<FakeClock>, validate_username: bool, app_id: &str) -> GarnetAadAuthenticator {
    let mut authorized = HashSet::new();
    authorized.insert(app_id.to_string());
    let mut audiences = HashSet::new();
    audiences.insert("test-audience".to_string());
    let mut issuers = HashSet::new();
    issuers.insert(ISSUER.to_string());
    let clock_ticks = Arc::clone(&clock);
    GarnetAadAuthenticator::with_clock(
      authorized,
      audiences,
      issuers,
      provider(),
      validate_username,
      Box::new(move || clock_ticks.ticks.load(Ordering::Relaxed)),
    )
  }

  /// unix 秒 → .NET 刻度
  fn ticks(unix_seconds: i64) -> i64 {
    unix_seconds * 10_000_000 + 621_355_968_000_000_000
  }

  /// 真实当前 unix 秒（jsonwebtoken 生命期校验走真实墙钟）
  fn real_now() -> i64 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_secs() as i64
  }

  /// 对标 garnet GarnetAadAuthenticatorTests.Authenticate_ReturnsTrue_WhenTokenInsideValidityWindow
  #[test]
  fn authenticate_true_inside_validity_window() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(token.as_bytes(), b"oid-1"));
    assert!(auth.is_authenticated());
  }

  /// 对标 garnet IsAuthenticated_GoesFalse_AfterTokenExpires
  #[test]
  fn is_authenticated_false_after_expiry() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 300,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(token.as_bytes(), b"oid-1"));
    assert!(auth.is_authenticated());

    // 推进时钟越过 exp（超出 5 分钟钟偏容忍）
    clock.ticks.store(ticks(now + 302), Ordering::Relaxed);
    assert!(!auth.is_authenticated());
  }

  /// 对标 garnet IsAuthenticated_StaysTrue_WhileTimeStillInsideWindow
  #[test]
  fn is_authenticated_stays_true_inside_window() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now,
      exp: now + 3600,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(token.as_bytes(), b"oid-1"));
    for step in 1..=5 {
      clock
        .ticks
        .store(ticks(now + step * 600), Ordering::Relaxed);
      assert!(auth.is_authenticated(), "should hold at step {step}");
    }
  }

  /// 对标 garnet Authenticate_ReturnsFalse_OnMalformedToken
  #[test]
  fn authenticate_false_on_malformed_token() {
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(1_700_000_000)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");
    assert!(!auth.authenticate(b"not.a.valid.jwt", b"user"));
    assert!(!auth.is_authenticated());
  }

  /// 对标 garnet FailedReauthentication_ClearsPreviouslyAuthorizedState
  #[test]
  fn failed_reauth_clears_state() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let good = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(good.as_bytes(), b"oid-1"));

    // 再以垃圾令牌认证：授权状态被清空
    assert!(!auth.authenticate(b"garbage", b"oid-1"));
    assert!(!auth.is_authenticated());
  }

  /// 应用主体校验：公开客户端（appidacr=0）与带 scope 的主体拒绝
  #[test]
  fn application_principal_rules() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    // appidacr=0（公开客户端）→ 拒绝
    let public_client = mint_token(Mint {
      app_id_acr: "0",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(!auth.authenticate(public_client.as_bytes(), b"oid-1"));

    // 主体带 delegated scope → 拒绝
    let scoped = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: Some("user_impersonation"),
      aud: "test-audience",
    });
    assert!(!auth.authenticate(scoped.as_bytes(), b"oid-1"));

    // 空白 scope 等价缺失 → 放行
    let blank_scope = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: Some("  "),
      aud: "test-audience",
    });
    assert!(auth.authenticate(blank_scope.as_bytes(), b"oid-1"));
  }

  /// appid 白名单：非白名单应用拒绝
  #[test]
  fn unauthorized_app_rejected() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-2",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(!auth.authenticate(token.as_bytes(), b"oid-1"));
    assert!(!auth.is_authenticated());
  }

  /// 用户名校验：OID 与组声明（大小写不敏感）
  #[test]
  fn username_via_oid_or_groups() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    // OID 命中（忽略大小写）
    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "OID-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(token.as_bytes(), b"oid-1"));

    // 组命中
    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &["group-a", "group-b"],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(auth.authenticate(token.as_bytes(), b"GROUP-B"));

    // 都不命中
    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &["group-a"],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(!auth.authenticate(token.as_bytes(), b"oid-9"));

    // 关闭用户名校验：用户名不匹配也放行
    let mut lax = build(Arc::clone(&clock), false, "app-1");
    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(lax.authenticate(token.as_bytes(), b"someone-else"));
  }

  /// 受众不符拒绝（jsonwebtoken 验证层）
  #[test]
  fn wrong_audience_rejected() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "other-audience",
    });
    assert!(!auth.authenticate(token.as_bytes(), b"oid-1"));
  }

  /// 签名密钥不匹配（JWKS 公钥被篡改）拒绝
  #[test]
  fn tampered_signing_key_rejected() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut authorized = HashSet::new();
    authorized.insert("app-1".to_string());
    let mut audiences = HashSet::new();
    audiences.insert("test-audience".to_string());
    let mut issuers = HashSet::new();
    issuers.insert(ISSUER.to_string());

    // 私钥派生的公钥 JWK 走序列化往返后篡改模数首字符 → 与私钥不配对
    let mut jwk = Jwk::from_encoding_key(&encoding_key(), Algorithm::RS256).unwrap();
    if let AlgorithmParameters::RSA(ref mut params) = jwk.algorithm {
      let mut tampered = String::from("0");
      tampered.push_str(&params.n[1..]);
      params.n = tampered;
    }
    let clock_ticks = Arc::clone(&clock);
    let mut auth = GarnetAadAuthenticator::with_clock(
      authorized,
      audiences,
      issuers,
      IssuerSigningTokenProvider::create_for_test(JwkSet { keys: vec![jwk] }),
      true,
      Box::new(move || clock_ticks.ticks.load(Ordering::Relaxed)),
    );

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now - 60,
      exp: now + 600,
      scope: None,
      aud: "test-audience",
    });
    assert!(!auth.authenticate(token.as_bytes(), b"oid-1"));
    assert!(!auth.is_authenticated());
  }

  /// 令牌有效期未开始（nbf 在未来，超出钟偏）拒绝
  #[test]
  fn token_not_yet_valid_rejected() {
    let now = real_now();
    let clock = Arc::new(FakeClock {
      ticks: AtomicI64::new(ticks(now)),
    });
    let mut auth = build(Arc::clone(&clock), true, "app-1");

    let token = mint_token(Mint {
      app_id_acr: "1",
      app_id: "app-1",
      oid: "oid-1",
      groups: &[],
      nbf: now + 3600,
      exp: now + 7200,
      scope: None,
      aud: "test-audience",
    });
    assert!(!auth.authenticate(token.as_bytes(), b"oid-1"));
  }
}

//! 认证器通用语义测试（对标 test/standalone/Garnet.test.acl/Resp/GarnetAuthenticatorTests.cs）
//!
//! C# 以 Mock IGarnetAuthenticator 挂到 GarnetServer 验证：会话构造即认证
//! （RespServerSession 构造调用 AuthenticateUser）、认证失效后命令按
//! CheckACLPermissions 拒绝并回 NOAUTH、AUTH 重新认证后恢复。
//! rust 侧会话域门禁接线在 wnode，本档对齐认证器本体语义与 NOAUTH 错误口径。

use wacl::IGarnetAuthenticator;
use wresp::cmd_strings;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 模拟认证器（对标 C# GarnetAuthenticatorTests.cs:MockAuthenticator）
///
/// C# 测试经 MockAuthenticationSettings 注入；认证回调内部直接置位
/// IsAuthenticated，rust 侧等价承接为「回调通过即置位已认证」。
struct MockAuthenticator<F>
where
  F: FnMut(&[u8], &[u8]) -> bool + Send,
{
  is_authenticated: bool,
  can_authenticate: bool,
  has_acl_support: bool,
  authenticate_callback: F,
}

impl<F> IGarnetAuthenticator for MockAuthenticator<F>
where
  F: FnMut(&[u8], &[u8]) -> bool + Send,
{
  fn is_authenticated(&self) -> bool {
    self.is_authenticated
  }

  fn can_authenticate(&self) -> bool {
    self.can_authenticate
  }

  fn has_acl_support(&self) -> bool {
    self.has_acl_support
  }

  fn authenticate(&mut self, password: &[u8], username: &[u8]) -> bool {
    if (self.authenticate_callback)(password, username) {
      // 对标 C# 回调体：auth.IsAuthenticated = true; return true;
      self.is_authenticated = true;
      true
    } else {
      false
    }
  }
}

/// C# GarnetAuthenticatorTests.cs:InvalidatingAuthorizationAsync
#[test]
fn invalidating_authorization() {
  let mut auth = MockAuthenticator {
    is_authenticated: false,
    can_authenticate: true,
    has_acl_support: false,
    authenticate_callback: |_password, _username| true,
  };

  // 会话构造即认证：等价 C# 首个 PING 放行（mock 认证恒成功 → 已认证）
  assert!(auth.authenticate(b"bar", b""));
  assert!(auth.is_authenticated());

  // 认证失效后命令应被拒：未认证会话按 C# CheckACLPermissions 走
  // RESP_ERR_NOAUTH（`NOAUTH Authentication required.`）错误口径
  auth.is_authenticated = false;
  assert!(!auth.is_authenticated());
  assert_eq!(
    cmd_strings::RESP_ERR_NOAUTH,
    "NOAUTH Authentication required."
  );

  // 重新 AUTH：等价 C# `AUTH bar` 后 PING 恢复
  assert!(auth.authenticate(b"bar", b""));
  assert!(auth.is_authenticated());
}

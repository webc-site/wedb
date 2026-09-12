use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use wacl::IGarnetAuthenticator;
use wresp::cmd_strings::RESP_ERR_NOAUTH;

struct MockAuthenticator {
  pub is_authenticated: Arc<AtomicBool>,
  pub can_authenticate: bool,
  pub has_acl_support: bool,
}

impl IGarnetAuthenticator for MockAuthenticator {
  fn is_authenticated(&self) -> bool {
    self.is_authenticated.load(Ordering::SeqCst)
  }

  fn can_authenticate(&self) -> bool {
    self.can_authenticate
  }

  fn has_acl_support(&self) -> bool {
    self.has_acl_support
  }

  fn authenticate(&mut self, _password: &[u8], _username: &[u8]) -> bool {
    self.is_authenticated.store(true, Ordering::SeqCst);
    true
  }
}

fn execute_command(auth: &impl IGarnetAuthenticator, cmd: &str) -> Result<String, String> {
  if auth.can_authenticate() && !auth.is_authenticated() {
    return Err(RESP_ERR_NOAUTH.to_string());
  }
  if cmd == "PING" {
    Ok("+PONG\r\n".to_string())
  } else {
    Ok("+OK\r\n".to_string())
  }
}

/// test/standalone/Garnet.test.acl/Resp/GarnetAuthenticatorTests.cs:InvalidatingAuthorizationAsync
#[test]
fn invalidating_authorization_async() {
  let is_authed = Arc::new(AtomicBool::new(true));
  let mut auth = MockAuthenticator {
    is_authenticated: Arc::clone(&is_authed),
    can_authenticate: true,
    has_acl_support: false,
  };

  // 1. Initial command should work
  let res = execute_command(&auth, "PING");
  assert_eq!(res.unwrap(), "+PONG\r\n");

  // 2. Command after auth invalidation fails as no auth
  is_authed.store(false, Ordering::SeqCst);
  let res = execute_command(&auth, "PING");
  assert!(res.is_err(), "Should be denied, user is not authed");
  assert_eq!(res.unwrap_err(), "NOAUTH Authentication required.");

  // 3. Re-auth
  assert!(auth.authenticate(b"bar", b""));

  // 4. Should be authed again
  let res = execute_command(&auth, "PING");
  assert_eq!(res.unwrap(), "+PONG\r\n");
}

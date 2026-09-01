use hipstr::HipStr;
use wedb_embed::is_default_namespace;

use crate::types::Cmd;

/// 连接上下文（包含当前数据库编号、命名空间、事务状态与鉴权/管理员状态，对标 Apache Kvrocks Connection）
#[derive(Debug, Clone)]
pub struct ConnectionContext {
  pub db: u64,
  pub namespace: HipStr<'static>,
  pub authenticated: bool,
  pub is_admin: bool,
  pub in_multi: bool,
  pub multi_queue: Vec<Cmd>,
  pub watched_keys: Vec<HipStr<'static>>,
  pub is_readonly: bool,
  pub is_asking: bool,
}

impl Default for ConnectionContext {
  #[inline]
  fn default() -> Self {
    Self {
      db: 0,
      namespace: HipStr::borrowed("default"),
      authenticated: false,
      is_admin: false,
      in_multi: false,
      multi_queue: Vec::new(),
      watched_keys: Vec::new(),
      is_readonly: false,
      is_asking: false,
    }
  }
}

impl ConnectionContext {
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  #[inline]
  pub fn set_db(&mut self, db: u64) {
    self.db = db;
  }

  #[inline]
  pub fn set_namespace(&mut self, ns: impl Into<HipStr<'static>>) {
    self.namespace = ns.into();
    self.db = 0;
  }

  /// 获取当前在当前租户命名空间下的组合命名空间
  /// - 若 namespace 为 "default" 且 db == 0: "default"
  /// - 若 namespace 为 "default" 且 db > 0: "db{db}"
  /// - 若 namespace 为其他且 db == 0: "{namespace}"
  /// - 若 namespace 为其他且 db > 0: "{namespace}:db{db}"
  #[inline]
  pub fn key_composer(&self) -> wedb_embed::KeyComposer<'_> {
    wedb_embed::KeyComposer::new_with_db(&self.namespace, self.db)
  }

  pub fn current_namespace(&self) -> HipStr<'static> {
    let db = self.db;
    if db == 0 {
      self.namespace.clone()
    } else if is_default_namespace(&self.namespace) {
      HipStr::from(format!("db{db}"))
    } else {
      let ns = &self.namespace;
      HipStr::from(format!("{ns}:db{db}"))
    }
  }

  #[inline]
  pub fn become_admin(&mut self) {
    self.authenticated = true;
    self.is_admin = true;
  }

  #[inline]
  pub fn become_user(&mut self) {
    self.authenticated = true;
    self.is_admin = false;
  }

  #[inline]
  pub fn is_admin(&self) -> bool {
    self.is_admin
  }

  #[inline]
  pub fn reset_multi(&mut self) {
    self.in_multi = false;
    self.multi_queue.clear();
    self.watched_keys.clear();
  }
}

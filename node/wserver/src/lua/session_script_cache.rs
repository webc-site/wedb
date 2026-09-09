//! 会话脚本缓存：SHA1 摘要 → 已编译函数的会话级映射
//! （对标 libs/server/Lua/SessionScriptCache.cs:SessionScriptCache）。

use std::{collections::HashMap, mem};

use super::script_hash_key::ScriptHashKey;

/// 会话脚本缓存。
#[derive(Default)]
pub struct SessionScriptCache {
  /// 已编译脚本的摘要集。
  scripts: HashMap<ScriptHashKey, Vec<u8>>,
  /// 正在运行的脚本（引用计数语义：Start/Stop 配对）。
  running: HashMap<ScriptHashKey, u32>,
  /// 关联的用户句柄（ACL 场景）。
  user_handle: Option<u64>,
  /// 超时请求标记（脚本超时中断后置位）。
  timeout_requested: bool,
}

impl SessionScriptCache {
  /// libs/server/Lua/SessionScriptCache.cs:SetUserHandle
  pub fn set_user_handle(&mut self, user_handle: Option<u64>) {
    self.user_handle = user_handle;
  }

  /// 关联的用户句柄。
  pub fn user_handle(&self) -> Option<u64> {
    self.user_handle
  }

  /// libs/server/Lua/SessionScriptCache.cs:StartRunningScript
  pub fn start_running_script(&mut self, hash: &ScriptHashKey) {
    *self.running.entry(hash.clone()).or_insert(0) += 1;
  }

  /// libs/server/Lua/SessionScriptCache.cs:StopRunningScript
  pub fn stop_running_script(&mut self, hash: &ScriptHashKey) {
    if let Some(count) = self.running.get_mut(hash) {
      *count = count.saturating_sub(1);
      if *count == 0 {
        self.running.remove(hash);
      }
    }
  }

  /// 是否有脚本正在运行。
  pub fn is_running(&self, hash: &ScriptHashKey) -> bool {
    self.running.contains_key(hash)
  }

  /// libs/server/Lua/SessionScriptCache.cs:RequestTimeout
  ///
  /// 标记超时请求（当前运行脚本应在下一检查点中断）。
  pub fn request_timeout(&mut self) {
    self.timeout_requested = true;
  }

  /// 消费超时请求标记。
  pub fn take_timeout_requested(&mut self) -> bool {
    mem::take(&mut self.timeout_requested)
  }

  /// libs/server/Lua/SessionScriptCache.cs:TryGetFromDigest
  ///
  /// 摘要取脚本源码。
  pub fn try_get_from_digest(&self, hash: &ScriptHashKey) -> Option<&Vec<u8>> {
    self.scripts.get(hash)
  }

  /// libs/server/Lua/SessionScriptCache.cs:TryLoad
  ///
  /// 登记脚本（EVAL 路径：脚本 + 摘要一起载入缓存）。
  pub fn try_load(&mut self, hash: &ScriptHashKey, script: &[u8]) -> bool {
    self
      .scripts
      .entry(hash.clone())
      .or_insert_with(|| script.to_vec());
    true
  }

  /// libs/server/Lua/SessionScriptCache.cs:TrySwapDatabaseSessions
  ///
  /// SWAPDB 场景：会话缓存与数据库解耦，脚本集合保持不变。
  pub fn try_swap_database_sessions(&mut self, _old_db: i32, _new_db: i32) -> bool {
    true
  }

  /// libs/server/Lua/SessionScriptCache.cs:GetScriptDigest
  ///
  /// 计算脚本 SHA1 摘要键。
  pub fn get_script_digest(script: &[u8]) -> ScriptHashKey {
    use sha1_smol::Sha1;
    let mut hasher = Sha1::new();
    hasher.update(script);
    let digest = hasher.digest().bytes();

    ScriptHashKey::new(&digest)
  }

  /// 已缓存脚本数（SCRIPT EXISTS 路径）。
  pub fn len(&self) -> usize {
    self.scripts.len()
  }

  /// 是否为空。
  pub fn is_empty(&self) -> bool {
    self.scripts.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use super::SessionScriptCache;

  #[test]
  fn load_get_and_digest() {
    let mut cache = SessionScriptCache::default();
    let script = b"return 1";
    let hash = SessionScriptCache::get_script_digest(script);
    assert!(cache.try_load(&hash, script));
    assert_eq!(cache.try_get_from_digest(&hash).unwrap(), &script.to_vec());
    assert_eq!(cache.len(), 1);

    // 摘要确定性。
    let hash2 = SessionScriptCache::get_script_digest(script);
    assert!(hash.equals(&hash2));
  }

  #[test]
  fn running_lifecycle_and_timeout() {
    let mut cache = SessionScriptCache::default();
    let hash = SessionScriptCache::get_script_digest(b"return redis.call('PING')");
    assert!(cache.try_load(&hash, b"return redis.call('PING')"));
    cache.start_running_script(&hash);
    assert!(cache.is_running(&hash));
    cache.request_timeout();
    assert!(cache.take_timeout_requested());
    cache.stop_running_script(&hash);
    assert!(!cache.is_running(&hash));
    cache.set_user_handle(Some(42));
    assert_eq!(cache.user_handle(), Some(42));
    // swap 语义：脚本保留。
    assert!(cache.try_swap_database_sessions(0, 1));
    assert_eq!(cache.len(), 1);
  }
}

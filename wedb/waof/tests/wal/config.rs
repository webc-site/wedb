use waof::{FsyncPolicy, WalConfig};

/// 默认与构造入口均取 Always 持久档（对标 C# AutoCommit=false 的默认保守语义）
#[test]
fn default_fsync_is_always() {
  assert_eq!(WalConfig::default().fsync, FsyncPolicy::Always);
  assert_eq!(WalConfig::new(1024).fsync, FsyncPolicy::Always);
}

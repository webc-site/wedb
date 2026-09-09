use std::sync::Arc;

use crate::server::migration::migrate_session::MigrateSession;

/// libs/cluster/Server/Migration/MigrationDriver.cs:MigrationDriver
pub struct MigrationDriver {
  session: Arc<MigrateSession>,
}

impl MigrationDriver {
  pub fn new(session: Arc<MigrateSession>) -> Self {
    Self { session }
  }

  /// libs/cluster/Server/Migration/MigrationDriver.cs:StartAsync
  pub async fn start_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrationDriver.cs:SendAsync
  pub async fn send_async(&self) -> bool {
    true
  }
}

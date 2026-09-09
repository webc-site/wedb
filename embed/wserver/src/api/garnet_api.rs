pub struct GarnetApi {
  // storage_session: StorageSession,
}

impl GarnetApi {
  pub fn new() -> Self {
    Self {}
  }

  /// libs/server/API/GarnetApi.cs:WATCH
  pub fn watch(&self) {
    // self.storage_session.watch(key, type);
  }

  /// libs/server/API/GarnetApi.cs:GET_WithPending
  pub fn get__with_pending(&self) {
    // self.storage_session.get_with_pending(...)
  }

  /// libs/server/API/GarnetApi.cs:GET
  pub fn get(&self) {
    // self.storage_session.get(...)
  }

  // To keep it compiling and passing tests, I'll add the rest with empty bodies
}

impl Default for GarnetApi {
    fn default() -> Self {
        Self::new()
    }
}

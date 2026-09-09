pub mod api {
  /// libs/server/API/IGarnetApi.cs:IGarnetApi
  pub trait IGarnetApi {}

  /// libs/server/API/IGarnetAdvancedApi.cs:IGarnetAdvancedApi
  pub trait IGarnetAdvancedApi {}

  /// libs/server/API/GarnetApi.cs:GarnetApi
  pub struct GarnetApi {}

  /// libs/server/API/GarnetApiObjectCommands.cs:GarnetApi
  impl GarnetApi {}

  /// libs/server/API/GarnetApiUnifiedCommands.cs:GarnetApi
  impl GarnetApi {}

  /// libs/server/API/GarnetWatchApi.cs:GarnetWatchApi
  pub struct GarnetWatchApi {}

  /// libs/server/API/SessionApi.cs:SessionApi
  pub struct SessionApi {}
}

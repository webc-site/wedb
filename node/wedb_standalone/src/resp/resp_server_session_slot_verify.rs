pub struct RespServerSessionSlotVerify;

impl RespServerSessionSlotVerify {
  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlot
  pub fn can_serve_slot(_slot: u16) -> bool {
    true
  }

  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlotForCustomCommand
  pub fn can_serve_slot_for_custom_command(_slot: u16) -> bool {
    true
  }

  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlotNoResponse
  pub fn can_serve_slot_no_response(_slot: u16) -> bool {
    true
  }
}

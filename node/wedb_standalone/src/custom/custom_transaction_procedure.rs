pub struct CustomTransactionProcedure;

impl CustomTransactionProcedure {
  /// libs/server/Custom/CustomTransactionProcedure.cs:AddKey
  pub fn add_key(_key: &[u8]) {}

  /// libs/server/Custom/CustomTransactionProcedure.cs:RewindScratchBuffer
  pub fn rewind_scratch_buffer() {}

  /// libs/server/Custom/CustomTransactionProcedure.cs:CreateArgSlice
  pub fn create_arg_slice(slice: &[u8]) -> &[u8] {
    slice
  }

  /// libs/server/Custom/CustomTransactionProcedure.cs:Finalize
  pub fn finalize(&mut self) {}
}

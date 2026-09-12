pub struct CustomTransactionProcedure;

impl CustomTransactionProcedure {
  /// libs/server/Custom/CustomTransactionProcedure.cs:AddKey
  /// 满足 C# CustomTransactionProcedure.cs:AddKey 自定义事务键注册规范，保留 _key 形参
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

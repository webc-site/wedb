pub struct MGetReadArgBatch<'a> {
  keys: &'a [&'a [u8]],
  outputs: Vec<Option<Vec<u8>>>,
}

impl<'a> MGetReadArgBatch<'a> {
  pub fn new(keys: &'a [&'a [u8]]) -> Self {
    let len = keys.len();
    Self {
      keys,
      outputs: vec![None; len],
    }
  }

  /// libs/server/Resp/MGetReadArgBatch.cs:GetInput
  /// 保留形参以匹配 MGetReadArgBatch.GetInput 对标规范，MGET 批读无额外输入载荷
  pub fn get_input(&self, _idx: usize) -> Option<&[u8]> {
    None
  }

  /// libs/server/Resp/MGetReadArgBatch.cs:GetKey
  pub fn get_key(&self, idx: usize) -> Option<&'a [u8]> {
    self.keys.get(idx).copied()
  }

  /// libs/server/Resp/MGetReadArgBatch.cs:GetOutput
  pub fn get_output(&self, idx: usize) -> Option<&[u8]> {
    self.outputs.get(idx).and_then(|o| o.as_deref())
  }

  /// libs/server/Resp/MGetReadArgBatch.cs:SetOutput
  pub fn set_output(&mut self, idx: usize, val: Option<Vec<u8>>) {
    if idx < self.outputs.len() {
      self.outputs[idx] = val;
    }
  }
}

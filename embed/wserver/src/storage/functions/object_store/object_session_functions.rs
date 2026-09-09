use crate::inputs::ObjectInput;

/// libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs:ObjectSessionFunctions
#[derive(Clone)]
pub struct ObjectSessionFunctions {}

impl ObjectSessionFunctions {
  pub const NEED_AOF_LOG: u8 = 0x1;

  pub fn new() -> Self {
    Self {}
  }

  /// libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs:ConvertOutputToHeap
  pub fn convert_output_to_heap(&self, _input: &mut ObjectInput, _output: &mut ()) {}
}

impl Default for ObjectSessionFunctions {
  fn default() -> Self {
    Self::new()
  }
}

use super::object_session_functions::ObjectSessionFunctions;
use crate::inputs::ObjectInput;

impl ObjectSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:GetRMWModifiedValueLength
  pub fn get_rmw_modified_value_length(
    &self,
    _value: &mut [u8],
    _input: &mut ObjectInput,
  ) -> usize {
    0
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:GetRMWInitialValueLength
  pub fn get_rmw_initial_value_length(&self, _input: &mut ObjectInput) -> usize {
    0
  }
}

use super::main_session_functions::MainSessionFunctions;
use crate::inputs::StringInput;

impl MainSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWModifiedValueLength
    pub fn get_rmw_modified_value_length(&self, _value: &mut [u8], _input: &mut StringInput) -> usize {
        0
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWInitialValueLength
    pub fn get_rmw_initial_value_length(&self, _input: &mut StringInput) -> usize {
        0
    }
}

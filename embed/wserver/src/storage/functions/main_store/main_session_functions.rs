use crate::inputs::StringInput;
use crate::arg_slice::ArgSlice;

/// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:MainSessionFunctions
#[derive(Clone)]
pub struct MainSessionFunctions {
    // functionsState and readSessionState are context objects in C#.
    // In Rust we might store a reference to the server state or just leave it empty for now.
}

impl MainSessionFunctions {
    pub const NEED_AOF_LOG: u8 = 0x1;

    pub fn new() -> Self {
        Self {}
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:ConvertOutputToHeap
    pub fn convert_output_to_heap(&self, _input: &mut StringInput, _output: &mut ()) {
        // TODO: Inspect input to determine whether we're in a context requiring ConvertToHeap.
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:PreSingleKeyConsistentRead
    pub fn pre_single_key_consistent_read(&self, _hash: i64) {
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:PostSingleKeyConsistentReadCallback
    pub fn post_single_key_consistent_read_callback(&self) {
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:PreBatchKeyConsistentReadCallback
    pub fn pre_batch_key_consistent_read_callback(&self, _parameters: &[ArgSlice]) {
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:PostBatchKeyConsistentReadCallback
    pub fn post_batch_key_consistent_read_callback(&self, _key_count: i32) -> bool {
        false
    }
}

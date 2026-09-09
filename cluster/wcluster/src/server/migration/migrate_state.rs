/// libs/cluster/Server/Migration/MigrateState.cs:MigrateState
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MigrateState {
    Success = 0x0,
    Fail = 1,
    Pending = 2,
    Import = 3,
    Stable = 4,
    Node = 5,
}

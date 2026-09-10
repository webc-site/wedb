pub use wresp::RespCommand;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GarnetObjectType {
  Null = 0,
  SortedSet = 1,
  List = 2,
  Hash = 3,
  Set = 4,
  All = 0xfb,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RespInputFlags: u8 {
        const SET_GET = 32;
        const DETERMINISTIC = 64;
        const EXPIRED = 128;
    }
}

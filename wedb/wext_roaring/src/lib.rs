mod error;
mod roaring_bitmap;
mod roaring_bitmap_commands;
mod roaring_bitmap_object;

pub use error::{Error, Result};
pub use roaring_bitmap::RoaringBitmapObj;
pub use roaring_bitmap_commands::{
  COMMAND_INFOS, RoaringBitmapCommands, RoaringCommand, RoaringCommandInfo, is_command_registered,
};
pub use roaring_bitmap_object::{RoaringBitmapObject, heap_estimate};

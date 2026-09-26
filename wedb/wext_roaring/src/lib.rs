mod error;
mod roaring_bitmap;
mod roaring_bitmap_commands;
mod roaring_bitmap_object;

pub use error::{Error, Result};
pub use roaring_bitmap_commands::{COMMAND_INFOS, RoaringCommand};

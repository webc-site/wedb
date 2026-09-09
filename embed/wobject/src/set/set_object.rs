use gxhash::GxBuildHasher;
use papaya::HashSet;

/// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetObject
pub struct SetObject {
  pub set: HashSet<Vec<u8>, GxBuildHasher>,
}

impl SetObject {
  pub fn new() -> Self {
    Self {
      set: HashSet::with_hasher(GxBuildHasher::default()),
    }
  }
}

impl Default for SetObject {
  fn default() -> Self {
    Self::new()
  }
}

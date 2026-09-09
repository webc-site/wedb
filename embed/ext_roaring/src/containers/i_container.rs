/// IContainer 接口
pub trait IContainer {
  /// garnet相对路径:modules/RoaringBitmap/Containers/IContainer.cs:Remove
  fn remove(&mut self);

  /// garnet相对路径:modules/RoaringBitmap/Containers/IContainer.cs:Last
  fn last(&self) -> u16;

  /// garnet相对路径:modules/RoaringBitmap/Containers/IContainer.cs:NextSetBit
  fn next_set_bit(&self) -> i32;

  /// garnet相对路径:modules/RoaringBitmap/Containers/IContainer.cs:NextUnsetBit
  fn next_unset_bit(&self) -> i32;

  /// garnet相对路径:modules/RoaringBitmap/Containers/IContainer.cs:SerializeBody
  fn serialize_body(&self);
}

use std::io::{Read, Write};

/// 表示一个用于创建 `DummyObject` 实例的工厂。
pub struct DummyObjectFactory;

impl DummyObjectFactory {
  /// 创建一个新的 DummyObject
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:Create
  pub fn create(obj_type: u8) -> DummyObject {
    DummyObject::new(obj_type)
  }

  /// 反序列化 DummyObject
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:Deserialize
  pub fn deserialize<R: Read>(obj_type: u8, reader: &mut R) -> DummyObject {
    DummyObject::new_with_reader(obj_type, reader)
  }
}

/// 表示一个虚拟的 Garnet 对象
#[derive(Clone)]
pub struct DummyObject {
  pub obj_type: u8,
}

impl DummyObject {
  /// 构造函数
  pub fn new(obj_type: u8) -> Self {
    Self { obj_type }
  }

  /// 使用读取器初始化
  /// 满足 Garnet 工厂反序列化接口规范，保留 _reader 参数
  pub fn new_with_reader<R: Read>(obj_type: u8, _reader: &mut R) -> Self {
    Self { obj_type }
  }

  /// 克隆对象
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:CloneObject
  pub fn clone_object(&self) -> DummyObject {
    self.clone()
  }

  /// 序列化对象
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:SerializeObject
  /// 满足 Garnet 对象序列化接口规范，保留 _writer 参数
  pub fn serialize_object<W: Write>(&self, _writer: &mut W) {
    // 无操作
  }

  /// 释放资源
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:Dispose
  pub fn dispose(&self) {}

  /// 扫描操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObject.cs:Scan
  /// 满足 Garnet 对象扫描接口规范，保留形参以匹配固定签名
  pub fn scan(
    &self,
    _cursor: i64,
    _count: i32,
    _pattern: Option<&[u8]>,
    _is_single_type: bool,
  ) -> (Vec<Vec<u8>>, i64) {
    let items = Vec::new();
    let cursor = 0;
    (items, cursor)
  }
}

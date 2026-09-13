use crate::error::{Error, Result};

/// 表示原始字符串无操作读取
pub struct NoOpCommandRead;

impl NoOpCommandRead {
  /// 获取初始长度
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:GetInitialLength
  /// 满足 Garnet 自定义命令读取接口规范，保留 _input 参数
  pub fn get_initial_length(&self, _input: &[u8]) -> Result<usize> {
    Err(Error::NotImplemented)
  }

  /// 获取长度
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:GetLength
  /// 满足 Garnet 自定义命令读取接口规范，保留 _value 与 _input 参数
  pub fn get_length(&self, _value: &[u8], _input: &[u8]) -> Result<usize> {
    Err(Error::NotImplemented)
  }

  /// 初始更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:InitialUpdater
  /// 满足 Garnet 自定义命令读取接口规范，保留形参以匹配固定签名
  pub fn initial_updater(&self, _key: &[u8], _input: &[u8], _value: &mut [u8]) -> Result<bool> {
    Err(Error::NotImplemented)
  }

  /// 原地更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:InPlaceUpdater
  /// 满足 Garnet 自定义命令读取接口规范，保留形参以匹配固定签名
  pub fn in_place_updater(
    &self,
    _key: &[u8],
    _input: &[u8],
    _value: &mut [u8],
    _output_length: &mut usize,
  ) -> Result<bool> {
    Err(Error::NotImplemented)
  }

  /// 复制更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:CopyUpdater
  /// 满足 Garnet 自定义命令读取接口规范，保留形参以匹配固定签名
  pub fn copy_updater(
    &self,
    _key: &[u8],
    _input: &[u8],
    _old_value: &[u8],
    _new_value: &mut [u8],
  ) -> Result<bool> {
    Err(Error::NotImplemented)
  }

  /// 读取器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRead.cs:Reader
  /// 满足 Garnet 自定义命令读取接口规范，保留形参以匹配固定签名
  pub fn reader(&self, _key: &[u8], _input: &[u8], _value: &[u8]) -> Result<bool> {
    Ok(true)
  }
}

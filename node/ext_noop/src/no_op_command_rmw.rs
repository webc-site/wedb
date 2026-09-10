use crate::error::{Error, Result};

/// 表示原始字符串无操作 RMW 操作
pub struct NoOpCommandRmw;

impl NoOpCommandRmw {
  /// 读取器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:Reader
  pub fn reader(&self, _key: &[u8], _input: &[u8], _value: &[u8]) -> Result<bool> {
    Err(Error::InvalidOperation)
  }

  /// 是否需要初始更新
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:NeedInitialUpdate
  pub fn need_initial_update(&self, _key: &[u8], _input: &[u8]) -> Result<bool> {
    Ok(false)
  }

  /// 获取初始长度
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:GetInitialLength
  pub fn get_initial_length(&self, _input: &[u8]) -> Result<usize> {
    Err(Error::InvalidOperation)
  }

  /// 初始更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:InitialUpdater
  pub fn initial_updater(&self, _key: &[u8], _input: &[u8], _value: &mut [u8]) -> Result<bool> {
    Err(Error::InvalidOperation)
  }

  /// 原地更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:InPlaceUpdater
  pub fn in_place_updater(
    &self,
    _key: &[u8],
    _input: &[u8],
    _value: &mut [u8],
    _value_length: &mut usize,
  ) -> Result<bool> {
    Ok(true)
  }

  /// 是否需要复制更新
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:NeedCopyUpdate
  pub fn need_copy_update(&self, _key: &[u8], _input: &[u8], _old_value: &[u8]) -> Result<bool> {
    Ok(false)
  }

  /// 获取长度
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:GetLength
  pub fn get_length(&self, _value: &[u8], _input: &[u8]) -> Result<usize> {
    Ok(0)
  }

  /// 复制更新器
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:CopyUpdater
  pub fn copy_updater(
    &self,
    _key: &[u8],
    _input: &[u8],
    _old_value: &[u8],
    _new_value: &mut [u8],
  ) -> Result<bool> {
    Ok(true)
  }
}

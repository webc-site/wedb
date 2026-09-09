/// 表示原始字符串无操作 RMW 操作
pub struct NoOpCommandRmw;

impl NoOpCommandRmw {
    /// 读取器
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:Reader
    pub fn reader(&self, _key: &[u8], _input: &[u8], _value: &[u8]) -> bool {
        unimplemented!()
    }

    /// 是否需要初始更新
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:NeedInitialUpdate
    pub fn need_initial_update(&self, _key: &[u8], _input: &[u8]) -> bool {
        false
    }

    /// 获取初始长度
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:GetInitialLength
    pub fn get_initial_length(&self, _input: &[u8]) -> usize {
        unimplemented!()
    }

    /// 初始更新器
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:InitialUpdater
    pub fn initial_updater(&self, _key: &[u8], _input: &[u8], _value: &mut [u8]) -> bool {
        unimplemented!()
    }

    /// 原地更新器
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:InPlaceUpdater
    pub fn in_place_updater(&self, _key: &[u8], _input: &[u8], _value: &mut [u8], _value_length: &mut usize) -> bool {
        true
    }

    /// 是否需要复制更新
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:NeedCopyUpdate
    pub fn need_copy_update(&self, _key: &[u8], _input: &[u8], _old_value: &[u8]) -> bool {
        false
    }

    /// 获取长度
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:GetLength
    pub fn get_length(&self, _value: &[u8], _input: &[u8]) -> usize {
        0
    }

    /// 复制更新器
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpCommandRMW.cs:CopyUpdater
    pub fn copy_updater(&self, _key: &[u8], _input: &[u8], _old_value: &[u8], _new_value: &mut [u8]) -> bool {
        true
    }
}

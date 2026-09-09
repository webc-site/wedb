//! 存储 API 面（对标 libs/server/API/：IGarnetApi / GarnetApiObjectCommands /
//! GarnetWatchApi / IGarnetAdvancedApi / GarnetStatus）

pub mod garnet_api_object_commands;
pub mod garnet_status;
pub mod garnet_watch_api;
pub mod i_garnet_advanced_api;
pub mod i_garnet_api;

use std::io::Cursor;

use wobject::{hash::hash_object::HashObject, set::set_object::SetObject};

/// SCAN 成员抽取：哈希信封 → 字段列表（信封不匹配返回 None）
///
/// 抽取器只负责取成员，排序由 `object_scan` 内部统一完成（一处定义）
pub(crate) fn hash_fields(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
  HashObject::deserialize(&mut Cursor::new(payload))
    .ok()
    .map(|o| o.get_keys())
}

/// SCAN 成员抽取：集合信封 → 成员列表
pub(crate) fn set_members(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
  SetObject::deserialize(&mut Cursor::new(payload))
    .ok()
    .map(|o| o.get_keys())
}

/// OBJECT SCAN 兜底成员抽取：哈希 / 集合信封自动识别
pub(crate) fn hash_or_set_members(payload: &[u8]) -> Option<Vec<Vec<u8>>> {
  hash_fields(payload).or_else(|| set_members(payload))
}

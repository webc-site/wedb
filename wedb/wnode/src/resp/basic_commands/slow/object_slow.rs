//! OBJECT 四子命令慢路径（NetworkOBJECT 异步对偶）与编码名映射单源
//!（信封内层标签 / Meta collection_type 同表，快慢路径单一真源）

use wdev::Device;
use wresp::{
  cmd_strings::{self as cs, abort_with_error_message},
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag};

use super::common::arity;
use crate::{
  resp::{basic_commands::ObjectSubCmd, vector::vector_manager::VectorManager},
  storage::session::{
    common::{UserReadAsync, ttl_sync::meta_collection_type_of},
    storage_session::StorageSession,
  },
};

pub(crate) const ENCODING_RAW: &[u8] = b"raw";
/// 未知信封标签兜底（对齐 C# UnifiedStore ReadMethods.cs:65 `_ => CmdStrings.hashtable`）
const ENCODING_HASHTABLE: &[u8] = b"hashtable";

/// OBJECT 编码名映射（信封内层标签 / Meta collection_type 同表，快慢路径
/// 单一真源；集合默认臂对齐 C# UnifiedStore ReadMethods.cs:65
/// `_ => CmdStrings.hashtable`）
///
/// RangeIndex 显式回 raw：C# RI 主存记录不置 ValueIsObject 位（仅以
/// RecordType 字节判别，RangeIndexManager.cs:54 RangeIndexRecordType = 2），
/// HandleObjectEncoding else 臂恒 raw（ReadMethods.cs:69-72，非对象记录
/// 无 native integer/embstr 表示）——hashtable 默认臂仅覆盖集合对象
pub(crate) const fn encoding_of_object_type(obj_type: GarnetObjectType) -> &'static [u8] {
  match obj_type {
    GarnetObjectType::SortedSet => b"skiplist",
    GarnetObjectType::List => b"quicklist",
    GarnetObjectType::RangeIndex => ENCODING_RAW,
    _ => ENCODING_HASHTABLE,
  }
}

/// 信封首字节→编码名共享解析（未知 / 扩展标签域 >= 0x40 恒兜底 hashtable，
/// 对齐 C# UnifiedStore ReadMethods.cs:48-77 HandleObjectEncoding）
#[inline]
pub(crate) fn encoding_of_envelope_payload(raw: &[u8]) -> &'static [u8] {
  raw
    .first()
    .copied()
    .and_then(GarnetObjectType::from_u8)
    .map_or(ENCODING_HASHTABLE, encoding_of_object_type)
}

/// 向量登记表命中探针（OBJECT 慢臂值域门用，判据与 exec 层值域门
/// `read_stored_index` 同一来源；SET 族第四态裁决已收拢窗内折叠单源
/// [`crate::storage::session::common::ttl_sync::registry_alive`]，本壳不再服务
/// SET 臂，票 zcode-r163c-setguard 案二）
fn reg_hit(
  storage: &StorageSession<'_, impl Device>,
  vector: Option<&VectorManager>,
  key: &[u8],
) -> bool {
  vector.is_some_and(|vm| {
    vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
      .is_some()
  })
}

/// OBJECT 出帧单源（向量登记特判与编码判定共用同一映射）：命中编码时按
/// 子命令回编码名 / 1 / 0 / FREQ 不支持帧，键缺失（C# status != OK）回版本 nil
fn object_frame(
  sub_cmd: ObjectSubCmd,
  encoding: Option<&'static [u8]>,
  resp_version: u8,
  output: &mut Vec<u8>,
) {
  match (encoding, sub_cmd) {
    (Some(enc), ObjectSubCmd::Encoding) => output.write_resp_bulk_string(enc),
    (Some(_), ObjectSubCmd::Refcount) => output.write_resp_int(1),
    (Some(_), ObjectSubCmd::Idletime) => output.write_resp_int(0),
    (Some(_), ObjectSubCmd::Freq) => {
      abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED)
    }
    (None, _) => output.write_resp_null_ver(resp_version),
  }
}

/// NetworkOBJECT 的异步对偶（四子命令；语义对照 UnifiedStore
/// ReadMethods:HandleObjectEncoding，与快路径 `network_object` 同口径：
/// 向量键登记特判 → String 域 raw / 信封标签映射 / 升阶键 Meta 映射 /
/// 缺失回 nil）
///
/// libs/server/API/GarnetApiUnifiedCommands.cs:OBJECT
///（C# GarnetApi.OBJECT(key) 转发 storageSession.Read_UnifiedStore →
/// HandleObjectEncoding 编码判定；rust 无该 API 包装层，四子命令读写折叠于
/// 本函数与快路径 `network_object`，本注释挂异步对偶即判定内核落点）
pub(crate) async fn object_slow(
  storage: &StorageSession<'_, impl Device>,
  sub_cmd: ObjectSubCmd,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some([key]) = arity(parse_state, sub_cmd.as_str(), output) else {
    return Ok(());
  };
  let resp_version = storage.resp_version;

  // 向量键登记特判（快路径同形：raw / 1 / 0 / FREQ 不支持）——等价 raw 编码
  if reg_hit(storage, vector, key) {
    object_frame(sub_cmd, Some(ENCODING_RAW), resp_version, output);
    return Ok(());
  }

  // 编码判定：String 域命中 → raw；信封域按内层标签映射；升阶键按 Meta
  // collection_type 同款映射；过期键与缺失键一致回 nil
  // 三域漏斗与信封/Meta 二级续探一律走零入账静默口（对位快臂
  // read_user_sync 传 None 与 C# Read_UnifiedStore 恒零计，单点真源注记见
  // user_read.rs read_user_sync 头注，票 zcode-r157c-objenc 案一；严禁按
  // GET/DUMP 簿记先例回补入账）
  let encoding = match storage.read_user_quiet(key, |_| ()).await.map_err(|_| ())? {
    UserReadAsync::Hit(()) => Some(ENCODING_RAW),
    UserReadAsync::WrongType => {
      let envelope = storage
        .read_tag_quiet(key, KeyTag::ObjectEnvelope, encoding_of_envelope_payload)
        .await
        .map_err(|_| ())?;
      match envelope {
        Some(enc) => Some(enc),
        None => storage
          .read_tag_quiet(key, KeyTag::Meta, meta_collection_type_of)
          .await
          .map_err(|_| ())?
          .flatten()
          .map(encoding_of_object_type),
      }
    }
    UserReadAsync::Missing => None,
  };
  object_frame(sub_cmd, encoding, resp_version, output);
  Ok(())
}

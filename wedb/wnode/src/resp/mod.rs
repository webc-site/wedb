//! RESP 协议元数据、通用命令与服务器会话体系（对标 libs/server/Resp）

pub mod acl_commands;
pub mod admin_commands;
pub mod array_commands;
pub mod basic_commands;
pub mod basic_etag_commands;
pub mod bitmap;
pub mod client_commands;
pub mod garnet_api;
pub mod hyperloglog;
pub mod key_admin_commands;
pub mod metrics_commands;
pub mod objects;
pub mod parser;
pub mod rangeindex;
pub mod resp_command_data_common;
pub mod resp_command_data_provider;
pub mod resp_command_docs;
pub mod resp_command_info_simplified_structs;
pub mod resp_commands_info;
pub mod resp_commands_info_data;
pub mod resp_server_session;
pub mod resp_server_session_output;
pub mod resp_session_consumer;
pub mod slow_path;
pub mod ttl_sync;
pub mod vector;

use std::sync::Arc;

use objects::collection_item_source::CollectionItemSource;
pub use resp_command_data_common::try_import_resp_commands_data;
pub use resp_command_data_provider::{
  DefaultRespCommandsDataProvider, IRespCommandData, get_resp_commands_data_provider,
};
pub use resp_command_docs::{
  RespCommandDocFlags, RespCommandDocs, RespCommandDocsTables, RespCommandGroup,
  try_get_resp_command_docs, try_get_resp_commands_docs, try_get_resp_sub_commands_docs,
};
pub use resp_command_info_simplified_structs::{
  SimpleRespCommandInfo, populate_simple_command_info, try_get_simple_key_spec,
};
pub use resp_commands_info::{
  RespCommandFlags, RespCommandsInfo, RespCommandsTables, get_resp_command_name,
  try_fast_get_resp_command_info, try_get_commandsfor_acl_category,
  try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name, try_get_resp_command_names,
  try_get_resp_commands_info, try_get_resp_commands_info_count, try_get_resp_sub_commands_info,
  try_get_simple_resp_command_info,
};
pub use resp_server_session::{
  ConnectionProtectionOption, DatabaseSessionSlot, RespServerSession, RespServerSessionOptions,
};
pub use resp_session_consumer::RespSessionConsumer;
use wcol::itembroker::item_broker_face::{BlockedWait as BaseBlockedWait, SharedItemBroker};

pub type ItemBroker = SharedItemBroker<CollectionItemSource<wdev::SegmentedDevice>>;
pub type BlockedWait = BaseBlockedWait<Arc<ItemBroker>>;

//! RESP 协议元数据、通用命令与服务器会话体系（对标 libs/server/Resp）

pub mod acl_commands;
pub mod acl_store;
pub mod admin_commands;
pub mod array_commands;
pub mod basic_commands;
pub mod basic_etag_commands;
pub mod bitmap;
pub mod client_commands;
pub mod config_commands;
pub mod custom_objects;
pub mod garnet_api;
pub mod hyperloglog;
pub mod info_provider;
pub mod key_admin_commands;
pub mod metrics_commands;
pub mod objects;
pub mod parser;
pub mod range_index;
pub mod resp_command_docs;
pub mod resp_commands_info_data;
pub mod resp_server_session;
pub mod resp_server_session_output;
pub mod resp_server_session_slot_verify;
pub mod resp_session_consumer;
pub mod session_dependencies;
pub mod slow_path;
pub mod txn_resp_commands;
pub mod vector;

use std::sync::Arc;

use objects::collection_item_source::CollectionItemSource;
pub use resp_command_docs::{
  RespCommandDocFlags, RespCommandDocs, RespCommandDocsTables, RespCommandGroup,
  try_get_resp_command_docs, try_get_resp_commands_docs, try_get_resp_sub_commands_docs,
};
pub use resp_server_session::{
  ConnectionProtectionOption, RespServerSession, RespServerSessionOptions,
};
pub use resp_session_consumer::RespSessionConsumer;
pub use session_dependencies::SessionDependencies;
use wcol::itembroker::item_broker_face::{BlockedWait as BaseBlockedWait, SharedItemBroker};

pub type ItemBroker = SharedItemBroker<CollectionItemSource<wdev::SegmentedDevice>>;
pub type BlockedWait = BaseBlockedWait<Arc<ItemBroker>>;

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};
pub mod argslice;
pub mod argument;
pub mod check_args;
pub mod cmd_strings;
pub mod command;
pub mod ext;
pub mod frame;
pub mod i_resp_serializable;
pub mod key_spec;
pub mod length;
pub mod options;
pub mod read;
pub mod resp_memory_writer;
pub mod session_parse_state;

pub use argslice::{ArgSlice, ArgSliceVector, DEFAULT_MAX_ITEM_NUM};
pub use argument::{
  ArgumentBase, RespCommandArgument, RespCommandArgumentFlags, RespCommandArgumentType,
};
pub use check_args::{ArgCountMatcher, check_exact_arg_count, check_min_arg_count};
pub use command::{
  RespCommand, is_cluster_sub_command, is_data_command, is_legal_on_range_index,
  is_legal_on_vector_set, is_range_index_command, is_read_only, is_vector_set_command,
  is_write_only, one_if_read, one_if_write,
};
pub use ext::{
  MAX_ERROR_MSG_LEN, RespSliceExt, RespVecExt, sanitize_error_str,
};
pub use frame::parse_resp_frame;
pub use i_resp_serializable::IRespSerializable;
pub use key_spec::{
  ALL_FLAGS, BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
};
pub use options::{
  ExistOptions, ExpirationOption, ExpirationWithOption, ExpireOption, SortedSetAddOption,
  SortedSetAggregateType, equals_ignore_case, expiration_option_from_token,
  expire_option_from_token, try_get_exist_options, try_get_expiration_option,
  try_get_expire_option, try_get_sorted_set_add_option, try_get_sorted_set_aggregate_type,
};
pub use read::MAX_ARGUMENT_LENGTH_BYTES;
pub use resp_memory_writer::{
  Resp2, Resp3, RespBuffer, RespMemoryWriter, RespProtocol, RespWriter, format_double,
};
pub use session_parse_state::{INLINE_PARAMS, SessionParseState};

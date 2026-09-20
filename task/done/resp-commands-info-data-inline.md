1. In `wedb/wresp/src/command.rs`, add `from_cs_name` and `to_cs_name` to `RespCommand` as associated methods.
2. Replace `resp_commands_info_data::resp_command_from_cs_name` with `RespCommand::from_cs_name` and `resp_command_to_cs_name` with `RespCommand::to_cs_name` in:
   - `wedb/wnode/src/resp/basic_commands/mod.rs`
   - `wedb/wnode/src/resp/resp_server_session/txn.rs`
   - `wedb/wnode/src/resp/parser/resp_command.rs`
   - `wedb/wnode/src/resp/resp_server_session/core.rs`
   - `wedb/wnode/src/resp/info_provider.rs`
   - `wedb/wnode/src/resp/resp_command_docs.rs`
3. Delete `wedb/wnode/src/resp/resp_commands_info_data.rs`.
4. Remove the module declaration `pub mod resp_commands_info_data;` from `wedb/wnode/src/resp/mod.rs`.
5. Run `cargo check`.

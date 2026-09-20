# StorageSession 与 object_store_utils async 读 API 命名风格统一

来源：next/zcode-r5-api.md 问题 2

## 问题

同一 StorageSession impl 块内存在两种命名：
read_tag_with / read_string_with（无后缀）与 read_user_async / probe_alive_domain_async（带 _async 后缀）。
object_store_utils.rs 中又采用 obj_load_typed_sync / obj_load_typed_async 双后缀。
三式并存导致调用方无法直观推断兄弟 API 命名。

## 涉及路径

- wedb/wnode/src/storage/session/storage_session.rs
- wedb/wnode/src/resp/objects/object_store_utils.rs

## 解决建议

采用「异步无后缀 + 同步 _sync」主流规范：
将 read_user_async 规整为 read_user，probe_alive_domain_async 规整为 probe_alive_domain，ri_write_gate_async 规整为 ri_write_gate。
相关调用点同步批量改齐。

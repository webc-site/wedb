# wkv StoreSession 键构造器前缀风格统一

来源：next/zcode-r5-api.md 问题 7

## 问题

StoreSession 的键构造方法中，session_tag_key / session_meta_key / session_string_key 带有 session_ 前缀，
而同为会话域标签物理键的 ttl_key / etag_key / vector_key 缺少前缀，风格不一致。

## 涉及路径

- wedb/wkv/src/session/keys.rs
- wedb/wkv/src/ttl.rs
- wedb/wkv/src/etag.rs

## 解决建议

1. 统一添加 session_ 前缀，例如 session_ttl_key / session_etag_key / session_vector_key。
2. 保留原无前缀方法作为废弃别名或一并完成全仓调用点规整。

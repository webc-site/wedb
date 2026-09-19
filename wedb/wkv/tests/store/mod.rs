//! 存储引擎底层分层测试聚合（CRUD / 防御语义 / 刷盘驱逐 / 按库清空 / 跨库交换 / 复活与读缓存 / 零拷贝批量读 / 碰撞链 / RangeIndex 会话层 / 分层排空信封墓碑 / 启动重建就绪门禁）

mod batch_prefix;
mod cold_tenant_lazy_load;
mod collision_chain;
mod concurrent_flush;
mod crud;
mod dbmeta_layout;
mod defense;
mod flush_database;
mod flush_evict;
mod range_index;
mod read_cache;
mod resize;
mod reviv;
mod swap_database;
mod tiered_drain_envelope;
mod ttl_gate_resize;
mod vdb_rebuild_gate;

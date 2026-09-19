优先级：中

5 [MED] DEL/UNLINK 一条命令两套删除原语：快臂带向量集登记面、慢臂没有，降级重放即漏删且应答计数偏离
具体问题：快路径 network_del 的删除判据是「wkv 域删 + 登记表删」两段（:176
try_delete_sync_with_prefix，未命中再 :186 delete_vector_set）；慢路径 slow::del 只有
:772 storage.delete_string 一段，函数签名（:765-769）连 vector 形参都没有，分派点也不给：
raw.rs:209 传 vector，slow.rs:561-568 不传。而向量集键在 wkv 域根本没有记录——登记表是纯内存
pin-map（vector_manager_locking.rs:416-423 read_stored_index/write_stored_index 直读写
key_index_registry），delete_string 对向量集键恒返回 Ok(false)。后果：network_del 的 :179 降级臂
（环形页翻转/复合对象元数据，任一键降级即整条命令 Ok(false)）把请求转交 slow::del 后，同批向量集
键既不计入 deleted_count（应答口径偏离 C#），也不会被摘除登记项——而登记表才是存在性权威（SETNX
存活判定 raw.rs:73-77、EXISTS/DBSIZE/KEYS 同口径，见 next/vector-registry-nsdb-isolation.md 的
域收敛改造），于是 DEL 返回后该键仍然存活，是一个删不掉的幽灵向量集。
rust：wedb/wnode/src/resp/array_commands.rs:161-194 network_del（:176 主删、:179 降级早返、
:186 向量集臂）、:765-777 slow::del（:772 单臂）；分派点
wedb/wnode/src/resp/garnet_api/raw.rs:209（C::Del | C::Unlink 传 vector）与
wedb/wnode/src/resp/garnet_api/slow.rs:561-568（同两条命令不传）；登记表实现
wedb/wnode/src/resp/vector/vector_manager.rs:489-498 delete_vector_set、
wedb/wnode/src/resp/vector/vector_manager_locking.rs:416-423
c#：garnet/libs/server/Resp/ArrayCommands.cs:93-112 NetworkDEL 单路径逐键 storageApi.DELETE
（UNLINK 与 DEL 共用同一实现：RespServerSession.cs:826/:888），向量集清退发生在存储层回调内
（garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:DELETE_MainStore → RemoveKey →
VectorManager.RequestDeletion），故 pending/重试没有第二口径可言径分裂
修法：二选一，不得并存。首选把向量集清退下沉到存储删除单点（delete_string 一侧接
delete_vector_set 或经域判定收口），RESP 层两臂都只调该单点，与 C# 的「存储层回调」同形；次选给
slow::del 加 vector 形参并在 slow.rs:561-568 分派点接线，同时在 network_del 的 :176-189 与
slow::del 的 :771-774 各加同一条降级重放用例（快臂已删项 + 慢臂补删项的计数一致性）。

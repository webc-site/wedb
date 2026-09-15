# design 待办

3. [P1] wnode 越层直依赖 wbftree，RangeIndex 双包装
   位置：wnode/src/aof/aof_processor.rs、wnode/src/service.rs、wnode/src/resp/rangeindex/ 8 文件（range_index_chunked_deserializer.rs、range_index_chunked_serializer.rs、range_index_manager_index.rs、range_index_manager_locking.rs、range_index_manager_migration.rs、range_index_manager_replication.rs、range_index_migration_reader.rs、resp_server_session_range_index.rs）
   问题：wnode 绕过 wkv 引擎门面直用 wbftree，层次依赖倒挂
   改法：门控并入 wkv::range_index 暴露面，wnode 仅经 wkv 消费

8. [P2] SET/SETEX 两跳 AOF 写放大
   位置：wnode/src/service.rs:210（StoreEvent::TtlWrite 汇聚为独立 Pexpireat/Persist 条目，值条目另发）、wnode/src/resp/key_admin_commands.rs:192（SET EX 命令端先算 expire_at_ticks 再写值）
   对标：garnet/libs/server/Resp/BasicCommands.cs:533 NetworkSETEX（input = RespCommand.SETEX，expiry 编入 valMetadata，单条 AOF）
   问题：rust 侧 SET EX 产生值条目 + 过期条目两条 AOF，高频场景写放大翻倍，偏离 C# 单条形态
   改法：值条目随行 expiration（对齐 C# SETEX valMetadata 单条），TtlWrite 事件仅服务独立 EXPIRE/PEXPIRE 命令

10. [P2] 自定义对象命令双执行器
   位置：wnode/src/resp/objects/custom_object_commands.rs:35 try_custom_object_command（同步）vs wnode/src/resp/garnet_api.rs:755 custom_object_slow（异步）
   对标：garnet/libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand
   问题：同一四接口分派（装载/NeedInitialUpdate/Updater/Reader/NotFound）两份实现，收敛易丢分支
   改法：抽共用执行器把「装载/回写/删除」原语参数化；对齐 custom_object_slow 的「信封未命中反探 String 域」WRONGTYPE 判定与同步 obj_load_typed_sync 语义

11. [P2] 两个 main 的 (recover, aof) 四路分派重复
   位置：wedb/src/main.rs:99 与 wedb_standalone/src/main.rs:87（同构 match，约 20 行逐字相同）
   问题：同一恢复装配逻辑双份维护
   改法：wnode 提供 open_from_args(args, session_factory) 便利构造，两 main 各一行

13. [P2] 配置文件入口断头
   位置：wconf/src/node_options.rs:222 from_args、:227 from_nested_text_str、:232 from_file（外部全仓零调用，from_file 内部转 from_nested_text_str）
   问题：SKILL 指定 nested_text 为配置格式，但两 main 直接 clap derive，配置文件能力未接
   改法：main 增 --config 分支接线，或明确放弃并删三个函数

16. [P2] src 内嵌跨 crate 集成测试迁 tests
   位置：wnode/src/resp/resp_server_session.rs:2725（tests 模块约 1270 行、62 个 #[test]，use wacl/wpubsub/wtxn/waof 全家装配）；同型：wnode/src/session_parse_state_extensions.rs（15 test）、wnode/src/resp/parser/resp_command.rs（13）、wnode/src/aof/garnet_log.rs（11）、wlua/src/lib.rs（12）
   对标：garnet/libs/server/tests/（集成测试独立程序集）
   问题：跨 crate 装配会话/ACL/PubSub/事务的测试占 src 编译单元（全量重编 + 二进制膨胀），与 wnode/tests/ 双轨
   改法：需装配多 crate 运行时或真实存储设备的迁 wnode/tests/（首批 resp_server_session.rs 62 个）；纯算法/纯解析留 src

20. 监听端口 dyn 消除终局（三端口处理记录）
   - VersionShiftFn：调用点组合消除。wkv CheckpointManager 不再持 hooks 槽（生产本就零接线，fire 恒 no-op），版本切换通知上提为 ClusterProvider::notify_version_shift_start/end 两个显式方法，检查点发起方在快照前后调用——注意生产接线仍待 AOF 门控复制面完工时补挂（对标 C# checkpointVersionShiftStart/End 委托的 rust 形态）
   - ReplicationSinkFn：信号化拉取消除。WalLog 环形缓冲本身是无锁 MPSC（帧已在共享容器），推流端口降级为容量 1 唤醒信号（crossfire MAsyncTx<Array<()>>，满即折叠）；AofReplicationPump::attach_wake 注册信号 + spawn 增量拉取循环（pump_backlog 从各副本已发位点扫至 safe_tail_address，序=环形缓冲线性化序，与原同栈直推的复制序保证等价）；sync_backlog 的 until 面从 committed 改 safe_tail（与原直推可见面一致，副本提前拿未 commit 帧、位点后 ACK 的语义不变）
   - StoreEventSink：保留 Arc<dyn Fn>（事件环拉取方案否决：借用事件 owned 化违反零拷贝 + 线性化保证退化，详见 wkv/src/store/event.rs 注释）
   - 定理：底层定义端口、上层注入捕获上层类型闭包的场景，dyn 是 Rust 的不动点；能消除的只有「回调可上提为调用者显式步骤」（组合）与「数据已在共享容器、回调可降级为信号」（拉取）两类

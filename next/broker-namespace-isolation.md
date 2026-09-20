# 阻塞族 CollectionItemBroker 跨租户命名空间隔离

来源：next/zcode-r7-redteam.md 问题 P0-1

## 问题

1. 阻塞命令（BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP）在向全局 CollectionItemBroker 注册观察者时直接使用原始用户键名，未包含当前会话的 namespace。
2. 经纪取件会话（broker_session）采用单例存储会话，默认在 ns0 上下文操作。
3. 导致任意非 0 租户可通过阻塞命令越权消费或注入 ns0 的队列数据，或与其他租户同名键发生串扰。

## 涉及路径

- wedb/wnode/src/resp/objects/list_commands/blocking.rs
- wedb/wnode/src/resp/objects/sorted_set_commands/
- wedb/wnode/src/resp/objects/collection_item_source.rs
- wedb/wcol/src/itembroker/collection_item_broker.rs
- wedb/wnode/src/resp/resp_server_session/pump.rs

## 解决建议

1. 观察者注册键与唤醒通知键采用 namespace 前缀隔离（类似 pubsub ChannelNsPrefix 模式）。
2. 取件执行时，使用观察者自身的会话上下文或根据观察者所属 namespace 动态设置会话前缀执行。
3. 增加跨命名空间同名阻塞队列互不干扰的回归测试用例。

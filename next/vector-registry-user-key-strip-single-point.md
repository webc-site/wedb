优先级：重复惯用语 + 反向兜底成泄漏面

4 单问题：登记表复合键「剥域取用户键」这一动作有两处各写一遍，且解码失败时的兜底方向是错的——
失败就把**带域前缀的整键**当用户键发出去，防御臂反成泄漏面。

取证现状（2026-09-19 由向量登记键修红票代理上报，主代理未复核，须自行验证）
- 两处同形写法：split_registry_key(rk).map_or(rk, |(_, k)| k)
  1 wedb/wedb/src/server/replication/snapshot_iter/replication_snapshot_iterator.rs:240
  2 wedb/wedb/src/server/cluster_session/migrate_session/migrate_session_vector_set.rs:75
  （路径若已位移，按符号 split_registry_key 重定位）
- 登记表写入侧单点是 registry_key（复合键 = [NsVarint][DbVarint] + 用户键，对位 C#
  ./garnet libs/server/Resp/Vector/VectorManager.cs:171 / :173 的 dbId 首参形态），
  帧面按 C# 剥域发用户键。

修法（一个机制只留一处）
把剥域动作收成一个公开单点（建议命名 registry_user_key，放登记表所在模块，与 registry_key 配对），
两处调用点改为直调；失败兜底不得返回带域前缀的键——登记表键必由本模块自身构造，解析失败即
不可达态，按本仓既有口径显式失败（panic!/unreachable 一类既有风格，取该模块现用的那种），
不许静默降级。禁止新增第二层可选返回或兼容分支，禁止保留旧的两处内联写法。
补一条确定性用例：对 registry_key 构造出的复合键，registry_user_key 还原结果逐字节等于用户键；
并按当前编码构造一个畸形输入证明其显式失败（断言不得恒真、不得用 catch 兜住后放过）。

改动域：向量登记表模块 + 上述两个调用点及其测试。
避让：wkv/src/vdb.rs、wnode/src/storage/**、waof、wresp 各有修红代理在跑；本票不碰登记键的
编码格式本身（格式改动须另票）。

优先级：高（dev 测试基线红，计数轮前置）

单题：运行时配置归一与 store GC 装配链在当前 dev 上红。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1 wconf::runtime_server_config::time_span_non_positive_means_infinite —
  wconf/tests/runtime_server_config.rs:117
  left: Ok(Some(ClusterNodeTimeout { ms: 0 })) / right: Ok(None)
  即「非正时长表示无限」的归一被绕过，0 被当有效值原样下发。该断言与把 node-timeout 接进
  热更新消费者链的落地（b304c5ee 一带）同时进树，怀疑归一段被旁路。
2 wnode::config_owner_bridge::provider_runtime_config_wired_to_store —
  wnode/tests/config_owner_bridge.rs:174 「生产 store_config 默认启用 GC，装配后循环必须在跑」
3 wnode::object_collect_task::replica_gate_suspends_and_resume_restarts —
  wnode/tests/object_collect_task.rs:266 「前置：GC 扫描在跑」
  2/3 同指生产装配路径下对象回收/GC 循环没起来：写侧齐、读侧零消费者，属功能缺口（须接线），
  不是死代码，禁止以「删常量/删旋钮」交差。

判读方向（须自行核实）
先 git log -p 追 wconf 运行时表归一段与 wnode/src/service.rs 的 store_config / HlogOptions 投影段
最近合入（b304c5ee 热更装配、reviv 旋钮批），确认丢的是归一还是消费者读了另一份配置副本。
只允许一条配置投影真源，不得新建第二套装配面。

避让（这些票/分支在途，勿越界）
fix-min-page-guard（wconf/src/size.rs、node_options.rs 页大小校验核）、fix-reviv-knobs
（wconf/wkv/wnode 的 HlogOptions 装配面）、fix-now-stopwatch（wbase/src/time.rs 计时域）。
wkv 存储层 dbmeta/NS_MAP 属 qw13-red-wkv-dbmeta-readcache-cluster；wnode/src/resp 会话门属
qw13-red-resp-null-and-command-gate。

改动域
wedb/wconf/src/**（运行时表归一段）、wnode/src/service.rs 的 store 装配段、对象回收任务启动判定，
以及上述三个测试文件。禁止触碰 wkv 存储层核心、waof、wresp。

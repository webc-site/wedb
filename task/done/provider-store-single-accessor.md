StorageSessionProvider 引擎双取口：pub store 字段绕过置换槽，与 store() 方法语义分叉

来源：glm.design 第 8 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状
- 两取口并存：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:846
  `pub store: SharedStore<SegmentedDevice>`（固定装配期引擎）与同文件 :1315-1320
  `pub fn store(&self) -> SharedStore<SegmentedDevice>`（:1317-1319 store_swap 槽优先、
  回落字段）。私有槽 `store_swap: StoreSwapSlot` 在 :850（其注释 :847-849 自述
  「读面统一走 [`Self::store`]」），句柄导出口 :1304 `store_swap_slot()`。
- 语义差无锚点提示：字段直取静默绕过在线引擎置换（副本检查点导入闭环，
  /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs 的 swap_online_store 面）。
  当下不构成 bug：集群侧 boot.rs:94 `cluster.set_store(Arc::clone(&provider.store))`
  为装配期注入（彼时槽为空），且 boot.rs:229 另调 `set_store_swap_slot(provider.store_swap_slot())`
  使运行期置换在 cluster 侧闭合——双写通道正是「两取口」的产物。
- 字段直取消费面清点（即本票射程）：
  /Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs:94；
  /Users/z/git/db/wedb/wedb/wedb/tests/cluster_flushall_broadcast.rs:71；
  /Users/z/git/db/wedb/wedb/wnode/tests/config_owner_bridge.rs:170/:176/:180/:181/:182/:184/:186；
  /Users/z/git/db/wedb/wedb/wnode/tests/recover_test.rs:150。
  同文件内部使用（service.rs:1088/:1097/:1102/:1103 等）属私有访问，不需改动。
- 同结构面 lock_table：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:869 `pub lock_table: TxnLockTable`
  外部零消费（全仓无 `provider.lock_table` 命中；grep 命中的均为
  wlua/wnode 会话自身字段与测试自建 env 字段），仅 service.rs 内部 :471/:1462 自用，pub 面过宽。
  其注释 :861-868 已明确「引擎置换不换锁表面」，故私有化不涉语义变更。
- 风险性质：非「按行数切文件」式美学主张，而是同一事实两声明点且二者语义不同——
  新增取口者按 pub 字段写即静默绕过置换，评审无从发现（无编译期提示）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:41
  `public TsavoriteKV<StoreFunctions, StoreAllocator> store => databaseManager.Store;`
  ——单一计算属性，每次取值转发当前库引擎，恢复/置换后全体调用方自动见新引擎，
  结构上不存在第二取口；同文件 :46 appendOnlyFile 同形态属性转发（rust 对位已是
  `pub fn aof()` 方法 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1323，即正确形态先例）。
- 对照组：rust 侧 aof()/wal()（:1323/:1329）均为方法形暴露，字段私有；唯独 store 与
  lock_table 保留 pub 字段，属同族结构里的漏网面。

修法
1. `store` 字段收私有（:846 去 pub），全体外部消费改调 :1315 `store()`：
   boot.rs:94 改 `Arc::clone(&provider.store())`（该处语义为「取当前在线引擎」，改后更正确），
   三处测试文件同改（字段访问改方法调用，期望不变）。
   禁为省改测试而保留 pub 字段并另加 `#[doc(hidden)]` 之类的第二形态。
2. `lock_table` 字段收私有（:869 去 pub）：外部零消费，仅需确认内部 :471/:1462 与
   session_dependencies 注入链不受影响；若测试确需句柄，经既有装配函数取，不开字段。
3. 取口单点性固化：在 :847-849 注释补一句「引擎取口唯一为 store()，置换后自动转发」，
   并把 swap_online_store 侧的双写要求收敛为「cluster 持 store_swap_slot 单源」——
   若 boot.rs:94 改为方法取口后 :229 的 swap 槽注入成为冗余，一并删除（禁留两条通道）；
   若仍必要（运行期置换须反映），则 :94 注入降级为初值、注释点名以槽为准。此判定须读
   cluster_provider 取用点后落，不许猜。
4. watch_version_map（:860）、vector_manager（:858）等同族 pub 字段不在本票射程：
   它们无第二取口，不动（避免把票扩成字段可见性大扫除）。

优先级
重复/多套架构（同一引擎取口两声明点语义分叉，是置换类缺陷的长期温床；
当前无已发故障，故列于死代码/污染扩散之后、功能缺口之前）。

协调
- 与 in-flight wnode service 巨峰文件拆分票（next/wnode-service-split.md，纯移动拆分）
  同文件：本票改可见性与调用点，须在该票之后或与其错开，禁在纯移动里夹带语义变更。
- 与 task/ing/endpoint-parse-fail-fast-multi-bind.md 同在启动装配链（boot.rs / server.rs）：
  两票若并行，boot.rs 改动顺序为「本票先收口取口、那条改 GarnetServer::new 签名」，避免同函数双改。
- 若 step 3 判定删除 boot.rs:229 的 swap 槽注入，须先确认
  next/wait-for-commit-chain.md 等集群装配票未在该处挂新钩子。

验收
- grep 归零：`pub store:`、`pub lock_table:` 于 service.rs 不存在；
  外部 `.store`（非 `.store()`）字段访问仅余同模块内部使用。
- 副本检查点导入置换后：新会话与 cluster 侧取到的引擎为同一新实例（经现有
  checkpoint_import 测试面断言，禁只看编译通过）。
- boot.rs 注入面若发生删并，须在注释点名 C# StoreWrapper.cs:41 单属性依据。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning。

落地记录（分支 provider-store-single-accessor，实施后行号为当下实况）
1. 引擎取口收口单点：
   - /Users/z/git/db/wedb/wedb/wnode/src/service.rs:852 `store` 字段去 pub，
     注释自述「非取口，直取绕过在线置换，仅限本模块装配期与未置换回落」。
   - /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1324 `pub fn store()` 为本
     结构唯一引擎取口，注释点名 C# libs/server/StoreWrapper.cs:41 单计算属性依据。
   - 外部消费改道：boot.rs:97 `cluster.set_store(provider.store())`；
     wedb/tests/cluster_flushall_broadcast.rs:71；
     wnode/tests/config_owner_bridge.rs:168-186（改绑定一次 `let store = provider.store();`
     后复用，取代 7 处字段直取）；wnode/tests/recover_test.rs:150。
     同文件内部 :1095-:1110（NodeService 装配）与 :1328（回落分支）为私有访问，未动。
2. lock_table 收私有：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:876 去 pub，
   注释补「对外无取口，会话侧经 session_dependencies(:1471) 注入」。
   复核全仓 `.lock_table` 命中均为 wlua/wtxn/wnode 会话与测试自建 env 自身字段，
   确为零外部消费；未新增 getter（不开字段）。
3. step 3 判定（读点清点后落，非猜）：ClusterProvider 引擎读点唯一为
   cluster_provider.rs:798 `try_store()`（消费面 suspend/resume_primary_tasks
   :808/:828/:838/:844、checkpoint_import_ctx :1098、
   cluster_manager_slot_gate.rs:473/:483、replication/assembly.rs:63、
   replica_diskless_sync.rs:138、replica_diskbased_sync.rs:129、
   replica_sync_session.rs:356、cluster_session/basic.rs:388 等），写点仅
   set_store(:793) 与 swap_online_store(:1086)。故 boot.rs:235 的置换槽注入
   不可删——删后宿主 store() 与集群 try_store 分叉，检查点导入置换的新引擎
   到不了新会话装配面（真功能断裂）。本票按「cluster 持引擎槽单源」收敛第二通道：
   - 删除 `store: RwLock<Option<Arc<WedbStore>>>` 拷贝字段与
     `store_swap_slot: RwLock<Option<StoreSwapSlot>>` 句柄字段的二元状态，
     合并为 cluster_provider.rs:133 单字段 `store_slot: RwLock<StoreSwapSlot>`；
   - swap_online_store(:1086) 由「本层视图 + 宿主槽」双写改为单写本槽；
   - set_store(:793) 语义为向本槽播种；set_store_swap_slot(:979) 采纳宿主槽时
     把已播种引擎迁入宿主槽，故与 set_store 先后次序无关；
   - boot.rs:92-97 注入降级为「装配期播种」并在注释点名以槽为准、
     boot.rs:232-235 注明采纳后两侧同源；未删任何注入点（协调条 3：
     wait-for-commit-chain 关心的钩子位置原调用保持原位，仅改注释与右值）。
   结果：C# 侧「cluster 经 storeWrapper 反查当前引擎、全链一份引擎状态」在
   rust 对位为同一 StoreSwapSlot（同一 Arc<RwLock<Option<_>>>）单源，
   双写通道与「两取口」产物一并消除。
4. 验收核对：
   - `rg 'pub store:|pub lock_table:' wedb/wnode/src/service.rs` 零命中。
   - 字段私有 + `cargo check --workspace --all-targets` 零 error 零 warning
     （私有 target /tmp/fork/provider-store-single-accessor/target；合入 dev
     后复跑一次，增量 10.07s，exit=0）——外部 `.store` 直取已成编译期不可能，
     强于 grep 归零；同仓其余 `.store` 字段命中均属测试自建 NodeStorage/fixture
     结构，与 provider 无关。
   - 同一新实例由结构保证：置换后 cluster.try_store 与 provider.store() 读同一槽，
     无可分叉的第二份。checkpoint_import.rs 断言面（:147/:368/:528/:815
     provider.try_store() 换持新引擎）语义未变；本票按纪律未跑测试，
     该回归留主门禁 test.sh 验证。
5. 本票不做：watch_version_map(:866)/vector_manager(:864) 等无第二取口的 pub
   字段维持可见性（第 4 条明确不扩围）。
6. 合入：dev e7cf61b（快进，代码提交 5fd714e + 注释校正 ea6544d）。
   门禁留主仓：本票按纪律仅跑 cargo check，未跑 test.sh/clippy.sh/check.js，
   checkpoint_import / cluster_flushall_broadcast / config_owner_bridge /
   recover_test 回归待主门禁复验。



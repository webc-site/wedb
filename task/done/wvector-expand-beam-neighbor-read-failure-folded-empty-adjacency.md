收口记录（2026-10-01 r8 波，主控全量审计 + 独立复验合入 dev 64e1c7b）：
- 席位 12559e1（基线 81a3b69 沙箱灭失后于重建 tip 9873ba5 复位重施），改动面实测
  dynamic_quant.rs +21/-3 与新册 wvector/tests/expand_beam_neighbor_read_failure.rs +279，
  与同波 filter 域（filter/mod.rs 等）零文件交集，未触 cache.rs / 回调层 / wnode/tests/*。
- 三臂（expand_beam / expand_beam_filtered / expand_beam_accept_only）各比照剪枝臂
  DelegateNeighborAccessor 既有 Err(ANNError::from(StoreError::Read)) 形制把 bool=false 单轨映射，
  错误路径先归还 id_buffer 再上抛，与 harvest(:90-114) 同一机制，零新机制。
- 覆盖边界裁定（合入注记原文）：accept_only 臂系 FilteredAccessor trait 实现位，本仓公开检索面不派发
  （唯一库侧消费方在 vendored 核 MultihopFilterSearch），集成册不可真实驱动，席未造公开入口亦未编 mock，
  只在册头明文登记边界——判定合规，不另号备案。
- 锁测两例（expand_beam_surfaces_adjacency_read_failure / expand_beam_filtered_surfaces_adjacency_read_failure）
  经注桩注入邻接读失败，断言报错帧而非静默空邻域。
- 主控独立复验：cargo check -q -p wvector --all-targets -j 3 EXIT=0 零警告；
  cargo nextest run -p wvector -j 3 → 65 tests run: 65 passed 0 skipped；
  bun js/check.js EXIT=0，重复定义仍仅 wmetric GetLatencyMetrics 一对、实现缺失清单无增项（零回归）。
- 禁触域清单核对：本票合入未含 §181 回调层三态化（该向另案在册，禁走口径已遵守）。

锁定注记（2026-10-01 主控 r8 波，只读甄别席双侧现码亲验 + 主控复核）：甄别结论：通过，定级 P3（触发前提为存储读失败窗，后果为召回静默退化，无数据错写）。现位订正（本票行为 09-30 tip 复测，行号以本注记为准）：
- rust 折叠点 dynamic_quant.rs:567/:619/:667 三臂 bool 裸丢弃 —— 同位精确（三函数体现位 550/602/650）；
  收割纪律注释 :573/:630/:684 同位，harvest 现位 90-114（false→Err(StoreError::Read)）。
- cache.rs 侧现位：get_neighbors 函数体 283-311，失败臂 finish(0) 置空邻接 :299-306；
  剪枝臂 DelegateNeighborAccessor::get_neighbors 透明上抛 :404-418；
  第三先例（neighbors 命令臂 false→Err）:238-240；悬垂竞态窗自陈注释段现位 :218-228（票面 :223-232 微漂）。
- 规范参照 webc-diskann-0.59.0-webc.7/src/graph/test/provider.rs 现位 :1203/:1220/:1224
  （`get_neighbors(...)?` 与 `allow_transient` 分臂；票面 :1222/:1226 微漂）。
- C# 无对位复核：garnet/libs/**/*.cs grep beam 零命中，判据落同仓双轨与 vendored 核规范形，成立。
- 本缝确作用于在线检索：库侧 graph/index.rs:2008/:2164、ext/labeled.rs:187/:205 调用本三臂。
- 重复性：五池 + deviations.md grep 零命中；邻案 task/done/wvector-store-read-failure-folded-to-empty-missing.md（§181）
  裁的是命令面（enumerate/exists/VREM）且其收口明文备案「回调层 IO 故障与缺失同形宜另案」，本票正交非并案。
- 执行口径钉死：只走票面方案 1 首案——三臂比照 DelegateNeighborAccessor 既有机制把 false 单轨映射 Err，
  零新机制、不动 cache.rs、不动回调层；方案 1 第二条（宿主读三态化）落 wnode/src/resp/vector/vector_store_callbacks.rs
  禁触域且系 §181 另案，本票禁走。
- 锁测只在 wvector/tests/ 新增册内做（邻接臂注桩注入读失败，断言检索报错帧而非静默空邻域），
  禁改 wnode/tests/*（同侪在途域）。

审核结论：通过（2026-09-30 甲轮48 审核席；P3。不撞 §181（该票裁决面为命令面折叠，备案明言宜另案）；前提句已按审核席订正改收割入场门形、C# 对位行补 cache.rs:238 第三先例与 allow_transient 语义对照）

检索 beam 三臂把邻接表读失败折叠为空邻域，静默半截检索与同仓剪枝臂透明上抛双轨

问题分析：
1. Garnet 契约对齐：webc-diskann vendored 核的规范实现 expand_beam 为 get_neighbors(...)? 错误传播（webc-diskann-0.59.0-webc.7/src/graph/test/provider.rs:1203 起参照形）；C# 无对位（diskann 图核自研 vendored 承接）。同仓剪枝臂 DelegateNeighborAccessor::get_neighbors（cache.rs:404-418）对同一 bool 失败映射 Err(ANNError) 透明上抛；dynamic_quant.rs:573/:630/:684 收割纪律自陈「冷区收割失败⇒报错终止（禁止静默半截结果）」。
2. 工程现状：DynamicAccessor 三臂 expand_beam/expand_beam_filtered/expand_beam_accept_only 对 provider.get_neighbors(context, nl_id, &mut id_buffer).await 的 bool 返回值裸丢弃（dynamic_quant.rs:567/:619/:667）；下游 cache.rs:299-306 get_neighbors 读失败臂 guard.finish(0) 置空邻接。展开节点以收割向量距离读成功为入场门（缺项即不入 beam），其邻接记录理应在场（残余竞态窗仅并发删除半途 mark_free 先于图边回收，cache.rs:223-232 自陈，与剪枝臂、neighbors 命令臂已接受姿态同型），邻接冷读失败（宿主 ReadOutcome::Failed 窗）时三臂把失败折叠为空邻域，beam 静默缺边。
3. 逻辑危害确证：检索召回退化零信号；同源错误在剪枝臂透明、检索臂折叠构成双轨，违板块 4.1 异常收敛与仓内 store.rs 回调契约自陈。触发前提为存储读失败窗（罕见），定 P3。

涉及代码：
rust 文件与函数：
wedb/wvector/src/provider/dynamic_quant.rs:567/:619/:667 DynamicAccessor::expand_beam / expand_beam_filtered / expand_beam_accept_only（bool 丢弃点）
wedb/wvector/src/provider/cache.rs:299 get_neighbors（失败臂 finish(0) 置空）

对应 c# 文件与函数：
无对位（webc-diskann vendored 核自研承接，garnet DiskANN 走原生服务互无托管 beam 面；对账锚为同仓 cache.rs:238 与 cache.rs:404 两处透明上抛臂、webc-diskann-0.59.0-webc.7/src/graph/test/provider.rs:1222 规范形 get_neighbors(...)? 及 :1226 allow_transient 对照——规范语义有意区分「距离读暂态可跳过、邻接读失败即硬错」，上抛方案系语义同构）

精炼执行方案：
1. 三臂比照 DelegateNeighborAccessor 同轨把 false 映射 Err 上抛（检索失败帧）；或随宿主读三态化（Miss/Failed 分辨，§181 备案「ExtMap/IntMap 层 IO 故障与缺失同形宜另案」同批）后仅 Failed 臂上抛、Miss 臂维持空邻域
2. 锁测：读桩在邻接臂注入 Failed，断言检索报错而非静默空邻域

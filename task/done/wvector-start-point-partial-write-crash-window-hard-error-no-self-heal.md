主控复验（2026-10-01 r8 波收票审计，交付 commit 6936696 + 补稳 a3ecbd6，改动面 cache.rs +65 / startpoint_cancel_reset_reload.rs +209 / deviations §192）：
- 自愈写「补的是零缓冲还是脏缓冲」这一关键前提亲验成立：store.rs:523 read_single_raw 仅在
  `data.len() == value.len()` 时才 copy_from_slice 并置 found，读失败与长度不合两形皆不动缓冲 ⇒
  cache.rs:51 的 `vec![0u32; max_degree + 1]` 到自愈写点必为全零（尾槽 len=0），补写即真空邻接表，无误落垃圾之虞；
  宿主臂（vector_store_callbacks.rs 的 read_varsize_owned 整读形）同读法复核同结论。
- 三处自愈臂（Neighbors 0 / Q8 量化记录重建 / Bin 量化态重建）均按票面方案 1 就地补写，
  未新建第二机制、未改 maybe_set_start_point 写序（claim 在先的幂等认领前提保住）；
  start_points_exist 加量化维门与 ensure_index_ready_or_init 失败臂复位口径相容（重试收敛而非重入硬错）。
- 崩溃窗锁测两例（Neighbors 前瞬态失败 / 量化 0 失败后重跑装载臂）在册；复验台账 §192 与票面判据一致。
- 门禁面：本波 wvector 定向 nextest 因并发席主树 clippy 长期占锁而未跑成，改由波次终轮 test.sh 覆盖（见 r8 波尾记录）。

锁定注记（2026-10-01 主控 r8 波，只读甄别席双侧现码亲验 + 主控现码复核）：甄别结论：通过，定级 P3（触发需段间崩溃或单次 write_iid 瞬态失败，罕见；后果为索引永久砖死须人工清库，属可用性级非静默错数据，不足 P2）。
- 现位复核（本波 tip 实测，票面行号微漂以本注记为准）：
  cache.rs:32-34 契约注释同位精确；read_start_point_core 函数体现位 38-60，硬错臂 52-56（票面 :52-57）；
  maybe_set_start_point 131-211，写序 claim :159 → Vector 0 :161-167 → 量化 0 :169-190 → Neighbors 0 :192-198；
  量化态同型第二腿 cache.rs:139-150（量化记录缺→Err :149）；
  装载臂 data_provider.rs:299 `?` 直传同位精确（new() 现位 297-303）；
  service.rs ensure_index_ready_or_init 现位 916-967（票面 :909-960），失败臂仅复位 NoStartPoints 现位 950-956；
  建籍即败 service.rs:990-991（create_index_impl provider `.await?`）；
  VADD 恒 ERR 佐证 service.rs:1181-1188（折 DiskAnnInsertResult::StoreError）；
  幂等认领相容性 fsm.rs:423-436 claim_start_id（next_id/max_block 首认 + mark_id_unchecked(0,true)）——补写无并发铸造者。
- C# 侧复核：VectorManager.Callbacks.cs:366-383 WriteCallbackUnmanaged 每笔独立 Upsert+CompletePending 落盘确认，
  全目录 grep StartPoint/start point 零命中，半截态无对位处理；判据为仓内自陈契约 + 双窗不对称（claim 后自愈、
  neighbors 前恒硬错），成立。
- 灭失核查：现码 read_start_point_core 仍恒报 Err、无补写臂，形态未变。
- 重复性：done/wvector-ensure-index-ready-startpoint-cancel-stuck.md（§174）只裁「取消窗状态机永卡」并引入幂等认领，
  恰好覆盖第一窗（claim 后 vector 前）；第二窗（vector 后 neighbors 前）与量化腿无票裁过，deviations.md 无相关条
  （§47 系 objects 读臂删空，正交）→ 属 §174 收口后暴露的残余缝，非并案。
- 执行口径：自愈只在 id 0 专属形态「claim 在先 ∧ Vector 0 在场 ∧ Neighbors 0 缺席」触发（起点邻接按构造恒空，
  补写空表与首建事实一致），量化记录缺同臂一并收口；改动面 wedb/wvector/src/provider/cache.rs 为主，
  仅当走 init 失败臂变体才动 wedb/wvector/src/service.rs（**注意是 wvector 侧 service.rs，非 wnode/src/service.rs 禁触域**）。
  C# 无对位的防御改良，须落 doc/zh/deviations.md（§18 谱；新号先 grep 裸号，现册最大在册号 §190，禁撞「号位空缺清单」占用位，禁钉行号）。
- 禁触域（同侪主树在途编辑）：wedb/wnode/src/resp/**、wedb/wnode/src/aof/**、wedb/wnode/src/service.rs、
  wedb/wedb/src/server/replication/**、wedb/wcpr/**、wedb/wdev/**。
- 锁测：复用既有册形 wedb/wvector/tests/startpoint_cancel_reset_reload.rs（:53/:88/:177 write_iid 闸门注桩）
  与 wedb/wvector/tests/insert_graph_stage_failure.rs，新增「Vector 落成功、Neighbors 注失败」崩溃窗断言，
  重载后起点可用、VADD/VSIM 不再恒 ERR。

审核结论：通过（2026-09-30 甲轮48 审核席；P3。C# 对位补 WriteCallbackUnmanaged 函数名与 :366 落盘确认、危害段补构造恢复先行中招两臂与在-life 瞬态失败窗、自愈方案与 claim_start_id 幂等认领相容性经读码确证）

向量索引起点三段写崩溃窗遗留「向量在邻接缺」半截态，重启恒硬错无自愈臂索引永久砖死

问题分析：
1. Garnet 契约对齐：C# 原位恢复不换实例、起点由原生库按同序回调写（VectorManager.Callbacks.cs 回调序），半截态无对位可见处理；判据落仓内自陈契约——cache.rs:32-34 注释「向量在而邻接缺失即存储损坏，报 StoreError::Read」与 claim_start_id 幂等认领自陈（cache.rs:156-158「装载 future 被取消丢弃后……幂等重认领即收敛」）。双窗不对称：claim 后 vector 前崩溃可幂等自愈，vector 后 neighbors 前崩溃恒硬错。
2. 工程现状：maybe_set_start_point 写序 claim_start_id（cache.rs:159）→写 Vector 0（:161-167）→[量化 0]（:169-190）→写 Neighbors 0（:192-198），每笔 write_iid 独立落盘确认，段间无原子性保障。重启后 read_start_point_core「向量在邻接缺」臂恒报 StoreError::Read（cache.rs:52-57），且构造恢复先行中招：WedbProvider::new 经 data_provider.rs:299 ? 直传同型硬错、create_index_impl 折 CreateIndexError 索引注册即败；ensure_index_ready_or_init（service.rs:909-960）失败臂仅复位 NoStartPoints，重试恒重入同一硬错路径，无清障/补写臂。窗口亦不限于断电：在-life Neighbors 0（或量化 0，:179-185）write_iid 单次瞬态失败即落同型半截态，无崩溃同砖，执行方案自愈臂同覆盖。
3. 逻辑危害确证：进程崩溃于 Vector 0 落盘后、Neighbors 0 落盘前→重启后该索引所有 VADD/VSIM 永久 ERR，fsm 占用位已置、无任何代码路径清除或补写半截起点，须人工清库。定 P3。

涉及代码：
rust 文件与函数：
wedb/wvector/src/provider/cache.rs:131 maybe_set_start_point（三段写序）
wedb/wvector/src/provider/cache.rs:38 read_start_point_core（:52-57 硬错臂）
wedb/wvector/src/service.rs:909 ensure_index_ready_or_init（失败臂复位重试无清障）

对应 c# 文件与函数：
libs/server/Resp/Vector/VectorManager.Callbacks.cs:WriteCallbackUnmanaged（:366 每笔回调独立 Upsert+CompletePending 落盘确认、原生库按同序逐笔回调写起点；全目录无对位半截态处理，判据为仓内自陈契约与双窗不对称）

精炼执行方案：
1. read_start_point_core「向量在邻接缺」改自愈语义：就地补写空邻接表（claim 在先的幂等认领已保证无并发铸造者；Bin 量化态起点量化记录一并重建），或 ensure_index_ready init 失败臂识别该形态重跑 maybe_set_start_point 尾段
2. 补崩溃窗锁测：写桩在 Vector 后 Neighbors 前注入失败，断言重试收敛起点可用

终态注记：
- 合入收口形态：在 wedb/wvector/src/provider/cache.rs 中增加起点半截态（Vector 0 存在但 Neighbors 0 缺失）与量化态下缺失量化记录时的就地补写自愈；台账登记入 doc/zh/deviations.md [§192]；补齐重试与重启自愈锁测。
- 合入哈希：6936696
- 状态：已收口归档。


甄别结论：通过（甄别席 zc-fix-r16-vadddrop，2026-09-26）定级 P2
核验记录（逐锚现码复跑）：
1. rust 现状锚成立：resp_server_session_vectors.rs:1055 drop(lock) 确在 read_or_create_vector_index(:1047-1054) 与 manager.try_add(:1069) 之间；头注 :1007-1016「guard 随 async 栈帧跨 await 覆盖 try_add 全程」与 :1018-1020「条带读锁立即 drop 绝不跨 await」两段自相矛盾、后者与代码同流属实；同文件全部只读臂 _index_guard/_guard(:1125/:1301/:1352-:1500) 与 network_vrem(:1539)/network_vsetattr(:1578) 均保活守卫跨写体 await，唯 VADD 截短，系现码实况。
2. C# 锚成立：garnet VectorStoreOps.cs:192 using(vectorManager.ReadOrCreateVectorIndex) 罩 TryAdd(:203) 与 OK 后 ReplicateVectorSetAdd(:208) 全程，:201-202 锁内注释「holding a shared lock / That lock prevents deletion」原文亲验；VectorManager.Locking.cs ReadVectorIndexCore(:88)/AcquireExclusiveLocks(:544)/ReadForDeleteVectorIndex(:556) 逐行在位，排他删除等共享锁释放的互斥协议成立。
3. 危害链锚成立：wvector/src/service.rs:1110-1112 index(context) miss 回 False、vector_manager.rs:697 False 折 Duplicate（客户端伪 :0 应答）；service.rs:1020-1022 index() 经 get(&context).cloned() Arc 保活、drop_index(:1089) 摘表后 insert 仍可续写保活旧索引；request_deletion(vector_manager.rs:770→:791/:1103) 同步摘 service 注册表属实；delete_vector_set 排空集注释(:805-808「VADD 持读或建锁……全部落在屏障内」)与现码分叉属实；测试 vector_delete_exclusive_lock_race.rs seed()(:206-232) 确按持守卫跨 try_add 插入范式书写。
4. 查重成立：deviations.md §22/§23/§44/§75/§79/§115/§127/§128 及 VectorSessionUAF 裁决面均不覆盖「键锁域被显式 drop 截短」维度；全仓 drop(lock) 三点中 vector_manager_quantization.rs:155 归姊妹票 wnode-quant-lock-drop-before-backfill（量化 worker 轴，另一 C# 锚 VectorManager.Quantization.cs）、vector_manager_replication.rs:255 系副本端顺序重放豁免位，与 wnode-quant 票同族不同轴各立独立票，非重复非灭失。
5. 架构合规：条带基座为 async_lock::RwLock（vector_manager_locking.rs:6-7 模块头自证守卫可跨 await），修复不持同步锁跨 await、不触 compio 禁忌；try_add 与 replicate_vector_set_add（纯同步 aof 入队）链路不取 vector_set_locks（全文件仅 :818/:936/:1047 删除位取锁），无重入自死锁；方案仅守卫保活改名+注释订正+交错测试，单向分层单套机制无过度设计无假桩。
6. 可执行度：改动点具体到行（:1047-1055 删 drop 改 let (index, _lock)），验证闭环（VADD×DEL 交错终态双闭合断言 + 既有 race 测试零回归），路径纯文本双侧齐全。

审核结论：通过
审核席：zcode-r17-review-vadd（2026-09-26，dev 分支）
双侧源码亲验属实：rust drop(lock) 位于 resp_server_session_vectors.rs:1055（read_or_create_vector_index 与 try_add:1069 之间）；C# VectorStoreOps.cs VectorSetAdd 以 using 罩 TryAdd 与 OK 后 ReplicateVectorSetAdd 全程，锁内注释原文一致，ReadForDeleteVectorIndex（VectorManager.Locking.cs:556）走 AcquireExclusiveLocks 排他删除互斥成立；service.rs:1110 index(context) miss 回 False、try_add:697 折 Duplicate 的伪应答映射链与 Arc 保活交错 AOF 序倒置危害链均成立；头注 :1007-1016 与 :1018-1020 两段自相矛盾、后者与代码同流属实；seed()（vector_delete_exclusive_lock_race.rs:206-232）确按持守卫插入范式书写。
修复合规性核验：条带锁为 async_lock::RwLock（vector_manager_locking.rs 模块头自证守卫可跨 await，compio 任务不迁线程），非同步锁，保活跨 await 不触「执行期严禁跨异步操作持有同步锁」红线，§128 在册先例同款（async_lock 守卫 Send 可跨 await）；try_add 体内不取 vector_set_locks 条带锁（存储回调走独立 StripedSerialLock 表），replicate_vector_set_add 纯同步入队无锁，无重入自死锁；VREM/VSETATTR 臂 _guard 保活跨 await 为编译在案先例，借用面同构（守卫与 try_add 同为 &self.manager 不可变借用）。查重：§22/§23/§44/§75/§79/§127/§128 与 VectorSessionUAF 裁决均不覆盖「键锁域被显式 drop 截短」维度，非重复提报。

整理执行方案（供 task/fix.md 直接消费）：
1. wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:1047-1055：删去 drop(lock)，绑定改 let (index, _lock)，守卫随 async 栈帧跨 index.to_bytes、manager.try_add 与 replicate_vector_set_add 存活至函数返回（对齐 C# using 全程与 network_vrem/network_vsetattr 的 _guard 同款；借用无冲突，VREM 臂为编译先例）。
2. 同步订正两处注释锚：函数头注 :1018-1020 第二段（「条带读锁仅在检索与建桩期持有……立即 drop 释放，绝不跨越 await……杜绝自死锁」）系旧 inline_wait 同步锁时代过时顾虑，删改后与第一段「覆盖 try_add 全程」统一；vector_manager.rs:805-808 delete_vector_set 排空集注释「VADD 持读或建锁……全部落在屏障内」修复后即为真，保持不动。
3. 测试验证点：wnode/tests 增补 VADD 与 DEL 交错测试（慢臂注入 await 让位点模拟 DEL 插队），断言终态为两闭合结局之一——VADD :1 后 DEL 生效、或 DEL 先行且 VADD :0——AOF 序与主端登记表终态一致，无孤儿物理记录；既有 vector_delete_exclusive_lock_race.rs 全绿零回归。

VADD 会话臂在 try_add 前显式 drop 索引读锁，锁域与 C# using 全程持锁分叉，并发删除竞态窗口敞开

问题分析：
1. Garnet 契约对齐：C# StorageSession.VectorSetAdd 以 using (vectorManager.ReadOrCreateVectorIndex(...)) 把共享锁的生存期罩住 TryAdd 与 OK 后 ReplicateVectorSetAdd 全程（VectorStoreOps.cs:192-215），锁内注释自陈 "After a successful read we add the vector while holding a shared lock / That lock prevents deletion"。并发 DEL/UNLINK/FLUSHDB 走 VectorManager.Locking.cs 的 ReadForDeleteVectorIndex 排他锁，必须等 VADD 的共享锁释放，故 C# 中删除永远落在 TryAdd 完成之后，AOF 条目序与内存终态天然一致。
2. 工程现状确证：rust 侧 network_vadd_slow 在 read_or_create_vector_index 返回 (index, lock) 后立即 drop(lock)，随后才执行 index.to_bytes 快照、manager.try_add（内含 service.insert 多段存储 await）与 replicate_vector_set_add 的 AOF 注入。函数头注反而宣称「共享索引锁……覆盖 try_add 全程（manager 契约『假定索引已锁定』，防并发 DEL/UNLINK/FLUSHDB 摘除 context）：guard 随本 async 栈帧跨 await 存活」，注释与代码直接矛盾。同文件 network_vrem/network_vsetattr 及全部只读臂（read_vector_index 的 _index_guard/_guard）均保活守卫跨写体，唯独 VADD 截短。vector_manager.rs 的 delete_vector_set 排空集注释亦宣称「VADD 持读或建锁……全部落在屏障内」，与实态不符。测试 vector_delete_exclusive_lock_race.rs 的 seed() 按正确范式书写（注释自陈「生产 VADD 语义：锁协议读命中后持守卫插入」），生产实现与自家测试所依赖的锁协议分叉。
3. 逻辑危害确证：drop(lock) 后 DEL 的条带独占锁不再被阻挡，可全链完成（request_deletion 同步 drop_index 摘 service 注册表、remove_stored_index 摘登记表、AOF 记 DEL、后续 cleanup 链 purge_context 物理清扫并归还 context）。VADD 的 try_add 打在旧 context 上分两种结局：(a) service.insert 的 index(context) 已 miss，回 False 被 manager.try_add 折成 Duplicate，客户端收到 VADD :0 的伪重复应答，与 C# 的 :1 分叉；(b) 交错在 insert 已 clone Arc 之后 DEL 才完成 drop_index，insert 继续在 Arc 保活的已弃索引上写元素四记录与 fsm 占用位并回 True，VADD 记 AOF VADD 条目——若该条目落在 DEL 条目之后（try_add 完成于 DEL 命令成功之后、replicate 尚未入队即被让出），副本重放序为 DEL、VADD，重建出集合与元素，而主端登记表已摘键空间无键，主从键空间发散；purge_context 先于 insert 写完成时补写的物理记录成永久孤儿（context 已归还，无人再清）。另有 bump_watch 在删除完成后推进 WATCH 版本的次级失真。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:RespServerSessionVectors::network_vadd_slow（drop(lock) 位于 read_or_create_vector_index 与 manager.try_add 之间）
wedb/wnode/src/resp/vector/vector_manager.rs:VectorManager::try_add（假定调用方已持锁的契约声明所在）
wedb/wnode/src/resp/vector/vector_manager.rs:VectorManager::delete_vector_set / VectorManager::delete_vector_set_of（并发删除链）
wedb/wvector/src/service.rs:DiskANNService::insert（index(context) miss 回 False 即误报 Duplicate 的映射源）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/VectorStoreOps.cs:StorageSession.VectorSetAdd（:192 using 锁域罩 TryAdd + ReplicateVectorSetAdd）
garnet/libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndexCore / ReadForDeleteVectorIndex（共享锁交还调用方与排他删除的互斥协议）

精炼执行方案：
1. network_vadd_slow 去掉 drop(lock)，改为 let (index, _lock) 保活，守卫随 async 栈帧跨 try_add 与 replicate_vector_set_add 的 await 存活至函数返回（与 network_vrem/network_vsetattr 的 _guard 同款；RwLockReadGuard 借用 self.manager 与后续 &self.manager.try_add 同为不可变借用，无借用冲突，VREM 臂已是编译在案的先例）。try_add 与 replicate 链路不取同键 vector_set_locks 条带锁，无重入自死锁。
2. 同步订正函数头注与 delete_vector_set 排空集注释中「覆盖 try_add 全程」的表述使其与代码一致（本票修复后注释即真）。
3. 测试验证点：在 wnode/tests 增补 VADD 挂起窗口与 DEL 交错测试（慢臂注入 await 让位点模拟 DEL 插队），断言终态为「VADD :1 后 DEL 生效」或「DEL 先行且 VADD :0」两种闭合结局之一，AOF 序与主端登记表终态一致；并发 VADD+DEL 压力下重启重放后键空间主从一致。

合入哈希：a6eab48 收口形态：network_vadd_slow 删 try_add 前 drop(lock) 改 let (index, _lock)，键排他锁域罩 TryAdd+AOF 注入全程（对标 C# VectorStoreOps.cs:192 using），头注过时段订正；新增 vector_vadd_guard_blocks_delete 交错回归（门闸让位×delete_vector_set 插队，伪 Duplicate :0/孤儿写双断言），既有 race 测试零回归。

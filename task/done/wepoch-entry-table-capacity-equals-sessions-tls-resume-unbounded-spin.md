甄别结论：通过（甄别席 J1，2026-09-27，定级 P2——满载 128 活跃会话下 TLS 轨 resume 无界自旋，TTL 清扫/向量清理/键空间扫描全挂死到断连）。双侧亲验成立——store/mod.rs:518 LightEpoch::new(config.max_sessions) 容量精确等于会话上限；:560 new_session 每 register 一槽、Participant Drop（participant.rs:180-192）会话终才 release_reserve，终身预留成立；entry.rs:209-210 try_claim 对 reserved 槽即使 epoch==0 亦拒，TLS 轨被挤死成立；epoch.rs:341-353 resume 慢路径无界 loop + snooze 无超时无出错口亲验；审核席硬约束有据：enter_with_tid（entry.rs:143-153）CAS 竞败即按重入递增，TLS 短借置非零纪元确会破 Dekker 互斥（entry.rs:49-54 注释自陈），故 reserved 空闲短借形态否决正确，容量加余量+register 限扫 [0, max_sessions) 子区间方案成立且 register（epoch.rs:213-222 线性扫全表）确需划界防准入放宽；C# kTableSize :100 按线程定容、ReserveEntryWait :627-657 信号量瞬态等待、Release :552-553 唤醒亲验，转写偏差（瞬态等待→可永久化自旋）成立。行号勘误：SESSIONS_PER_CORE/MIN_SESSIONS 实位 config.rs:51/:54、clamp 实位 :363-365、garnet_api 实位 wedb/wnode/src/resp/garnet_api/mod.rs:463，票面前缀缺一级。派沙箱席 c01d。

审核通过（独立方案审核席，亲验全部对照点）。形态裁定：容量加余量通过，附带硬约束——max_sessions 准入门唯纪元表耗尽兜底（全库唯一消费点 store/mod.rs:518），故 register 须限扫会话子区间 [0, max_sessions)，余量槽仅 TLS 轨可达，否则准入门静默放宽 cores*2；reserved 空闲短借形态否决——TLS 短借置非零 epoch 后 Participant::enter_with_tid（entry.rs:143-150）会把其 CAS 竞败误判为重入递增，双轨槽位属主互斥（entry.rs:50-53 Dekker 契约）被破坏。

纪元表容量等于 max_sessions 且会话槽终身预留，满载时 TLS 轨 resume 无界自旋挂死后台任务

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# LightEpoch.cs:100 kTableSize = max(128, ProcessorCount*2) 按线程数定容、与会话数解耦，槽位仅保护期持有无永久预留；表满 ReserveEntryWait（:627-657）阻塞信号量由 Release（:552-553）唤醒，等待随任意保护者释放终止。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wkv/src/store/mod.rs:518 LightEpoch::new(config.max_sessions) 表容量精确等于会话上限；mod.rs:560-561 + wnode garnet_api/mod.rs:463 每连接 new_session 各 register() 终身预留一槽；wepoch/src/entry.rs:209 try_claim 对 reserved 槽即使 epoch==0 空闲也拒绝 TLS 占用；epoch.rs:342-352 resume 慢路径无界 loop + backoff.snooze() 无超时无出错口。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
活跃会话达 max_sessions 时（register Err 即准入门恰好停在满载）TLS 轨永久自旋：TTL 清扫（wkv/src/gc/ttl_sweep.rs:82）、vector 清理（session/vector_cleanup.rs:77）、键空间/FLUSH 族扫描（store/keyspace.rs:546、store/mod.rs:649）、紧缩扫描全部挂死，直到客户端断连；瞬态 register 路径（cpr_host.rs:272、resize.rs:582）显式 Err。默认 8 核 max_sessions=128（wkv/src/config.rs:53/364-366 clamp [128,1024]），满载可达。容量模型转写偏差（C# 瞬态等待 → rust 可永久化自旋）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/mod.rs:WedbStore 装配（LightEpoch::new 容量）
wedb/wepoch/src/epoch.rs:resume 慢路径

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:kTableSize / ReserveEntryWait（按线程定容+瞬态信号量等待）

精炼执行方案：
1 容量与预留解耦：LightEpoch 表容量取 max_sessions + TLS 余量（如 saturating_add(cores*2) 或 next_power_of_two 上取），reserved 槽占用判定保持；或 try_claim 对 epoch==0 空闲 reserved 槽允许 TLS 短借（exit 即还）——审核席裁定形态（倾向容量加余量，最小改动不改槽位语义）
2 测试验证点：满 max_sessions 会话下 protected_scope 仍可进入、TTL 清扫任务正常推进

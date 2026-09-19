DbMeta 换号原子批持久化收口与紧缩死域判定修正

来源：next/dbmeta-atomic-batch.md（判定成立，已剪除）；前置 wave3-c 记录编解码
单点（task/ing/dbmeta-record-single-source-handoff-to-atomic-batch.md）已落地，
本单只做原子批与判定修正，不新增第二套 writer。取证基线：主仓 dev，
garnet C# 侧 FlushDatabase 即 ShiftBeginAddress 物理截断（libs/server/
StoreWrapper.cs:613、DatabaseManager FlushDatabase），无换号撕裂窗口，
本机制真值源为 doc/zh/db.md 1.4「新映射与旧 ID 待回收项原子批处理持久化
（KeyTag::DbMeta 0x0E）」与 0x05 全局自增标量布局行。

复核修订（与在册原文的差异）

一、set_context 建档记录前缀已在 wave3-c 改为 ROOT_DBMETA_PREFIX (0,0)，
「建档随会话前缀漂移」一项已消灭；本单只补其信号丢弃处置与水位并入。
二、rebuild「value 先按 8B 试解再 match payload[0]」的旧门控已被
DbMetaRecord::decode 的按子类型分派取代，新增 0x05/0x06 只需在 decode 内
补臂；其中 0x06 值 16 字节，decode 现形态先于分派强解 8B 值，须把该前置
强解移入 8 字节臂（0x01-0x05），0x06 单独校验 16 字节。
三、A 段三设想的 parking_lot Mutex 不可跨 await 持有：compio 为线程核绑
单线程运行时，parking_lot 阻塞会挂死整核且同核两任务（两连接各发 FLUSHDB）
即成死锁；改为既有自研形态——WedbStore 一个 AtomicBool 认领位
（swap(true, Acquire) 抢占）加 wbase::future::yield_now 协作让渡的获取循环，
Drop 守卫释放（? 早退不遗留），先例即 barrier_enter 的 PREPARE_GROW
自旋挂起协议与 YieldNow 协作让步原语。仍不换号热路径触锁（锁只在管理命令面）。
四、批降级条目只集中重投降级者（非整批重放）：DbMeta 各记录键唯一、值幂等，
后写覆盖语义与崩溃前缀口径一致（安全顺序下任意交错最坏旧域泄漏或换号丢失，
绝不数据复活或撞号），实现取「同步尝试收集降级项，异步逐条兜底」。

方案

1. vdb.rs DbMetaRecord 增两变体（布局仍单点，编解码只出现在该 enum）：
   NextId：键 [0x05] + b"next_virtual_id"（15B 标记，键载荷 16B），值
   [下一可分配号: 8B be]；子类型常量与标记长度派生为私有 const，
   KEY_NEXT_ID_LEN 由标记长度推导，不留魔数。
   DbSwap：键 [0x06][vns:8][logic_db1:8][logic_db2:8]（25B），值
   [db1 新指向:8][db2 新指向:8]（16B），swap 成对映射单记录真原子。
   value() 返回定长缓冲 DbMetaValueBuf（容量 16B + 长度，Copy，先例
   DbMetaKeyBuf）；decode 按子类型校验键长与值长（8B 族与 0x06 的 16B）；
   key()/value()/decode 六变体覆盖于既有单测 test_dbmeta_record_roundtrip。
   KEY_MAX_LEN 保持 25（0x06 与 GcDeadDb 同长）。
2. session/mod.rs 原子批单点入口：
   try_persist_dbmeta_sync(&[Option<DbMetaRecord>]) 内核——一次 enter_batch
   以栈上 ROOT_DBMETA_PREFIX 经 try_upsert_tag_sync_unprotected_with_prefix
   逐条同步尝试（不持守卫跨 await 的红线不破），返回降级条目引用（Vec 仅
   在降级发生时分配），硬错上抛；Option 槽承载条件记录（旧域墓碑可缺席），
   零分配零魔数。
   persist_dbmeta_batch(items)：同步内核后仅对降级条目走
   upsert_raw（显式根前缀完整键，先例 delete_dbmeta 异步臂同构）兜底保达。
   persist_dbmeta(&DbMetaRecord) 收敛为单元素批（签名从 (key,val) 裸片改
   记录引用，杜绝手拼值），vdb_load.rs 两处调用点随之收口；delete_dbmeta
   语义本就单条，不动。
   set_context 建档：两处 let _ = 纯静默改判为显式——构造 [NS_MAP?, DB_MAP?,
   Some(NextId)] 走同步内核，降级与硬错 log::warn 带记录 Debug 留痕
   （同步域无异步兜底窗口，冷装载 persist 与后续批的水位收尾兜底收敛）。
3. keyspace.rs flush_database / flush_namespace：编排体首尾持换号锁；两条
   独立 enter_batch 同步写改走 persist_dbmeta_batch 单批，安全顺序为
   [新映射, 旧域墓碑?, 0x05 水位]（0x05 值取换号后 next_virtual_id 现读，
   锁内无并发换号、锁外并发建档只会抬高水位不会回落，号不复用由 0x05 兜底，
   任意崩溃前缀最坏旧域泄漏，绝不数据复活或撞号）；树回收仍在 persist 之后
   按旧域分支执行；session.set_context(0,0) 前缀锚定随固定根前缀机制移除。
4. swap.rs swap_databases：持换号锁全程（消除读旧值竞态与双指向中间态），
   落盘收敛为安全顺序批 persist_dbmeta_batch([0x06 成对记录, 两条同值 0x02
   映射])——成对记录先行、映射随落（纯 0x06 单记录对非 root 租户的冷启探针
   不可见，重启后 swap 蒸发且盘上直接写旧域，见实施补充）。模块注释
   第 3 步口径同步改写。
5. store/mod.rs 换号锁本体：dbmeta_lock: AtomicBool 字段 + dbmeta_lock()
   async 获取器 + Drop 守卫（garnet 无对位注释写明）；rebuild 侧
   rebuild_apply_record 补 0x05 臂（水位候选 = 落盘值饱和减一，收尾
   fetch_max(max_vid+1) 恰等于 max(0x05, max_vid+1)，消除对每个号都落盘的
   隐式依赖）与 0x06 臂（两条映射按扫描地址序 insert_db_mapping 覆盖生效，
   仅根域装载同 0x02 臂，两值并入 max_vid）。
6. vdb.rs 死域判定收口：is_virtual_id_dead_and_expired（全仓唯一消费者
   compact.rs）并入 GcDeadEntry::vns 角色比对与过期判定（库级键须
   vns=Some、空间级键须 vns=None 且 expired_at<=now），与 is_dead_domain
   语义对齐，不留裸 id 判定第二消费者；compact.rs is_deleted 解码出
   KeyTag::DbMeta 直接豁免不判死（DbMeta 记录由其专属墓碑退出，rebuild
   0x03/0x04 删除臂承接），修掉根库退役窗口整批误删活域记录与全部换号
   元数据的数据丢失洞。
7. gc.rs sweep_vdb 墓碑注销的 let _ = 整体丢弃改为硬错 log::warn 留痕
   （条目已离册，盘上记录未注销则重启重扫重投，幂等）。
8. doc/zh/db.md 1.4（主树修订，B 段）：
   即时原子提交段补 0x05/0x06 布局行（与实现同字节口径）、0x03/0x04 值侧
   status:1B 修正为 tail_address:8B be（既有实现对位），原子批描述改为
   「引擎单条日志追加即原子提交单元；换号批按新映射、旧域墓碑、0x05 水位的
   安全顺序成组落盘，swap 成对记录先行、两条同值映射随落成批；任意崩溃前缀最坏旧域泄漏，
   绝不数据复活或撞号」；写路径墓碑校验与纪元保护段改为实际机制：写落盘前
   不逐条查墓碑，会话前缀经换号代数快照自愈强制命令边界收敛（实现位置
   wkv/src/session/mod.rs session_prefix 与 set_context 的代数-解析不可
   交换口径），纪元内慢窗产生的死域孤儿对主从读写与统计面均不可见（实现
   位置 keyspace_stats is_dead_domain 过滤 / matches_logic_db / 会话前缀
   迭代过滤），物理清退由纪元屏障后 begin_address 推进与紧缩死域跳过承接
   （实现位置 gc.rs sweep_vdb 的 expired_at+begin 双条件与 compact.rs
   is_deleted 角色版判定），如实声明残余窗口：孤儿最长滞留一个回收延迟
   周期；惰性过滤段删去「点查命中死 ID 视为空」承诺，保留严禁全表扫描与
   紧缩顺带回收两真值。

实施补充（swap 批形态修订，对上方第 1、4 点的落地勘正）

原方案「swap 收敛为 0x06 单记录一次追加，真原子」在甄别实现时改为
[0x06 成对记录, 0x02 记录 db1 新指向, 0x02 记录 db2 新指向] 三条成批：
probe_db_mapping 冷启探针只点查 [0x02][vns][db] 映射键，0x06 对其不可见；
纯单记录在非 root 租户上重启后探针未命中，租户把 db 解析回原始 vdb，swap
落单丢失，且 swap 后对新指向空间的写入直接落在旧目标键上造成幽灵键可见性
错乱。root 重建本身可识别 0x06 无此问题，但口径必须全局一致。三条成批后：
成功路径三记录同值落齐；任意前缀崩溃中 0x06 先落于两条 0x02 之前，root 重建
与探针两通道各自前缀一致（0x06 先到即互换、两条映射同值随后覆盖为幂等重放），
降级窗口的部分落盘与既有单记录直写失败残余同阶，且 swap 全程受换号串行锁
与内存先行 CAS 约束，最坏仅回退为 swap 前映射，绝不数据复活或撞号。swap 不
分配新号，其批不带 0x05；水位由创建路径 set_context 与 resolve 落盘承接。

测试

wkv/tests/store/flush_database.rs 增换号批落盘重建用例：写数据 → FLUSHDB →
flush_all 刷净 → 同 device open_shared 重建，断言旧域不可读、gc_dead 恢复
（含退役角色）、next_virtual_id 不小于 0x05 落盘水位且新分配号不与磁盘在用
域撞。swap_database.rs 增成批重建用例：swap 后重建两库指向互换无中间态，
后续分配不与互换号撞。断言复用 vdb.rs 单测扩展（0x05/0x06 往返、值长守卫、
角色版过期判定）。既有仅覆盖成功路径的断言不改动。

协调与门禁

不做向下兼容：不留非批量两条写路径、不留裸 id 版死域判定的第二消费者、
0x01-0x06 之外不引入双写开关；js/check.js 无新增缺失（garnet 无对位，按
自研注释口径）。只跑 CARGO_TARGET_DIR=/tmp/dbmeta-atomic-target cargo check
--manifest-path wedb/Cargo.toml --workspace --all-targets，不跑 test.sh /
clippy.sh；合并与门禁由主智能体承接。B 段中「每写必进 gc_dead 判定」的
原案（next/qcode.my.md 第 2 轮条 2 机制本体）判为自造额外层，违背
rust_review 复杂度对标 C# 不加额外层红线，按本单第 8 点文档口径收口。

实现代理复核（2026-09-19，f19-dbmeta-batch 认领后逐点核实主仓 dev 现状）

方案 1-8 全部已被并行开发落地，符号级证据：

1. vdb.rs DbMetaRecord 六变体齐（NextId/DbSwap :142/:147），DbMetaValueBuf
   :180，decode 按子类型分派且 0x06 前置 16B 强解独立臂 :354-411，
   test_dbmeta_record_roundtrip 六变体覆盖 :1252。
2. session/mod.rs try_persist_dbmeta_sync :488（Option 槽批内核，降级返回
   下标），persist_dbmeta_batch :517（降级逐条 upsert_raw 兜底），
   persist_dbmeta :543（单元素批，收记录引用），set_context 建档显式判定
   warn 留痕 :337-360。
3. keyspace.rs flush_database :98 / flush_namespace :154 均持 lock_dbmeta
   全程 + persist_dbmeta_batch 三记录安全序批。
4. swap.rs swap_databases :33 持锁全程 + [0x06, 0x02, 0x02] 三记录批（与
   实施补充的三条成批口径一致）。
5. store/mod.rs dbmeta_lock AtomicBool :160 + lock_dbmeta yield_now 获取
   :191 + DbmetaGuard Drop :166；rebuild_apply_record 0x05 臂（饱和减一）
   :573、0x06 臂（根域装载）:577，收尾 fetch_max :504。
6. vdb.rs is_virtual_id_dead_and_expired 角色版 :1029；compact.rs
   is_deleted 对 KeyTag::DbMeta 豁免 :196。
7. gc.rs 墓碑注销 warn 留痕 :424（qcode-rounds-glm-round8-covered 审计
   同结论）。
8. doc/zh/db.md 1.4 即时原子提交段 0x05/0x06 布局行、写路径防护段、惰性
   过滤段均已按本单口径改写。

测试面：swap_database.rs test_swap_pair_record_survives_rebuild :256
（成批重建闭环）、vdb_rebuild_gate.rs 水位不回退 :102、dbmeta_layout.rs
冷解析闭环均已就位。

残余工作（本单收尾三件，缺一即闭环不完整）

a. wkv/tests/store/flush_database.rs 缺 FLUSHDB 换号批落盘重建用例：
   写数据 → flush_database → flush_all 刷净 → 同 device open_shared 重建，
   断言旧域不可读（逻辑库按新号解析为空）、gc_dead 恢复（含退役角色，
   is_dead_domain 对旧 vdb 判死且同号活域不误伤）、next_virtual_id 不小于
   0x05 落盘水位且 FLUSHDB 再换号取严格新高号（零撞）。
b. store/mod.rs open_shared 文档注释 :393-395 仍写「分配水位的持久化收口
   属 DbMeta 原子批落盘待办（dbmeta-atomic-batch，0x05 自增标量），两单须
   同棒或明确先后」——0x05 已落地，待办陈述过期，改为已落地口径
   （水位由换号批与建档批收尾携带，rebuild 0x05 臂折叠）。
c. keyspace.rs flush_database 文档注释 :75 仍写「（批收敛另见
   task/ing/dbmeta-atomic-batch.md）」——批已收敛且本票将移 done，
   引用失效，删除该括注。

门禁不变：只跑 cargo check --workspace --all-targets（worktree 内），
不跑 test.sh / clippy.sh。

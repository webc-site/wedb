甄别结论：通过（甄别席 zc-fix-r16-bitopdst，2026-09-26）定级 P2
核验记录（逐锚现码复跑）：
1. C# 锚成立：BitmapCommands.cs:131 NetworkStringSetBit 经 :142 IsValidBitOffset 收 512MB（BitmapManager.cs:19-24 MaxBitmapPayloadBytes=512MB、MaxOffset=4294967295）；票面 NetworkStringBitField 名误，现树实名 StringBitField（:408），其 offset 经 TryValidateBitfieldOffset→IsValidBitOffset（BitmapManager.cs:62）同闸，实质成立；RMWMethods.cs InPlaceUpdater SETBIT 臂 :568-582 用 CanGrowPinnedValue/TrySetPinnedValueLength、CopyUpdater :1040/:1133 回落，AllocatorBase.cs:1463 抛 "Entry does not fit on page"，BasicCommands.cs NetworkSetRange :460-466 显式前置拒，逐条亲验。
2. C# BITOP 锚成立：BitmapOps.cs StringSetBit :16、StringBitOperation :70-220（NOTFOUND continue :131、InvokeBitOperationUnsafe :188、maxBitmapLen>0 才 SET :190、:0 不触碰 dest :208/:220），BitmapManagerBitOp.cs:26。
3. rust 现状锚成立：bitmap_commands.rs network_string_set_bit 算 need(:72) 后未接 string_record_fits_page 即取闩(:76)，回落臂 with_capacity(need)(:110)/resize(:114) 盲目分配（最大 512MB），全文件零处 fits_page 调用；slow.rs SETBIT 臂 need(:861)→rmw_window(:862)→resize(:869)、slow BITFIELD 写臂 need(:1015)→resize(:1018) 同样无门，取闩均在门位之前；对照 string_record_fits_page（set.rs:864 定义）现仅接 SETRANGE(:291/:537)/APPEND(:910/:573) 两臂，r143 先例与缺口并存坐实。
4. 败局闭环成立：whlog record_fits(hlog/mod.rs:749)→写侧 RecordTooLarge(:732)，快路径落 RESP_ERR_GENERIC(bitmap_commands.rs:122)；订正一点：慢路径经 slow_arm err_frame 落 RESP_ERR_SLOW_PATH_STORAGE(garnet_api/slow.rs:74) 而非票面所写通用错误，注定失败整轮与无谓分配之实质不变。
5. BITOP 多源写侧闭环复核成立：network_string_bit_operation dest 窗先建罩折叠全程(:283-345)、全缺失 :0 不写(:334)、RI 门与向量分流(:315-321)，与慢臂同构，无增量缺口，本票不触 BITOP。
6. 非重复非灭失：deviations.md §18（SETBIT/BITFIELD TTL 保留）、§87（BITOP 源键闩裁决）均异轴；task 四池无 SETBIT/BITFIELD 单页门同轴票（reject/rangecold 系 SETRANGE/GETRANGE 判净票，恰引 setrangex 先例划界）；缺陷现码仍在。
7. 架构合规：方案复用既有单源判据 string_record_fits_page（pub(crate) 可跨模块接线），取窗前拦截、不新增第二常量/第二锁表/无分层树，测试对标既有 setrange_append_page_gate.rs，单机制最小改动，无假桩无过度设计。
定级理由：单条畸形请求（SETBIT k 3000000000 1 / BITFIELD 大偏移写）即可在持键级排他闩期间诱发至多 512MB 堆分配加全量 memset，锁窗拉长且并发可放大为 OOM，破坏 review.md 4.1 单页钳制基座约束且注定失败纯耗，参照 §87 BITOP 资源面先例取 P2。

审核结论：通过，定级 P3。
确证 SETBIT 与 BITFIELD 写子命令未接线 string_record_fits_page 单页容量前置门，在 (page_size, 512MB] 败窗内持排他闩期间触发盲目堆分配与零填充，破坏单页钳制物理约束；BITOP 多源写侧契约经核验已闭环。方案明确在取窗前插入单页门，供 task/fix.md 消费。

SETBIT与BITFIELD写臂超大offset扩容缺位单页容量前置门与BITOP多源写侧契约闭环

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
在 Garnet 中，BitmapCommands.cs 的 NetworkStringSetBit 与 NetworkStringBitField 校验 offset 上限为 BitmapManager.IsValidBitOffset（512MB，即 4294967295 位）。在底层存储层 MainStore/RMWMethods.cs 的 InPlaceUpdater SETBIT 分支中，当值增长时调用 CanGrowPinnedValue 与 TrySetPinnedValueLength；若记录无法原位扩容，则回落至 CopyUpdater，在 Tsavorite 引擎层通过 TryAllocate 申请新记录。当请求尺寸超出单页容量硬上限时，底层 AllocatorBase.cs 抛出 Entry does not fit on page 拒绝写入。在 BasicCommands.cs NetworkSetRange 中，Garnet 显式前置了超大尺寸校验，防止超页导致底层存储异常。
同时，Redis 与 Garnet 契约中 BIT 族命令（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD）严格依附于 String 基础类型（KeyTag::String 扁平字节数组），与 HyperLogLog 的 sparse 转 dense 稠密存储升级以及集合类型的自适应分层存储（collection.md）有严格界线。位图并不存在稀疏转稠密的独立分层结构，不能也无需引入动态分层树，其最大容量刚性受制于单页存储上限。
关于 BITOP 多源运算与目标键写入：Garnet BitmapOps.cs 的 StringBitOperation 中，源键缺失走 NOTFOUND continue 跳过；当全源缺失或全空源时（maxBitmapLen == 0），应答 :0 且不触碰目标键（不写入亦不删除）；当有命中源时（maxBitmapLen > 0），通过 SET 覆盖写入目标键并清除 TTL，若目标键命中 Vector Set 则先 Delete 再 SET；多源运算 InvokeBitOperationUnsafe 严格按照算子语义处理源键长度不一时短源尾部零填充（AND 尾部清零，OR/XOR 恒等承接，DIFF 尾部恒等）。全流程在 keys[0] Exclusive 锁保护下串行化执行。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
在工单 zcode-r143c-setrangex（SETRANGE/APPEND 增长臂两案回归）中，wedb 已为 SETRANGE 和 APPEND 建立了单页容量前置门 string_record_fits_page，并在取窗前显式拦截败窗 (page_size, 512MB] 的超大扩容。
然而核查发现，SETBIT 与 BITFIELD 写子命令未接线该单页前置门：
快路径 wedb/wnode/src/resp/bitmap/bitmap_commands.rs 的 network_string_set_bit 中：
计算 need = length_in_bytes(offset)（最大可达 512MB）后，未执行 string_record_fits_page 校验，直接通过 store.try_rmw_window 获取键排他排队闩，并在 read_user_sync 盲目执行 Vec::with_capacity(need) 和 val.resize(need, 0) 进行大内存堆分配与全量零填充。
慢路径 wedb/wnode/src/resp/basic_commands/slow.rs 的 slow_basic_command SETBIT 分支中：
同样在获取异步排他闩 rmw_window 后，在 val.resize(need, 0) 处盲目进行最大 512MB 的零填充分配，随后调用 storage.rmw_string。
底层存储 whlog 在 record_fits 处判定超页并返回 RecordTooLarge，导致整轮大内存分配与加锁写回必定失败，最终落为 RESP_ERR_GENERIC 通用错误。
BITFIELD 的 string_bit_field_action 与 slow_bit_field 同理，new_block_alloc_length_from_type 计算所得 need 同样未经过单页门校验即在 buf.resize(need, 0) 触发大内存分配。
在 BITOP 方面，wedb/wnode/src/resp/bitmap/bitmap_commands.rs 的 network_string_bit_operation 与 wedb/wnode/src/resp/basic_commands/slow.rs 的 slow_bit_operation 经 BitOpAccumulator 流式折叠，对源键缺失（Missing）、长短不一零填充、目标键写入覆写与 TTL 清除、目标键排他窗口全程持有、RI 门与向量登记清理等各环节均与 Garnet 逐位对齐，逻辑完备。但 SETBIT 与 BITFIELD 在超页边界上的资源防护存在明确余缝。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
在败窗 (page_size, 512MB] 内，客户端单次畸形请求（如 SETBIT k 3000000000 1）将诱发服务端 worker 线程在手持键级排他闩期间，突发申请数百兆堆内存并逐字节 memset 零填充。
这不仅浪费数十至数百兆内存并导致 CPU 缓存被垃圾冷页冲刷，若并发多个类似请求极易诱发内存耗尽（OOM）或严重抖动。
此外，加锁后的大内存分配延长了锁持有窗口，造成本键及同桶键并发请求的长时间串行阻塞，而该操作注定在底层写回时被 RecordTooLarge 拒绝，属于纯粹的无界资源开销，破坏了 review.md 板块 4.1 物理存储单页尺寸钳制的基座约束。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:RespServerSession::network_string_set_bit
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:RespServerSession::string_bit_field_action
wedb/wnode/src/resp/basic_commands/slow.rs:slow_basic_command
wedb/wnode/src/resp/basic_commands/slow.rs:slow_bit_field
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:RespServerSession::network_string_bit_operation
wedb/wnode/src/resp/basic_commands/slow.rs:slow_bit_operation
wedb/wbitmap/src/bit_op.rs:BitOpAccumulator

对应 c# 文件与函数：
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:RespServerSession.NetworkStringSetBit
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:RespServerSession.NetworkStringBitField
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:RespServerSession.NetworkStringBitOperation
garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:StorageSession.StringSetBit
garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:StorageSession.StringBitOperation
garnet/libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:BitmapManager.InvokeBitOperationUnsafe
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:MainSessionFunctions.InPlaceUpdater
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:MainSessionFunctions.CopyUpdater

精炼执行方案：
1. 在 wedb/wnode/src/resp/bitmap/bitmap_commands.rs 的 network_string_set_bit 入口处，解析完 offset 与 need 后，在 store.try_rmw_window 取闩前，前置插入 string_record_fits_page(store, key, need) 校验，若超页则直接 output.write_resp_error(RESP_ERR_GENERIC) 并早退返回 Ok(true)。
2. 在 wedb/wnode/src/resp/basic_commands/slow.rs 的 SETBIT 慢路径分支中，同样在 storage.batch.rmw_window 取闩前前置校验 !string_record_fits_page(&storage.batch, key, need)，超页直接写错早退。
3. 在 string_bit_field_action 与 slow_bit_field 中对写子命令涉及的 need 进行 string_record_fits_page 门控拦截。
4. 参考 wedb/wnode/tests/setrange_append_page_gate.rs 增加 SETBIT 超页败窗拦截与零大块分配探针测试，断言超页输入下不触发大内存分配且稳定回通用错误帧。

视角结论:有增量

合入哈希：22e87b3（fix 提交 293f6f7） 收口形态：SETBIT/BITFIELD 写臂快慢四落点取闩前接入 string_record_fits_page 单页门，超页败窗于物化前以快慢同帧形（generic）早退拦截、零大块分配（tests/setbit_bitfield_page_gate.rs 6 例锁测），BITOP 面与计账面零触
补稳收口：探针整二进制序列化锁（ec898e7）随 fix-bitopdst 二次合入，最终合入哈希：0fc1417；check --workspace --all-targets 净、setbit_bitfield_page_gate 6/6 连跑三轮绿

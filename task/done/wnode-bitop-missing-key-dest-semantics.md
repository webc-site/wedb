终态:闭环(2026-09-29)。甄别 adcd275 → 沙箱 d957051 → 并 dev 7c265bf。快慢双臂 Missing 以 acc.fold(&[]) 参与折叠(簿记面零改动),空结果删 dest(快臂窗内 try_delete_sync 失闩降级/慢臂 delete_string,登记键经删除单点 hook 清退),DIFF 单命中 Err 臂保留防御位;审查席 P1(resp_vector_set_wrong_type 旧「不触目的键」锁)与 P3(头注残留)修于 916f0e1。金样经主控 redis 8.10.1 亲测。终验 test.sh 5205/5205+clippy 0。
甄别结论:通过(P1 级,2026-09-29 主控席)。亲验:快臂 :321 Missing 只计数不折叠、慢臂 :1263 Hit|Missing 同静默、fold 内计 keys_found+首命中定基+DIFF resize 补零臂、finish DIFF 单命中 Err、快 :362-364 慢零写不删、slow.rs:385 delete_string 先例、C# BitmapOps.cs:131-132 NOTFOUND continue;本机 redis-server 8.10.1 实测六金样全吻合。fold(&[]) 单点方案逐算子推演成立。登记面真身 wkv/src/session/collection.rs(语义钉测 delete_miss_watch.rs)。审核席四点遵照。

审核结论：通过（2026-09-29 独立审核席，P1 级）。rust 缺形逐点亲验、C# NOTFOUND continue 与 keysFound==0 臂确证、
Redis 标准锚经本机 redis-server 8.10.1 实测逐金样吻合（AND 缺源回 maxlen 全零串、全缺失回 0 删 dest、
OR/XOR 恒等、DIFF 首参缺位全零/非首参缺位拷贝、NOT 缺源删 dest，审核席临时实例 6399 实测）。fold(&[])
单点方案逐算子推演成立、无需 finish/DIFF 计数配套修正，keys_found Err 臂自愈为不可达防御位。
审核席整理四点（执行席遵照）：
1. 票面危害 c) 表述订正：DIFF 单命中两形本仓现状观测面均为「回通用错误帧（误报错）」，非「错抄」；
   「以 k1 为基」系机制层描述，缺陷本体（背离 Redis 应答）不变。
2. 慢臂删除口对准 storage.delete_string（slow.rs:385 既有先例），即票面「异步对偶」落点。
3. 空结果删 dest 与 bitmap_commands.rs:336-337「向量登记保留」注记的语义张力：落笔前必须核对登记面
   （collection.rs:125 命中登记视同删除成功）并同步改注，票面预留的执行席终裁不可跳过。
4. 测试验证点 a) 金样预期与 Redis 8.10.1 实测值逐项吻合，可直接采括。

原票面：
BITOP 把缺失源键当「跳过折叠」而非 Redis 标准的「零长空串参与运算」：AND 遇缺源错抄他源、DIFF 首源缺位错位/误报错、空结果不删 dest 留残值

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。Redis 官方标准（src/bitops.c:bitopCommand，
unstable 与 7.2 同构；redis.io/docs/latest/commands/bitop 原文「Non existing keys are
considered like empty strings」）：
a) 缺失源就地保留为零长串：1294-1301 `objects[j]=NULL; src[j]=NULL; len[j]=0; minlen=0; continue;`
——键不删除、仍进折叠数组；字节循环 1506-1536 对越界/缺失源取 byte=0 参与运算。后果分算子：
AND 任一缺源 → 全结果字节 0，dest=零串、回 maxlen（ longest 在场时长照旧 ）；OR/XOR 缺源零字节
为恒等 → 等价跳过（此两形两侧无差分）；DIFF(X,A...) X 缺位 → output 起点 0 → 零串；DIFF 非首参
缺位 → 对析取贡献 0 → 恒等。
b) 结果空（全部源缺失或全空串，maxlen==0）：1607-1618 尾部 `if (maxlen) setKey else
dbDelete(targetkey)`，恒 addReplyLongLong(maxlen)——即回 0 且删除 dest（残值不留）。
c) wrongtype 逐源短路（1303-1317）与 NOT 一元、DIFF 至少两源 arity 文案（1275-1284）两侧已等形，
不在本票。
C# 原型上游缺形：BitmapOps.cs:StringBitOperation（70-232）`if (status == GarnetStatus.NOTFOUND)
continue;`（131-132）把缺失键彻底丢出折叠数组，内核（BitmapManagerBitOp.cs:InvokeBitOperationUnsafe
26-54，srcCount==1 非 NOT 整串拷贝 :33-47、DIFF srcCount==1 抛 GarnetException :35）只见命中源；
keysFound==0 臂（约 204-209）回 0 且不触 dest。即 C# 对 a/b 两面均背离 Redis 标准。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）。rust 忠实转写 C# 缺形：
bitmap_commands.rs:network_string_bit_operation（254-380）逐源循环 314-332，
`Ok(UserRead::Missing) => notfound += 1`（321）不入折叠；BitOpAccumulator（wbitmap/src/bit_op.rs
175-266）fold（197-251）以首个「命中」源定基（204-210）——参数位次语义自此丢失；finish（256-265）
DIFF keys_found==1 → Err（257-259），会话层转通用错误帧（bitmap_commands.rs:366-370、
slow.rs:1304-1308）；longest==0 → 回 0 不写（339/362-364，注释 336-338 自证「C# 对齐」系转写
注记非在册裁决）。慢臂 slow.rs:slow_bit_operation（1206-1340）同形（1263 Hit|Missing 均不折叠、
1275-1276/1300-1304 longest==0 不删）。
3. 逻辑危害确证。默认配置可达、数据正确性级差分（TTL 过期键/尚未写入的集合键为日常场景）：
a) BITOP AND dst k1 nosuch（交集运算一方缺席）：Redis dst=零串(len k1 字节)；本仓 dst=k1 逐字节
拷贝——本应为空集的交集静默产出全量他方数据，下游按「交集」消费即读入不该存在的位。
b) 全源缺失/全空串且 dest 既有旧值：Redis 回 0 并删除 dest；本仓回 0 保留旧值——陈旧位图被
当作本次运算结果续读（残值复活）。
c) BITOP DIFF dst nosuch k1：Redis dst=零串；本仓以 k1 为基错抄。BITOP DIFF dst k1 nosuch：
Redis dst=k1 拷贝回 len；本仓回通用错误帧（C# 形为 GarnetException 抛出，会话面更劣）。
三形均为协议面可观测、金样可锁定。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:network_string_bit_operation（254-380；缺源跳过
321；零写臂 336-338/362-364；DIFF 单命中 Err 臂 366-370；arity/64 上限 263-282 不动）
wedb/wbitmap/src/bit_op.rs:BitOpAccumulator::fold（197-251，首命中定基 204-210）、finish
（256-265，DIFF 单命中 Err 257-259、AND 清尾 261-263）
wedb/wnode/src/resp/basic_commands/slow.rs:slow_bit_operation（1206-1340；Hit|Missing 同静默
1263；longest==0 臂 1273-1304；DIFF Err 臂 1304-1308）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitOperation（70-232；NOTFOUND
continue 131-132；keysFound>0 门约 176；SET 仅 maxBitmapLen>0 约 190-202；keysFound==0 回 0 不触
dest 约 204-209）
garnet/libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:InvokeBitOperationUnsafe（26-54；srcCount==1
拷贝/NOT 取反 33-47；DIFF 单源 throw :35）

Redis 标准锚（非本仓文件）：src/bitops.c:bitopCommand（1240-1618；缺失=零长串 1294-1301；字节
循环越界源 byte=0 1506-1536；尾部 setKey/dbDelete 1607-1618；NOT/DIFF arity 1275-1284）；
redis.io/docs/latest/commands/bitop。

精炼执行方案：
1. 缺失源改为「以零长切片参与折叠」：快臂 321 与慢臂 1263 的 Missing 分支追加 acc.fold(&[])
（found/notfound 计数语义零改动：Missing 仍只加 notfound、不加 found——簿记面系票
wnode-string-bitmap-found-notfound-accounting-matrix 已收口形，禁动）。该单点改动经现有
fold/finish 机制自然复现 Redis 全算子分形，不新建第二折叠器：AND——空源使 shortest=0，finish
既有「dst[shortest..] 清零」臂直接产出全零串（a 形修复）；OR/XOR——fold(&[]) 零贡献恒等（与
Redis 等形，行为不变）；DIFF——首参缺位时基为空 vec，后续 fold 走 resize 补零臂产出全零串；
非首参缺位时 keys_found 计入位次，DIFF dst k1 nosuch 不再触发单命中 Err（c 形修复，且
keys_found==1 Err 臂自此仅剩防御位，保留）。
2. 空结果删 dest：longest==0 分支（快 362-364、慢 1300-1304）在回 0 前删除 dest——复用本函数
已建立并全程持有的 dest 读改写窗口（快 292 try_rmw_window / 慢 1246 rmw_window）内既有删除口
（try_delete_sync / 异步对偶），禁开第二张锁表或新窗口；Redis 形为 dbDelete 成功才计数 dirty、
应答恒 maxlen（bitops.c:1613-1618），本仓删除失败（dest 本不存在）仍回 0。dest 为存活向量登记
键时「保留登记」现注记（336-337）与该臂的相互作用交执行席按登记面业主核对后落注释，票内预留：
若终裁为「仅清字符串残值、登记保留」则删除口按字符串域执行。
3. 测试验证点：
a) wnode 集成 garnet_bitmap.rs 新增金样锁：SET k1 \xff\xff 后 BITOP AND dst k1 nosuch → 回 :2
且 GET dst == 两字节零串；预置 dst 旧值后 BITOP AND dst nosuch1 nosuch2 → 回 :0 且 EXISTS dst == 0；
BITOP DIFF dst nosuch k1 → 回 len 且 dst 全零；BITOP DIFF dst k1 nosuch → 回 len 且 dst==k1
（替换现「报错」预期）；NOT dst nosuch + 预置 dst → 回 0 删 dest。
b) 既存锁零改动通过：NOT arity（:112）、DIFF arity 文案（:596-620）、64 键上限（:642-646）、
快臂正常多源 NOT 拷贝（:84-104）；wbitmap 单测为 BitOpAccumulator 增「空切片参与折叠」朴素对拍
（逐算子 vs Redis 语义神谕）。
c) C# 对照面 GarnetBitmapTests.cs:BitmapOperationNonExistentSourceKeys（2646-2656）仅锁结果
size==0、未锁 dest 残值，与本修复不冲突；慢臂同字节复核（快慢共用 acc/窗口契约）。
4. 禁触线：rmw 窗口建立点位与持窗全程（票 zcode-r32-rmwmatrix 立项一）不动；ri_write_gate /
向量预清退次序不动；found/notfound 簿记矩阵不动；§92（BITCOUNT 钳形锁）、§110（BITFIELD 未知
子命令回显）不触碰；BITOP 未知算子/arity 错误帧（syntax error 族，已等 Redis 金样）不动；
COMMAND 自省面（在途 manifest 票域）不扩面。本票为 Redis 标准回改，deviations.md 不登记。

查重结论：
task/ 全树 grep -ril「bitop|BITOP」仅命中 task/ing/wnode-command-family-extension-manifest-unwired.md
（COMMAND 表面对 R.* 模块名接线，非 BITOP 执行语义，不重叠）；done 内四件（wrecord-modified-bit、
r435 审阅、b2 注记、checkjs 锚注册）均为注记/锚形非语义裁决。代码注释「C# 对齐」（336-338/1273-1274）
系转写来源注记，非在册偏差裁决。deviations.md grep「BITOP/位图/bitmap」零命中；§92 裁 BITCOUNT、
§110 裁 BITFIELD 回显，均不覆盖本域。C# 测试仅 size==0 断言，无反向裁决。无重叠票。

未尽面：
1. 双侧 64 源键上限（RESP_ERR_BITOP_KEY_LIMIT，快臂 279-281 与 C# 同形字节）背离 Redis 无上限
（bitops.c 无 numkeys 限制）：双侧等形的容量护栏，拒收 Redis 可执行命令，>64 源 BITOP 罕见且
错误帧明示，危害低，本票不裁，另案登记或维持。
2. Redis unstable 扩算子 DIFF1/ANDOR/ONE（bitops.c:1260-1267）双侧枚举均无（BitmapOperation
五变体）——较 Redis 7.2 属双侧等形新增缺席（DIFF 本体两侧在场，系 C#/rust 自有扩形），是否对
Redis 新版对齐另案。
3. dest 存活向量登记与删除臂交互的执行席终裁细节（本票 2 预留）。
4. GEO 族双侧均实装，逐命令深比对未竟（预算内），另轮审计。
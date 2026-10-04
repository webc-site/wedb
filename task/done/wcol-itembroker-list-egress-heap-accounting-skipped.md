终态注记（2026-09-28 主控）：已合入 dev（合入 commit 9a71ee9）。
收口形态：
1. wedb/wcol/src/itembroker/collection_item_broker.rs：try_get_next_list_item 弹出 item 后调用 list_obj.update_size(&item, false) 扣减体积账；try_move_next_list_item 从 src 弹出 next_item 后调用 src_list_obj.update_size(&next_item, false)，推入 dst 前调用 dst_list_obj.update_size(&next_item, true)。
2. wedb/wcol/tests/collection_item_broker_tests.rs：扩展 list_item_helpers 测试，断言弹出/搬移前后 heap_memory_size 精确变化量与 should_promote 门限判定，消除体积失真。

甄别结论：通过（2026-09-28 主控逐点现码复跑，真实性/非重复/架构合规/可执行度/格式纯粹五条全验成立，定级 P2 维持，派席沙箱开发修复）

经纪列表出件助手弹出推入绕过 update_size 致 heap_memory_size 失真并永久拒写 BLPOP 出件

问题分析：
1. Garnet 契约对齐。C# 经纪出件绝不原地改堆对象：CollectionItemBroker.cs:TryGetNextListResult（394 行起）文档注释（391-392 行）明言 Pops the next available item(s) from the list at asKey using RMW, so that the update is applied by Tsavorite rather than by mutating the heap object in place，其弹推全部经 storageSession.ListPop（408、439、477 行）与 storageSession.ListPush（453 行）走 RMW 通道；而 RMW 通道落进 ListObjectImpl.cs 的 ListPush（242 行 UpdateSize(value)）与 ListPop（289 行 UpdateSize(node.Value, false)），即 C# 出件路径的体积账与命令路径同机制、逐元素成对加减。collection.md 3.2 与 object_store_utils.rs:obj_save_or_gc 文档（1246-1253 行）登记的口径亦是：体积维吃 heap_memory_size 单源账，写回判据不新造第二套门限。
2. 工程现状确证。wcol/src/itembroker/collection_item_broker.rs:try_get_next_list_item（1073-1084 行）对 list_obj.list 裸 pop_back/pop_front，try_move_next_list_item（1089-1121 行）src 裸弹出、dst 裸 push_back/push_front，两助手全程未调 ListObject::update_size（单点账 wcol/src/list/list_object.rs:182-190，SLOT*2 加 round_up_ptr 口径），也无任何刻意偏差申报（1070-1072、1086-1088 行注释自称对位 C# TryGetNextListResult）。消费侧 wnode/src/resp/objects/collection_item_source.rs 出件臂 list_outcome 在 BLPOP/BRPOP（172 行）、BLMPOP（206 行）、BLMOVE 跨键弹推（313 行）调用之，随后 save_list（430-440 行）经 obj_save_or_gc（object_store_utils.rs:1264-1280）判 !is_empty && obj.should_promote()（1272 行）落笔。wcol/src/lib.rs:50-52 升阶门为条目数或体积双维 OR，体积维吃的正是这本账（types/garnet_object.rs:66-68）。同键旋转臂（261-273 行）在同对象上弹出再推回同元素，账净零，本案不涉。对照同构义务：命令层 list_object_impl.rs:list_push（234 行）/list_pop（284 行）与 list_commands/write.rs:list_move_core（500-501 行 src.update_size(&element,false)、dst_loaded.update_size(&element,true)）均成对记账；wcol/tests/memory_accounting.rs 钉死的正是 promote-on-real-heap-bytes 单账契约。经纪臂与命令臂、C# 原型三者不同构，属真实契约分叉而非上游缺域产物。
3. 逻辑危害确证。失真方向双侧：其一，dst 少计——BLMOVE 推入后 dst 的账停留在推前旧值（少一整条目 round_up_ptr 加 SLOT*2），obj_save_or_gc 体积门吃偏小值放行，写出真值恰越 4MB 的信封列表，直接打破信封持久态恒小于升阶体积门这一由逐级写回门维持的全局不变量（collection.md 3.1/3.2 双水位设计前提）。其二，src 多计——出件重放每次经 from_blob 精确复算，弹出后账不回落，判门用的恒等于弹出前旧值；一旦该键落入前述越门信封态（或由任何他径达之），真值已随弹出回落至 4MB 之下，旧值仍大于等于 4MB，1272 行门永真，save_list 恒 false，出件臂只回 TryGetOutcome::with_count（181、219、322 行），items 未送达、观察者继续挂队；经纪臂无 Degrade 转异步升阶漏斗（ObjLoad::Degrade 仅 156-160 行分层键分支；信封装载恒 Present），命令层 LPOP 判门用弹出后精确账可自愈，唯独 BLPOP/BRPOP/BLMPOP/BLMOVE 出件面在该键上永久拒写拒投递，timeout 0 即永久悬挂，timeout 有限亦必然超时。违反审查板块三记账真实性与板块四多路径行为同构两项硬维度。

涉及代码：
rust 文件与函数：
wedb/wcol/src/itembroker/collection_item_broker.rs:try_get_next_list_item（1073-1084 行）、try_move_next_list_item（1089-1121 行）
wedb/wcol/src/list/list_object.rs:ListObject::update_size（182-190 行，账单点）
wedb/wnode/src/resp/objects/collection_item_source.rs:list_outcome（172、206、313 行调用点）、save_list（430-440 行）
wedb/wnode/src/resp/objects/object_store_utils.rs:obj_save_or_gc（1264-1280 行，1272 行判门）
对照同构臂：wedb/wcol/src/list/list_object_impl.rs:list_push（234 行）/list_pop（284 行）、wedb/wnode/src/resp/objects/list_commands/write.rs:list_move_core（500-501 行）

对应 c# 文件与函数：
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetNextListResult（394 行起，弹推经 408/439/453/477 行 RMW 通道）
garnet/libs/server/Objects/List/ListObjectImpl.cs:ListPush（242 行 UpdateSize）、ListPop（289 行 UpdateSize）

精炼执行方案：
1. 在两助手内复用既有唯一具名口径补账，不引入第二套机制：try_get_next_list_item 命中弹出分支后调 list_obj.update_size(&item, false)；try_move_next_list_item 于 src 弹出后调 src_list_obj.update_size(&next_item, false)、dst 推入前调 dst_list_obj.update_size(&next_item, true)。同键旋转臂（collection_item_source.rs:261-273）净零不动；判门、漏斗、写回落点一律不增改。
2. 测试验证点：扩展 wcol/tests/collection_item_broker_tests.rs:list_item_helpers（330-395 行）——夹具经 operate 推入后逐次断言助手弹出前后 heap_memory_size 恰差 round_up_ptr 加 SLOT*2 组合值；try_move_next_list_item 后断言 src 账下修、dst 账上修；再以 should_promote 判据断言：向账距 TIERED_PROMOTE_BYTES 差一小条目内推入时 dst 越门即真（少计漏判消除）、跨门列表弹出一条目后判门即假（多计误判消除）。
3. 查重结论：task/ 全域 grep try_get_next_list_item、try_move_next_list_item 零命中；task/done/wcol-blmove-broker-half-commit-dst-wake-lost.md 议题为半提交唤醒派发（notify_key），全文无 update_size/heap_memory 字样，不重叠；doc/zh/collection.md 与 deviations.md 无经纪臂免账裁决登记（§120 仅涉降阶轮次旋钮）；助手注释自称 C# 对位且 C# 原型经 RMW 通道逐元素记账，非刻意偏差、非上游缺域，立案成立。
4. 未尽面：zset/set 经纪出件助手已逐臂对账（走对象层 operate 记账通道）未见同类漏账，未在本案展开；越门信封态的端到端复现（BLPOP 于 4MB 级信封键悬挂）属长测，未编入本票验证点，执行席可另入集成回归；除 obj_save_or_gc 两收口点外是否存在其他直写信封免门路径未穷尽排查。

## 主控后验注记（2026-09-28 r438 收口席，独立审核席结论补册）

一、流转经过（本案系跨席竞用实录）
- 本案为我方 r438 阶段一发现席起草（其引用路径缺 wedb/ 层前缀，真身在
  wcol/src/itembroker/collection_item_broker.rs 与 wcol/src/list/list_object.rs），
  我方独立审核席在途时，并发席于 588dbca「甄别通过入 ing」抢先领票，
  7 分钟内落码合入 26c1880（fix 9a71ee9）并归档 eeda83e。
- 处置：按其已领不重派、不返工，改由本席做落码后验——审核席裁决与本席复核一致，
  落码与审头方案全等，无需回改。

二、独立审核席裁决（裁 通过，维持 P2）与本席落码后验
1. 账点唯一性与消费者（成立）：ListObject::update_size 系全仓 List 堆账唯一具名加减点
   （口径 round_up_ptr(len)+SLOT*2，wbase/src/heap.rs），heap_memory_size 除该点与
   Default 基线外无直写点；装载 deserialize_from_slice / from_blob 逐项重算、序列化不吃账；
   账有四处生产消费闸点（obj_save_or_gc 与 run_sync_rmw 前置判、懒降阶臂、STORE 冷第二收口），
   故账失真直接驱动写回拒判，非死账。
2. 分叉唯一性（成立）：命令臂 list_object_impl.rs push/pop 与 list_commands/write.rs
   list_move_core 成对记账，Set 族 slow/write 同构；zset 出件臂经 pop_min_or_max 内
   update_size 已对象内记账（BZPOPMIN/BZMPOP 免账疑义不成立）；全仓生产调用方唯
   collection_item_source.rs BLPOP/BRPOP、BLMPOP 循环、BLMOVE 三处
   ——List 出件臂系全仓唯一免账出件点。同键旋转臂弹后推回同元素、净零无罪，未纳入本票。
3. 危害可达性（成立并两处收窄，本席认列）：
   a) 缺省页容量 16MB（DEFAULT_HLOG_PAGE_SIZE，wbase/src/cfg.rs）⇒ envelope_overflow
      只拦超页记录，16MB 页下 4MB 级信封物理可写，obj_save_or_gc 体积门是唯一拦截点
      ⇒ 本案免账恰在缺省大页下畅通（小页反被超页门提前拦截），非异值配置产物，缺省即达不降档。
   b) 票面「或由任何他径达之」收窄为「唯此一路径」：其余写回臂逐臂 grep 覆按均经
      obj_save_or_gc / rmw_helpers / dest_cold 单源收口吃真账，无第三越门信封来源。
   c) 票面「永久悬挂」限定订正：越门信封存在时经纪臂自身确无收敛出口（装载恒 Present，
      Degrade 仅分层键分支，出件臂仅回 with_count、元素弹出即弃、盘上未写、无双付无丢失），
      但该键此后若再获一发命令层写入（RPUSH 族触发升阶漏斗换入分层树，或 LPOP 族精确记账
      缩回门内自愈），悬挂随之外力解除；纯阻塞消费且键上无命令写（timeout=0 常态队列）
      即永久悬挂，有限超时白等至超时。外力收敛非本臂职责，拒投窗口期内出件面已坏，定级不改。
4. C# 原型六锚逐字核正：CollectionItemBroker.cs 文档注释「Pops ... using RMW, so that the
   update is applied by Tsavorite rather than by mutating the heap object in place」属实，
   ListPop 于 408/439/477、ListPush 于 453，落 ListObjectImpl.cs ListPush 的 UpdateSize(value)
   与 ListPop 的 UpdateSize(node.Value,false)；C# 反序列化构造亦逐条 UpdateSize，与 rust
   from_blob 复算同构 ⇒ 本病灶系转写分叉非上游缺域，补成对 update_size 即与
   Redis 语义、C# 原型、本仓命令臂三面同构，无按 C# 机械回改问题。

三、落码后验（对照审头第六节方案）
- 9a71ee9 改动面恰两处（try_get_next_list_item 弹出后 src 扣账；try_move_next_list_item
  src 扣账、dst 推入前加账），复用既有唯一账点，未新增第二套计账机制，未在调用方补判；
  同键旋转臂、判链、序列化、notify_key 语义零动，与审头禁触线全等。
- 等价性复核：原 list.is_empty() 早退折叠为 pop_back/pop_front 返 None 即 ?，
  弹出前无副作用，语义无损；dst 加账置于 push 之前，与 Set 族成对形同序。
- 测试面覆盖审头两判据：逐条目 heap_memory_size 精确差额断言（BLPOP/BRPOP/BLMOVE 双臂）、
  TIERED_PROMOTE_BYTES 边界「少计漏判消除 / 多计拒判自愈」should_promote 双向断言，
  撤补账即转红（revert-proof 形）；既有 collection_item_broker_tests 全家与 §100 唤醒锁测未弱化。

四、门禁实证
- 本票并入后单轮主控门禁实测：./test.sh --no-fail-fast 于 dev 尖 5198 tests run、
  5198 passed、1 skipped、EXIT=0（含本票新增 wcol 记账臂与 r437 姊妹两票新断言）；
  clippy 与 check.js 续跑结果见本波门禁记录。

五、未尽面（审头划界在册）
1. 越门信封端到端复现（BLMOVE 铸错 → BLPOP timeout=0 悬挂）属长测，可另入集成回归。
2. 除 obj_save_or_gc 与 store_dest_cold_common 两收口点外，残余直写信封免门路径未穷尽排查。
3. 分层键慢路径出件臂不经本助手，不在本案面。

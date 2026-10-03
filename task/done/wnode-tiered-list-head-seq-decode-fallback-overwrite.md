终态注记: 已合入 main（commit: edad5b7）。收口形态：list_head_seq 最左记录解码失败置 corrupt 旗标并上抛 Err(())，空树回落 LIST_SEQ_BASE；订正 list.rs:45 注释锚为 :162 并明确解码失败上抛不变量；新增 wnode/tests/tiered_list_corrupt_seq.rs 覆盖非 16B 记录防覆写与防虚增断言。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 审核席）。缺陷本体确证：list.rs:75 解码失败臂无上抛通道（回调签名 FnMut(&[u8],&[u8])->bool，wbftree/src/service/ops.rs:217-225 亲核），违反 :66-69 自身文注，push 双臂以错误 head 现算 base（:132/:138-142/:148-155 逐点核对）。两点订正随执行落：一、危害算术订正——树内键族排查确证元记录不落集合树（save_bftree_meta_stub 走独立 Meta 键空间 stub.rs:206-237）、树内零墓碑、旧 8B 形态已删，正常排布 head_real ≤ BASE 恒成立，故覆写实际发生于 LPUSH 臂（head_real = BASE-d 且 d ≤ n+size-1 时写区 [BASE-n, BASE-1] 与真实区重叠，例 d=1、size=10 万、n=1 时 base=BASE-1 恰为真实首元素槽，逐条覆写+虚增+成功帧），RPUSH 臂为孤儿段+size 虚增+后续物化降阶整树污染，票面「head_real 落 [BASE+1,BASE+n]」表述作废；二、行号锚订正——export_entries 升阶排布实锚 wcol/src/types/garnet_object.rs:146-167（:162 排布式），票面与 list.rs:45 文注的 :243 同漂移，执行时一并订正文注。方案可落度亲核：scan_count（common.rs:29-34）已收 IO Err，Option+旗标均为栈上标量、空树回落 BASE 语义保持，层级正确（16B 序号键是列表层知识，不上提 scan 通用原语）；测试夹具参照 wnode/tests/range_index_tests.rs 的 get_or_open_tree 直操作先例注入异长键，驱动面用 tiered_list_lrange_window.rs 的 open_env + auto_exec。查重：deviations 与 task 五池零命中，相邻票 wnode-tiered-dead-key-read-arms-touch-tree-stale-emit 系本函数 Lpos 读面触树，与本票写面解码回落异面正交。

原票面：
分层列表 list_head_seq 解码失败臂静默回落 LIST_SEQ_BASE，违反自身文注必须失败而非猜默认的明令，树内容异常时 push 静默覆写既有元素并虚增计数

问题分析：
1 Garnet 契约对齐（C# 原型行为与协议约定）：C# 无序号键概念——libs/server/Objects/List/ListObjectImpl.cs:229-244 ListPush 为内存链表指针 O(1) AddLast/AddFirst 追加，永不覆写既有元素；rust 内存态对位 list_push（wcol/src/list/list_object_impl.rs:248-266）同为纯追加。分层态与内存态对同一数据集行为必须同构：任何输入下 push 均不得销毁既有元素。
2 工程现状确证（Rust 现有实现路径与代码缺陷）：list_head_seq（wedb/wnode/src/resp/objects/tiered_collection_ops/list.rs:71-82）以 limit 1 扫最左记录取头序号，回调 `if let Ok(arr) = <[u8; 16]>::try_from(k)` 无 else 臂——最左键非 16B（页损坏、外族记录混入，第一字节非 0x01 即触发，无需恰好 16B 之外的特定条件）时 head 静默保持 LIST_SEQ_BASE（garnet_object.rs:25，1<<64）。函数文注 :66-69 自陈「静默回落基准会让 push 把序号覆盖到既有元素上（数据销毁），故必须失败而非猜默认」：扫描 Err 臂已经 scan_count 上抛收口，但解码失败臂正是它自己禁止的猜默认形态，守约面只收了一半。push 双臂（list.rs:127-155）以 head 现算 base：RPUSH base = head + meta.size、LPUSH base = head.saturating_sub(n)，写入槽位逐条 tree_put_ok 且每条计 pushed、size += n。
3 逻辑危害确证（数据销毁与计数虚增）：解码失败即 head 回落 BASE，RPUSH 写入槽位 [BASE+size, BASE+size+n-1] 与真实占用区 [head_real, head_real+size-1] 相交（head_real 落 [BASE+1, BASE+n] 任意点即中招）时逐条覆写既有元素、应答仍成功帧、size 虚增 n；LPUSH 同理向已占用区下压。内存态同一逻辑数据上 push 正常追加零覆盖——分层态静默销毁数据 + 计数虚增 + 成功帧三重背离（与内存态行为、与函数自述不变量、与 C# 纯追加语义）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/list.rs: list_head_seq（:71-82 解码失败静默臂）、tiered_list_arm push 双臂（:127-155 base 现算消费点）
wedb/wcol/src/types/garnet_object.rs: LIST_SEQ_BASE 常量（:25）与 export_entries（:243 升阶排布基准）

对应 c# 文件与函数：
garnet/libs/server/Objects/List/ListObjectImpl.cs: ListPush（:229-244，内存链表纯追加语义锚，无序号键概念）

精炼执行方案：
1. list_head_seq 解码失败臂改上抛（回调签名 FnMut 无 Err 通道，用旗标收口）：head 改 Option<u128> 初值 None、辅 corrupt 旗标——回调解码成功置 Some、解码失败置 corrupt；扫描返回后 corrupt 为真即 return Err(())，None（空树，回调未运行）维持现行回落 LIST_SEQ_BASE 语义（文注 :66-67 已裁决的合法形态），Some 直用。空树回落与升阶排布同基准的行为不变。
2. list_head_seq 文注 :66-69 同步订正：把「解码失败同样必须失败」并入不变量表述，消除守约面只收一半的自相矛盾。
3. 测试验证点：tiered 列表夹具注入非 16B 最左键（外族记录）后 RPUSH/LPUSH/LPUSHX/RPUSHX 四臂必须失败上抛（存储错误帧）、不覆写不虚增 size；正常 16B 树 push 与空树首推回落基准两形态回归不回摆。

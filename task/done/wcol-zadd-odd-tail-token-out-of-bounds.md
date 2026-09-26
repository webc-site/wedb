归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 1baf277（P3 登记级），收口形态：§151 登记 ZADD 奇数尾巴 C# 越界 UB / rust 防御截断宽向分叉（三处守卫自陈注回指、锚按内容引），双态锁测 wnode/tests/resp_sorted_set.rs＋tiered_cmds_align.rs 共 +107，零行为改动。主代理亲验 1baf277 已在 dev 祖先链、§151 册内在位、sorted_set_object_impl.rs:360 守卫回指注在场。

甄别结论：通过（甄别席 zc-fix-r16-zaddtail，2026-09-26）定级 P3
C# 四锚亲验全中：SortedSetCommands.cs:25-26 前置门仅 parseState.Count<3（4 token 放行）；InputHeader.cs:240-243 ObjectInput startIdx:1 切片（对象层 Count=3 即「1 m 5」）；SortedSetObjectImpl.cs:130 主循环 GetArgSliceByRef(currTokenIdx++) 无界读、:90 起循环 idx 追平 Count 后仍取成员；SessionParseState.cs:369-373 GetArgSliceByRef 仅 Debug.Assert(i<Count)，release 直读 bufferPtr+i 越界槽；GetOptions 偶数校验实测在 :81（票称 :80-87，容差内）且仅首 token 非分值形进入，「ZADD k 1 m 5」确漏出。
rust 双态守卫亲验在位：wcol/src/zset/sorted_set_object_impl.rs:360-363 if curr_token_idx >= count break；tiered_collection_ops/zset.rs 解析扫描 :135-138（curr == args.len() break）与主循环 :206-210（args.get(curr) else break）同款截断——「ZADD k 1 m 5」稳定 :1 只落 m=1，自洽成立。
台账查重：deviations.md 册尾现 §150，§142 系中段非浮点部分提交轴、§5/§138 系首字节空串族、§149 系 keynum 规格槽校验轴，均不覆数组索引越界奇数尾巴形，全册零登记属实；各池无同轴他票（reject/zcode-r167c-watchver 仅列举 ZADD 非本轴），源出 review_history/zcode-r19-zset.md 系提案留痕非重复票；resp_sorted_set.rs/tiered_cmds_align.rs 现无该形锁测，缺陷现状（台账+锁面缺位）在现码仍成立。
架构合规：纯登记级零行为改动（补条目+三处守卫自陈注+双态锁测），合 transpile 单向分层与既有登记票体例（§138/§149 先例），裁决锁 rust 截断现状不盲从 C# UB 形，方向正确；格式纯粹、双侧路径齐全、步骤最小可落。落册顺编取号纪律已在方案内申明（禁预拟号）。

审核结论：通过（登记级，够立案门槛，审核席 zcode-r19-review-zset，2026-09-26）

亲验记录：C# 越界点四点全中——SortedSetCommands.cs:SortedSetAdd 前置门仅 Count < 3（Count=4 放行）、InputHeader.cs:240-243 startIdx:1 切片（对象层 parseState.Count=3 即「1 m 5」）、SortedSetObjectImpl.cs 主循环 GetArgSliceByRef(currTokenIdx++) 无界读（:130）、SessionParseState.cs:369-373 仅 Debug.Assert(i < Count) 单防线 release 直读 bufferPtr+i 越界槽；GetOptions 偶数校验（:80-87）确仅在首 token 非分值形态生效，首 token 即分值形漏出。rust 双态守卫亲验——内存态 sorted_set_object_impl.rs:361-363（curr_token_idx >= count break）、分层臂 tiered_collection_ops/zset.rs 解析扫描 :135-139 与主循环 :206-210（args.get(curr) else break）同款截断，ZADD k 1 m 5 稳定回 :1 只落 m=1。deviations.md 查重：§142 系「数据段中段非浮点词形」部分提交轴、§5/§138 系首字节空串族，数组索引越界形全册零登记。三方对照补注（审核席加，供对账与后席判向）：真 Redis t_zset.c zaddCommand 对 score/member 奇数尾巴报 syntax error（成对校验在前），即本形态三方分立——C# 越界 UB / rust 防御截断回 :N / Redis 拒绝错误帧。本票裁决锁 rust 截断现状（确定且无害），不盲从 C#；若后续有对齐 Redis 拒绝形的诉求，另行立案评估（改变现有应答 :N → error，影响面独立），本条登记不预设该方向。

精炼执行方案：
1. doc/zh/deviations.md 补登记级条目（落册顺编取号，禁预拟号）：ZADD 数据段奇数尾巴（首 token 为分值形）C# GetArgSliceByRef 数组索引越界 UB（debug Assert 掐连 / release 越界槽垃圾成员写入）不复刻，rust 双态（信封 / 分层树）防御截断回 :N 为确定侧；条目内注明三方对照（真 Redis 该形报 syntax error，rust 截断系防御容忍非 Redis 对齐形）与划界（本条为数组索引越界形；§142 中段非浮点全退、§5/§138 首字节越界族不覆；选项段形态的奇数尾巴双侧同回 syntax error 不在本条）
2. 双态守卫处补一行自陈注回指偏差条号（防后席按 C# 形改写为越界读或 panic）：内存态 :360-363 与分层臂 :135-139/:206-210 三处
3. 测试验证点：resp_sorted_set.rs / tiered_cmds_align.rs 补 ZADD k 1 m 5 双态锁——应答 :1、成员集恰 {m:1}、会话存活（PING 通）、错误帧零输出；ZADD k 1 m（Count=3 过门后单对完整）正常 :1 对照

ZADD 数据段奇数尾巴（首 token 为分值形）C# 主循环 GetArgSliceByRef 越界读 UB 未登记，rust 防御截断静默 :N 侧缺台账

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 会话层 ZADD 前置门仅查 parseState.Count < 3（libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd :25-26），「ZADD k 1 m 5」（Count=4）放行；ObjectInput 以 startIdx:1 切片（InputHeader.cs:240-244）后对象层主循环（libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd :105-194）先 TryGetDouble 吃分值再 GetArgSliceByRef(currTokenIdx++) 取成员（:130）——末轮 currTokenIdx==parseState.Count 时 GetArgSliceByRef 越界（SessionParseState.cs:369-373 仅 Debug.Assert(i < Count)，release 直读 bufferPtr+i 越界槽）。GetOptions 的「剩余段偶数校验」（:80-87）只在首 token 非分值（走选项段）形态生效，首 token 即分值的奇数尾巴（ZADD k 1 m 5）完全漏出：debug 构建断言掐连，release 构建读同缓冲越界垃圾 slice——垃圾成员名被当作新增成员写入集合（TryGetValue/Add/UpdateSize/AOF 全走）或指针垃圾抛 NRE，行为不可预测。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 两侧均已防御闭环：内存态 wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_add（:360-363）取成员前 if curr_token_idx >= count break（已消费合法对落账，尾巴分值丢弃，收尾回 :N）；分层臂 wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs 的 Zadd 解析扫描（:135-139）与主循环（:206-210）同款截断。即 rust 对 ZADD k 1 m 5 稳定回 :1、只落 m=1，双态自洽。问题在台账缺位：doc/zh/deviations.md 现有 §142 只登「数据段中段非浮点词形」（选项词混入数据段），§5/§138 只登首字节越界族（ZCOUNT 空串 / lex 空串），数组索引越界的奇数尾巴形全册零登记。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
rust 无实害（防御在位、应答确定）。危害落治理面：双侧对拍遇 ZADD 奇数尾巴用例时 C# 侧为 UB（debug 掐连 / release 垃圾成员），对账席无登记可引必误判为 rust 转写缺陷（rust :1 对 C# 不可预测形）；且后席若按「对齐 C#」名义撤销 rust 截断守卫（改成越界 panic 或 unwrap）即引入真缺陷。

涉及代码：
rust 文件与函数：
wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_add（奇数尾巴截断守卫 :360-363）
wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs:exec_tiered_zset 的 Zadd 臂（截断守卫 :135-139 / :206-210）

对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd（主循环 GetArgSliceByRef 无界读 :130；GetOptions 偶数校验只罩选项段形态 :80-87）
garnet/libs/server/Resp/Parser/SessionParseState.cs:GetArgSliceByRef（:369-373 Debug.Assert 单防线）
garnet/libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd（:25-26 前置门）

精炼执行方案：见文件顶部审核结论内整理版（含三方对照补注，以顶部为准）。

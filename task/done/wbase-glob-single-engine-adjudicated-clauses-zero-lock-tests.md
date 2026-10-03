甄别结论：通过（甄别席 J7，2026-09-27，定级 P3——登记+锁测级零行为）。glob.rs 全文件零 cfg(test)、wbase/tests/suite 无 glob 目，亲验；头注 :3/:89 条款与非递归贪心 :94-101 形态在；C# GlobUtils.cs:33 星递归锚实；deviations glob 引擎条款全册零在册。派沙箱席 c01o。

审核结论：通过（登记+锁测级零行为成立。deviations 全册 glob 零命中查重净；rust 贪心单回溯两指针形（glob.rs:101-170）经构造审仅末星回溯重扫，最坏 O(n²) 多项式无指数面；C# GlobUtils.cs:33 星递归实锚，util.c:70 nesting>1000 守卫恰证上游自认病理形，("*","") 三侧皆 false 亲验；八点消费行号全中单引擎成立，acl_parser:198 零匹配属实；§108 rust 优侧纯登记、§114、§155 同谱门槛够。登记要点订正：措辞收紧「glob 消费面无第二引擎」（wext_json regex 系 JSONPath 他域）；util.c 锚在本仓外按内容引；墙钟帽宜宽松防 CI 抖动）

wbase::glob 全仓通配单点引擎十余条反向直觉判定条款仅注释在册、零锁测，对拍轮误判双报风险无锚（登记级＋锁测级，零行为改动）

问题分析：
1 现状确证（共用单点成立，无分叉源）：全仓 glob 消费点均汇 wbase/src/glob.rs 单引擎（glob_match / glob_match_nocase，feature="glob" 门控），八点核验——键空间 SCAN/集群键计数/merge_vector_keys（array_key_iteration_functions.rs:277/:668/:671、garnet_api/slow.rs:164）走 nocase；HSCAN/SSCAN/ZSCAN/COSCAN（wcol hash_object.rs:412、set_object.rs:280、sorted_set_object.rs:533、tiered_collection_ops/scan.rs:445）与 PSUBSCRIBE（wpubsub subscribe_broker.rs:88/:252）走 case 敏感；ns 前缀通道面（channel_ns.rs）为既有用法示例。仓内无第二 glob/regex 引擎；ACL 键模式两侧同为零匹配面——C# 仅收 `~*`/ALLKEYS no-op（garnet/libs/server/ACL/ACLParser.cs:257-264），rust 对位（wacl/src/acl_parser.rs:198-202），故「SCAN glob vs ACL glob 语义差」在本仓不可达，非分叉案源。
2 可证伪实证（本席差分对拍，全轴零分叉）：将 C# GlobUtils.Match（garnet/libs/server/GlobUtils.cs:17-161）指针形逐句等价改写（越界读按哨兵 0 形建模，RESP 构帧下 pattern 后恒 \r\n 非元字符，UB 臂不可达）与现码 glob.rs 直编译对拍：①十字符字母表 {a,b,*,?,[,],^,\,-,!} 模式≤4B × 目标≤3B × 大小写双模全穷举 24,688,642 对，零失配；②十四字符扩展（加 x,4,1,A，含 `\x41` 词形）长模式随机 800,000 对，零分叉。逐条对照 C# 与 Redis 现规范（redis/unstable src/util.c stringmatchlen 一手复核）：\x 十六进制转义 Redis 与 C# 均不支持、rust 同不支持（`\x41` 两侧皆匹配字面 "x41"）；未闭合 `[` Redis 现行形与 C# 同为「恢复末字符、按已扫描集判定」非拒模式（Garnet 系逐句转写，任务书所疑「拒模式 vs 当字面」三叉在本版 Redis 不存在）；`!` 字面成员、`^` 取反、`[a-]x]` 之 `]` 兼区间端点、空类 `[]` 恒不匹、`*` 不匹空目标（(" *","")=false）——三侧全同。大小写轴：键空间 glob 两侧同 nocase（C# ArrayKeyIterationFunctions.cs:303/:306 显式传 true；rust glob_match_nocase），集合内 SCAN 与 PSUBSCRIBE 两侧同 case 敏感（C# 其余五调用点默认 false；rust glob_match）；UTF-8 多字节键 `[]` 范围两侧同按字节比较（C# byte*/rust &[u8]），ignoreCase 仅 ASCII 折叠且逆序字母区间 `[k-M]` 折叠后判恒不匹两侧同形。
3 性能红线确证（结果零差、可用性单向超集，未在册）：C#/Redis 参考形星号递归回溯最坏指数＋栈溢出面——本席实测参考形 k 星 `a*a*…*b` × 24B 目标：k=6 1.6ms、k=8 6.8ms、k=10 26ms、k=12 37ms 随 k 指数爬升；rust 非递归贪心单回溯点（glob.rs:101-104 star_p/star_t）同输入 <1µs 且 O(1) 栈、恒定。该「DoS 面结构性消除＋判定结果全等」形制仅 glob.rs 头注自陈（:3/:89「杜绝 ReDoS 与递归栈溢出」），deviations.md 与五池 glob 关键词零在册（全库 grep 独中），属 C# 上游缺陷面之 rust 收口，按仓内「注入形/崩溃形不复刻、纯登记」先例应锚明防回改。
4 逻辑危害：单引擎承载 SCAN/KEYS/PSUBSCRIBE/集合 SCAN/集群键计数全家族，其头注＋match_bracket doc 所载 ≥8 条精确对齐裁决条款（本席逐条复核通过者）现全仓零 #[test] 钉锁（wbase/tests/main.rs 与 suite/ 无 glob 目、glob.rs 无 cfg(test) 块）——`*` 不匹空、未闭合 `[` 当完备集、`]` 兼端点、`!` 字面、`[k-M]` nocase 恒不匹等反直觉条款无锚，后续任何触 glob 的回归或对拍轮必生「误判 bug 回改 / 重复疑报案」双型误报（§155 一致口径之缺锁面同族）。

涉及代码：
rust 文件与函数：
wedb/wbase/src/glob.rs（glob_match_opt/glob_match/glob_match_nocase/match_bracket/in_range，全文件零锁测）
消费点锚：wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:277/:668/:671、wedb/wnode/src/resp/garnet_api/slow.rs:164、wedb/wcol/src/hash/hash_object.rs:412、set/set_object.rs:280、zset/sorted_set_object.rs:533、wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:445、wedb/wpubsub/src/subscribe_broker.rs:88/:252、wedb/wacl/src/acl_parser.rs:198（零匹配面对位锚）

对应 c# 文件与函数：
garnet/libs/server/GlobUtils.cs:17-161；garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:303/:306（ignoreCase=true 单点）；garnet/libs/server/PubSub/SubscribeBroker.cs:404；garnet/libs/server/Objects/{Hash/HashObject.cs:407,Set/SetObject.cs:218,SortedSet/SortedSetObject.cs:502}（默认 false）；garnet/libs/server/ACL/ACLParser.cs:257-264；Redis 一手：redis/unstable src/util.c stringmatchlen（同构逐句形）

执行方案：
1 新增 wbase glob 锁测目（tests/suite/glob.rs 或 glob.rs 内 cfg(test)，遵 suite 既形制），钉本席已复核条款表逐条用例：①条款十字面——`*` 不匹空目标/尾部多星吞尽、`?` 单字节、未闭合 `[^abc` 完备集＋取反判定、`[a-]x]` 端点兼 `]`、`[]` 恒不匹、`[!a]` 字面 `!`、`\` 末字节面（模式尾裸 `\` 匹字面反斜杠）、`\x41` 匹字面 "x41"（无 hex 语义防误加）、`[k-M]` nocase 恒不匹（C# 先交换后折叠形）、转义字节类内恒大小写敏感；②规模可控穷举臂：8 字符子集字母表模式≤3B × 目标≤3B 双 case 全枚举（≤26 万对，秒级，遵禁长测纪律，勿搬本席 24M 全量形入仓）；③性能红线臂：k=12 星对抗输入 × 24B 目标恒返 false 且墙钟帽（防回改递归/正则形引入指数面），注释回指本票与 GlobUtils.cs:33 递归源。
2 纯登记零行为改动零代码改动：不改 glob.rs 判定一字；deviations 侧若甄别席认定需入册，采「C# 递归 DoS 面 rust 贪心收口＋结果全等」单条形制，与 §138/§144 崩溃形不复刻族同谱；锁测完工前禁另立第二 glob 测试轨（消费点侧 wpubsub channel_ns.rs 既有用法示例臂不动）。

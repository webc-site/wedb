check.js 的 C# 语料被 tree-sitter 语法解析失败静默截断，完备性门禁系统性漏报且无失效告警

来源：next/glm.design.md 第 9 轮条 1（票面数据按当下重测，样本面有两处修正，见下）。取证基线：
主仓绝对根 /Users/z/git/db/wedb，分支 dev，工具链版本取 package.json 与 node_modules 现装态。

结论
garnetScan 用 tree-sitter 的 c-sharp 语法解析 C# 语料，该语法遇到 unsafe 指针写法即整段退化为
ERROR 碎片，其后方法全部丢失；而 check.js 的缺失判定只遍历提取成功的方法名，丢掉的既不进 miss
也不进 documented 比对，门禁对受影响文件永不上报。工具链自身有 corpus_invalid 硬失败纪律（针对
YAML 语料），却没有针对 C# 解析失效的同等纪律。判定成立且待做。

实测复现（node --input-type=module 加载 /Users/z/git/db/wedb/js/check/treeSitter.js:11 csParser
直测，全部为当下读数）
- 全仓 /Users/z/git/db/wedb/garnet 下 .cs 共 1425 个，rootNode.hasError 为真 194 个，全仓提取
  方法总数 16148。
- 炸点形态确认：/Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/MainStore/
  PrivateMethods.cs:59 `*tmp++ = (byte)'\$';` 一类指针后缀递增赋值（:58-62 连续三段），此后该
  文件整棵 AST 破碎。
- 受影响样本与提取读数（括号内为提取到的方法名清单摘要）：PrivateMethods.cs 仅 1 个（CopyTo），
  :396 的 TryInPlaceUpdateNumber 实存未提取；SessionParseState.cs 仅 3 个（Initialize、
  Initialize、InitializeWithArgument），:183 的 EnsureCapacity 实存未提取；BasicCommands.cs 46
  个；RespServerSession.cs 15 个（ERROR 节点 441 处）；AofProcessor.cs 12 个（ERROR 节点 225
  处）。
- 票面两处修正（按当下重测）：其一，AofAddress.cs 并非「只提取到 3 个」，实测提取 28 个，且
  ToByteArray / FromByteArray / Serialize / Deserialize / MonotonicUpdate / MaxExchange / Diff /
  AggregateDiff 全在列，check/miss/libs/server/AOF/AofAddress.yml 只登 3 条（MinExchange、
  AnyGreater、IsOutOfRange）是「其余已被 rust 注释或 ignore 认领」的正常结果，该文件不作为受害者
  样本；其二，RespServerSession.cs / AofProcessor.cs 的「粗估 184 / 114」一类真实方法数估算无凭
  据，本单不采用，只以「提取数远低于同规模文件」与「hasError + ERROR 节点数」作判据。

后果链
- /Users/z/git/db/wedb/js/check.js:414-433：all_file_set 由 fn_map / test_map 的键构成，逐文件
  的 miss 判定只 filter `fn_map[rel_path]` 里的名字；提取不到的名字结构性地不可能进 miss，也不可
  能进 documented 比对，因此 194 个受影响文件里的未转写方法对门禁完全隐形。
- /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:97 的验收口径「直到 check.js 没有缺失的
  输出」与 /Users/z/git/db/wedb/task/refine.md:4 的「运行 ./js/check.js……该补全的补全」在这些文
  件上失真；/Users/z/git/db/wedb/js/check/README.md 通篇未声明该解析限制（grep tree-sitter /
  解析失败 零命中），读 README 的代理会把它当无损语料。
- 连带暗条目：登记在解析失败文件上的 ignore 条目当前永不参与判定，例如
  /Users/z/git/db/wedb/js/check/ignore/server.yml:268 的 TryInPlaceUpdateNumber（位于
  PrivateMethods.cs 的失活区内）。语料一旦修复，这类条目会突然全量激活，需整批复核。

C# 参考
无对位（本单是转写门禁工具链自身的语料完整性，不涉 C# 语义）。受影响语料的 C# 侧事实即上述文件
与行号。

修法
1. 先量化再动手：在 garnetScan 汇总一遍 hasError 清单与每文件提取数，落一份
   js/check/ 下的语料健康读数（一次性诊断即可，不留常驻产物），据此判断受影响面的广度。
2. 换语法或补兜底：优先验证是否存在能解析 unsafe 指针语句的 c-sharp 语法版本（依赖现取
   @2h2d/tree-sitter-wasms，见 /Users/z/git/db/wedb/package.json devDependencies），可行则升版；
   不可行则对 hasError 文件走声明正则兜底提取（对齐 /Users/z/git/db/wedb/js/check/
   symbolCheck.js 既有的词法断言口径），兜底只补 method_declaration 名录，不参与 test/非 test 分
   类以外的判断，避免把语法缺失伪装成语义结论。
3. 无论走哪条，check() 必须对 hasError 文件大声报语料失效：与 YAML 侧 corpus_invalid
   （/Users/z/git/db/wedb/js/check.js:436、:493-497「语料失效期间 miss 目录未同步，判定不可作甄
   别依据」）同纪律，把「解析失效文件数 > 0」并入 corpus_invalid 口径并硬失败退出，不允许静默通
   绿。
4. 语料修复后立刻复核 ignore 台账中新激活的条目（server.yml 等登记在失活文件上的函数名），逐条判
   「已转写 / 该删 / 该继续忽略」，避免暗条目复活即假绿。
5. 在 /Users/z/git/db/wedb/js/check/README.md 第 1 节流程处补一段语法能力边界说明（哪些 C# 构造
   解析不动、门禁如何兜底），使后续代理不再把 check.js 输出当无损完备性证明。

优先级
污染扩散（判定器本身漏报，会带着错误结论派出更多实现票；修复成本集中在工具链，收益覆盖全部语
料）。

边界
task/ing/cs-anchor-dup-single-mount.md 管重复定义信息节的锚点噪声，不动语料广度；既有的
corpus_invalid 纪律只管 js/check/ignore 下 YAML 语料自身的解析失效，本单补的是 C# 源语料失效这一
维。

落地（fix-cs-corpus-parse-gate，dev 侧核实后实施）

裁决：成立，已实现。票面主张逐条重测复现，读数一致——全仓 1425 个 .cs、
rootNode.hasError 194 个、ERROR 节点 7506 处；PrivateMethods.cs 断裂前只提出
1 个名字（TryInPlaceUpdateNumber 在 :396 与 :473 两个重载全丢）、
SessionParseState.cs 的 EnsureCapacity（:183）全丢；check.js 判定只遍历
fn_map/test_map 的既有键名，故这 194 个文件的未转写方法结构性不可能进 miss。
合并前基线跑 bun js/check.js 是 exit 0、实现缺失段为空，门禁全绿，即漏报实证。

三处对票面的修正：

1 修法第 2 条的首选分支（升语法版本）不可行，已按数据排除：
   @2h2d/tree-sitter-wasms 只发布过 0.1.0 与 0.2.1，现装即最新 0.2.1，无版可升。
   另测「剥离预处理指令」这条更优路径：把全部 # 指令行原地替换为等长空白后只
   修复 7/194，且反丢 6 个方法名，故弃。词法兜底是唯一可行机制。
2 票面把炸点归给 unsafe 指针一族。按首枚 ERROR 节点文本统计，指针构造与
   #if NET9_0_OR_GREATER 条件块约各半（99 对 55，另 39 无法归类），但条件块
   多只是断裂的显示位置而非成因（见上一条剥离实验）。不承诺枚举构造清单，
   README 只登记「语法覆盖不到」这一事实与兜底机制。
3 修法第 3 条「把解析失效文件数 > 0 并入 corpus_invalid 并硬失败」不采纳，
   已改为大声而非致命，理由写进 js/check.js 注释：YAML 语料失效是本仓自己写坏、
   可修且修前判定必错，该硬失败；C# AST 断裂是固有能力边界且已由兜底补偿、
   判据可信。若并入，则 194 > 0 恒成立，missSync 与符号断言被永久跳过，
   SKILL.md:97「直到 check.js 没有缺失的输出」这条验收口径反而彻底失效——
   那是把门禁打砖。现每次运行在 stderr 报出降级文件数、补回名数、按 ERROR
   节点数排序的 top 5，并单独点名兜底后仍零名录的文件（唯一一例
   SpanByteKey.cs 经核为「只有属性与构造函数」，确实无方法，非漏报）。

实现：js/check/garnetScan.js 加 csDeclFallback，仅对 hasError 文件补
method_declaration 名录、按 is_test_file 落桶、不做额外语义推断；实测补回
388 个方法名（首轮 438，自查发现兜底绕过了 TEST_LIFECYCLE_FN_SET 整族排除，
把 Setup/TearDown 灌成 3 份假缺失，已收口并补断言）。完好文件不跑兜底，
在 1231 个 AST 完好文件上假阳性实测为 0；同形噪声两族（主构造函数
class Foo(int x)、元组字段 private static readonly (int A,int B)[] T=…）
由「返回类型必填 + 修饰符/类型关键字不占名位」剔除。
js/check_selftest.js 第 6 节 11 项断言（26 → 27 项），js/check/README.md
第 1 节补语法能力边界段。合入后 check.js exit 0、stderr 降级汇报在位、
B 层锚点提示 129 处与基线逐字节一致、js/check/ignore 语料零回写零删除。

遗留复核（修法第 4 条，本单不做，交语料甄别波）：修复使
登记在失活文件上的 109 条 ignore 条目首次真正参与判定（票面点名的
PrivateMethods.cs:TryInPlaceUpdateNumber 即在列，README 已就此警告）。
抽查发现至少一条可疑：incr.rs:182 与 resp_tests.rs:428 已有
TryInPlaceUpdateNumber 的在位实现与测试叙述，但该名仍记在 server.yml 的
忽略侧、且现有注释是「路径.cs:行号」或空格分隔的叙述形态，CS_REF_REGEX
不认，故未登记成映射——属「已实现却挂忽略」族，须逐条判
「已转写应改注释 / 该删 / 该继续忽略」。重跑清单：
bun 脚本取 hasError 文件的「仅 AST 名集」与「AST+兜底名集」之差，
与 js/check/ignore 下的条目名求交。

另有 10 份新 miss（31 名）为本次修复暴露的真实缺口，抽查项几乎全带
byte* 形参（DebugSend、DenseCountNonZero、GetSerializedRecordSpan、
BeginReplayOp、TraceBackForOtherChainStart 等），正是语法看不见的那一族，
下一步该按 miss 派实现票而不是再动工具链。

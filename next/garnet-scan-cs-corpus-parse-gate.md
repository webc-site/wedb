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

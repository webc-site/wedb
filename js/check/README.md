# 代码对标检查规范

## 1. 怎么复现检查

在项目根目录运行命令：

./js/check.js

执行流程：

扫描 garnet/ 目录获取 C# 源码的类与方法定义

扫描项目所有 .rs 文件的文档注释提取 C# 引用标注

读取 js/check/ignore/ 目录下的 YAML 忽略规则

输出两项结果：

重复定义：同一个 C# 方法被多个 Rust 函数标注引用

实现缺失：未在 Rust 注释中引用且未配置 ignore 的 C# 方法（同时自动同步到 js/check/miss/ 目录）

符号存在性断言：解析注释中全部 `路径.cs:符号` 锚点，断言符号词法存在于被引 C# 文件内（见第 4 节；libs 族违规硬失败）

### C# 语料的语法能力边界（check.js 不是无损完备性证明）

扫描 C# 侧用的是 tree-sitter 的 c-sharp 语法（@2h2d/tree-sitter-wasms，现装 0.2.1，
是该包已发布的最新版，无可升版）。该语法不完整支持 unsafe 指针文法，遇到下列构造
会把该处往后的整棵 AST 退化成 ERROR 碎片，其后的 `method_declaration` 全部丢失：

- unsafe 指针写法，如 `*tmp++ = (byte)'$';`、`*(int*)payloadPtr = ...`、`byte*` 形参
- 成员列表里的 `#if NET9_0_OR_GREATER` 一类预处理条件块（语法不跑 C# 预处理器）

别把它当单一根因：194 个降级文件里首枚 ERROR 节点的落点约各半是指针构造与条件块，
但实测把全部 `#` 指令行原地替换成等长空白后，只有 7/194 恢复解析、反而多丢 6 个
方法名，可见条件块多是断裂的显示位置而非成因，其余落点无法归一。因此这里只登记
「语法覆盖不到」这个事实与兜底机制，不承诺枚举构造清单。

实测规模：garnet 全仓 1425 个 .cs，其中 194 个 `rootNode.hasError`，ERROR 节点共
7506 处。断点位置靠后的文件受影响最重，例如
`libs/server/Storage/Functions/MainStore/PrivateMethods.cs` 断裂前只提出 1 个名字、
`libs/server/Resp/Vector/VectorManager.Callbacks.cs` 提出 0 个（整文件对门禁隐形）。

门禁兜底：garnetScan 对 hasError 的文件再跑一遍词法声明提取（口径同第 4 节的词法
断言），把补回的方法名并入同一文件的名录，实测补回 438 个。该兜底只在 AST 确认
断裂的文件上生效，完好文件仍以 tree-sitter 为权威；提取到的只是方法名录，不用于
test/非 test 之外的任何语义推断（按文件路径的 test/benchmark 判据落桶）。兜底不
覆盖构造函数与属性，这与 tree-sitter 路径的既有口径一致。

判读注意：C# 语料降级不是语料失效。AST 侧的失效由语法能力边界造成、已由兜底补偿，
每次运行在 stderr 大声报出断裂文件数与补回名数，但不阻断判定；而 js/check/ignore
下 YAML 语料自身解析失败是本仓写坏的数据，会整体并入语料失效并硬失败退出，期间
不同步 miss、不做符号断言。运行 check.js 若看到 stderr 的降级汇报，那是长期存在
的背景噪声；只有语料失效红字才说明判定不可信。

登记在降级文件上的 ignore 条目，在修复前从不参与判定（名字根本不在语料里），
修复后会突然开始生效（实测 109 条）。撤换这类条目要逐条复核，别当成假绿直接清掉。


## 2. 怎么写文档注释

在 Rust 函数、方法或结构体上方添加三斜杠文档注释。

格式：

/// <C#相对路径.cs>:<C#方法名>

示例：

/// libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF
pub fn network_commitaof(...)

/// libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync
pub async fn recover_async(...)

注意要点：

路径可使用以 libs/、modules/、test/ 等开头的相对路径，无需带 garnet/ 前缀

冒号后必须精确匹配 C# 方法名（区分大小写）

同一个 C# 方法不要在多个 Rust 函数注释中重复声明，否则会触发重复定义警告

若测试代码中测试了对应逻辑，可标注到测试函数上方


## 3. 怎么配置 ignore

忽略规则文件存放在 js/check/ignore/ 目录下的各个 YAML 文件中。

配置结构分为两种：

1. 指定方法级别忽略

- 文件:
    - libs/server/Resp/AdminCommands.cs:
        - NetworkModuleLoad
        - NetworkRegisterCs
  理由: 不实现 C# 模块动态加载

2. 整文件级别忽略

- 文件:
    - modules/NoOpModule/NoOpModule.cs
  理由: 示例模块，无需移植

配置规范：

无关模块不要盲目配置 ignore

先检查 Rust 是否已有等价实现，若有则优先在 Rust 函数上方补齐文档注释

若确属平台专属特性、弃用命令或已驳回需求，按上述 YAML 格式在对应类别文件中添加条目并说明理由

若属于暂未实现的缺失项，记录到 task/issue/miss.md，避免用 ignore 掩盖未完成工作

## 4. 符号存在性断言（js/check/symbolCheck.js）

qcode.design 条 9 机制位：把条 6 的「路径存在性」升级为「符号存在性」断言，防止映射注释回潮。

口径：扫描全部 .rs 注释行中的 `路径.cs:符号` 锚点（复用 CS_REF_REGEX 与 csPathNormalize），

- 被引文件在 garnet/ 下不存在 → 路径失真
- 文件存在但符号不在文件文本中（\b 词法）→ 符号错挂（他文件有定义，附真实落点提示）或虚构符号（全树不存在）

分层：归一后 `libs/` 开头的锚点为 A 层，违规由 check.js 输出「# 虚构锚点」并 exit 1；

其余（test/ 族、截断路径、裸文件名）为 B 层存量口径外族，仅 stderr 提示计数，可单独运行

`bun js/check/symbolCheck.js` 查看全量清单。

豁免：叙述性假阳性（如 ReadCache.cs:DRAM 层级名、RespCommand.cs:OBJECT_ 前缀）或已立项在飞的

失真，登记 js/check/symbolignore.yml（锚点 + 理由），不复用 ignore 目录——ignore 语料默认只读：

「已在注释中出现」的可淘汰条目只作为建议清单打印，不删档、不回写；仅显式带 `--prune-ignore`

（或 `CHECK_PRUNE_IGNORE=1`）运行 `bun js/check.js` 时才执行整删 / 裁剪收口，语义相反；在途项落地后应撤销对应豁免。
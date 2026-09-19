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

实现缺失：未在 Rust 注释中引用且未配置 ignore 的 C# 方法（同时自动同步到 check/miss/ 目录）

符号存在性断言：解析注释中全部 `路径.cs:符号` 锚点，断言符号词法存在于被引 C# 文件内（见第 4 节；libs 族违规硬失败）


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

若属于暂未实现的缺失项，记录到 next/miss.md，避免用 ignore 掩盖未完成工作

## 4. 符号存在性断言（js/check/symbolCheck.js）

qcode.design 条 9 机制位：把条 6 的「路径存在性」升级为「符号存在性」断言，防止映射注释回潮。

口径：扫描全部 .rs 注释行中的 `路径.cs:符号` 锚点（复用 CS_REF_REGEX 与 csPathNormalize），

- 被引文件在 garnet/ 下不存在 → 路径失真
- 文件存在但符号不在文件文本中（\b 词法）→ 符号错挂（他文件有定义，附真实落点提示）或虚构符号（全树不存在）

分层：归一后 `libs/` 开头的锚点为 A 层，违规由 check.js 输出「# 虚构锚点」并 exit 1；

其余（test/ 族、截断路径、裸文件名）为 B 层存量口径外族，仅 stderr 提示计数，可单独运行

`bun js/check/symbolCheck.js` 查看全量清单。

豁免：叙述性假阳性（如 ReadCache.cs:DRAM 层级名、RespCommand.cs:OBJECT_ 前缀）或已立项在飞的

失真，登记 js/check/symbolignore.yml（锚点 + 理由），不复用 ignore 目录——ignore 语料会把

「已在注释中出现」的条目自动淘汰，语义相反；在途项落地后应撤销对应豁免。

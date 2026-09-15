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

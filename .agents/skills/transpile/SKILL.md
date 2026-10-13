---
name: transpile
description: garnet 转写 rust
---

把 ./garnet 的 c# 代码转写为 rust

技术选型参考 ./.agents/skills/rust_review/SKILL.md

尽量 1:1 对标 c#的代码实现，不要实现自己的优化（如果有，也撤销，尽量完全对标 c#，避免出现错误），除了以下几点

- 不需要向下兼容，不需要旧版数据迁移，直接删除相关代码
- 不实现  Azure Active Directory 微软内部的服务认证
- 不用实现加载 c#模块，删除动态注册管理层（CustomCommandManager、ModuleLoadContext、GarnetModule、load_module 等），模块与扩展命令（如 RoaringBitmap、JSON）采用编译期静态特性（Cargo feature，默认启用 default = ["roaring", ...]）与静态枚举分发（enum / match / enum_dispatch），杜绝运行时动态查表与加锁
- 前缀用 enum u8，而不是字符串，也别加冒号
- c# 的 Guid 统一用 u128，内部纯二进制不转字符串，落盘文件名转 base32
- 并发字典、set 用 papaya + gxhash （在 wedb/wbase/map.rs 中定义，用 map 或 set 特性启用）
- HashMap、HashSet 用 gxhash
- 编码尽量用 bitcode，而不是手写，不需要和 c#格式兼容
- 锁用 parking_lot
- hash 一律用 gxhash；种子策略：gxhash 默认（不加 deterministic）随机种子防碰撞 DoS，
  迭代序稳定由连接任务线程钉定承接；确定性哈希仅限 whasher 显式种子域（可落盘派生值）
- json 用 sonic_rs
- 取时间戳用 coarsetime
- simd 用 fearless_simd
- 配置文件用 nested_text 格式，删除对其他配置文件格式的支持
- 集合类型自适应混合分层存储架构（详见 [doc/zh/collection.md](../../../doc/zh/collection.md)）：
  - 小中规模集合（条目数 $\le 65536$ 且 体积 $\le 4\text{MB}$）采用 `wcol` 纯内存信封与 `OBJECT_DELTA` 增量日志；
  - 超大规模或大体积集合自动透明就地升阶为 `wbftree` 独立页级分层持久化（页级冷热换入换出由 B+ 树页缓存承担；存根句柄经 `RIPROMOTE`/`RIRESTORE` 原语保序；消除全量反序列化读放大，支持千万至上亿条目）；
  - 具备条目数（65536/32768）与内存体积（4MB/2MB）双维度双门限迟滞死区与懒降阶双向转换保护；对外 RESP 命令透明统一并支持删空自愈；
- 数据库隔离、Namespace 与 ACL 存储架构（详见 [doc/zh/db.md](../../../doc/zh/db.md)）：
  - 单日志多库共享：物理键前缀刚性隔离 `[NsVarint] + [DbVarint] + [KeyTag] + [Payload]`；彻底移除 `database_sessions` 数组，原生 `u64` 库 ID 纯寄存器标量更新，零堆分配；删除 `allow_multi_db` 与集群限制，全模式支持自由切库
  - 命名空间多租户打通：认证格式 `<ns>#用户名`（无 `#` 缺省为会话当前绑定的命名空间，未认证即 ns 0），认证成功自动绑定会话存储前缀；ns 0 具超管权限
  - ACL 数据库持久化与零全局内存：彻底废弃文件配置，以 `KeyTag::Acl (0x0D)` 存入底层存储；全局无用户大字典，认证按需点查，权限句柄连接本地持有并随连接析构释放，内存与用户总量彻底脱钩
  - 集群以 namespace -> db 为唯一分片：彻底废除 Key Hash 与 CROSSSLOT；同一个 DB 对应同一个槽位（物理共处同一机器节点），同一个 Namespace 的不同 DB 可以是不同槽位，实现多库分布式负载均衡
  - 虚拟数据库 ID 映射与秒级清库：逻辑库映射至底层物理 virtual_db_id（papaya + ArcSwap 零锁查询）；冷租户与冷库 0 内存常驻按需加载，空闲自动析构释放；FLUSHDB / FLUSHALL 瞬间分配新虚拟 ID 替换映射（< 1 微秒），会话纪元保护杜绝脏写；FLUSHALL 通过 Cluster Bus 广播全网 Master 原子换号；新映射与旧 ID 待回收项原子批处理持久化（KeyTag::DbMeta 0x0E）；偏序 GC 屏障（先摘除哈希索引度过安全纪元，再由日志紧缩物理丢弃）与高低水位熔断；从库回放换号条目（FlushDb/FlushNs）异步投递本地 GC 队列；重启自动恢复映射表与未完成 GC 队列
- 性能优化与零拷贝工程准则：
  - 读路径借用零拷贝：统一提供 `*_with` / `*_callback` 闭包只读 API，暴露 `&[u8]` 切片视图，消除点查 `Vec<u8>` 堆内存分配
  - 循环前缀外提（Prefix Hoisting）：批量遍历中，单次获取并外提 `session_prefix()`，经 `*_with_prefix` 系列消除逐字段重复读取原子变量与重算 Varint
  - 批量接口（Batching API）单次折叠机制：
    - 单次获取条带写锁与进入纪元，避免循环反复争用
    - 树内写入先做栈上局部排序，批量集中命中单页，压降页分裂
    - 单次维护 meta.size 增量回写，消除 N 次元数据写放大
- 严格删空生命周期与原子墓碑：
  - 删空自愈（Strict Empty Deletion）：元素计数减至 0 时原子写元记录墓碑、清理随键 TTL、排空在途写者并释放底层树文件，杜绝幽灵空元记录与孤儿 TTL
  - O(1) 墓碑逻辑秒删：DEL key 写物理墓碑（WATCH 版本栅栏由 wtxn watch_version_map 单点推进，对标 C# watchVersionMap.IncrementVersion），物理垃圾交由后台 Compaction 异步回收
- O(1) 复杂度计数规约：
  - HLEN / SCARD / ZCARD / LLEN：直读 wcol 内存对象计数 O(1)
  - RI.COUNT（本仓自定义扩展，别名 RI.LEN 在解析期归一到同一 RespCommand::Ricount）：全区间短路直读 MetaValue.size，严禁扫树或全量迭代
  - 非全区间计数不设第二个计数命令，由 RI.SCAN / RI.RANGE 的 FIELDS KEY 纯键投影（ScanReturnField::Key 真实区间迭代）承担；区间计数严禁误用 MetaValue.size（会返回错值）
- 类型枚举：
  - wedb/wval::GarnetObjectType（Null=0, SortedSet=1, List=2, Hash=3, Set=4, RangeIndex=5, All=0xfb）
  - 严禁散落裸 const u8 常量，全链路一处定义


运行时用 compio （一个线程一个 cpu）
消息队列用 crossfire
lua 用 luau
只能使用 cargo add 添加依赖，禁改 Cargo.toml

让子代理通过运行 `./fork.sh <分支名>` 一键在 `/tmp/fork/<分支名>` 创建 worktree 分支。在分支中优化，写完、测试后合并到主目录，删除分支。

运行 `./js/check.js` 可以看到缺失实现或者文档注释的 c# 函数，还可以看到重复定义的 c#函数

对于重复定义的函数，思考如何去重，删除重复代码，一处定义，对标 c#，简洁优雅的实现（禁止简单的通过修改注释绕过检查）

数据格式别搞多格式，对标 c#，但优先用 rust 的方式。比如，能用 bitcode，就别用 json，更别搞双格式

可以查看 `js/check/miss` 下面的文件，明确还缺少哪些函数和测试，并在 rust 相关的包中实现（或在 rust 相关的函数添加文档注释）

在 rust 函数文档注释中写清楚和 c# 的映射关系，格式是如下：

/// 在 garnet 中的相对路径:函数名

如果缺少相关的包，也可以用各个模块的 `./sh/new.sh` 创建新的 crate，合理规划模块，低耦合，高内聚

如某函数或文件无需在 rust 中实现，或经重构移除，必须在 `js/check/ignore/garnet下面相对路径.yml` 中配置相关的 ignore（文件/函数名: 为什么无需实现），使 `check.js` 忽略检查

严禁在代码中写占位函数或虚设实现，我们不是为了跑通检查，我们是为了更好的实现

让子代理每次都先对照 garnet c# 代码审查 rust 的代码架构、模块依赖，思考如何让其结构更加合理，可以拆分、修订，让其拓扑和 c#更加吻合

缺失的函数，不单单是要实现函数本身，更要打通上下游的调用链路，杜绝写死函数，杜绝重复定义函数

区分单元测试和集成测试，集成测试要放到 crate 的 tests 文件夹。测试对标 c#，清理 c#没有的测试（ai 写了很多废话测试，删除）

如果遇到主分支修改，请提交，然后合并（注意更新 worktree，避免落后）。

实现后，必须严格审查代码（调用 rust_review 规范与子代理），并彻底清理所有旧代码，不需要向下兼容；js/check.js 要配置相关的 ignore

写完之后 ./clippy.sh 和 ./test.sh，确保没有警告(必须用 rust 的编程风格重构，禁写 allow)

子代理开发，要效率最大化，分析拓扑，并发启动

开发与审查流水线重叠（一边审查上一层 crate，一边开发下一层 crate）

不断循环，开新子代理 code review ，运行 check.js，直到 check.js 没有缺失的输出，直到连续三次子代理认为完备完整的实现了 garnet 的代码，并且实现达到了生产级别

全程自主完成决策，禁止请求人工确认
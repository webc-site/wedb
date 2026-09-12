---
name: transpile
description: garnet 转写 rust
---

把 ./garnet 的 c# 代码转写为 rust

采用微模块结构，所有包均位于 `./wedb` 目录下：

- 存储引擎层：wbase, whlog, wram, wreviv, windex, wbftree, wkv, wcol, wval, wcpr, wcompact, wepoch, whasher, wrecord, wdev
- 服务器与集群层：wresp, wconn, waof, wacl, wlua, wedb_standalone, whost, wedb, ext_*

技术选型参考 ./.agents/skills/rust_review/SKILL.md

尽量 1:1 对标 c#的代码实现，不要实现自己的优化（如果有，也撤销，尽量完全对标 c#，避免出现错误），除了以下几点

- 不实现  Azure Active Directory 微软内部的服务认证
- 前缀用 enum u8，而不是字符串，也别加冒号
- c# 的 Guid 统一用 u128，内部纯二进制不转字符串，落盘文件名转 base32
- 并发字典、set 用 papaya + gxhash （在 wedb/wbase/map.rs 中定义，用 map 或 set 特性启用）
- HashMap、HashSet 用 gxhash
- 编码尽量用 bitcode，而不是手写，不需要和 c#格式兼容
- 锁用 parking_lot
- hash 一律用 gxhash
- json 用 sonic_rs
- 取时间戳用 coarsetime
- simd 用 fearless_simd
- 配置文件用 nested_text 格式，删除对其他配置文件格式的支持
- 集合类型，按大小拆分、类型不同的数据结构：
  - 离散点查型（Hash）：
    - 小集合（<=32768 项且 <=1MB）：Compact 紧凑二进制内联存入 whlog
    - 大集合（>32768 项或 >1MB）：采用 wkv whlog 打平存储（Flattened SubKey），点查 <50ns 保持 O(1)，版本号栅栏实现 O(1) 级联失效
    - 50% 迟滞防震荡参数：
      - 升级门限：`HASH_UPGRADE_ITEM_THRESHOLD = 32768` 或 `HASH_UPGRADE_BYTE_THRESHOLD = 1MB` (1048576 B)
      - 降级门限：`HASH_DOWNGRADE_ITEM_THRESHOLD = 16384` 且 `HASH_DOWNGRADE_BYTE_THRESHOLD = 512KB` (524288 B)
      - 紧凑条目最大开销：`COMPACT_HASH_ENTRY_MAX_OVERHEAD = 13` (2B flen + 2B vlen + 1B exp_flag + 8B exp_ticks)
      - 平滑迁移：`migrate_compact_to_flattened_hash` 过滤已过期字段（`now_ticks()` 门控），精准同步 `meta.size`
  - 严格有序型（ZSet / List / RangeIndex）：
    - 大集合采用独立持久化 wbftree，支持 O(log N + K) 物理保序扫描与单文件原子 unlink 销毁
    - ZSet：单树双前缀（0x01 分值序主索引 + 0x02 成员反查索引），IEEE 754 零分支保序位运算；全区间 ZCOUNT 短路 O(1) 直读
    - List：定长 51B `ListStub`（`RangeIndexStub: 35B` + `head: i64` + `tail: i64`）零堆分配，i64 符号位翻转单调大端编码，LLEN O(1) 算术差值求解
    - RangeIndex：对齐 C# `RangeIndexManager` / `RangeIndexStub`，单树单文件生命周期，提供 ri_set, ri_get_callback, ri_del, ri_exists, ri_len, ri_scan, ri_range
  - 去重型（Set）：
    - 采用独立 wbftree 或打平存储，单字节 4B 占位值 `SET_VAL_PLACEHOLDER`（`&[0x00; 4]`）满足底座 4B 最小记录约束，支持 sadd, srem, sismember, scard, sscan, smembers 及批量折叠算子 sadd_batch, srem_batch, smismember
- 二进制 Key 刚性防穿透与保序公理：
  - 定长刚性帧隔离公理（Fixed-Length Framing）：
    - 打平子键统一采用前 17B 定长帧：`[tag: 1B][key_id: 8B be][version: 8B be][payload]`，前 17B 长度刚性固定，载荷切片严格为 `S[17..]`，数学级杜绝前缀歧义穿透
    - 会话完整物理键：`[NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]`，单缓存行对齐，<=62B 全栈零堆分配
  - 单树多前缀物理隔离公理：
    - 全集合严格采用单字节 TreePrefix 枚举（`ZSetScore=0x01`, `ZSetMember=0x02`, `SetMember=0x03`, `ListIndex=0x04`, `RangeIndexKey=0x05`, `HashField=0x06`）作为键首字节，字典序物理分流，遍历回调首字节不等立即终止
  - 数值保序双射映射：
    - 浮点数：IEEE 754 符号翻转保序位运算（区分 -0.0 与 +0.0，拦截 NaN）
    - 整数：i64 符号位翻转映射到 u64 全域大端编码 `((index as u64) ^ (1 << 63)).to_be_bytes()`
  - 载荷与键严格分离，禁止非索引大载荷污染键
- 性能优化与零拷贝工程准则：
  - 读路径借用零拷贝：统一提供 `*_with` / `*_callback` 闭包只读 API，暴露 `&[u8]` 切片视图，消除点查 `Vec<u8>` 堆内存分配
  - 循环前缀外提（Prefix Hoisting）：批量遍历与全量打平迁移中，单次获取并外提 `session_prefix()`，经 `sub_key_with_prefix` 消除逐字段重复读取原子变量与重算 Varint
  - 批量接口（Batching API）单次折叠机制：
    - 单次获取条带写锁与进入纪元，避免循环反复争用
    - 树内写入先做栈上局部排序，批量集中命中单页，压降页分裂
    - 单次维护 meta.size 与版本回写，消除 N 次元数据写放大
- 严格删空生命周期与原子墓碑：
  - 删空自愈（Strict Empty Deletion）：元素计数减至 0 时原子写元记录墓碑、清理随键 TTL、排空在途写者并释放底层树文件，杜绝幽灵空元记录与孤儿 TTL
  - O(1) 版本号栅栏逻辑秒删：DEL key 时推进版本号 / 墓碑元记录，旧子键逻辑失效，物理垃圾交由后台 Compaction 异步回收
- O(1) 复杂度计数规约：
  - HLEN / SCARD / ZCARD / RI.LEN：直读主存 MetaValue.size，严禁扫树
  - LLEN：ListStub 算术差值 (tail - head) 求解，严禁扫树
  - 全区间 ZCOUNT / RI.COUNT：短路直读 MetaValue.size；非全区间传 ScanReturnField::Key 纯键扫描
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

可以查看 `check/miss` 下面的文件，明确还缺少哪些函数和测试，并在 rust 相关的包中实现（或在 rust 相关的函数添加文档注释）

在 rust 函数文档注释中写清楚和 c# 的映射关系，格式是如下：

/// 在 garnet 中的相对路径:函数名

如果缺少相关的包，也可以用各个模块的 `./sh/new.sh` 创建新的 crate，合理规划模块，低耦合，高内聚

如某函数无需在 rust 中实现，在 `js/check/ignore/garnet下面相对路径.yml` 中配置(函数名: 为什么无需实现)，这样 `check.js` 忽略

严禁在代码中写占位函数，我们不是为了跑通检查，我们是为了更好的实现

让子代理每次都先对照 garnet c# 代码审查 rust 的代码架构、模块依赖，思考如何让其结构更加合理，可以拆分、修订，让其拓扑和 c#更加吻合

缺失的函数，不单单是要实现函数本身，更要打通上下游的调用链路，杜绝写死函数，杜绝重复定义函数

区分单元测试和集成测试，集成测试要放到 crate 的 tests 文件夹。

如果遇到主分支修改，请提交，然后合并（注意更新 worktree，避免落后）。

写完之后 ./clippy.sh 和 ./test.sh，确保没有警告(必须用 rust 的编程风格重构，禁写 allow)

子代理开发，要效率最大化，分析拓扑，并发启动

开发与审查流水线重叠（一边审查上一层 crate，一边开发下一层 crate）

不断循环，开新子代理 code review ，运行 check.js，直到 check.js 没有缺失的输出，直到连续三次子代理认为完备完整的实现了 garnet 的代码，并且实现达到了生产级别

全程自主完成决策，禁止请求人工确认
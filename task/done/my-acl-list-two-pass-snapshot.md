优先级：低

问题
ACL LIST 与 ACL USERS 两遍扫描无快照，并发增删时应答数组头与实际元素数漂移破坏客户端协议解析。第一遍全 hlog 阻塞扫描仅计数并写数组长度，第二遍重新全扫描逐条写正文；两遍之间同命名空间并发 SETUSER/DELUSER 即长度与条数背离。代码注释自称「与 C# 侧同一非原子窗口口径一致」，实际 C# 是 GetUserHandles() 单快照先写 Count 再遍历同一快照，窗口仅 ConcurrentDictionary 弱一致枚举（微秒级）；rust 窗口是两次全日志阻塞扫描（大日志可达秒级），宽度远超 C#，等价性主张不成立。

取证（dev 当下代码重取）
wedb/wnode/src/resp/acl_commands.rs:112-155 network_acl_list（第一遍 :118-138 计数 + 写长度 :146，第二遍 :151 起重扫直写）；:151-155 注释自述非原子窗口与 C# 对齐。:178-227 network_acl_users 同型。扫描内核 wedb/wnode/src/resp/acl_store.rs:122-152 for_each_user（全 hlog scan + 链首地址校验去重）。

C# 对标
garnet/libs/server/Resp/ACLCommands.cs:54-77 NetworkAclList（:66 GetUserHandles() 取快照后 :67 写 Count、:70 遍历同一快照）；:83-106 NetworkAclUsers 同。

修法建议
第一遍在栈上或小 Vec 收集用户名快照（ACL 用户量级小，与零大字典设计不冲突），或单遍收集后统一整形输出，保证数组头与元素数强一致；default 兜底单例（in_memory_default）与解码失败关闭语义保持。来源 next/agy.my.md 条 14 与 next/muse.my.md 条 14 后半（前半 SETUSER 活连接传播已由 next/acl-setuser-live-connection-propagation.md 承接）合并处理。

---

核销 2026-09-19（dev 代理分支 acl-list-snapshot，取证基线 = 认领时主仓 dev 当下 HEAD 957e153；
落地后 dev = ecced18）。行号按符号重定位。

判词：成立，已修
1. 主张「两遍扫描无快照、数组头与元素数可漂移」——成立。改前 wedb/wnode/src/resp/acl_commands.rs
   :99-171 network_acl_list（第一遍 :112-138 只计数、:146 写数组长度，第二遍 :155-169 重扫直写正文），
   :178-228 network_acl_users 同型（:194-206 计数、:213 写长度、:218-226 重扫直写）。
   扫描内核 wedb/wnode/src/resp/acl_store.rs:122-152 for_each_user：每次调用现取
   `[store.begin_address(), store.tail_address())` 全 hlog 区间 + 索引链首地址校验去重，
   故两次调用之间是两份各自独立的可见集，条数可背离。
2. 主张「注释自称与 C# 同一非原子窗口口径一致」——成立。改前 :151-155 注释原文
   「……实际写出条数可偏离合符——与 C# 侧同一非原子窗口口径一致」。
3. 主张「C# 是 GetUserHandles() 单快照先写 Count 再遍历同一快照」——成立但措辞须精确化：
   garnet/libs/server/Resp/ACLCommands.cs:66 取 `GetUserHandles()`、:67 写
   `TryWriteArrayLength(userHandles.Count)`、:70-73 遍历同一 `userHandles`；而
   garnet/libs/server/ACL/AccessControlList.cs:118-121 的 `GetUserHandles()` 直接
   `return _userHandles;`（ConcurrentDictionary 本体，非拷贝）。即 C# 同样是
   「Count 与枚举之间可被并发改」的弱一致面，真正差别是**窗口宽度**：C# 一次取表、
   枚举走 ConcurrentDictionary 快照式枚举（微秒级），rust 是两次全日志阻塞扫描
   （日志越大越慢）。本票「单快照」按此更正为「一次取表、Count 与枚举同表」。
4. 复现取证（负控）：本票新增回归框在**旧两遍实现**上连跑三次全红，首框即
   `assertion left == right failed: LIST 符头与元素数背离 ... *201`（declared 201、
   实写条数不等）；换回新实现连跑四次全绿（每次约 60ms）。主张 1 由实测坐实，非仅静态推断。

落地
1. wedb/wnode/src/resp/acl_commands.rs:100-159 network_acl_list：单遍扫描内「解码即渲染」
   收成 `described: Vec<String>`（:116-140），:147 由该快照写数组长度，:154-156 正文
   取自同一快照——符头与元素数同源，扫描之后不再触存储。
2. 同文件 :165-208 network_acl_users：单遍收 `names: Vec<Vec<u8>>`（:179-190），
   :200 写符头、:205-207 正文同源。USERS 遍仍不触碰规则正文。
3. 语义保持位（未动）：in_memory_default_user 兜底单例 :86-92 与其居首位次
   （:141-145 default 记录在场即不兜底、:147-150 兜底项排在符头后第一条），
   解码失败关闭 :126-138（写符头**之前**只回一条错误帧，不留半截数组框）。
4. 输出形态与 C# 1:1、无新参数面：LIST 每条 `describe_user()` 的 bulk string、
   USERS 每条用户名的 bulk string；`check_arg_count!(..=0)` 与 C#
   `parseState.Count != 0` 拒绝路径不变；`for_each_user_blocking` 仍唯一扫描内核
   （acl_store.rs:161-166），只把调用次数由 2 降为 1。
5. 文档口径同步：acl_commands.rs:1-11 模块头、acl_store.rs:116-124（扫描区间即
   调用时刻日志尾、并发更新键的区间内旧版本因链首落选，等价 C# 弱一致枚举窗口）
   与 :157-160、doc/zh/db.md §3.4 ACL LIST/USERS 条目。
   C# 锚点 `libs/server/Resp/ACLCommands.cs:NetworkAclList` / `:NetworkAclUsers`
   逐枚保留，check.js 映射登记无增无减。
6. 新增回归框 wedb/wnode/tests/acl_tests.rs:795 acl_list_and_users_snapshot_frame_consistent_under_concurrent_mutation：
   200 位常驻用户把单遍扫描摊厚（旧口径即把两遍之间窗口拉宽）+ 写侧线程净增长抖动
   （每轮新增一名、每四名回收一名；等量写后即删两遍采到同值，测不出背离），
   读侧逐框断言 declared == items.len() 且 default 居首、无外命名空间用户入框。
   旧实现三次全红 / 新实现全绿（约 60ms）。

验收实测（本棒门禁，私有 target CARGO_TARGET_DIR=/tmp/ct-acllist）
- cargo check --tests -p wnode -p wacl：exit 0、零警告。
- cargo nextest run -p wnode --test acl_tests --test acl_namespace_admin_tests：24/24 通过。
- cargo nextest run -p wacl：31/31 通过。
- 负控（同框跑在改前两遍实现上）：3 次全红于 LIST 符头背离断言；正控：4 次全绿。
- rustfmt --check 三文件（acl_commands.rs / acl_store.rs / acl_tests.rs）干净。
- 未跑 ./test.sh、./sh/clippy.sh、bun js/check.js（按规程交主代理合并后统一跑）。
- 改动统计：4 files changed, 153 insertions(+), 57 deletions(-)
  （acl_commands.rs 75、acl_store.rs 14、acl_tests.rs 119、doc/zh/db.md 2）。
- 双花与射程：开工前 `git branch --list '*acl*'` 零命中、worktree 清单无同域树；
  合并前 dev 两次推进（2b6a4d0、5e7b5bc）均已 merge 入本分支后 FF，
  `git log 477474f..dev -- <四个 payload>` 零命中（dev 未同期动这四文件）。
  禁改清单（wkv/compact.rs、vdb.rs、wresp/cmd_strings.rs、tiered_collection_ops.rs、
  wconn/*、wlua/redis.rs、js/check/ignore/storage.yml）一律未触碰；射程外未发现新缺口。


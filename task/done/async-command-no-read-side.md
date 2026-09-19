ASYNC 命令只写会话布尔、全仓无读者：RESP3 下 ASYNC ON 回 +OK 却零效果，删掉这份装饰状态

来源：qcode 第 10 轮 net 视角审查条 6（原主张文件 next/qcode10.net.md 已清空删除，
裁决记录见 /Users/z/git/db/wedb/task/reject/qcode10.net.md 与
/Users/z/git/db/wedb/task/reject/qcode10-net-async-read-side-dup.md），本文是该条的唯一载体。
取证基线：主仓 /Users/z/git/db/wedb 当前 dev 工作树，行号按符号定位。

现状

写侧齐、读侧零。/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/mod.rs:750-779
`apply_async_param` 是 ASYNC 的唯一定义：RESP2 先回
`RESP_ERR_NOT_SUPPORTED_RESP2`（:756-758），RESP3 下 ON 置真（:766）、OFF 置假（:768）、
BARRIER 为空臂（:770，注释自述「C# 等待在途异步操作清零；rust 恒同步无在途操作，空等待」），
末了统一 `write_raw(output, cs::RESP_OK)`（:776）。字段声明在
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:260-261
（`pub use_async: bool`，注释「ASYNC 命令切换的异步处理模式（C# useAsync，NetworkASYNC 置位）」），
构造置 false 于 :464。全仓读点只有单测：
/Users/z/git/db/wedb/wedb/wnode/tests/resp_tests.rs:1897 与 :1903 断言该字段的值本身，
没有任何生产代码读它。GET 的执行面（/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/get.rs）
无异步旁路——同文件另一条真旁路 sg_batched_keys（:131 写、
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:512 读）恰好证明本仓的批量 deferred
通道是有读侧的，use_async 属没有读侧的那一类。

分派面：命令名注册于
/Users/z/git/db/wedb/wedb/wnode/src/resp/parser/command_table.rs:18
`("ASYNC", RespCommand::Async, false)`，枚举项
/Users/z/git/db/wedb/wedb/wresp/src/command.rs:258 `Async = 230`，慢路径前置分派在
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1620-1628
（注释「BARRIER 的批量等待由 compio 单线程模型天然串行化承接」）。

后果：RESP3 客户端执行 `ASYNC ON` 拿到 +OK，即按 Redis/Garnet 语义认为后续命令走异步完成
通道（应答可与命令序解批、由异步处理器回填），本仓实际仍逐命令同步出应答；客户端据此做的
流水线/异步编排假设与线上行为相反，而 ON/OFF 两臂的置位成了纯装饰状态，字段还以 pub 挂在会话上。
本仓对异步处理器面的既定口径是「不移植」：
/Users/z/git/db/wedb/js/check/ignore/server.yml:203 已把
libs/server/Resp/AsyncProcessor.cs 整文件登记为无需实现（理由段见同文件 :355-370）。
既然宿主机制刻意不实现，留着它的开关位与置位语义就是 SKILL.md「严禁在代码中写占位函数或
虚设实现」点名禁止的形态。

C# 参考（写读两端都在）

- garnet/libs/server/Resp/AsyncProcessor.cs:20 `bool useAsync = false;` 宿主字段声明，
  同文件 :25 `long asyncStarted = 0, asyncCompleted = 0;` 与 :29/:33/:38 的等待体原语。
- garnet/libs/server/Resp/BasicCommands.cs:70-71 读点：GET 入口
  `if (useAsync) return NetworkGETAsync(ref storageApi);`；:207 `NetworkGETAsync<TGarnetApi>` 实现体。
- garnet/libs/server/Resp/BasicCommands.cs:1716-1745 NetworkASYNC：:1731（ON）/:1735（OFF）置位，
  :1739-1760 BARRIER 有真实等待体（`while (asyncCompleted < asyncStarted) asyncDone.Wait();`）。
- 佐证 C# 无「异步不可用」错误常量：garnet/libs/server/Resp/CmdStrings.cs 无 ASYNC_REQUIRED 项，
  本仓该文案是 rust 自有（/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:264-265 已注明）。

修法

首选（与 ignore 登记口径一致，消假状态）：

1. 删 resp_server_session.rs:260-261 的 `pub use_async` 字段与 :464 构造行，
   删 basic_commands/mod.rs:766 / :768 两处置位。
2. ASYNC 命令臂保留在命令表与枚举（ RESP 面存在性与 C# 一致），但把 ON/OFF/BARRIER 三臂改为
   诚实回错：复用既有单点文案 `RESP_ERR_ASYNC_REQUIRED`（wresp/src/cmd_strings.rs:265，
   已是本仓「本命令需异步完成通道」的通用降级出口，慢路径十余处在用），不再回 +OK。
   RESP2 分支（回 RESP_ERR_NOT_SUPPORTED_RESP2）保持不动。
3. 单测随判据改写：resp_tests.rs 的 ASYNC RESP2/RESP3 用例段（:1880-1915，含 :1897 与 :1903
   两处字段值断言）改为断言三臂回 RESP_ERR_ASYNC_REQUIRED，并删去对已消失字段的断言，
   禁留恒真断言。
4. 文档注释写明本仓不实现 C# AsyncProcessor 面，并指回
   js/check/ignore/server.yml:203 的登记（该文件整体已登记，删字段不会新增 check.js 缺失项）。

反向方案（不推荐，成本与收益不对称）：按 C# :70 形态给 GET 补异步完成旁路
（asyncStarted/asyncCompleted 计数 + BARRIER 等在途清零），等于重开 AsyncProcessor 移植，
与既有 ignore 登记冲突，且需在 compio 单线程每连接一协程的模型下另造在途回收通道；
若将来确要做，另立功能缺口票并同步撤销该 ignore 条目，不在本票内夹带。

优先级

死代码（写侧齐、读侧零的假状态；附带一处面向客户端的行为欺骗）。

交叉引用

- 同文件会话族的巨峰拆分只管移动：task/ing/resp-server-session-file-split.md；
  本票是删字段 + 改回包，同文件开工需与纯移动票错开，禁在移动里夹带删改。
- 零消费面清理的批票与本票不同性质（那些是纯删无行为面），不并档：
  task/ing/zero-consumer-dead-surfaces-batch-five.md。

验收

- 全仓 grep `use_async` 归零（含 tests）。
- RESP3 下 `ASYNC ON|OFF|BARRIER` 回 `-ERR command requires asynchronous completion`，
  RESP2 下仍回 not supported in RESP2；`ASYNC` 未知参数仍回 syntax error（该臂不动）。
- js/check.js 无新增缺失/虚构锚点报告（AsyncProcessor.cs 登记已覆盖）。
- cargo check --workspace --all-targets 零 error 零 warning；test.sh/clippy 由中央整合轮执行。

落地记录

分支 async-no-read-side / async-no-read-side-redo，合入 dev 7a82b35（代码提交 c45567e，
前序合入 69e9aca 见下回滚条）。甄别复验全部成立，无裁剪、无 reject 追加。

按首选方案（与 ignore 登记口径一致）落地，未反向补异步旁路：

1. 删 resp_server_session.rs 的 `pub use_async: bool` 字段声明与其文档注释、删构造行
   `use_async: false`；删 basic_commands/mod.rs `apply_async_param` 的 ON/OFF 两处置位，
   并删 BARRIER 空臂（连带其「rust 恒同步无在途操作，空等待」注释）。
2. 三臂收敛为一条判定：参数属 ON/OFF/BARRIER 则回既有单点常量
   `cs::RESP_ERR_ASYNC_REQUIRED`，否则回 syntax error；删去恒执行的
   `write_raw(output, cs::RESP_OK)`。命令名注册（command_table.rs）与
   `RespCommand::Async` 枚举（wresp/src/command.rs）保留，RESP 面存在性与 C# 一致。
   RESP2 分支、unpack_args 参数数量校验原样不动。
3. 单测随判据改写：resp_tests.rs `async_command_resp2_and_resp3` 的 RESP3 段由
   「三臂 +OK 且断言字段值」改为循环断言三臂（含 on/Barrier 大小写形）回
   `err_frame(RESP_ERR_ASYNC_REQUIRED)`，删去两条已消失字段的断言，无恒真断言。
   另两处 ASYNC 用例（resp_tests.rs:AsyncTest1、resp_server_session_tests.rs
   async_resp2_reports_unsupported）只走 RESP2 路径，行为未变，不动。
4. 注释与文档：`apply_async_param` 文档注释写明本仓不移植 C# 异步处理器面并指回
   js/check/ignore/server.yml 的 AsyncProcessor 登记；同处修正分派面残留的
   「BARRIER 的批量等待由 compio 单线程模型天然串行化承接」失配注释（等待体已随字段删除，
   该句不再成立）。

验收读数

- 全仓 grep `use_async`（限定 wedb/ 下 *.rs，含 tests）归零。
- RESP3 三臂回 `-ERR command requires asynchronous completion\r\n`（常量经
  write_error_raw 加 `-` 前缀与 CRLF，与票内目标帧逐字一致）。
- cargo check --workspace --all-targets 零 error 零 warning（target
  /tmp/fork/async-no-read-side/target，合并 dev 后复跑）。
- js/check.js 未跑（本票门禁口径按 brief 限 cargo check），改以静态等价核验：
  用 check.js 自身的 rustScan.js CS_REF_REGEX 扫本票三个改动文件，确认
  `libs/server/Resp/BasicCommands.cs:NetworkASYNC` 锚点仍单点挂在 `network_async`；
  首版新注释曾在 `apply_async_param` 复写该锚点，会触发 dupDefFind 的「重复定义」
  （其键为 `cs_path + ":" + fn_name`，跨 rust 函数计数），已改叙述式指代消除，
  故无新增缺失/虚构/重复锚点。

一次回滚与复建（值得记入流程）

首次合入 69e9aca 后，dev 366015a「docs: mark papaya-random-seed-anti-dos done」这一
纯文档提交，在 pre-commit fixrs 的 `git add -u` 中把主工作树里本票三个 payload 文件的
过期副本（plumbing update-ref 只推 ref 不动工作树，遂成幽灵脏）暂存搭车进提交，
等于整体回滚本改动。且因 69e9aca 已是 dev 祖先，三方合并会把该回滚判为「对侧更新」，
再并本分支必失效——复建只能走差分形态：以 cherry-pick -n 在本票两个代码提交上生成
c45567e，合入后立刻 `git checkout <new> -- <payload>` + `git add` 把工作树归一到 HEAD，
消掉幽灵脏。dev 侧并发邻居（7103bd8 的 incr strict_i64 改动）在复建中逐字保留。


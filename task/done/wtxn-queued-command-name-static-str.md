优先级：中（污染扩散：事务排队路径逐命令堆分配，源数据本就是 &'static）
来源：next/agy.db.md 条 3（仅采纳其「命令名 String → static str」半条；
「把 TxnProcApi / TxnWatchApi 搬出 transaction_manager.rs」半条判为不成立，理由见
task/reject/agy.db.md 条 3）。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
TxnQueuedCommandInfo.name 与 TxnProcHandle.name 两个字段是 String，而其两个唯一数据源
（resp_command_to_cs_name、wcustom::txn_proc_meta）本就返回 &'static str，于是 MULTI 排队
每条命令白付一次堆分配；改成 &'static str 即可，接口面与消费方零改动。

现状（主仓 HEAD 实测）
1. 字段：wtxn/src/txn_proc.rs:11 pub name: String（TxnQueuedCommandInfo，:9-21）、
   :26 pub name: String（TxnProcHandle，:24-30）。
2. 构造点一：wnode/src/resp/resp_server_session.rs:1560 fn txn_queued_command_info(cmd) ——
   :1563 let name = super::resp_commands_info_data::resp_command_to_cs_name(cmd)，
   :1583 写 name: name.to_string()。
   该 helper 定义在 wnode/src/resp/resp_commands_info_data.rs:24
   pub fn resp_command_to_cs_name(cmd: RespCommand) -> &'static str。
3. 构造点二：wnode/src/resp/txn_resp_commands.rs:456-462 get_custom_transaction_procedure ——
   源为 wcustom/src/txn_proc.rs:94 pub const fn txn_proc_meta(id: u8) -> Option<(&'static str, i32)>，
   取到后 :460 name: name.to_owned()。
4. 消费面唯一一处读名字：wnode/src/resp/txn_resp_commands.rs:281
   session.abort_wrong_num_args(&command_info.name)（元数错误回显，&String 位置传 &str 后自动
   deref，调用点无需改动）。
5. 测试构造点：wnode/tests/transaction_tests.rs:47、:88、:116、:150、:164（排队命令元数据）、
   :249（TxnProcHandle），字面量赋 name 字段。

C# 参考
1. libs/server/Resp/RespCommandsInfo.cs:116 private static SimpleRespCommandInfo[]
   SimpleRespCommandsInfo —— 命令元数据在进程启动期一次性预构建成静态数组
   （:164-165 按 RespCommand 序号填充），排队期按下标直取，无逐命令名字串分配。
2. libs/server/Transaction/TransactionManager.cs 的排队面持 RespCommand 枚举值 + 输入缓冲引用，
   命令名只在错误回显时从静态表读；libs/server/Custom/CustomTransactionProcedure.cs 的
   过程名同为编译期常量（本仓 wcustom 静态枚举分发即其转写形态）。

修法
1. wtxn/src/txn_proc.rs:11、:26 两处 String 改 &'static str（结构体随之可 Copy 不必加，
   保持现有 Clone/Debug 派生）。
2. wnode/src/resp/resp_server_session.rs:1583 去掉 .to_string()；
   wnode/src/resp/txn_resp_commands.rs:460 去掉 .to_owned()。
3. 测试面 5 处字面量赋值由 String 改 &'static 字面量（transaction_tests.rs:47/:88/:116/:150/:164/:249）。
4. 若 wtxn 侧需要跨 crate 暴露该名字，一律 &str 借用而非 to_string 复制；
   禁新增 OnceLock/Lazy 之类缓存层（C# 是静态表直读，无需额外结构）。

边界
1. 同文件 resp_server_session.rs:1564-1580 的 args_buf / args / key_specs 三次 Vec 分配是
   排队期另一处放大，属另一问题：本票不动，落地时勿顺手改（需先核 C# 排队期是否重扫 parseState
   还是持缓冲引用，判清后再立单条票）。
2. 与 next/wtxn-lock-stripe-count-parity.md（事务锁面粒度）不同面。
3. 与 task/ing/windex-private-2pl-engine-removal.md 无交集（后者只动 windex/wkv ttl）。

验收判据
1. grep 对 TxnQueuedCommandInfo 与 TxnProcHandle 的 name 字段定义处零 String 命中
   （即 wtxn/src/txn_proc.rs 内不再有 name: String）。
2. wnode/src/resp/resp_server_session.rs 的 txn_queued_command_info 与
   wnode/src/resp/txn_resp_commands.rs 的 get_custom_transaction_procedure 体内
   零 .to_string() / .to_owned()。
3. RespServerSession::abort_wrong_num_args 调用面（txn_resp_commands.rs:281）签名兼容不变，
   错误回显文案逐字不变（判据：现有 transaction_tests 断言的错误串字面不变）。
4. cargo check 通过（禁跑 test.sh / clippy.sh，由主代理合并后统一跑）。

双花登记
并发代理就条 3 另立薄票 next/db-txn-proc-api-out-of-core.md，它把「TxnProcApi 搬出
transaction_manager.rs」与本条的 String→&'static str 合为一票。搬迁半条已判不成立并归档
task/reject/agy.db.md 条 4（该 trait 是事务门面能力面，搬迁只是文件搬运、无收益且拆散
js/check.js 的 File.cs:Fn 锚点）；本票只保留堆分配半条。若对方薄票仍被派发，须先剔除搬迁段，
两票禁同棒双花。

## dev 落地核销（本棒，分支 wtxn-static-str）

判词：成立，开工。步骤 0 甄别在 HEAD 656e2c4 逐条复核通过（回合基点已推进至 5e7b5bc，
本票四个 payload 文件在 656e2c4..5e7b5bc 区间零改动，票面行号除下述两处外原样命中）。

证据（现刻 HEAD 重取，行号按符号定位）
1. 字段确在：wtxn/src/txn_proc.rs:11、:26 `pub name: String`。落地后同为 :13、:28
   `pub name: &'static str`。
2. 构造点一：wnode/src/resp/resp_server_session.rs:1577 `fn txn_queued_command_info`
   （票面 :1560 已漂移，lua/custom-obj 两棒推后 17 行），:1579 取
   resp_command_to_cs_name，旧 :1601 `name: name.to_string()` 为 MULTI 逐命令那次分配。
3. 名字源面全静态：helper wnode/src/resp/resp_commands_info_data.rs:24 返回 &'static str，
   实现是 `cmd.into()`，即 wresp/src/command.rs:15 的 strum::IntoStaticStr derive
   （:18 serialize_all = "SCREAMING_SNAKE_CASE"）生成的编译期字面量表，零动态拼接路径，
   故本票无需按纪律保留 Box<str>/栈缓冲回退口。
4. 构造点二：wnode/src/resp/txn_resp_commands.rs:457-463，源
   wcustom/src/txn_proc.rs:94 `pub const fn txn_proc_meta(...) -> Option<(&'static str, i32)>`
   （:96-97 两个字面量），旧 :460 `name: name.to_owned()`。
5. 消费面确为唯一一处：txn_resp_commands.rs:281；错误帧由
   resp_server_session.rs:2330 `abort_wrong_num_args(&mut self, cmd_name: &str)` 转
   wresp::cmd_strings::abort_with_wrong_number_of_arguments 单点生成，形参本就是 &str，
   文案逐字不变。另核得 wtxn 域内除两字段定义外对 name 零读点（事务本体不消费名字），
   TxnProcHandle.name 在生产路径亦只由 RUNTXP 持 arity 校验、名字不外流。
6. 构造点全集：2 生产 + 6 测试（transaction_tests.rs:47/:88/:116/:150/:164/:249），
   Grep 工具复核 TxnQueuedCommandInfo|TxnProcHandle 全仓命中零遗漏；
   wtxn_test / wnode_test 均不构造该结构体（已随 check --tests 编译印证）。

复杂度与生命周期传染甄别（判拒条件未触发）
字段收口为 &'static str 后结构体不带生命周期参数，跨 crate 公共 API 面零改动：
TxnProcResolver::get_custom_transaction_procedure 仍返回 Option<TxnProcHandle>、
network_skip 仍收 Option<&TxnQueuedCommandInfo>，未出现泛型生命周期参数扩散，
Clone/Debug 派生保持（未顺手加 Copy，遵票面）。

C# 对照（本票成立的关键反证）
- garnet/libs/server/Resp/RespCommandInfoSimplifiedStructs.cs:15-35：排队期用的
  SimpleRespCommandInfo 结构体根本不持命令名字段，只有 Arity/AllowedInTxn/IsParent/IsSubCommand。
- garnet/libs/server/Resp/RespCommandsInfo.cs:116 静态数组 + :388-401
  TryGetSimpleRespCommandInfo 按 `(ushort)cmd` 下标直取，名字错误回显时才读表。
- 即：旧 String 是转写时把 C# 静态表里的名字逐命令复制一份的产物，C# 从无此分配。

改动统计与提交
- 1dade58 refactor(wtxn)：类型收口 + 两构造点去 to_string/to_owned + 消费点直传 &str
  （3 文件 +9/−7；旧 `&command_info.name` 收口后是 &&str，改为直传消除双重引用，
  避免主代理 clippy 轮次吃 needless_borrow）。
- adc854d test(wnode)：测试面 6 处字面量去 `.into()`（1 文件 +6/−6）。
- 合计 4 文件 +15/−13。边界守住：resp_server_session.rs 只动 QueuedCommand 构造行，
  同函数 :1580-1595 的 args_buf/args/key_specs 三次 Vec 分配按票面边界未碰；
  未新增 OnceLock/Lazy 缓存层；未搬 transaction_manager.rs（双花薄票的搬迁半条零动作）。

验收实测（私有 CARGO_TARGET_DIR=/tmp/ct-wtnstr；未跑主仓 test.sh / sh/clippy.sh）
- cargo check --tests -p wtxn -p wnode -p wcustom：exit 0 零告警（冷 target 1m02s）。
- cargo check --workspace --tests：exit 0。
- cargo nextest run -p wtxn：35/35 通过；-p wcustom：14/14；
  -p wnode --test transaction_tests：8/8 通过（含错误串断言，文案字面不变）。
  其中 txn_lock_table::distinct_buckets_do_not_conflict 被 nextest 判 LEAK
  （锁面遗留线程分类，非失败），先于本棒存在、与本票无关。
- 合并 dev（5e7b5bc）后复跑：workspace check exit 0、wtxn nextest 35/35。
- 判据 1 复核：HEAD 内 `grep -c "name: String" wedb/wtxn/src/txn_proc.rs` = 0；
  判据 2：两构造体体内 to_string()/to_owned() 命中 0。

落地：分支 wtxn-static-str，主仓 dev FF 合入 59511ed（dev 由 5e7b5bc → 59511ed，
纯 FF 无 merge commit），test.sh / clippy / check.js 留主代理门禁统一复验。
双花复述：开工前 `git branch --list '*wtxn*' '*static*'` 零命中、worktree list 无同题树。

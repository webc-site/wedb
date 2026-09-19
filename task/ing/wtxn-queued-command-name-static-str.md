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
   :1563 取 let name = resp_commands_to_cs_name…（实为
   super::resp_commands_info_data::resp_command_to_cs_name(cmd)），:1583 写 name: name.to_string()。
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

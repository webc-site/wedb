甄别结论：通过（主代理现码实证，2026-09-26）定级 P1

红用例闭环修复：分层树态 ZADD 奇数尾巴截断后会话指令流错位（test.sh 门禁 4002/4003 唯一红）

复跑实证（主代理亲验双侧）：
1 ./test.sh 全量：Summary 4003/4743 run: 1 failed；FAIL wnode::tiered_cmds_align test_tiered_zadd_odd_tail_token_truncated，tiered_cmds_align.rs:836 断言 Ping 期望 +PONG，left 实收 -ERR unknown command\r\n（20 字节错误帧）
2 同 binary 其余 14 测全绿（含 test_tiered_zadd_signed_zero_parity、test_tiered_zadd_opts_zrange_geo 等 ZADD 常规形），内存态 twin zadd_odd_tail_token_truncated_defensive（resp_sorted_set.rs）单跑绿——缺陷仅现于分层升阶树臂截断路径，非指令表/解析器通用面
3 引入面定位：外席 wcol-zadd-odd-tail-token-out-of-bounds 票合入 14f6e7a（fix-zaddtail，三处守卫+双态锁测）与其前 dev 提交交互；前序断言（ZADD :1、ZCARD :5）通过而末拍 Ping 收到 unknown command，形态符合「截断臂提前 return 未消费/未回灌完整参数窗剩余 token，会话输入缓冲残字节与下一请求帧粘连」或「错误应答帧多余半帧错位」两类，须执行席现码断案

执行方案：
1 断案先行：在 tree 态复现最小序列（升阶门限夹具→ZADD k 1 m 5→逐拍 dump 会话输入/输出游标），锁定是消费缺失（残 token "5" 滞留）还是应答多帧；对照内存态臂（绿形）消费与应答两侧逐位差
2 修复对标：C# 该形系对象层主循环越界 UB（deviations §151 在册），rust 防御截断语义保留不动；修其臂内参数窗消费闭环（消费全部入参 token、单帧 :1 应答、零残字节），与内存态臂收敛同一套截断-消费机制，禁双份实现
3 锁测保持 test_tiered_zadd_odd_tail_token_truncated 与 twin 双态同断言不放宽；补残字节探针断言（截断拍后连续两拍常规命令零错位）

验证：cargo check --workspace --all-targets 零警；定向 cargo test -p wnode --test tiered_cmds_align 与 --test resp_sorted_set 全绿；合并后主代理 ./test.sh 全量回归该用例转绿

合入哈希：49c022d（dev 快进至 04b3a4b） 收口形态：断案证伪两错位假说——树臂截断参数窗消费与应答帧双侧实测零错位，红因系末拍 Ping 误走存储 exec 分派漏斗（@fast 会话侧命令恒落 unknown 兜底）；锁测全拍收敛到与内存信封锁同套 roundtrip 线协议机制，补流水线双拍零残余探针，产品码零改动、§151 语义与双态断言不放宽；tiered_cmds_align 15/15、resp_sorted_set 47/47、cargo check --workspace --all-targets 零警。

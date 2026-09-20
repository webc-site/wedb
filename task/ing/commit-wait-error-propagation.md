# wait 模式刷盘失败错误传播 + 检查点失败重试环副本丢窗(混沌剧本1c/1d)

来源:next/zcode-r9-chaos.md 剧本1c/1d(已随本票认领从 next 移除)。

## 问题 1c(P0):wait 模式刷盘失败仍回 +OK

链:设备 ENOSPC → flush_and_sync_range Err → 提交水位不动 →
wait_for_commit_async(wnode/src/aof/waof_sublog.rs 约 :418-422)仅 log::error
吞错返回 → drive.rs(约 :222-228 与 :338 推送帧臂)注明「等待结果一律弃用…照常
发出应答」→ +OK 照发。--aof-commit-wait 下磁盘满+崩溃 = 已确认写丢失窗口。

C# 对位:TsavoriteLog.cs:2776-2789 提交失败设 cannedException 并
TrySetException,:1866-1882 WaitForCommitAsync await 即重抛,沿 Send 路径使
会话故障,客户端得不到 +OK。rust 注释「与 C# 同口径」只对齐了「丢弃返回值」,
漏掉异常传播臂。

修法:wait 模式提交等待面区分成功/失败——失败时错误应答或断会话
(对齐 C# 重抛语义;非 wait 模式的 fire-and-forget 面不动,其自愈由周期
提交重试承接)。waof 提交管线需把失败态暴露给等待者(group_commit 广播
Broken 已有,核查等待者侧是否可读失败原因)。

## 问题 1d(P0):磁盘满 × 检查点失败重试 → 副本模糊区反复丢弃

链:take_database_checkpoint_async(约 :280-345)先 set_current_version+
写 CheckpointStartCommit 再 create_checkpoint_with_token;磁盘满 create 失败
上抛,End 标记永不写,current_version 不回滚;spawn_aof_size_limit_task
单次失败仅日志不退出循环(rust 刻意差异,C# catch 在 while 外任务即死)→
周期失败环;副本侧 record_gate 模糊区内新代条目只入缓冲不应用,下轮 Start
全弃——磁盘满持续 N 轮 = 副本丢 N 窗写入,条目已消费永不重放,直到全量重同步。

C# 对位:AofProcessor.cs:276 同形丢弃,但 C# 任务死后只留一个悬挂模糊区,
不反复丢弃——副本数据损失组合为 rust 引入。

修法(二选一):
1. 检查点失败后回退版本并补发 CheckpointEndCommit(或等效终态帧),
   使副本模糊区闭合;
2. 副本对悬挂模糊区超时(如 N 秒无 End)强制请求全量重同步。
两案任一,勿两挂;证据入回报。

## 纪律

- 1c 只动 wait 等待面与应答分派,不动非 wait 主路径;
- 1d 只动检查点失败臂与副本模糊区守卫,不动 create 正常路径;
- 禁止顺手扩面;不需要向下兼容。

## 验收

1. cargo check -p waof -p wnode -p wedb 零 error 零 warning。
2. 1c:故障注入设备失败 → wait 模式客户端收错误帧/断连而非 +OK(定向测试)。
3. 1d:检查点失败注入 → 副本不丢窗(模糊区闭合或触发全量,定向测试)。
   夹具不足则回报说明。

## 门禁

只跑 cargo check(-p 收窄)与定向测试。严禁 ./test.sh 与 ./sh/clippy.sh。

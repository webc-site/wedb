# wait 模式刷盘失败错误传播 + 检查点失败重试环副本丢窗(混沌剧本1c/1d)

甄别结论:1c 接受,1d 拒绝。

## 1c(P0) 接受:wait 模式刷盘失败仍回 +OK

rust 现状(错误被吞、恒 Ok 链):
- wedb/wnode/src/aof/waof_sublog.rs:418 wait_for_commit_async 仅 log::error 吞 WalLog::wait_for_commit 的 Err
- wedb/wnode/src/aof/garnet_log/commit.rs:149 wait_for_commit_async / :174 wait_for_commit_all_async 无返回值
- wedb/wnode/src/database/single_database_manager.rs:319 wait_for_commit_to_aof_async 恒 Ok(true)
- wedb/wnode/src/service.rs:1826 StoreWrapper face 把 Result 压成 bool
- wedb/wnode/src/traits.rs:184 SessionProviderFace::wait_for_commit_async 默认 Output=bool
- wedb/wnode/src/net/handler/drive.rs:221-225 与 :349-353 两处等待点弃用结果照发应答;
  注释「等待结果如 C# 一律弃用」是对 C# 的误读——C# 弃用的只是返回 bool(false=跳过),异常臂从不弃用

C# 对位(异常传播臂完整):
- TsavoriteLogSettings.cs:198 TolerateDeviceFailure 默认 false,Garnet 未覆盖
- TsavoriteLog.cs:2768-2791 SerialCommitCallbackWorker:提交失败设 cannedException 并 commitTcs.TrySetException
- TsavoriteLog.cs:1866-1879 WaitForCommitAsync await 即重抛 CommitFailureException
- GarnetLog.cs:541-555 → SingleDatabaseManager.cs:240-243 → StoreWrapper.cs:504-510 无捕获穿透
- RespServerSession.cs:1453-1459 Send 内 BlockingWait 抛出 → 应答不发出
- RespServerSession.cs:566-572 catch (Exception) → networkSender.Dispose() 断连

方案(错误类型沿 C# 异常穿透路径原样上浮,单一机制;非 wait 主路径不动——
wait_for_aof_blocking 未置位不进等待):
1. waof_sublog.rs:418 签名改 waof::Result<()>,透传 Err,删吞错日志
2. commit.rs:149/:174 签名改 waof::Result<()>;分片聚合 join_all 跑完取第一个 Err
   (对齐 C# WhenAll:不取消兄弟、聚合后抛首个异常;沿用 :113 注释同款口径)
3. i_database_manager.rs:86 wait_for_commit_to_aof_async trait 签名改 waof::Result<bool>
   (使用方 single_database_manager.rs:319/:534 与 service.rs:1833,随失败域 waof 穿透,
   同构 C# 异常类型穿透)
4. traits.rs:184 默认实现改 async { Ok(false) },输出 waof::Result<bool>
5. service.rs:1826 无 AOF → Ok(false),等待 → map(|_| true)
6. drive.rs 两处等待点:Ok 照常发出(含推送臂);Err → log::error 留痕 + 丢弃本批应答 +
   break 断连(命令臂 break 'drive,推送臂 break ReadEnd::Cancelled 走断连收尾),
   对齐 C# 应答未发出即 Dispose;同步修正三处「一律弃用」注释
7. wedb/wnode/tests/net_pump_consume_tests.rs:588 测试提供者签名同步

## 1d 拒绝:磁盘满 × 检查点失败重试环副本反复丢窗

票面断言「C# 任务死后只留一个悬挂模糊区,不反复丢弃——副本数据损失组合为 rust 引入」不成立:
- C# 检查点异常根本到不了外层任务循环:SingleDatabaseManager.cs:203 调用的是
  DatabaseManagerBase.cs:185 TakeCheckpointAsync(protected 版),其 :200-205
  catch (Exception) 吞掉 InitiateCheckpointAsync 全部异常仅 log 返回 null;
  StoreWrapper.cs:648-668 AutoCheckpointBasedOnAofSizeLimitAsync 的 while 外 catch
  收不到检查点异常 → 周期任务不死 → 磁盘满周期重试环 C# 同样存在
- 副本侧 AofProcessor.cs:274-278 与 rust aof_processor.rs:438-449 同款:新 Start
  遇未闭合模糊区即丢弃缓冲(rust 注释自证同源);C# 失败轮同样 Start 已写、End 永不写
  (ReplicationManager.cs:311-349 标记由状态机触发,快照失败 End 不入 AOF)
- rust spawn_aof_size_limit_task 注释自称「刻意差异:C# catch 在 while 外任务即死」
  同为误读:外部可观测行为两侧一致(异常传达路径不同:rust 沿 Err 链到任务循环,
  C# 被更深一层吞掉,殊途同归)
- 票面两个修法(失败补发 CheckpointEndCommit/回退版本、副本悬挂模糊区超时全量重同步)
  均为 C# 无的自创机制,违反 transpile 总规范 1:1 对标纪律,不采纳

## 纪律

- 1c 只动 wait 等待面与应答分派,不动非 wait 主路径;
- 禁止顺手扩面;不需要向下兼容。

## 验收

1. cargo check -p waof -p wnode -p wedb 零 error 零 warning。
2. 1c:故障注入设备失败 → wait 模式客户端收不到 +OK、连接断开(定向测试)。

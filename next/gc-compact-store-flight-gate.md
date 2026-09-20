# 紧缩与换号物理回收内核单飞互斥门控

来源：next/zcode-r5-timers.md 问题 1

## 问题

常驻 bftree 回收任务与 GC 扫描循环各持一个独立 GcManager 实例，inflight 单飞闸是实例级的。
扫描在跑则让位判定读的是共享配置快照，而一轮回收含大量 await 点：
当 CONFIG SET 翻转 expired-key-deletion-scan-freq 恰落在回收任务紧缩中途时，两驱动可同时进入 try_compact/sweep_vdb，
而 wcompact 全链无互斥设施。
C# 周期 CompactionTask 与检查点驱动紧缩经 isFromCheckpoint 判定互斥，任一时刻至多单驱动。

## 涉及路径

- wedb/wkv/src/gc/reclaim.rs
- wedb/wkv/src/gc/compact.rs
- wedb/wkv/src/gc/mod.rs
- libs/server/Databases/DatabaseManagerBase.cs:DoCompactionAsync
- libs/server/StoreWrapper.cs:CompactionTaskAsync

## 解决建议

1. 将紧缩与物理回收内核收口为 store 级单飞闸（例如 WedbStore CAS 状态位，或让 reclaimer 与扫描循环共享同一 GcManager 实例）。
2. 保持替换重拉路径通过 JoinHandle fire-and-cancel 的已有取消语义不变。
3. 增加并发紧缩与回收的互斥单飞测试。

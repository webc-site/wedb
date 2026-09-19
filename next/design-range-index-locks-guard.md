优先级：中
来源：next/agy.design.md 条 15 与 next/muse.design.md 条 7（两轮同题合并）。
取证基线：主仓 dev 当下代码。

问题
范围索引锁面双入口管同一棵树：wbftree RangeIndexManager::locks() 直接暴露 StripedRwLock
裸引用供跨 crate 读写；wkv StoreSession::acquire_tree_write 是异步守卫封装。裸入口绕过守卫
的等待/纪元语义，同一资源的加锁纪律不唯一。

取证
- wedb/wbftree/src/manager/mod.rs:209 pub(crate) locks 字段、:378 pub fn locks()
  返回 &StripedRwLock<(), NUM_LOCK_STRIPES>
- 守卫封装：wedb/wkv/src/range_index/stub.rs:346 pub async fn acquire_tree_write
- 裸引用消费点（跨 crate 直接持锁，不经守卫）：
  wedb/wkv/src/range_index/stub.rs:168（mgr.locks().write）、:295（.read）、:355（.write）
  wedb/wkv/src/range_index/migration.rs:49、:84、:173（.write）
  wedb/wnode/src/rangeindex/range_index_manager_migration.rs:124（engine.locks().write）
- C# 对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs——锁经
  internal ref struct ReadRangeIndexLock（IDisposable 封装）与 AcquireExclusiveForDelete
  等命名方法暴露，协议注释明确「shared for data ops / exclusive for lifecycle ops，
  striped by key hash」；外部不直接摸锁数组

修法建议
locks() 收敛为 wbftree 内可见（或仅诊断面），跨 crate 写路径统一走 StoreSession 异步守卫或
wbftree 提供的命名锁方法（对标 AcquireExclusiveForDelete 形态）；七处裸消费点逐一改走
封装口。同步读锁消费可保留薄读口但须挂同一锁协议注释。禁双轨并存。

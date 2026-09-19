优先级：高（死代码：零生产消费入口 + 其独占的裸名工件族与恢复分支）
来源：next/agy.db.md 条 19。核销 2026-09-19，取证基线 = 主仓 /Users/z/git/db/wedb 分支 dev 当下 HEAD。

结论一句话
wbftree 的 on_flush 只是 on_flush_address 的 None 包装，生产永不走该臂，但它牵出一整条
「裸名刷盘工件」族（bare_flush_path 命名、截断覆盖写、恢复期裸名优先分支与目录扫描），
C# 的 SnapshotTreeForFlush 签名恒带 logicalAddress，无裸名形态。应删无地址入口并连带清除裸名工件族。

现状（主仓 HEAD 实测）
1. 双入口：wbftree/src/manager/flush.rs:13 pub fn on_flush（转 on_flush_internal(.., None)）、
   :18 pub fn on_flush_address（转 Some(logical_address)）、:27 fn on_flush_internal 以
   Option<u64> 分流 :41-44：Some → log_flush_path，None → bare_flush_path。
2. 生产消费点唯一：wkv/src/store/flush.rs:84 调 on_flush_address(user_key, stub, record_addr)，
   全仓 src 内 on_flush 裸名入口零调用者；仅测试在用
   （wbftree/tests/manager_and_stub/manager.rs:44、:234、:265，另 :94/:131/:568/:569 走 on_flush_address）。
3. 裸名工件族（随 None 臂一起失去来源）：wbftree/src/manager/mod.rs:452 pub fn bare_flush_path；
   消费方 flush.rs:43、lifecycle.rs:119（建树时删旧裸名件）、lifecycle.rs:149（惰性恢复取裸名件）；
   恢复契约与「裸名优先于地址扫描」的论证见 lifecycle.rs:152-161。
4. 文档牵连：wbftree/src/lib.rs:43、manager/mod.rs:192/:197/:310/:451、
   manager/lifecycle.rs:154-160 均以 on_flush / 裸名件为叙述前提。

C# 参考
1. libs/server/Resp/RangeIndex/RangeIndexManager.cs:681 internal void SnapshotTreeForFlush(
   ReadOnlySpan<byte> key, Span<byte> valueSpan, long logicalAddress) —— 逻辑地址是必填参数，
   刷盘文件名恒为 {prefix}.{addr:16}.flush.bftree（同文件 :901 EnumerateFlushFiles 的
   命名校验 FlushFileNameLength / AddrStartIndex / AddrHexLength 亦按带地址格式写死）。
2. 触发方 libs/server/Storage/Functions/GarnetRecordTriggers.cs:99
   rangeIndexManager.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan, logicalAddress)
   —— 与 wkv/src/store/flush.rs:84 同位，永远带地址。
3. C# 全仓无裸名 flush 文件形态（grep FlushSuffix 的产生点均带地址段），故裸名臂属 rust 侧多出来的
   第二套命名约定。

修法
1. 删 flush.rs:13 on_flush，on_flush_internal 的 logical_address 形参由 Option<u64> 收敛为 u64
   （或直接把体并入 on_flush_address，只留一个 pub 入口），并在其文档注释写明
   「1:1 对标 SnapshotTreeForFlush(key, valueSpan, logicalAddress)」。
2. 删 mod.rs:452 bare_flush_path 与 lifecycle.rs:119 的裸名清理、lifecycle.rs:149/:152-161 的
   裸名优先分支：恢复期一律走带地址扫描（addr_flush_scan_pending 门控与最大地址胜出逻辑保留）。
3. 测试改判：manager.rs:44/:234/:265 三处改为 on_flush_address 并断言带地址工件；
   若某用例的立论本身就是「无地址刷盘」，该用例属 C# 无对应的自研形态，删用例（spec：清理 C# 没有的测试）。
4. 同步订正 lib.rs:43、manager/mod.rs:192/:197/:310/:451、lifecycle.rs:140-161 的注释叙述，
   禁留「两种命名互斥、裸名优先」这类已失效契约。

边界
与 task/ing/wbftree-flush-file-enumerator-single-source.md 同文件族（replication.rs / lifecycle.rs
的刷盘文件枚举）。先落本票（少一处枚举点与一种命名），再做枚举器收敛，避免同一循环改两遍。

验收判据
1. 全仓 grep 对 RangeIndexManager::on_flush（词界，不含 on_flush_address）与
   RangeIndexManager::bare_flush_path 零命中；on_flush_address 为唯一刷盘入口且
   其调用面（wkv/src/store/flush.rs）签名不变。
2. wkv::RangeIndexStub 的 IsFlushed 判定与 wkv/src/store/flush.rs 刷盘链行为不变
   （带地址工件路径与 addr_flush_scan_pending 门控仍在）。
3. cargo check 通过（禁跑 test.sh / clippy.sh）；js/check.js 对 SnapshotTreeForFlush 的
   锚点仍单点挂在 on_flush_address 上。

双花登记
并发代理的同号薄票 next/db-bftree-on-flush-single-entry.md（自称并入 next/muse.db.md 条 11）已由
主仓 commit 0f7ce71 作「双载体薄壳」删除，载体统一为本票，故本票为该题唯一正文，无对手票待删。
判词一致留档：双方均判 on_flush（无地址包装）与 bare_flush_path 属自造变体，C# 只有带地址签名。

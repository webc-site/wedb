whlog 尾部地址恒等别名挂错位 C# 出处：safe_tail 系无对位物的第三套尾地址口径

来源：qcode10.db 条 4 立项（并按现 HEAD 修正其中两处取证）。取证基线：主仓 HEAD f974dd1f。

现状
- 恒等别名链：/Users/z/git/db/wedb/wedb/whlog/src/address.rs:122-128
  `pub fn safe_tail(&self) -> u64 { self.tail() }`（体即 tail()，:117-120 是 tail），
  再由 /Users/z/git/db/wedb/wedb/whlog/src/hlog/shift.rs:269-274
  `HybridLog::safe_tail_address` 转发一层。
- 出处错位：address.rs:122-124 的 doc 称「对标 C# TsavoriteLog.SafeTailAddress，
  在无独立在途槽位登记的混合日志模型下，已分配发布的 tail 即安全边界」，
  shift.rs:269 再写一遍「对标 C# TsavoriteLog.SafeTailAddress」。C# 的 SafeTailAddress
  属 TsavoriteLog（追加日志）侧缓存值，语义正是「可小于 TailAddress」：
  /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:104
  `public long SafeTailAddress => Volatile.Read(ref cachedSafeTailAddress);`，
  :102 与 :108 的 doc 以 `<see cref="RefreshSafeTailAddress"/>` 明示该缓存依在途状态重算；
  消费点 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:155、:196、:786-790、:920-924
  （均与 TailAddress 并列取值）。混合日志/分配器侧无该符号：
  grep `SafeTailAddress` 于 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 零命中。
  即「行为恒等于 tail」与「对标某物」互相打架，是把「无对位物」写成了「对标某物」。
- 本仓真正的 TsavoriteLog 对位面已另有活件且被真实消费：
  /Users/z/git/db/wedb/wedb/waof/src/wal/log.rs:79 `pub fn safe_tail_address`
  （消费 /Users/z/git/db/wedb/wedb/waof/src/wal/flush.rs:74、:147、
  /Users/z/git/db/wedb/wedb/waof/src/wal/iterator.rs:74、
  /Users/z/git/db/wedb/wedb/waof/src/wal/log.rs:584、
  /Users/z/git/db/wedb/wedb/wnode/src/aof/waof_sublog.rs:199）。
  whlog 侧再立一个同名恒等口，等于同一名字两套语义并存的第三套尾地址口径。
- 消费面：生产零引用，只剩两处测试断言它等于 tail_address——
  /Users/z/git/db/wedb/wedb/whlog/tests/hlog/append_scan.rs:64-65、
  /Users/z/git/db/wedb/wedb/wkv/tests/store/flush_evict.rs:401-402。

同单登记的两处测试专用面（按现 HEAD 修正原报事实）
- 原报「shift_addresses_with_wait 与 set_page_id 全仓含测试零引用」在现 HEAD 不再成立，
  实况为「生产零引用、仅测试在用」：
  /Users/z/git/db/wedb/wedb/whlog/src/hlog/shift.rs:332-355 的
  `shift_addresses_with_wait` 读者为 /Users/z/git/db/wedb/wedb/whlog/tests/hlog/flush_and_shift.rs:213、:339；
  /Users/z/git/db/wedb/wedb/whlog/src/buffer.rs:206-216 的 `set_page_id` 读者为
  /Users/z/git/db/wedb/wedb/whlog/tests/hlog/append_scan.rs:455。
- 两者仍属「有实现无生产消费」面，且前者的 C# 对位在源侧是活链：
  /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:1212
  ShiftAddressesWithWait → 唯一生产消费
  /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:171
  （上层 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:442）。
  rust 侧对应编排走自有链路（/Users/z/git/db/wedb/wedb/wkv/src/store/flush.rs），未接该口。

C# 参考
- 见上：TsavoriteLog.cs:102/:104/:108、TsavoriteLogScanIterator.cs:155/:196/:786-790/:920-924、
  AllocatorBase.cs（无 SafeTailAddress）、AllocatorBase.cs:1212、LogAccessor.cs:171、
  LogSizeTracker.cs:442。

修法
1. 删 whlog 两级恒等别名（address.rs:122-128 与 shift.rs:269-274），
   两处测试断言（append_scan.rs:64-65、flush_evict.rs:401-402）改直读 `tail_address()`；
   尾地址口径全仓只留 tail（whlog）与 safe_tail_address（waof 追加日志侧真对位面）两套，
   不再保留「注释声称对标、实则恒等」的第三套。
2. `shift_addresses_with_wait`：判定为 C# AllocatorBase.cs:1212 的对位需留，则把它接进真实
   调用点（与 wkv/src/store/flush.rs 的自有编排二选一，禁两套等待路径并存）；
   判定不留则删函数 + 删/改两处测试，并在
   /Users/z/git/db/wedb/js/check/ignore/storage.yml:2 既有的
   `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs` 块内登记不转写理由
   （该块已存在，五件套口径见 task/ing/gate-anchor-drift-reclean.md）。
3. `set_page_id`：生产写页号链路若确实自行维护 page_ids，则删该口并把
   append_scan.rs:455 的用例改走生产写路径；不得留「仅测试可写槽位」的公开后门。

优先级
死代码（三处生产零消费）+ 污染扩散（错位 C# 出处误导后续转写会话）。

交叉引用
- 零消费面普查批五：task/ing/zero-consumer-dead-surfaces-batch-five.md（whlog 三件属本单，
  该批不含 whlog，两面不重复立项）。
- ignore 登记机制与 AllocatorBase 五件套口径见 task/ing/gate-anchor-drift-reclean.md。

验收
- grep `safe_tail` 在 wedb/whlog 与 wedb/wkv 面零命中（waof 侧活件不受影响）。
- 混合日志侧注释中不再出现 TsavoriteLog 符号名。
- `cargo check --workspace --all-targets` 零 error 零 warning（私有 target 目录），
  含 tests 面。

落地记录（首落 dev af9d235，终态 dev 07d197b）
- 甄别：三条按现 HEAD 复核全部成立。safe_tail（address.rs:126 即 :119 tail 转发）与
  HybridLog::safe_tail_address（hlog/shift.rs:272）生产零引用；C# SafeTailAddress 确属
  追加日志侧 cachedSafeTailAddress 缓存（TsavoriteLog.cs:104，doc 明示依 RefreshSafeTailAddress
  在途重算可小于 TailAddress），Allocator/ 目录零命中；本仓追加日志真对位面 waof
  WalLog::safe_tail_address（wal/log.rs:79）活读者五处不动。shift_addresses_with_wait
  （hlog/shift.rs:334）与 set_page_id（buffer.rs:207）生产零引用仅测试在用，与档案
  32-38 行修正口径一致；C# ShiftAddressesWithWait（AllocatorBase.cs:1212）唯一生产消费
  LogAccessor.cs:171、上层 LogSizeTracker.cs:442，rust 无 SizeTracker 修剪编排对位
  （workspace 仅 wmetric 文案与 wresp 错误串提及），wkv 驱逐走 store/flush.rs
  flush_and_evict_all 自有链路，组合壳判不留。
- 修：删两级恒等别名与组合壳 set_page_id 后门（净差 -143/+8，7 文件）；append_scan.rs
  恒等断言删、尾推进断言与 wkv flush_evict.rs:401-402 改直读 tail_address()；溢出防御
  用例改走生产写页号链路 buffer.seal_page（append.rs:153 同源，清标不变式内置）；
  shift_addresses_with_wait 测试 17/20 整删；AllocatorBase.cs ignore 块（js/check/ignore/
  storage.yml）登记 ShiftAddressesWithWait 并扩写理由。wait_safe_head_drained 留（wkv
  session/raw/mod.rs:273 活消费）；shift_read_only_address_with_wait 属他面不随删（其
  C# 对位 Recovery.cs:535 另有活链）。
- 验收口径注：scan.rs:82「在途尾部与 C# SafeTailAddress 的对照」系如实陈述混合日志
  无提交协议、扫描终点取裸 tail 的行为分歧，属转写规范要求的对位说明而非谎称对标，
  保留；本条验收按「不再以 TsavoriteLog 符号自称对位」执行。
- 验证：`cargo check --workspace --all-targets` 于合并树 af9d235 零 error 零 warning；
  yml 经 yaml.safe_load 解析复核 232 块完好、ShiftAddressesWithWait 入列且块理由可取。
- 重落：首落 af9d235 随后被陈旧工作树检查点 0bf0942（f06-incr-strict，以 af9d235 为父、
  7 文件整树回写旧内容）反向吞回；以 cherry-pick -m 1 于 dev 最新树重放净差后双父合入，
  终态 dev 07d197b，重放树组合再验 cargo check --workspace --all-targets 零 error 零
  warning（1m51s 全量），三条验收于 07d197b git grep 复核通过。

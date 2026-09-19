紧缩上界改用 SafeReadOnlyAddress：内核自保收回，撤「外放给调用方」的契约

来源：next/glm.db.md 条 4（该文件已清空删除）。逐条按主仓 HEAD 复核后判定成立且待做。

结论
C# 把「不得越过 SafeReadOnlyAddress」做在紧缩内核入口，属内核自保；rust 把同一校验放在
read_only（Unsafe RO）上，并把它写成对调用方的外放契约，但唯一生产调用方既没排空也没封印，
熔断档还显式把上界推到 read_only。模糊区 [safe_ro, ro) 内在途原位写真实存在（本仓 whlog
自己的文档就是这么论证的），于是紧缩可把旧值搬回尾部并 CAS 顶掉索引，原位新值随 begin 推进
被物理截断；对称时序下原位墓碑被架空，表现为丢更新与已删键复活。修法是把 C# 的内核自保收回：
入口硬校验/钳制换 safe_ro，gc 与 lazy 两处的上界计算同源改口径。

现状（主仓 HEAD 实测行号）

1. 内核校验：wedb/wcompact/src/compactor/mod.rs:174 `let read_only_addr = self.store.read_only_address();`、
   :175-179 `until_address > read_only_addr` 即拒；方法文档 :156 亦以 read_only_address 表述该校验。
   头注 :26-28 自认「未用 C# 的 SafeReadOnlyAddress……依赖调用方保证紧缩窗口内无与封印/驱逐并发的
   在途写入」——外放契约的出处即此。
2. 唯一生产调用方无排空动作：wedb/wkv/src/gc.rs:550 try_compact（:569 取 read_only、:587-589
   `until = read_only - seg*(max-n)`、:612 `store.compact(until, tier)`），全臂无 seal / 无
   在途写排空；熔断档 :581-584 令 n = max，until 恰等于 read_only，把窗口推到模糊区上界。
   包装链 wedb/wkv/src/compact.rs:250 compact → :265 compact_with_filter → 内核。
   第二处上界同源：wedb/wcompact/src/compactor/mod.rs:249 compact_lazy 于 :251 以 read_only 为界。
3. 竞态面在本仓自证：wedb/whlog/src/hlog/mod.rs:399-409 论证「无锁直读门槛为 read_only，覆盖
   模糊区 [safe_read_only, read_only)，原位更新者仅在页写锁内复验可变区后落笔，read_only 推进
   不取页锁」，:406-409 更明写「模糊区内仅存在复验于封区之前的在途原位写的值字节覆写……
   无锁读者可能观察到值字节的中间态」；原位写者侧 wedb/whlog/src/hlog/inplace.rs:208
   with_mutable_record 以 :215 / :229 两处 read_only 为唯一边界（不含 safe_ro）。
   紧缩读路径已单源并在该门槛下：冷读端口 wedb/wcompact/src/compactor/run.rs:273 与
   wedb/wcompact/src/compactor/probe.rs:77 一律转调 session.read_record_at（分派单口已收口，
   见 task/done/cold-read-dispatch-single-port.md），其无锁直读门槛即 read_only。
4. 复核拦不住：wedb/wcompact/src/compactor/run.rs:355 conditional_copy_to_tail 的 find_latest
   比对的是索引槽内地址，原位写不改槽位，故旧值副本 + CAS 成功后原位新值被架空，
   shift_begin_address 截断即物理丢失；对称时序下 wedb/whlog/src/hlog/inplace.rs:33
   try_set_tombstone_in_place 写入的墓碑被旧值副本顶掉，即已删键复活。
5. 换界件已在位：wedb/whlog/src/hlog/shift.rs:375 safe_read_only_address（hlog 侧）、
   wedb/wkv/src/store/addr.rs:91 safe_read_only_address（store 侧），推进点
   wedb/whlog/src/hlog/shift.rs:68、:230；紧缩器宿主 trait wedb/wcompact/src/host.rs:38
   CompactStore 现仅暴露 :55 read_only_address，补一个同源读口即可。

C# 参考

garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:35-36
  CompactLookup 入口 `if (untilAddress > hlogBase.SafeReadOnlyAddress) throw … "Can compact only until Log.SafeReadOnlyAddress"`
同文件 :72-73 CompactScan 同款硬校验（两处均为内核自保，不依赖调用方）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:114-124
  注释 "Mutable region (even fuzzy region is included here)"——模糊区记录按可变记录处理，
  紧缩不得越界读取，正是 C# 取 safe 界的理由

修法

1. wedb/wcompact/src/host.rs:38 CompactStore 增 `fn safe_read_only_address(&self) -> u64;`，
   WedbStore 侧一行转调既有 wedb/wkv/src/store/addr.rs:91，不新造地址源。
2. 内核入口校验臂（mod.rs:174-179）改以 safe_ro 为界并保留硬拒（形态与错误档文案对齐 C# 的
   "Can compact only until Log.SafeReadOnlyAddress"），把 :26-28 的外放契约声明撤除，:156 方法
   文档同步改口径；不采用「静默钳制到 safe_ro」以免与调用方的段数计算口径脱钩。
3. 上界计算同源换 safe_ro：wedb/wkv/src/gc.rs:569 的 read_only 改取 safe_read_only_address，
   :581-589 的 until/回退段与 :595-598 的 Shift 档一并随动（Shift 档同样不得把 begin 推入
   模糊区，其「绝不外推到只读线」的注释按新界复核）；
   wedb/wcompact/src/compactor/mod.rs:251 compact_lazy 同改。
4. 节流判据核对：wedb/wkv/src/gc.rs:578 的积压段判定亦以 read_only 计算，换界须逐处显式决定
   （判「积压是否超阈值」可仍以 ro 度量，但推进上界必须 safe_ro），并把两者口径写进方法文档，
   杜绝一处 safe 一处 unsafe 的混用。
5. 回归用例：构造模糊区在途原位写与紧缩交错的确定性用例（原位写者持锁复验后由测试推进
   read_only 制造窗口），断言紧缩后读到的是原位新值而非搬回的旧值副本；同法覆盖墓碑臂不被
   架空。按 SKILL 归集成测试面（wkv/tests 或 wcompact/tests）。

优先级
功能缺口（丢更新与已删键复活，静默且不可自愈；根因是一条写在头注里的自保外放契约未被任何
调用方承接，属正确性口径分叉而非结构重复）。

边界
本单只改紧缩上界的取界与内核校验臂。冷读分派单口已落地（task/done/
cold-read-dispatch-single-port.md），本单不再动读路径；升降阶与驱逐判定面属票
tiered-promote-demote-key-ttl（task/ing 在册）与 obj-save-or-gc-promote-gate，
safe_ro 读数的观测接线属票 info-store-snapshot-channel，三者各自收口互不替代。

验收
1. 紧缩上界一律来自 safe_read_only_address，grep 紧缩链上以 read_only_address 作上界的判据
   不再存在（度量类用法除 :578 一处外应清零，并在文档注明其非上界职责）。
2. 新增用例在人为放大的模糊区窗口下仍守恒：原位新值与原位墓碑不被旧值副本顶掉。
3. 不引入排空/封印等额外机制——收口只做在界的选择上，与 C# 同形态。
4. clippy 无新增告警（禁写 allow）。

细化方案（实现代理 f31-compact-readonly，甄别后追加）

甄别结论：票据成立。C# 侧 TsavoriteCompaction.cs:35-36/:72-73 与
DatabaseManagerBase.cs 私有 DoCompactionAsync（驱动以 ReadOnlyAddress 度量与回退）均核实；
rust 侧 mod.rs:174-179 / gc.rs try_compact / compact_lazy:251 / host.rs:55 全对上。
当前 HEAD 实际行号与票面漂移：try_compact 落在 gc.rs:597-662，read_only 取值 :608，
阈值判定 :617，回退段 :622-625，Shift 档 :629-641。

改动清单

1. wedb/wcompact/src/host.rs：CompactStore 增 `fn safe_read_only_address(&self) -> u64;`
   （紧邻 read_only_address，文档注明 SafeReadOnlyAddress 对标与「紧缩上界唯一口径」职责）。
2. wedb/wkv/src/compact.rs：WedbStore 实现一行转调 `self.hlog.safe_read_only_address()`
   （与 addr.rs:91 同源，不新造地址源）；wcompact/tests/compact/support.rs FixtureStore
   同步补一行转调。
3. wedb/wcompact/src/compactor/mod.rs：
   - :26-28 头注「未用 C# 的 SafeReadOnlyAddress……依赖调用方保证」整段撤除，改为
     C# 同形的内核自保声明（对标 TsavoriteCompaction.cs:35,72）；
   - compact_with_filter 入口 :174-179 换 safe_ro 快照硬拒，Error::UntilAddressOutOfRange
     语义字段随界更名（until 超出的是 safe_ro），:156 方法文档口径同步；
   - 传入 compact_lookup/compact_scan 的边界参数即该 safe_ro 快照（Scan 阶段 2 上界
     同源换 safe_ro，对标 C# CompactScan:108 `scanUntil = hlogBase.SafeReadOnlyAddress`）；
   - compact_lazy :251-260 区间上界换 safe_ro，文档随动。
4. wedb/wcompact/src/compactor/run.rs：compact_lookup/compact_scan 形参 read_only_addr
   更名 safe_ro_addr（含 :98/:150/:153/:198/:233/:236/:239/:243 全部判据与注释）；
   :381 reviv_put 第三参保留 read_only_address（对标 C# Helpers.cs:107
     GetMinRevivifiableAddress(tail, ReadOnlyAddress)），注释注明「复活资格度量，非紧缩上界」。
5. wedb/wkv/src/gc.rs try_compact：read_only 单变量改 safe_ro 单源（阈值判定 + 回退段 +
   Shift 档全随动）；方法文档写明双口径：C# 驱动以 ReadOnlyAddress 度量、内核以
   SafeReadOnlyAddress 硬拒，此处单源取 safe_ro（= 可紧缩积压口径），safe_ro 单调不回退
   保证 until 恒过内核校验；Shift 档「绝不外推到只读线」注释改「安全只读线」。
6. 回归用例（wcompact/tests/compact/，fixture 已有 hlog+epoch+participant 全套件）：
   - 内核界用例：until > safe_ro 双模式均 UntilAddressOutOfRange；until == safe_ro 放行；
   - 值臂（确定性交错）：写者线程持纪元保护，经 try_modify_record_in_place 闭包（页写锁内
     双检后）驻留 → 主线 shift_read_only_address(tail) 制造模糊区（断言 safe_ro <= A < ro）→
     compact(until = safe_ro) 不触 A（索引仍指 A、begin < A）→ 释放写者落新值 → 读得新值；
   - 墓碑臂：写者持保护先原位墓碑（A >= ro 时落笔）→ 主线推 ro 制造模糊区 →
     compact(safe_ro) 不触 A → 读得 None（不复活）。
   依据：在途原位写者持纪元保护 ⇒ safe_ro 排空先于其落笔，until <= safe_ro 即永不截断/
   架空在途写；until > safe_ro 被内核硬拒 ⇒ 破坏性交错无法再经公共 API 构造。
7. 既有测试影响评估（不改动语义，仅换界）：fixture seal_read_only 与 wkv 测试的
   shift_read_only_address 均为单线程无保护窗口调用，bump_current_epoch_action 的
   help_drain 在注册线程内联排空（epoch.rs:533），safe_ro 同步达 until，既有
   compact(until <= ro) 用例不受影响；错误路径用例（tail+100 / begin+1 > safe_ro）
   仍触发 UntilAddressOutOfRange。

边界重申：不动 flush 面（evict_pages_for / FlushStep / shift_begin 补刷属
flush-safe-read-only-bound），不动读路径分派，不动升降阶判定。

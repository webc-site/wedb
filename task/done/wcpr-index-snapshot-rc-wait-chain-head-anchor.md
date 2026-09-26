甄别结论：通过（甄别席 zc-fix-r16-rcwait，2026-09-26）定级 P0
1 C# 侧锚逐条亲验成立：ReadCache.cs:101-115 ReadCacheNeedToWaitForEviction 判定走查当前位置 LatestLogicalAddress（:105 AbsoluteAddress），:119-155 SkipReadCache 每步先判定、命中滑出 SpinWaitUntilRecordIsClosed 后经 :111 UpdateRecordSourceToCurrentHashEntry 回链头，:159-180 SkipReadCacheBucket 无等待；IndexCheckpoint.cs:146-157 epoch.Resume 冻结窗内 SectorAlignedMemory 桶拷贝直走链。
2 rust 现状锚逐条亲验成立：window.rs:116-118 与 :138-140 中段滑出归 Gone，:201 prev_address_of 折 None，:212-220 skip_read_cache 于 :215 整体返 None，:158-168 need_to_wait_for_eviction 以入参地址 abs>=head 直返 false；batch.rs:175-188 resolve_slot 于 :181 以槽头 raw & ADDRESS_MASK 锚定等待、:182 return 0 归零——链中段滑出而槽头在窗形态穿透确证。
3 触发时序窗成立：append.rs:175 武装拍即推 head_address，清洗晚至 :282 close_pending_page 内 cleanse_page、:288 才推 closed_until；cleanse.rs:29-32 自证「被驱逐记录多在链深处」为常态。
4 危害与契约自违背成立：codec.rs:197-213 None 交消费侧、sanitize_data_slot(0)=0（:164）固化空槽；recover.rs 重插闸实为 :453（票面注 :457 偏移 4 行、同论断实质成立），主日志真身低于 index_start 的老键无重插无自愈；codec.rs:189-191 端口文档明定 None 绝不可归零落盘。
5 其余消费锚成立：cpr_host.rs:478-495 两端口、compact.rs:169、probe.rs:60-66 'restart 环等待后回链头不折 0、copy_to_tail.rs:140-152 无条件 restart 行为正确但忙重探。
6 查重成立：deviations.md 无驱逐等待锚定登记（命中仅 §69/§70/§111 内存尺寸族异面）；todo/ing/reject/issue 各池无同轴票；rc_eviction_ckpt/rc_tag/roundtrip/checkpoint_slot/concurrent_ckpt 在册，broken_rc_chain_slot_sanitized_on_roundtrip 亲验于 rc_tag.rs。
7 架构合规：单口折叠消除三处锚点分叉、复用既有 spin_wait 原语与纪元 refresh 闭包，符合单套机制单向分层无过度设计；补记：方案未列 wcpr/src/manager/mod.rs:117/128 trait 声明与 :257-262 无 RC 默认实现、tests/cpr/support.rs:431/443 测试宿主须随折叠同步改签名，改动面内可落不阻。

索引快照 ReadCache 驱逐等待锚定链头，链中段滑出形态被误判归零落盘，恢复后老键永久不可见

审核结论：通过（审核席 zcode-r17-review-rcwait，2026-09-26）

审核亲验记录：
1 缺陷真实性双侧确证。C# ReadCache.cs:101-115 ReadCacheNeedToWaitForEviction 判定对象为走查当前位置 stackCtx.recSrc.LatestLogicalAddress（:105 AbsoluteAddress），:119-155 SkipReadCache 每步先判定再读记录、命中滑出即 SpinWaitUntilRecordIsClosed 后 goto RestartChain 回链头（UpdateRecordSourceToCurrentHashEntry 重读哈希项）；快照面 SkipReadCacheBucket（:159-180）无等待，靠 IndexCheckpoint.cs:146-157 epoch.Resume 冻结 + SectorAlignedMemory 桶拷贝内直走链，纪元保护窗内清洗方无法推进。rust 侧瞬态窗亲验成立：append.rs:175 武装拍即推 head_address，清洗晚至纪元延迟动作 close_pending_page（append.rs:282）执行、:288 才推 closed_until，此间链中段记录经 page_view 判 Gone（window.rs:116-118 与 :138-140），prev_address_of 折 None（:201），skip_read_cache 整体返 None（:215）；batch.rs:181 等待端口入参锚 raw & ADDRESS_MASK 即槽位链头，need_to_wait_for_eviction（window.rs:163-165）以槽头在窗（abs >= head）直接返 false，resolve_slot 即 return 0 归零落盘。槽头自身滑出（位置 0 Gone）的形态现码恰好正确（锚点即当前位置），唯独链中段形态穿透——与诊断完全吻合。
2 危害链闭环确证。write_bucket 经 sanitize_data_slot(0)=0（codec.rs:164）固化空槽进快照；恢复内核 run_recovery_kernel 仅重插 addr >= index_start 记录（wcpr/src/manager/recover.rs:457），老键主日志真身低于 index_start 无重插；槽位归零即恢复后键不可见，无自愈通道。触发常态性成立：清洗时序窗横跨武装拍到纪元延迟动作收尾，cleanse.rs:29-32 自身已承认「被驱逐记录多在链深处」为常态形态。
3 契约自违背佐证，非对上游误读。codec.rs:189-191 端口文档明定 None 语义为「槽位属存活数据，绝不可归零落盘，由消费侧等待落定后重读槽位重探」，batch.rs 实现违反本仓自有端口契约。
4 查重通过。deviations.md 无驱逐等待锚定相关登记（§27 前缀短读、§29 守护任务、§81 旋钮拆分、§95 无盘键门均不同面），非重复提报。

优化执行方案（供 task/fix.md 消费）：
1 wkv 侧新增带等待走查单口 ReadCache::skip_read_cache_with_wait(head, refresh)：head 为链头重读闭包（返回槽位当前地址值，对齐 C# RestartChain 重读哈希项形态——清洗方以槽位 CAS 恢复主日志地址时，重走旧链头地址会命中已清零页误判链尽，故重读槽位为正确性必需，非锦上添花）。走查内核逐位判读：Unparsable（慢路径页读锁下定谳的真实零头/残损）按链尽返回 Some(0)；Gone（abs < head）以该中段地址就地调既有 spin_wait_until_record_is_closed（window.rs:177-187）等待清洗落定，随后经 head 闭包回链头重探，循环直至落定返回 Some。RC 未启用与非 RC 地址恒等透传零开销。refresh 闭包沿用各调用方既有口径（快照面 epoch.drain、会话面 participant.refresh）。
2 端口收敛：CprStore 的 skip_read_cache 与 wait_read_cache_eviction 两端口折叠为单口（宿主 cpr_host.rs:478-495 端口体同步替换），BatchWriter::resolve_slot 删除 None 分支链头锚定等待与 return 0 降级路径，快照只消费 Some；紧缩面同步切换单口（wkv/src/compact.rs:169 端口体与 wcompact/src/compactor/probe.rs:60-66 消费点，probe 内 'restart 环收敛为单口内循环）；订正 batch.rs:172-173「端口判非驱逐窗时按链尽归零」注释为新内核口径（链尾 Unparsable 由内核区分、Gone 就地等待重探）。同款两步形态的第三消费点 copy_to_tail（wkv/src/session/raw/write/copy_to_tail.rs:140-152，现无条件 restart 无折 0、行为正确但忙重探）一并切单口，消除全链路锚点分叉；注意其调用须保持在 enter_gated 短守卫之外（自旋不持守卫）。
3 测试验证点：wcpr tests/cpr 新增驱逐竞态用例（多代 RC 记录链 + tag 碰撞链构造，驱逐推进至链中段时并发触发索引快照，断言快照槽位非零且等于清洗后主日志地址、恢复后键可见）；回归 rc_eviction_ckpt / rc_tag / roundtrip / checkpoint_slot / concurrent_ckpt 套件，重点复核 rc_tag.rs broken_rc_chain_slot_sanitized_on_roundtrip 在新内核 Unparsable 链尽口径下断言语义不变；无 ReadCache 宿主恒等闭包路径零改动验证。

问题分析：
1. Garnet 契约对齐。C# 对带 ReadCache 指针的哈希槽在快照与走查两路径上的驱逐等待锚点均为「不可判读发生的那个记录地址」。快照面 SkipReadCacheBucket（ReadCache.cs:159-180）在 epoch.Resume() 保护窗内拷贝桶到 SectorAlignedMemory 副本后直走链换写地址（IndexCheckpoint.cs:146-159 的拷贝 + skipReadCache 循环），epoch 保护期间驱逐清洗方无法推进，链上记录不会中途滑出，结构上免等待。通用走查面 SkipReadCache（ReadCache.cs:119-155）每步先调 ReadCacheNeedToWaitForEviction（ReadCache.cs:101-115），判定对象是走查当前位置 stackCtx.recSrc.LatestLogicalAddress（:105 AbsoluteAddress(stackCtx.recSrc.LatestLogicalAddress)，即链上当前记录而非链头），命中滑出（logicalAddress < readcacheBase.HeadAddress）即 SpinWaitUntilRecordIsClosed 就地等待后 goto RestartChain 回链头重探。两条路径都不存在「以链头地址判定链中段滑出」的形态。
2. 工程现状确证。rust 侧 wedb/wcpr/src/index_ckpt/batch.rs:175-188 BatchWriter::resolve_slot 在 rc_skip 返回 None 后，等待端口入参锚定为 raw & HashBucketEntry::ADDRESS_MASK，即槽位首地址（链头）。而 wkv 宿主 ReadCache::skip_read_cache（wedb/wkv/src/read_cache/window.rs:212-220）的 None 可发生在链中段：prev_address_of（window.rs:194-203）对链上任意中段记录的页视图返回 Gone（window.rs:138-140，abs_addr < head_address 即该记录已滑出环形窗口）时整体返 None。ReadCache 链上 prev 地址严格递减、驱逐自最老侧推进，「中段记录已滑出而槽头仍在窗内」是驱逐进行中的常态形态。此时 need_to_wait_for_eviction（window.rs:158-168）以槽头地址判定，abs_addr >= self.head_address 成立（槽头在窗）直接返回 false 不等待，resolve_slot 即 return 0，槽位归零写入快照字节。
3. 逻辑危害确证。该槽位对应存活键（RC 记录仅是读缓存副本，主日志有真身）。驱逐清洗方随后会把活跃索引的槽位 CAS 回主日志地址，但快照文件已固化 0；恢复后该键在索引中不存在。模糊区重插（run_recovery_kernel 的 [index_start, tail) 区间）只覆盖窗口内记录，主日志真身地址低于 index_start 的老键无重插；AOF 重放的位点闸与版本闸均按「快照已物化」跳过该键既有写入，不补。净效果：恢复后老键永久丢失，无自愈通道。触发面为快照扫描大索引期间驱逐持续推进，中段滑出常见，非罕见边角。

涉及代码：
rust 文件与函数：
wedb/wcpr/src/index_ckpt/batch.rs:BatchWriter::resolve_slot
wedb/wkv/src/read_cache/window.rs:ReadCache::skip_read_cache
wedb/wkv/src/read_cache/window.rs:ReadCache::need_to_wait_for_eviction
wedb/wkv/src/store/cpr_host.rs:WedbStore::skip_read_cache 与 WedbStore::wait_read_cache_eviction
wedb/wcpr/src/index_ckpt/codec.rs:resolve_read_cache

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheNeedToWaitForEviction（判定走查当前位置）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache（每步判定 + RestartChain 回链头）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket（快照面 epoch 冻结免等待）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs:useReadCache 桶拷贝段（epoch.Resume 冻结窗内走链）

精炼执行方案：
1 wkv 侧 ReadCache 增加带等待的走查单口 skip_read_cache_with_wait(addr, refresh)：走查内核遇 Gone 就地以该中段地址调 spin_wait_until_record_is_closed（window.rs:177-187 既有原语）等待清洗落定，随后回链头（重读入参槽位当前值）重探，循环直至解析落定返回 Some；对齐 C# SkipReadCache 的「每步判定当前位置 + RestartChain 回链头」形态。refresh 闭包沿用 need_to_wait_for_eviction 的纪元 drain 入参。RC 未启用与非 RC 地址保持恒等透传零开销。
2 wcpr 侧端口收敛：CprStore 的 skip_read_cache 与 wait_read_cache_eviction 两端口折叠为上述单口（宿主 cpr_host.rs 端口体同步替换），BatchWriter::resolve_slot 删除 None 分支的链头锚定等待与 return 0 降级路径，快照只消费 Some；紧缩面（wedb/wkv/src/compact.rs:169 与 wcompact 消费点）同步切换单口，两处锚点分叉一并消除。订正 batch.rs 注释中「端口判非驱逐窗时按链尽归零」的契约表述为「链尾 Unparsable 形态由内核区分、Gone 形态就地等待重探」。
3 测试验证点：wcpr tests 增加驱逐竞态用例（构造多代 RC 记录链，驱逐恰好推进到链中段时并发触发索引快照，断言快照槽位非零且等于清洗后主日志地址，恢复后键可见）；既有 rc_eviction_ckpt / rc_tag / roundtrip 套件回归；无 ReadCache 宿主恒等闭包路径零改动验证。
合入哈希：c685760 收口形态：wkv 增带等待走查单口 skip_read_cache_with_wait（逐位置判定、Gone 就地等待后回链头重读重启、链尽 Unparsable 归零），CprStore/CompactStore 双端口折叠单口并删 BatchWriter::resolve_slot None 降级锚定，快照面与紧缩面共用同一等待内核，probe/copy_to_tail 重启环收敛；rc_mid_chain 双竞态回归 + wkv/wcompact/wcpr 套件全绿

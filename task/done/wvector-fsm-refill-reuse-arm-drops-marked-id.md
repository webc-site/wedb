甄别结论：通过（甄别席 J2，2026-09-27，定级 P2——refill 复用臂 let-chain 短路丢已置位 id，转铸新 id 致 id 空间耗散）。let-chain 推演现码复跑属实：fsm.rs:419-421 refill? && let Some(id)=pop() && !mark_used?，mark 成功即链假不入 continue，已置位 id（mark_id_unchecked :285/:303-309 记账）落 None 转铸新 id；第一臂 :412-416 形正确成对照；构造参数序亲核（fsm.rs:175-178 quantization_enabled 在前）坐实非量化构造 reuse 即开（data_provider.rs:398-403 第四参）。FAST_SIZE :47、契约头注 :144-146 成立。勘误：原生对账源 diskann-garnet 现树缺席，:336-348 不可现码复验，唯 §127 在册记录其锚；本票裁决根基在仓内 fsm 自陈契约与 rust 侧机械证明，不受影响。行漂：:417-425 实测 :419-424。另勘：quantization_needed 引 :662 未命中该行，训练门槛掺水系 count :528 虚报之派生面，不碍主缺陷。派沙箱席 c01c。

审核结论：通过（P2 转写缺陷真案。①let-chain 推演属实：refill && pop && !mark_used，changed=true 时链假不入 continue，已置占用位、total_used+1（:303-309）的 id 落 None 转铸新 id，与第一臂相反，非票面误读；②上游对账：DiskANN/diskann-garnet/src/fsm.rs:336-348 原生链仅 refill&&pop、mark_used 体内成功即 Some(id)——wedb 转写把 !mark_used 入链、交付折进 continue，系转写缺陷非既定改良（§127 只裁量化屏障）；③phantom 不可回收：mark_free 外部唯一点按 IntMap 反查键（data_provider.rs:485），回滚臂仅限已交付 id，load_state（:245-247）读回虚增；非量化构造 reuse 即开（:402）触发常态；现有单测零覆盖；④危害：count/approximate_count 虚报、训练门槛（:662）掺水、random_members 缺员、next_id 单向膨胀；⑤五池唯一，visit_used 票正交。票面锚 :417-426 实测 :419-424，行漂 2 行不碍裁决）

整理执行方案（供 fix 消费）：
1 重排为「pop 空则 refill?continue:break；!mark_used 则 continue；否则 return id」，单一交付点零新机制
2 锁测二则：churn 后复用 id 属已删集、max_id 不推进、total_used 与实占恒等

wvector FSM reuse_or_mint 重填臂成功占用的 id 被丢弃：每次 refill 命中泄漏一个已置占用位无主的内部 id，total_used 虚增且 next_id 水位单向膨胀

问题分析：
1 契约对齐：fsm.rs 模块头与 FreeSpaceMap 头注（:144-146）自陈契约 next_id() 返回新铸或复用自已删元素的 id（已置占用位），即占用置位与该 id 交付必须同单原子对称（先占 id 后写数据）。快速空闲队列语义系已删 id 必被复用归还，不允许占位无主。
2 工程现状确证：wedb/wvector/src/fsm.rs:408-454 reuse_or_mint 第一臂（:412-416）pop 后 mark_used 成功（changed=true）即 Some(id) 交付，形正确；第二臂（:417-425）快速队列耗尽经 refill_fast_free_list 重填后，成功占用被 let-chain 吞丢：条件 refill() && let Some(id) = pop() && !mark_used() 在 mark_used 返回 changed=true 时整体为假，if 体（仅 continue 分支）不进入，已置占用位并已 total_used+1（mark_id_unchecked :303-309）的 id 随绑定出作用域被丢弃，落 :426 None 后 break 转铸造全新 id。触发面常态化：非量化索引构造即 reuse_enabled（data_provider.rs:398-403 quantizer.is_none() 臂），删除位图空闲超快速队列容量（FAST_SIZE=1024）或队列耗尽而他线程抢先消费间隙后，每次 refill 命中恰泄漏 1 个 id；phantom 占用位无外部映射（IntMap/ExtMap 无该 id 条目），delete_element 的 mark_free（data_provider.rs:485）永不可达，重启 load_state（:212-268）按位图读回为占用，随 VADD/VREM 循环往复单调累积至整库 drop。
3 逻辑危害确证：其一，记账不对称膨胀：total_used 虚增 → data_provider.rs:528 count / service.rs:461 approximate_count 对外虚报，quantization_needed 训练门槛（data_provider.rs:662、dynamic_quant.rs:148 required_vectors 判据）被 phantom 提前凑数触发；其二，水位单向推进：复用失败臂每次转铸新 id，max_id/回填上界（max_id_for_backfill）与 visit_used 遍历区间随 churn 无界膨胀，加速逼近 u32 铸造耗尽臂（wrapping_add 回绕后全表 mark/is_free 恒 IdOutOfRange）；其三，VRANDMEMBER 缺员：random_members（data_provider.rs:543-587）remaining 按虚增 total_used 起算，batch 到顶 id_space 即 break，可回短于实际存活数的成员集；其四，vector_iid_exists（:522-525）对 phantom 恒判存活。

涉及代码：
rust 文件与函数：
wedb/wvector/src/fsm.rs:FreeSpaceMap::reuse_or_mint（:417-426 重填臂 let-chain 吞丢）、mark_id_unchecked（:303-309 占用记账）、FAST_SIZE（:47）；消费面 wedb/wvector/src/provider/data_provider.rs:set_element 取 id（:657）、count（:528）、random_members（:543）、delete_element mark_free（:485）、FreeSpaceMap::new 接线（:398-403）；wedb/wvector/src/service.rs:approximate_count（:461）

对应 c# 文件与函数：
N.A.（向量 FSM 系 rust 自建基座面，C# 仅经 garnet/libs/server/Resp/Vector/DiskANNService.cs P/Invoke 转发；原生对账源 diskann-garnet fsm.rs 复用臂契约为 mark 置位成功即返回该 id 不丢弃，deviations §127 已钉原生 next_id :322-326 参照锚）

精炼执行方案：
1 改 reuse_or_mint 重填臂为「重填成功即 continue 回循环顶由第一臂统一 pop+占用+交付」：if self.refill_fast_free_list(ctx).await? { continue; } break;，删除 let-chain 吞丢形，零新机制零第二队列；refill 返回假（无空闲位）落入铸造臂形态不动
2 锁测一（fsm.rs 单测）：铸 N 个→mark_free 溢出快速队列容量使位图有空闲位而队列为空→next_id 命中重填臂，断言返回 id 属已删复用集、max_id 不推进、total_used 与实占恒等
3 锁测二（回归）：循环（填充队列→溢出删除→排空→next_id）多轮 churn，断言 total_used 与 next_id 水位零漂移；扩既有 wedb/wvector/tests/set_element_rollback.rs 或 service.rs 锁族接线面

收口记录（收票席 R3 批次，2026-09-28）：合入 e46216ef（验货 commit 873f2c87）。收口形态=reuse_or_mint 重排为单一交付点（pop 命中占用成功即交付，重填真则回循环顶，删 Option 双交付吞丢形），零新机制；锁测 fsm.rs::tests::refill_reuse_delivers_marked_id（票指定单测形）+ tests/service.rs::fsm_refill_reuse_churn_zero_drift，两测均反证敏感（回装缺陷形转红）。

甄别结论：通过（2026-09-29 主控甄别，定级 P2——红一双侧亲验：write/mod.rs:596/:665 `?` 透传与票面收口注记直接矛盾、c4354c4 处两例复红实证合入未验；红二四场景探针实证恢复后非严格 set_context 盲分配 vdb 02→03、生产会话恒严格无恙系测试误用；修复均为单机制收口无新机制）

门禁双红（wkv::ttl_sidecar_order 二例）：GETDEL/DEL 后置 TTL 腿失败 `?` 透传违背弃置收口；崩溃前缀恢复用例恢复后非严格 set_context 盲分配 vdb 致全键读 miss

问题分析：
1. 门禁现状确证：./test.sh 首红 wkv::ttl_sidecar_order 二例（getdel_returns_value_and_swallows_ttl_leg_failure、crash_prefix_recovery_residues_never_value_immortal），fail-fast 挡掉 2829 例未跑。worktree 于票自身落点 c4354c4 复跑二例同样全红——f936e8f 合入时回归测试未过门禁，终态注记「DEL/GETDEL 后置腿失败弃置不降级」与现码不符。
2. 红一（TTL 腿错误政策）：wedb/wkv/src/session/raw/write/mod.rs 三处消费点对后置腿助手 try_strip_ttl_sync_unprotected_with_prefix 的失败处置与票面语义相反——a) GETDEL try_take_sync_with_unprotected :665 `let _ = strip(...)?`：let _ 丢内层、`?` 仍透传外层 Err，镜像注错（测试 ttl_sidecar_order.rs:206 注入）原样上抛，取删值应答丢失；b) DEL try_delete_sync_unprotected_with_prefix :596 同形；c) SET 同步臂 try_upsert_tag_sync_unprotected_with_prefix :212-216 `!strip(...)?`：硬失败上抛而非沿降级臂交异步幂等重做。三处均违背票 wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal 终态注记 1/3 条（SET 失败沿降级臂；DEL/GETDEL 弃置不降级）。异步臂（collection.rs delete/take_string）已正确 `let _ = self.del_ttl(key).await` 弃置，同步内核为漏网。
3. 红二（恢复面上下文物化）：crash_prefix 用例恢复后直调非严格 set_context(5,2)。恢复面 ns 标量映射在册（rebuild_apply_record NsMap 臂无条件装载）而非根域库级路由冷态不装载，非严格 set_context 跳过冷检门（session/mod.rs:539 `strict &&`）走 get_or_create_db 盲分配新 vdb（2→3）并落盘 DbMap 覆盖磁盘权威，会话前缀 [01,03,00] 与物理记录 [01,02,00] 错位，全键 read/ttl_of 全 miss。四场景探针实证：默认上下文恢复读正常、ns5/db2 上下文全 miss、恢复后物理键第二字节 02→03。违背 vdb_load.rs 头注铁律「任何路径都不得绕过点查直接盲分配」。生产无恙（wnode/service.rs:2117 协议会话恒 set_strict_context(true)，冷检门回 false 交 resolve_context 挂起面），系测试误用非严格路径。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/write/mod.rs:try_take_sync_with_unprotected（:665 GETDEL 后置腿）
wedb/wkv/src/session/raw/write/mod.rs:try_delete_sync_unprotected_with_prefix（:596 DEL 后置腿）
wedb/wkv/src/session/raw/write/mod.rs:try_upsert_tag_sync_unprotected_with_prefix（:212-216 SET 同步臂）
wedb/wkv/tests/ttl_sidecar_order.rs:crash_prefix_recovery_residues_never_value_immortal（:278-280 恢复后上下文物化）

对应 c# 文件与函数：
（无直位对；C# UpsertMethods.cs:21-23 记录一体结构无独立 TTL 腿失败面；参照维度：板块 2.2 持久可靠「恢复与重放幂等性」，Ticket 上游映射权威在磁盘 DbMeta 与 TryGetOrSetDatabaseSession success 门）

精炼执行方案：
1. GETDEL/DEL 两处 `let _ = strip(...)?` 去尾 `?`：记录已摘除后腿失败（页翻转降级形与 Err 形同权）弃置即无主 TTL 记录良性自愈态，bump_watch_version 照常收口
2. SET 同步臂改判 `!matches!(strip(...), Ok(true))` 即回 Ok(Err(DEGRADE_ASYNC))：页翻转与硬失败同沿既有降级臂交异步幂等重做（值幂等、应答 +OK 不失真），与票面收口形态 1 对齐
3. crash_prefix 用例恢复后、set_context 前插 store.resolve_context(NS, DB).await?：点查磁盘 DbMeta 装载既有映射（vdb_load 单点，keyspace.rs:390 首访冷装载同形），杜绝盲分配覆写权威
4. 验证：wkv::ttl_sidecar_order 全绿 + write_kernel_failpath 全绿 + ./test.sh 全量（--no-fail-fast 补跑被挡 2829 例）+ ./sh/clippy.sh 零警

查重：task 五池无同票；本票系票 wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal（done）合入门禁未验的收口残面，与其归档注记 1/3 条直接对应，不另立第二机制。

终态注记（2026-09-29 执行收口）：
修复合入 ac22e75（并发席现场代收本席工作区，含 write/mod.rs 三处失败处置 + crash_prefix resolve_context 装载），本席终态以本提交为准。
收口形态：
1. GETDEL try_take_sync_with_unprotected / DEL try_delete_sync_unprotected_with_prefix 后置腿 `let _ = strip(...)` 同权弃置页翻转形与硬失败形（严禁 `?` 上抛撕裂已提交写），bump_watch_version 照常收口，残留为无主 TTL 记录良性自愈态（四通道闭环）
2. SET 同步臂 `!matches!(strip(...), Ok(true))` 回 Ok(Err(DEGRADE_ASYNC))——页翻转/冷数据/硬失败同沿既有降级臂交异步幂等重做，应答 +OK 不失真
3. crash_prefix 用例恢复后 insert store.resolve_context(NS, DB).await? 再 set_context（点查磁盘 DbMeta 装载既有 vdb，杜绝盲分配覆写权威）
验证：ttl_sidecar_order + write_kernel_failpath 10/10 绿；全量门禁 5264/5264 绿（--no-fail-fast）；clippy 零警。

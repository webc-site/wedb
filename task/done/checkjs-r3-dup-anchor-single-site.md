# checkjs-r3：重复定义簇锚点单点收口（35 簇逐簇甄别）

## 前置
**必须等 checkjs-r2（B 层符号断言修正票）合入 dev 后开工**——两票同改注释锚点面，先并后改防撞面。

## 背景
check.js r3（/tmp/_rs/checkjs-r3.txt）「# 重复定义」段仍列约 35 簇：同一 C# `路径.cs:函数` 锚被多个 rust 函数文档挂。主控已抽查 10+ 簇判为合法形制：
- 测试撞名（GarnetJsonObject.cs:Set、HashCommands.cs:HashExpire、SortedSetAdd、HashSet、ServerConfig、msetnx 等——簇员多为 tests/ 函数）：假阳
- 调用链分层（VectorStoreOps×2、FreeRecordPool:TryTake、SkipReadCache 收口单口、update_cluster_auth trait 委托、read_list_position_params 壳、HLL sparse_fits/can_grow、ensure_split→split_buckets、InfoCommand 三段、InitSparse 姊妹）：核心位挂锚合法

## 任务
逐簇（35 簇全量，勿抽样）判定并收口：
1. **真重复**（两处 rust 各长了一份同逻辑）→ 合并一处定义（这是本票核心价值，须先对 garnet .cs 核实语义再动手）；
2. **合法分层/壳/委托** → 锚点单点化：C# 锚只挂真正对位件（通常核心/末端），次要位把 `路径.cs:Name` 形态改写为叙述性引用（如「C# 同名件」「对位 X 之壳臂」）——注意：这是锚点形制收敛，不是为绕过检查改注释掩盖真重复；
3. **tests/ 撞名簇** → 测试函数若确为该 C# 场景对标，锚保留在**唯一主测**；非主测成员改叙述。

## 疑点簇优先（未验证过，先查）
- TaskManager.cs:CancelAsync → wbase/src/supervise.rs:142 (Supervised\<F>::drop) + wkv/src/gc/mod.rs:122 (enabled_by_config)：语义错位，疑误挂
- MainStoreOps.cs:GETDEL → user_read.rs:149 record_outcome + storage_session.rs:395 read_user_quiet
- AllocatorBase.cs:TryAllocate → set.rs:1001 string_record_fits_page + object_store_utils.rs:1298 envelope_overflow
- StoreWrapper.cs:StoreWrapper → info_provider.rs:59 startup_ticks + :69 init_startup_ticks（明显不相关，疑错锚）
- AllocatorBase.cs:OnPagesClosed → read_cache/append.rs:290 + raw/mod.rs:362 evict_pages_for
- CertificateUtils.cs:GetMachineCertificateByFile、GarnetTlsOptions.cs:GetSslClientAuthenticationOptions（wtls 双位）
- GarnetServerBase.cs:DisposeActiveHandlers、SingleDatabaseManager.cs:RecoverCheckpointAsync（service.rs:1933 engine_swap_hook_bundle 疑错锚）
- RangeIndexManager Locking/RestoreTree 与 DisposeTreeUnderLock、KeyAdminCommands NetworkRESTORE、RespCommandDocs/RespCommandsInfo 初始化族

## 验收
- `bun js/check.js` 重复定义段清零或仅余书面论证保留项（说明放票面执行注记）
- 零行为改动面：若某簇收敛暴露真重复需改函数体，单独立 fix 票勿在本票夹带
- worktree 流程：./fork.sh checkjs-r3-dupanchor；cargo check -q --workspace --all-targets 零告警；主控合 dev 后跑门禁
- 禁触他席在途域（开工时看 task/ing/ 现册）

## 收口记录（2026-09-28 主控）
- 席：`checkjs-r3-dupanchor`（tip f215603a，含主控代提交与 amend）→ dev 合并 **55454a26**（`--no-ff`）。
- 席于 150 轮上限爆停（0 提交 / 75 脏档），按抢救规程 1d 处置：主控在席 worktree 跑齐
  `cargo check --workspace --all-targets`、`cargo fmt --all -- --check`、`bun js/check.js`、
  `bun js/check/symbolCheck.js` 后代理提交，末三簇（tiered_field_ttl / latch_concurrency /
  vector_read_lock_arms + vlinks_vrem）测试锚由主控亲办叙述化，未起续席。
- 收口形态：46 重复定义簇全量裁定清零——同名 `path.cs:Fn` 锚留主实现站点单一处，
  次站点（壳/委托/调用方/非主测试/顺带提及）改叙述形（去 `.cs:Fn` 形态，语义与 C# 指涉不减）。
  信息灭失审计：99 处被删锚逐条对账，恰 1 处无踪迹 → 已补回。
- 面额：75 文件 134+/120−，**非注释增删 0、非注释删 0**（主控逐档复核，含 fmt 尾）。
- 合并后台账复净：`bun js/check.js` 重复定义段 0、实现缺失 0；
  `bun js/check/symbolCheck.js` 锚点 5506 处、违规 0、豁免 1。
- 同日余波：本波新并的 wconn 拆连票自带 `GarnetClient.cs:Dispose` 四站点重复，
  主控另笔 7b77e591 单点化收口（同形态，非本票面）。

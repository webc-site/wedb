甄别结论：通过（2026-09-29 主控甄别，定级 P2——resp_server_session_range_index.rs:104 cache_size < 4*leaf_page_size 裸乘法，leaf=2^62（ri_option_long 域内合法到达）溢出回绕击穿守卫，连带 bf-tree-0.5.6 config.rs:521 引擎侧同款被击穿。C# RespServerSessionRangeIndex.cs:136-141 仅查正数同缺。修复：saturating_mul 与入口域钳制两步同批不可拆单）

审核结论：通过（2026-09-29 甲轮27-A 独立审核席，P2 级）。:104 裸乘法、:211-220 u64→i64 域放行无上界钳制、release overflow-checks=false 回绕击穿 4x 守卫与引擎同款、:88-96 注释承诺连带击穿，全复核成立。审核席执行面修正（执行席遵照）：
1. 票内第 2 步入口域钳制是必要项非可选：仅 saturating_mul 不封死（leaf=2^62 配 cache_size=i64::MAX 饱和值仍放行、2^62 as usize 下传照样回绕），两步须同批落地不可拆单。
2. C# 锚路径勘误：实际为 garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs（票写 Warehouse/ 有误），行号内容无误。

原票面：
RI.CREATE 选项校验 4 * leaf_page_size 无保护 i64 乘法，PAGESIZE 巨值 debug 远程 panic、release 回绕击穿 4x 容量守卫并连带击穿引擎同款守卫

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# RespServerSessionRangeIndex.cs:136-141 CREATE 选项仅查 > 0，容量不足组合由 native 引擎 validate 拒回 NULL、文案泛化，无乘法算术。rust 侧入口补定向文案守卫系 deviations.md 第 83 条裁决的合理偏离——缺陷在守卫实现自身的算术（无保护乘法），非裁决面。板块 4.1 明文：算术溢出保护、参数越界必须严格拦截。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:RiCreateOptions::validate（:104）`if self.cache_size < 4 * leaf_page_size`——leaf_page_size 显式路径取用户 PAGESIZE 原值，ri_option_long（:128-）放行整个 i64 域（u64 解析后 i64 域内全放行，无上界钳制），PAGESIZE 4611686018427387904（2^62）合法到达该乘法。i64 乘法 4 * 2^62 = 2^64 溢出。:88-96 注释承诺「守卫通过的组合在两种后端下均过引擎 validate、InvalidConfig 穿透帧在本命令面不可达」被回绕共同击穿：引擎 bf-tree 0.5.6 src/config.rs:522/:530 为同款裸 4 * leaf_page_size（usize），2^62 输入下同样回绕 0 后判 cb_size_byte < 0 恒 false。TreeTuning（:112）将 leaf_page_size as usize 原样下传 wkv。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
触发链：客户端单条命令 RI.CREATE k PAGESIZE 4611686018427387904 即达——debug 构建 i64 溢出直接 panic，远程可触发进程崩溃，踩「生产路径严禁未受控 panic」红线；release 构建回绕 0，cache_size < 0 恒 false 守卫静默放行，2^62 级 leaf_page_size 进入建树/页分配路径存在大分配 abort 风险（引擎 :455 leaf_page_size / cb_min_record_size > 4096 检查在用户同传大 MINRECORD 时亦可构造通过）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:RiCreateOptions::validate
wedb/wnode/src/resp/range_index/resp_server_session_range_index.rs:ri_option_long

对应 c# 文件与函数：
garnet/libs/server/Resp/Warehouse/RespServerSessionRangeIndex.cs:CREATE 校验段（:136-141 仅查 > 0）

精炼执行方案：
1 守卫改饱和算术 leaf_page_size.saturating_mul(4)（或 checked_mul 越界即拒绝并回定向文案）
2 入口对 PAGESIZE/MAXRECORD 巨值补域钳制（引擎侧算术不可控，入口收口是唯一可靠防线；钳制上界与 compute_leaf_page_size 派生域对齐自洽）
3 测试验证点：PAGESIZE=2^62 与 2^61 回定向错误帧不 panic；回装裸乘法 debug 即红

来源：甲轮12-C 审查席（2026-09-28），主控亲验（validate :104 裸乘法 / ri_option_long i64 全域放行 / 注释承诺与回绕击穿链）。

终态注记（2026-09-29 执行席收口）：
合入 fix 5ca4409、merge 6157799（fix-ri-pagesize-satmul → dev，--no-ff）。两步同批落地：
1 守卫乘法改 leaf_page_size.saturating_mul(4)（resp_server_session_range_index.rs validate）；
2 入口域钳制：PAGESIZE/MAXRECORD > 32KiB（compute_leaf_page_size 派生域封顶，收敛为 wbftree RangeIndexManager::MAX_LEAF_PAGE_SIZE 公常量单点）单点拒绝，回定向帧 "ERR PAGESIZE must not exceed 32768" / "ERR MAXRECORD must not exceed 32768"（按既有 RI 参数错误文案风格），同步封死 TreeTuning as usize 下传、引擎 4 × leaf_page_size usize 乘法与 cb_max_record_size(max_record_size + 1) 折算三处回绕面。注释 :85-98 承诺恢复真实并收窄为容量维度口径。worktree 内 cargo check --all-targets 零警告通过（test.sh/clippy.sh 由主代理集成门禁统一跑）。
测试 wnode/tests/range_index_ricreate_domain_clamp.rs：PAGESIZE 2^61/2^62/i64::MAX/上界+1 × CACHESIZE i64::MAX（仅饱和乘仍放行的审核钉组合）与 MAXRECORD 同域极值（含 MINRECORD=MAXRECORD=2^62 关系放行形）全拒；边界恰取 32KiB——PAGESIZE 32768 建树、32769 拒，MAXRECORD 32768 过钳制交引擎比例裁决、32769 拒；记账零滞留断言。
遗留（非本票判据，建议另开票）：CACHESIZE/MAXKEYLEN/MINRECORD 巨值仍无上界钳制——CACHESIZE i64::MAX 在预算不设限（cache_budget==0）配置下可达引擎整环大分配 abort 面，MAXKEYLEN i64::MAX 命中 wbftree lifecycle `max_key_len + 1` 折算回绕；另 CACHESIZE 非 2 的幂（如 16385）与 MAXRECORD 派生叶页比例失配组合可过 wnode 守卫、由引擎 validate 拒（帧为引擎翻译文案，注释「仅供恢复/升阶运维面」在非容量维度不成立）。


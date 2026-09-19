优先级：中高（重复：同一目录枚举 + 文件名解析循环三处内联，C# 是一处迭代器）
来源：next/agy.db.md 条 20（其第三条 cite 有误，见下更正）。核销 2026-09-19，
取证基线 = 主仓 /Users/z/git/db/wedb 分支 dev 当下 HEAD。

结论一句话
刷盘快照目录的「read_dir + 文件名严格解析」循环在 wbftree 内联了三份，而 C# 是一处
EnumerateFlushFiles 迭代器供多消费方复用；本仓文件名解析已单点（parse_flush_file_name），
缺的是枚举与门控那一段，须收敛为 flush_files 单点。

现状（主仓 HEAD 实测）
1. 已单点的部分：wbftree/src/manager/replication.rs:26 pub(super) fn parse_flush_file_name
   （26 位 Base32 前缀 + 地址段 + .flush.bftree 后缀严格解码，产出 (key_id, addr)），
   其文档 :22-25 明写「C# EnumerateFlushFiles 在 rust 不设独立函数，由本方法与消费方的
   read_dir 循环内联承接」。
2. 三处内联枚举（同一目录 self.ri_log_root、同一解析器、同一 flatten + to_str + let-chain 样板）：
   replication.rs:38 on_truncate（循环 :43，按 addr < new_begin_address 删旧件）、
   replication.rs:62 remove_addr_flush_files（循环 :63，按 key_id 删全世代件）、
   manager/lifecycle.rs:177 惰性恢复取最大地址件（循环 :177，配 addr_flush_scan_pending 门控，
   胜出者单次 join，见 :175-176 注释）。
3. cite 更正：票内第三条 recover_all_trees_from_dir（replication.rs:147，循环 :166）扫的是
   检查点快照目录、匹配 .bftree 后缀，与 enumerate_checkpoint_snapshots（:97，循环 :106）同域，
   不属刷盘件枚举，本票不动这两处（其目录/后缀/解码规则与 flush 件不同）。

C# 参考
1. libs/server/Resp/RangeIndex/RangeIndexManager.cs:901 private IEnumerable<(string Path,
   string Name, long Address)> EnumerateFlushFiles()：目录有效性预检（:903-908）、
   按 *{FlushSuffix} 过滤（:910）、长度与地址段十六进制解析（:915-923），产出三元组。
2. 两处复用：同文件 :872（截断清理）与 :986（复制期文件枚举）——即 C# 的口径就是「一处枚举、
   多路分发」，与 check.js 的去重取向一致（重复定义的函数要思考如何去重、一处定义）。

修法
1. 在 wbftree/src/manager/replication.rs 增设一处枚举器（与 C# 同名同形）：
   RangeIndexManager::flush_files(&self) -> Result<Vec<(PathBuf, u128, u64)>> 或
   impl Iterator<Item = (PathBuf, u128, u64)>，内部承担 ri_log_root 存在性预检、
   read_dir、file_name().to_str() 容错与 parse_flush_file_name 解码，跳过外来文件。
   返回 Vec 还是迭代器按三处消费形态定：on_truncate 与 remove_addr_flush_files 可随时
   remove_file（迭代器即可），lifecycle.rs 需遍历取最大地址（同一实现亦可），
   若借用地狱使迭代器签名不干净，则取 Vec（目录条目数级、非常态路径已被
   addr_flush_scan_pending 门控）。
2. 三处消费方改为转调 flush_files，删各自 read_dir 样板；parse_flush_file_name 降级为
   枚举器私有（其 pub(super) 面若无外部消费者则改 fn）。
3. 保留 lifecycle.rs:174-176 的「单次 pin / 只跟踪胜出文件名」优化与
   task/ing/wbftree-on-flush-bare-surface-removal.md 落地后的裸名分支删除结果，
   本票不得复活裸名枚举。
4. 文档订正：把 replication.rs:22-25 中「在 rust 不设独立函数」的论述改为指向新枚举器，
   保持 C# 锚点 EnumerateFlushFiles 单点挂载。

边界
与 task/ing/wbftree-on-flush-bare-surface-removal.md 同文件族，先落那一条（少一种命名、
少一个分支），再做本票枚举收敛。与 next/checkpoint-store-purge-entry-list-csemantics.md
（检查点条目清单）不同域。

验收判据
1. RangeIndexManager::flush_files 一处定义；grep 对 fs::read_dir(&self.ri_log_root) 在
   wbftree/src 内命中数由 3 降为 1（仅剩 flush_files 体内）。
2. RangeIndexManager::on_truncate、RangeIndexManager::remove_addr_flush_files 与
   lifecycle 惰性恢复三处的删除/胜出行为逐条不变（按 addr 阈值删、按 key_id 删、取最大 addr 胜出）。
3. parse_flush_file_name 不再被枚举器之外的调用方引用。
4. cargo check 通过（禁跑 test.sh / clippy.sh，由主代理合并后统一跑）。

双花登记
并发代理的同号薄票 next/db-bftree-flush-dir-scan-single.md（自称并入 next/muse.db.md 条 12）已由
主仓 commit 0f7ce71 作「双载体薄壳」删除，载体统一为本票，本票为该题唯一正文。
在途冲突（重要）：worktree /tmp/fork/wbftree-flush-enum（分支 wbftree-flush-enum）已开工同文件
wedb/wbftree/src/manager/replication.rs，当下 diff 仅一行文档注释改写、尚无实现——本票须并入该棒续做，
禁另派第二棒（两棒同改 replication.rs 必冲突）。
口径差异留档：对方薄票把 replication.rs:106 与 recover_all_trees_from_dir（:147/:166）的快照目录扫描
一并计入「四度扫描」；本票按当下代码判其属另一枚举域（.bftree 检查点快照目录 vs ri_log_root 刷盘日志
目录，文件名与解析器均不同），只收 ri_log_root 三处（:43、:63、lifecycle.rs:177），禁为凑数合并两套迭代。

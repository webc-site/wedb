wcol SortedSetOrderOperation 死枚举收口：删除，顺序语义只留位标志一套机制

来源：task/ing/wcol-sorted-set-order-operation-dead-enum.md（next/glm.db.md 条 10 立项，该源文件已剪空删除）
判定：成立，走开发路径（删除 + 不登记 ignore）
分支：wcol-sorted-set-order-operation-dead-enum（工作树 /tmp/fork/同名），改动 1 文件 -13 行
dev 合入：payload 提交 c8ee858 首次入 dev（经 26de243 的祖先链 1667453），被并发 merge-repair
7a83b8c0 连带回滚后重落，终态 merge commit a196a48（父母 dev d9dc9c0 + 分支尖，
`git diff d9dc9c0 a196a48 --stat` 只有 wedb/wcol/src/zset/sorted_set_object.rs 13 行删除，零夹带）

甄别（全部主仓 dev 实测重取行号，未照抄票面）
1. 读侧死、写侧也死，不只是「C# 顺序语义投影」断线：定义块在
   wedb/wcol/src/zset/sorted_set_object.rs:101-112（票面写 102-112，实际首行 :101 是
   「/// 排序维度」，:103 为 C# 锚点 doc，:105 enum，:107/:109/:111 三变体）。删除前
   `grep -rn SortedSetOrderOperation wedb/ js/` 只命中 :103、:105 两行，即定义与自身锚点；
   零构造（无 `SortedSetOrderOperation::` 任一处）、零匹配、零泛型界 / trait 侧消费、
   零 re-export（wcol 各 lib.rs 不含该名）、零测试引用，也没有只被它引用的 helper（ByRank
   全仓另两处命中均为无关 C# 锚点注释：wnode/tests/resp_sorted_set.rs:1160、
   wcol/src/zset/sorted_set_object_impl.rs:704）。票面「被泛型界/trait 侧消费」的成立性反证不成立，
   故不走拒绝路径。
2. 同一语义在 rust 只有一套承载（活链，逐行核实）：
   - wedb/wnode/src/resp/garnet_api/raw.rs:292-307 会话 API 层按 RespCommand 直构位标志
     （:301 Zrangebyscore → SortedSetRangeOpts::BY_SCORE，:306 Zrevrangebyscore →
     BY_SCORE|REVERSE，lex 族 :294-300 同理，ZRANGE/ZREVRANGE :292-293）；
   - wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:379-387 装载 + operate 形态
     再映射同一组位标志（:380 NONE、:382 BY_LEX、:386-387 BY_SCORE/BY_SCORE|REVERSE）；
   - wedb/wcol/src/zset/sorted_set_object_impl.rs:468-497 统一入口
     sorted_set_range(arg2: i32) 以 :475 from_bits_truncate 承接，:492 投影为
     ZRangeOptions{by_score, by_lex, reverse, with_scores}。
   C# 的「API 形参位」（SortedSetOps.cs:452 sortedSetOrderOperation）在 rust 由 arg2 位标志占住，
   属同一职责两种形态中的孤儿形态，不是缺功能。
3. C# 侧核实为活环（决定「删除即收口」而非「补接线」的依据）：
   garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:121 定义；
   garnet/libs/server/Resp/RespServerSession.cs:915-936 把 ZRANGE / ZRANGEBYLEX / ZRANGEBYSCORE /
   ZREVRANGE / ZREVRANGEBYLEX / ZREVRANGEBYSCORE 六条路由到 SortedSetRange(cmd, ref storageApi)；
   garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:452 形参、:466-471 enum→opts
   翻译、:483 LIMIT 仅 ByScore/ByLex 合法。rust 的 LIMIT 合法性由 args 解析按位标志判定，
   不经该 enum，删除零语义损失。

修法取舍
- 整块删除，含 :103 的 C# 锚点 doc：锚点随类型消失是有意的，不留「有映射、无消费」的假登记；
  也没按票面禁止项给死类型补「已接线」注释，更没为对标 C# 形状给 sorted_set_range 增设 enum 形参
  （那会把单套位标志表达改成两套顺序表达，违背「一处定义」）。
- 不登记 ignore，与票面「check 可能变红则追加」的分支相对，实测落在「不追」分支，证据两条：
  js/check/garnetScan.js:31 只采集 tree-sitter method_declaration 节点，C# enum 及其枚举项不入
  判定；删除前后各跑一次 bun js/check.js，stdout 逐字一致（48 行，diff 为空），exit 0，
  js/check/ignore 与 check/miss 语料零回写、工作树零 diff。为一个门禁不追踪的名字写 ignore
  条目，按 check.js 的淘汰语义（仅当该名进入 documented_set 才淘汰，js/check.js:174-189）
  会永久滞留成假语料，故不写。若后续 symbolCheck B 层把 enum/字段纳入判定，再按
  js/check/ignore/libs/server/Objects/SortedSet/SortedSetObject.yml 既有形状补登记。
- 邻居边界核实：同文件 SortedSetExpireResult（删除后锚点 doc :103、enum :106）在
  wedb/wcol/src/zset/sorted_set_object.rs:710-780 有生产构造与返回，活，未动；
  SortedSetOperation / SortedSetRangeOpts 亦为活件，未动。
- 与在册零消费批不重叠（批二批五及票面点名的批三批四清单均无该符号），本单为该符号的唯一收口票，
  后续普查以本单为准。

验收读数
- cargo check --workspace --all-targets（worktree 内，workspace 根为 <wt>/wedb，
  CARGO_TARGET_DIR=<wt>/target 隔离）：删除后各轮全绿——并入 dev 772f324 后 exit 0
  （-check3.log 5.94s）、dev 2e51599 后 exit 0（-check4.log）、dev 8047cf3 后 exit 0
  （-check5.log 13.61s）、-check7.log 复跑全 Fresh，error 0、warning 0。
- 重落轮（并入 dev 0eeeaac 后）cargo check exit 101：唯一错误是
  wnode/src/service.rs:1473 `acl_settings: None` 对 E0560（struct SessionDependencies 已无该字段），
  实证与本单无关——把本文件改回 dev 版（即还原枚举）单跑一次，同一错误原样复现
  （-check9-devbase.log），且本单相对 dev 只差这一处未用枚举删除。该红由并发 merge-repair 夹带，
  现已被后续会话修好（当前 dev `git grep acl_settings dev -- wedb/wnode/src/service.rs` 无命中）。
- bun js/check.js（worktree 内）：删除前后 stdout 逐字一致、exit 0、语料零回写（首轮）；
  重落轮 exit 0，仅 js/check/ignore/common.yml 被并发 dev 代码变更触发改写（他票 ignore 面，
  未随本单提交，已在工作树回退）。
- 主仓 dev 树复验：`git grep -c SortedSetOrderOperation dev -- <本文件>` 无命中。
- 未跑 ./test.sh 与 ./sh/clippy.sh（按规程交主代理集中回归）。

并发事故记录（本单为污染源，主代理需按此对账）
事实链：
1. 首次合入用「无条件取分支树 + commit-tree」的 plumbing，未校验 dev 头是否已并入本分支；
   门禁期间 dev 已从本分支基点 8047cf3 推进到 0a42c90（夹带 vector preview、replhist 落盘互斥等
   并发 payload），生成的 844c6f6 取分支树 → 整树反吞这些 payload（20 文件）。
2. 约一分钟后用 update-ref 乐观锁把 dev 从 844c6f6 退回 0a42c90。但退回前的窗口内，
   resync-strategy-split（a4253a0 第二父即 844c6f6）与 wacl-deadchain-b2（1690b41 同）已把
   当时指向 844c6f6 的 dev 并入自己分支，于是回退内容以「分支侧变更」身份经 26de243 重回
   dev 第一父链 —— 844c6f6 并未悬空，`git merge-base --is-ancestor 844c6f6 dev` 为真。
3. 并发会话已自查并修复：7a83b8c0 fix(merge-repair)「回滚 26de243 合并夹带的并发反吞」以
   0a42c90 为源逐文件 checkout，vector preview 族（node_options、wnode/src/resp/vector 四件、
   tests/vector_set_production_switch.rs、wnode/src/service.rs）与 waof/initiate-replica-sync
   文档归档均已复原；但它同时把本单 c8ee858 的 13 行删除一起回滚（该文件 0a42c90 版仍含死枚举），
   并以整文件回滚的方式在 service.rs 夹带了 E0560 编译红（后被更后会话修掉）。
   replhist 线也自行重落：dev d9dc9c0f 记「3a92cb9 被 26de2434 夹带反吞后按当下 dev 重贴」，
   终落 a3f6cef5。当前 dev 复核：`git grep -l history_flush_lock dev -- wedb` 命中
   wedb/wedb/src/server/replication/replication_manager.rs，vector 族五件与
   wedb/wnode/tests/vector_set_production_switch.rs 均在位，两族 payload 已闭合。
4. 本单据此重落：worktree 内三方并入 dev d9dc9c0，核 `git diff <dev尖> HEAD --stat` 只含本文件，
   再 commit-tree → dev a196a48，`git diff d9dc9c0 a196a48` 只有 13 行删除，零夹带。
   另注：本档草稿曾被并发 checkpoint 提交 a3f6cef5 顺带入库（他票 add 扫面所致，内容为回滚前的
   旧叙述），终版以本票在 task/done/ 上的最后一次提交为准。

仍需对账（逐文件三方比对 dev / 0a42c90 / 844c6f6，脚本 /tmp/fork/zset-dead-enum-clobber-audit.sh，
判读口径：STILL = 仍等于 844c6f6 版；本文件为故意例外）
- next/qw.design.md、next/qw.my.md、task/ing/client-latency-histogram-unwired.md 三处内容仍为
  844c6f6 版（scratchpad 与文档面，非 payload，交各自主人或被剪或被留）。
- wedb/wconf/src/node_options.rs、wedb/wnode/src/service.rs、
  wedb/wedb/src/server/replication/replication_manager.rs 与其 tests 现为 DRIFT（后续会话又改过，
  不等于两版中任一），replhist 的 history_flush_lock 已在 dev 复现，建议由 wbftree/wacl 线各自
  主人复核一轮，不必再整文件回滚。

口径修正（对本仓所有走 plumbing 合入的子代理）
取分支树做 merge commit 前必须校验 `git merge-base --is-ancestor dev <分支>`，dev 每推进一次都要
重新三方并入 + 复核 `git diff <dev尖> <分支> --name-status` 只含自己那几处，再 commit-tree；
合入后立即用 `git diff <旧dev> <新dev> --stat` 复验零夹带。与本仓已记的「陈旧工作树 add -u
整树回写」（36df578、0f928e9、6d27b2c 系列）同族，区别只在污染源是 merge 树而非工作树；
修复方（如 7a83b8c0）按 0a42c90 逐文件 checkout 时同样会回滚期间已合入的他单 payload，
宜先 `git log --since=<坏点> --name-only` 列出被波及票再动手。

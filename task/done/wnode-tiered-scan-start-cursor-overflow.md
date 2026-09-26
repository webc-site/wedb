甄别结论：通过（甄别席 zc-fix-r16-cursorovf，2026-09-26）定级 P1

甄别核验记录（现码逐点复跑，非票面背书）：
1 C# 三守卫亲读在位且行号吻合：HashObject.cs:373 `if (hash.Count < start) { cursor = 0; return; }`、
  SetObject.cs:195、SortedSetObject.cs:471 同型入遍历前早退，漏抄指控属实。
2 rust 侧亲验：内存态 hash_object.rs:390 / set_object.rs:267 / sorted_set_object.rs:510 守卫在位；
  exec_tiered_scan（tiered_collection_ops/scan.rs:301-483）全函数读毕，refresh 三态分支（:356-374）后
  直至 :472 `scan_converge_cursor(start + scanned, expired, meta.size as i64)` 无同型守卫；
  read_scan_input（scan_input.rs:44-51）与 scan_validate（shared_object_commands.rs:47-67）仅非负门，
  i64::MAX 游标可合法送入，全部成立。
3 算术链复跑：start=i64::MAX 时存活条目全走 skipped 臂、scanned 恒 0，:472 加法不溢出；唯判定体
  scan_input.rs:231 `cursor + expired_keys_count >= total` 裸加法在 expired>=1 时 MAX+expired 溢出——
  debug 默认 overflow-checks 开启 panic，release 显式 false（wedb/Cargo.toml:247）回绕负值恒不命中，
  回显 i64::MAX 垃圾游标 + 空条目无限轮询、每轮全树 O(N) 空走；Set 臂 SET_MEMBER_DUMMY_VALUE=b"1111"
  （wcol/src/lib.rs:29）首字节出 member_ttl 旗标域恒假豁免，属实。双态分叉违 collection.md 双态应答
  透明承诺（实核在第 5 节第 4 条 :69，票面注「第 4 节」系行号级小偏，不影响定罪）。
4 非重复非灭失：deviations.md 全册 grep 零本轴登记（§10 修 L<start<=N 尾判定域、§129 锁窗刷新，均与
  本票 start>total 且 expired>=1 正交）；同批 wnode-scan-consistent-read-protocol-missing.md 系副本
  一致读接线轴，不同案不并案；ing/done/reject/issue 无撞票；测试越界档仅 99（
  scan_family_dualstate_frames.rs:329/333/337/593/782-784），i64::MAX 档零覆盖（该文件 i64::MAX 均系
  promote 键 TTL 实参）。
5 方案合规：单点守卫 1:1 对位 C# 原型早退形，复用 key_vanished 臂预留-回填出 [0, 空]，零新机制零新
  帧形无假桩，合 transpile/rust_review 纪律；与 §20 d)「严禁 read_scan_input 增设下限门」不冲突（守卫
  落分层臂非解析门）。惟注：票面「review.md 板块 4.1」引用系历史失准（现 review.md 仅 22 行无该板块），
  防溢出取向以 §20 d) 与编译配置为据，不影响定罪。定级 P1：任意客户端单命令可触发 debug panic /
  release 垃圾游标死循环 + 全树空走资源耗尽。

合入哈希：eded88c 收口形态：exec_tiered_scan 锁窗刷新后补单点 start 越界守卫（(meta.size as i64) < start
并入 key_vanished 臂不触碰树收敛），双态逐字节出 [0, 空]，尾段 scan_converge_cursor 加法操作数回界，
i64::MAX 溢出死循环消除；wnode tests/scan_family_dualstate_frames.rs 追加 i64::MAX 档双态锁测
（红形经去守卫复跑坐实）。

审核结论：通过（审核席 zcode-r18-review-cursorovf，2026-09-26）

独立复核记录（双侧源码亲验，逐项坐实）：
1. C# 三守卫亲读在位：HashObject.cs Scan `if (hash.Count < start) { cursor
   = 0; return; }`（:372-376），SetObject.cs `if (Set.Count < start)`（:194-198），
   SortedSetObject.cs `if (sortedSetDict.Count < start)`（:470-474），入遍历前
   早退，游标代数操作数恒有界，漏抄指控属实。
2. rust 内存态三臂守卫亲读在位：hash_object.rs `if (self.hash.len() as i64)
   < start { return 0; }`（:388-391）、set_object.rs（:266-269）、
   sorted_set_object.rs（:509-512），分层态 exec_tiered_scan 全函数无同型
   守卫，refresh 三态分支后直入 scan_all_from_head。
3. 游标协议面亲验：read_scan_input（scan_input.rs:44-51）strict_i64 仅
   filter 非负，scan_validate（shared_object_commands.rs:47）同口径光标
   非负门，无上限校验，i64::MAX 客户端可合法送入。
4. 溢出路径亲验可达：start = i64::MAX 时全部存活条目走 skipped 分支
   （skipped < start 恒真），scanned 恒 0，`start + scanned` 自身不溢出；
   唯溢出点即 scan_converge_cursor 判定体 `cursor + expired_keys_count`
   （scan_input.rs:231 裸加法）。分层 Hash/ZSet 成员级 TTL 到期未收集成员
   驻留树内（纯读面不出账，scan.rs:417-421 expired 计数臂），expired >= 1
   即 i64::MAX + expired 溢出：debug overflow-checks panic；release 回绕
   负值，`负值 >= total` 恒假，返回原值 i64::MAX + 空条目，客户端无限轮询
   且每轮 O(N) 全树空走。Set 臂 SET_MEMBER_DUMMY_VALUE 首字节不在
   member_ttl 旗标域，member_expired_at 恒假，天然豁免属实。
5. 双态分叉亲验成立：内存态守卫早退出 [0, 空]，分层态溢出/死循环，违
   collection.md 第 4 节双态应答等价承诺；与 §10（== 改 >= 修 L < start
   <= N 尾判定死锁死角）、§129（锁窗刷新）维度正交不重叠，全册 grep 无
   start 越界守卫登记，非重复提报。既有越界游标测试仅 99 档
   （scan_family_dualstate_frames.rs:329/:333/:337/:593/:782-784），i64::MAX
   档零覆盖（该文件 i64::MAX 均系 promote 的 key TTL 参数，非游标）。
6. 方案最小性：补守卫即根因修复——操作数回界后 cursor = start + scanned
   <= 2 * meta.size 物理不可能溢出，无需再动 scan_converge_cursor 加法
   （改饱和算术属非必要第二重防御）；复用 key_vanished 臂既有「不触碰树 +
   预留-回填出 [0, 空]」形态，零新机制零新帧形，与 §129「禁第二套锁窗
   机制」纪律兼容。

优化执行方案（供 task/fix.md 直接消费）：
1. exec_tiered_scan 于 refresh_tiered_meta 三态分支（scan.rs:356-374）之后、
   now_ticks/帧头预留（:381/:388）之前补单点守卫：`if (meta.size as i64)
   < start` 置树遍历跳过（扩展 :410 `!key_vanished &&` 条件为
   `!key_vanished && (meta.size as i64) >= start`，或独立 skip 布尔并入同
   一条件），复用 key_vanished 臂同款收敛：不触碰树、n = 0、
   scan_converge_cursor(start + 0, 0, meta.size as i64) 因 start >= total
   恒真自然归零，无需第二处特判
2. 守卫取锁窗内刷新后 meta.size（与 :472 收敛 total 同源）；claim 回退臂
   （MigrationBusy 装载快照）同样被守卫覆盖，快照 size 越界 start 亦早退，
   与「读面不扩大忙拒面」裁决不冲突（早退是正常应答非错误帧）
3. 注释锚：守卫处标注对位 HashObject.cs:373 三臂同型 + 本票回指，防止
   后续审查误判为可删防御分支
4. 测试验证点：scan_family_dualstate_frames.rs 追加分层 Hash/ZSet 用例——
   promote 挂成员级 TTL 到期成员（encode_member 带 expiry 刻度 < now）后
   HSCAN/ZSCAN key 9223372036854775807 断言逐字节回 [0, 空]（与内存态
   同参数全等）；debug 构建（overflow-checks 开启）跑通即证守卫先于加法
   拦截；既有 99 越界游标用例、claim 窗回退族、双态全等族零回归；跑
   ./test.sh 与 ./sh/clippy.sh 验收

分层 HSCAN/ZSCAN 起始游标越过总量无守卫，scan_converge_cursor i64 加法溢出回绕出垃圾游标死循环

问题分析：
1. Garnet 契约对齐（C# 原型行为）：C# 对象族 Scan 三臂在进入遍历前一律先做
   起始游标越界守卫，HashObject.cs:373 `if (hash.Count < start) { cursor = 0;
   return; }`，SetObject.cs:195 与 SortedSetObject.cs:471 同型。守卫保证 start
   恒 <= 集合总量，后续游标代数（cursor + expiredKeysCount == Count 尾段判定）
   的加法操作数有界。rust 内存态三臂 1:1 保留该守卫（见下锚）。
2. 工程现状确证：rust 分层态 exec_tiered_scan（wnode/src/resp/objects/
   tiered_collection_ops/scan.rs:301）复刻对象层游标口径，但漏抄了这条入遍历
   前守卫：起始游标 start（read_scan_input 仅校验非负，上限 i64::MAX）大于
   meta.size 时仍进入 scan_all_from_head 全树遍历（skipped 恒到不了 start，
   逐记录空走一遍），随后 :472 调 scan_converge_cursor(start + scanned,
   expired, meta.size as i64)；wcol/src/types/scan_input.rs:231 判定体
   `cursor + expired_keys_count >= total` 为裸 i64 加法。当 start 接近
   i64::MAX（如 9223372036854775807）且树内存在至少一个已到期未收集成员
   （expired >= 1，Hash/ZSet 分层键可挂成员级 TTL；Set 恒 0 天然豁免）时，
   i64::MAX + expired 加法溢出：debug 构建直接 panic；release 构建回绕为负，
   负值 >= total 恒假，判定不命中，向客户端回显原值 i64::MAX 作为续页游标。
3. 逻辑危害确证：(a) 客户端可触发的算术溢出 panic 面（review.md 板块 4.1
   算术溢出保护红线）；(b) release 回绕后应答帧携带非零垃圾游标（空条目 +
   游标 9223372036854775807），标准客户端持非零游标无限轮询，且每轮都对
   分层树做一次全量遍历（O(N) 空走），连接与 CPU 双耗尽，与 §10 修复掉的
   原游标死锁同族形态；(c) 同参数下内存态（守卫早退回 [0, 空]）与分层态
   （溢出/死循环）应答分叉，破坏 collection.md 双态全等承诺。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan（:410 起树遍历无 start 守卫、:472 尾段收敛调用）
wedb/wcol/src/types/scan_input.rs:scan_converge_cursor（:231 裸 i64 加法判定）
wedb/wcol/src/hash/hash_object.rs:HashObject::scan（:390 守卫在位，对照锚）
wedb/wcol/src/set/set_object.rs:SetObject::scan（:267 守卫在位，对照锚）
wedb/wcol/src/zset/sorted_set_object.rs:SortedSetObject::scan（:510 守卫在位，对照锚）

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObject.cs:Scan（:373 hash.Count < start 早退守卫）
garnet/libs/server/Objects/Set/SetObject.cs:Scan（:195 Set.Count < start 早退守卫）
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Scan（:471 sortedSetDict.Count < start 早退守卫）

精炼执行方案：
1. exec_tiered_scan 在锁窗内元记录刷新完成后（refresh_tiered_meta 三态分支
   之后、帧头预留之前）补对象层同型守卫：`if (meta.size as i64) < start` 即
   走既有预留-回填出 [0, 空] 帧（key_vanished 臂同款收敛，不触碰树），零新
   机制零新帧形
2. 守卫取刷新后 meta.size（锁窗内新值），与 :472 收敛 total 同源，杜绝装载
   快照陈旧 size 与树内容失配
3. 测试验证点：scan_family_dualstate_frames.rs 追加分层 Hash/ZScan 用例——
   挂成员级 TTL 到期成员后 HSCAN key 9223372036854775807 须逐字节回
   [0, 空]（与内存态同参数全等）；既有 99 越界游标用例与双态全等族零回归

wreviv 测试夹具复抄生产对齐常量 RECORD_ALIGNMENT：测试侧第二真源（二选一收口）

来源：qcode10.design 条 7（LOW，「测试夹具复制生产对齐常量」）。按主仓 HEAD 逐条复核后判定成立待做，
但射程比原报窄：三件夹具常量里只有 RECORD_ALIGNMENT 一件是生产常量的复抄，另两件不属本单（见「不立项段」）。

结论
wreviv 的共享测试夹具把自己对标的那件生产常量原值抄成第二处字面量，且 wreviv 与承载该单点的 wrecord
之间连一条 dev 依赖都没有，所谓「对标」只是注释里的一句话。收口有两条路：要么引用单点，要么承认它是
夹具自取值并剪掉对位声称；现状（自造值 + 声称对标生产常量）是三态里最差的一态。

现状（主仓 HEAD 实测行号）
- 复抄点：/Users/z/git/db/wedb/wedb/wreviv/tests/reviv/support.rs:9
  `pub const RECORD_ALIGNMENT: u32 = 8;`，其上一行注释自称「对标
  libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:kRecordAlignment = 8」。
- 生产单点在另一 crate：/Users/z/git/db/wedb/wedb/wrecord/src/header.rs:57
  `pub const RECORD_ALIGNMENT: usize = 8;`，经 /Users/z/git/db/wedb/wedb/wrecord/src/lib.rs:29 再导出，
  生产侧自用该单点做对齐断言与 RoundUp（同文件 :127 `const _: () = assert!(align_of::<RecordHeader>()
  == RECORD_ALIGNMENT)`、:132 `round_up_to_alignment`）。
- 依赖面缺口：/Users/z/git/db/wedb/wedb/wreviv/Cargo.toml 的 [dependencies] 只有 thiserror、
  wbase（features addr），[dev-dependencies] 只有 aok、ctor、gxhash、log、log_init，无 wrecord，
  故夹具当前无法引用单点，只能复抄。
- 唯一消费点：/Users/z/git/db/wedb/wedb/wreviv/tests/reviv/free_bin_allocation.rs:17 引入、
  :31 `let bin_size = TAKE_RECORD_SIZE + RECORD_ALIGNMENT;`——用作分桶尺寸的一个增量参数。
- 全仓 `const RECORD_ALIGNMENT` 命中仅上述两处定义，无第三处。

C# 参考
- 单点：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs
  的 kRecordAlignment。
- 测试引用常量而非复抄：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/test/test.recordops/
  RevivificationTests.cs:2170 `RecordSize = TakeRecordSize + Constants.kRecordAlignment`（同件
  :82、:279、:1452、:2009 亦一律引 Constants.kRecordAlignment）；
  /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/test/InitialIORecordSizeTests.cs:133、:182
  的对齐断言同样引该常量。C# 测试工程直引 core 程序集内部常量，故天然单源。

修法
优先走引用单点：`cargo add --dev wrecord`（SKILL 规定依赖只经 cargo add 引入，禁手改 Cargo.toml），
support.rs:9 改为由单点派生
（`const RECORD_ALIGNMENT: u32 = wrecord::RECORD_ALIGNMENT as u32;` 或就近 use），删自造字面量。
wrecord 的依赖面只有 log、thiserror、wbase（见 /Users/z/git/db/wedb/wedb/wrecord/Cargo.toml），
不依赖 wreviv，故该 dev 边不构成回环。
若判定不该让 wreviv 的测试面耦合记录头 crate，则取另一条：把夹具常量改名或加注为「夹具自取的桶尺寸
增量，与 wrecord::RECORD_ALIGNMENT 无契约」，并删去 support.rs:8 那句 C# 对位锚——即不允许
「复抄同值 + 注释声称对标生产单点」的第三态。
二选一并留痕，不得两法各做一半（如引了单点又另留同值常量）。

不立项段（原报的另两件）
support.rs:6 `TAKE_RECORD_SIZE = 40` 与 :12 `ADDRESS_INCREMENT = 1_000_000` 复抄的是 C# 测试件常量
（RevivificationTests.cs 的 TakeRecordSize / AddressIncrement），本仓无生产对应物，属夹具正当输入，
不属双真源，本单不碰。原报「生产侧改对齐后用例继续按旧对齐断言而全绿」的风险表述也需下调：
free_bin_allocation.rs 的对齐增量只是一个分桶尺寸选择，用例并不断言对齐不变量，
本单治的是第二真源与锚点失真，不是行为漏检。

优先级
打磨（测试面双真源一处，无生产行为影响）；排在任何触碰 wreviv/wrecord 公开面的在途票之后。

协调
- 与 task/ing/zero-consumer-dead-surfaces-batch-five.md、task/ing/zero-consumer-dead-surfaces-batch-four.md
  不同域（那两单在 wlua/wbase/wnode/wedb 域），无文件冲突。
- 若 wrecord 侧对齐口径将来变更（8→16），本单是唯一让 wreviv 用例随之漂移的接线点。

验收
- 全仓 `const RECORD_ALIGNMENT` 命中归一：要么只剩 wrecord 单点（夹具改引用），要么夹具常量不再自称
  对标 Constants.cs:kRecordAlignment。
- ./js/check.js 不因该锚点改动新增缺失/虚构报告（support.rs:8 的锚注若删，须确认该注释不是
  check.js 的映射登记来源）。
- cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning；
  test.sh/clippy 由中央整合轮执行。

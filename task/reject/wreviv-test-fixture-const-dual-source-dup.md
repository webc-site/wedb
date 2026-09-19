重复：task/ing/wreviv-test-record-alignment-second-source.md（关键符号 RECORD_ALIGNMENT/测试夹具复抄 命中）
优先级：低

7 [LOW] 测试夹具复制生产对齐常量 RECORD_ALIGNMENT
问题：wreviv 的测试底座把生产常量原值抄成第二处真源，wreviv/Cargo.toml 也不依赖 wrecord，
生产侧改对齐（如 8→16）不会传导到用例，用例继续按旧对齐断言而全绿。
rust：/tmp/rev10/wedb/wreviv/tests/reviv/support.rs:9 `pub const RECORD_ALIGNMENT: u32 = 8;`
（同文件 :6 TAKE_RECORD_SIZE = 40、:12 ADDRESS_INCREMENT 同型复制）对位
/tmp/rev10/wedb/wrecord/src/header.rs:57 `pub const RECORD_ALIGNMENT: usize = 8;`。
c#：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs 的
kRecordAlignment 全局单点，测试直接引用常量而非重抄（rust 该单点在 wrecord，测试侧应引用）。
修法：wreviv dev-dependencies 增 wrecord，support.rs:9 改 `use wrecord::RECORD_ALIGNMENT`
（或本地 `const RECORD_ALIGNMENT: u32 = wrecord::RECORD_ALIGNMENT as u32;`），删自造值；
TAKE_RECORD_SIZE 若生产侧已有对位出口一并改引用。

---

浅核附记（拆票代理，主仓 dev 复核）
- 现状成立：主仓 /Users/z/git/db/wedb/wedb/wreviv/tests/reviv/support.rs:6
  TAKE_RECORD_SIZE=40、:9 RECORD_ALIGNMENT=8、:12 ADDRESS_INCREMENT=1_000_000 三常量复抄
  在位；/Users/z/git/db/wedb/wedb/wrecord/src/header.rs:57 生产单点在位；
  wreviv/Cargo.toml 仍无 wrecord 依赖（dev-dependencies 亦无）。
- C# 对位在：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs:14
  `public const int kRecordAlignment = 8;`（:15 派生 Mask、:16 Shift 同源单点）。
- 查重：RECORD_ALIGNMENT 在 next/、task/ 无在册票命中。
- 落地注意：dev-dependencies 只能 cargo add 加（transpile 规矩），TAKE_RECORD_SIZE 在
  wrecord 侧是否有对位出口先 grep 再决定引用还是保留注释说明。

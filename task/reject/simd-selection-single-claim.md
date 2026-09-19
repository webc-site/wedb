拒绝原因：条二（wlua 补取舍声明）的修法措辞含一条不成立立论——「正确性优于 C# 双载 Vector256」，
落地时已按代码事实改写为「等价 + 前提差异」。条一（bit_op.rs 头注矛盾）与条三（全仓 grep 收口）
成立并已合入 dev 6311510，落地档案见 task/done/simd-selection-single-claim.md。

被剪掉的原文（/Users/z/git/db/wedb/task/ing/simd-selection-single-claim.md 改动范围段第二句括号内）
「另在 wlua/src/hash_key.rs 的 ScriptHashKey equals 处补一句同款取舍声明（[u8;40] 切片相等 memcmp，
正确性优于 C# 双载 Vector256，性能面刻意走编译器自动向量化）。」

拒绝理由（对照两侧代码事实）
- 比较结果两侧完全等价，无正确性高低可言：C# /Users/z/git/db/wedb/garnet/libs/server/Lua/ScriptHashKey.cs:55-61
  载 `Vector256.Load(a)`（字节 0..32）与 `Vector256.Load(a + 1)`（`long*` 加一即 +8B，字节 8..40），
  两次 32B 载入重叠覆盖恰为 40 字节，零越界零遗漏，末了 `EqualsAll & EqualsAll`。rust 侧
  /Users/z/git/db/wedb/wedb/wlua/src/hash_key.rs:94 `self.buf == other.buf` 为定长 `[u8; 40]`
  整段比较，逐位同果。故「正确性优于」不成立，属票面（AI 生成）拔高。
- 真实差异在前提而非结果，值得写的只有这一层：C# 靠 ScriptHashKey.cs:46 的
  `Debug.Assert(SessionScriptCache.SHA1Len == 40)` 与 :23-33 的裸指针 + POH 数组保活维持
  「40 字节可读」；rust 把 40B 做进结构体字段，长度即类型不变式，无指针保活前提。
  落地措辞据此写「本类型自带定长缓冲、整段比较即等价」，不写「正确性优于」。
- 「性能面刻意走编译器自动向量化」保留，但与正确性脱钩，并统一口径到 fearless_simd 选型面：
  本仓 SIMD 单点 /Users/z/git/db/wedb/wedb/wbase/src/simd.rs:13、:28（`fast_key_eq` 走
  `dispatch!(Level::new(), ..)`，wbase feature `simd`，由 wrecord/wval 启用）覆盖的是变长键比对；
  40 字节定长小比较引向量臂只增分派面、不增语义。

附带事实校正（不构成立论问题，仅记档免得后人按过期前提找分支）
- 票面「不得扩展或修改 wbase::simd（该文件与在途 bitcount-simd-dispatch-parity 分支共用）」的
  分支前提已过期：认领时 `git branch -a` 无该分支，dev 上 wedb/wbase/src/simd.rs 自 3c4f74a（init）
  起零改动。约束本身照守——本票纯注释，无需改该文件。

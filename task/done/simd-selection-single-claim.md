simd 选型承接声明单口径收口（仅 wbitmap/wlua 注释面）

来源 next/simd-selection-single-claim.md。采纳其「声明收口」最小方案（方案 b）。

背景与判定
主仓 wbitmap/src/bit_op.rs 头部注释声称「Rust 侧无稳定跨平台向量抽象，以 u64 字批处理承接」，与本仓 simd 选型（SKILL 明文 simd 用 fearless_simd，且 wbase::simd 已有 fast_key_eq 走 fearless_simd dispatch）自相矛盾。该表述会误导后续维护永久停在标量路径。C# 对位为 BitmapManagerBitOp.cs 的 Vector512/256/128 三档 8 路折叠。BITCOUNT 侧（wbitmap/src/bit_count.rs）以 u64::count_ones 承接硬件 POPCNT 的声明成立，保留不动。

改动范围（严格限定，越界即停）
仅修正 wbitmap/src/bit_op.rs 头部承接声明注释，改为显式取舍：位折叠以 u64 字批处理 + 编译器自动向量化等效承接，批宽形态保留、语义逐位一致；本仓 fearless_simd 选型面覆盖键比对（wbase::simd::fast_key_eq），BITOP 位折叠刻意走标量批处理。另在 wlua/src/hash_key.rs 的 ScriptHashKey equals 处补一句同款取舍声明（[u8;40] 切片相等 memcmp，正确性优于 C# 双载 Vector256，性能面刻意走编译器自动向量化）。全仓 grep 不得残留「无稳定跨平台向量抽象」这类与选型矛盾的表述。

明确禁止
不得扩展或修改 wbase::simd（该文件与在途 bitcount-simd-dispatch-parity 分支共用，改动会撞车）；不得改 bit_count.rs（BITCOUNT 承接成立）；不做换装实现，纯声明收口。

优先级
打磨（消除自相矛盾的维护误导注释）。

验收
grep 全仓无矛盾表述；cargo check --workspace --all-targets 绿。

甄别结论（本轮 dev 复核，认领基线 dev 066dc8a；三条观点逐条取证）
条一 成立：/Users/z/git/db/wedb/wedb/wbitmap/src/bit_op.rs:5 原文「Rust 侧无稳定跨平台向量抽象，
以 u64 字批处理承接」在位（全仓该表述仅此一处命中），与 .agents/skills/transpile/SKILL.md:25
「simd 用 fearless_simd」及本仓 SIMD 单点 wedb/wbase/src/simd.rs:13、:28（该模块唯一 pub 面
fast_key_eq 走 fearless_simd dispatch，wbase feature `simd`，由 wrecord/wval 启用，消费面
wedb/wval/src/ns_codec.rs:186 等）直接冲突。C# 对位属实：
garnet/libs/server/Resp/Bitmap/BitmapManagerBitOp.cs:87/:96/:105 三档运行期探测 + `Count * 8`
批宽，:115 标量 8 字节×4。
条二 成立但措辞修订：ScriptHashKey::equals（wedb/wlua/src/hash_key.rs:93）确无取舍声明；票面
「正确性优于 C# 双载 Vector256」不成立（两侧逐位同果，差异只在 C# 的 Debug.Assert 与裸指针前提），
剪入 task/reject/simd-selection-single-claim.md，落地按事实改写。
条三 属验收项非改动项：同口径的其余声明面 wedb/wbitmap/src/bit_count.rs:5、:8-11 与
wedb/wbase/src/simd.rs:1-10 本就自洽，不另立第二处口径。

落地（分支 9763e43 → dev 合并 6311510）
- wedb/wbitmap/src/bit_op.rs:4-9 头注改为显式取舍：位折叠刻意走 u64 字批处理、三档批宽形态保留
  （最宽档依次消化对齐前缀）、剩余走标量字批与逐字节尾部、逐位结果一致；fearless_simd 选型面
  = 本仓 SIMD 单点 `wbase::simd` 的变长键比对；实际向量宽度由编译期 target-cpu 档位经自动向量化
  承接，故不复刻 C# 的运行时三档探测。措辞沿用 bit_count.rs:9 的「本仓 SIMD 单点 `wbase::simd`」，
  两处口径同名。
- wedb/wlua/src/hash_key.rs:83-91 equals 补同款取舍：定长 `[u8; 40]` 缓冲整段比较，等价于 C#
  ScriptHashKey.cs:55-61 双载 Vector256（0..32 与 8..40 重叠覆盖）的结果，本类型自带长度不变式、
  无指针保活前提，向量宽度交编译期自动向量化，不引向量臂。
- 禁止项照守：未触碰 wedb/wbase/src/simd.rs、wedb/wbitmap/src/bit_count.rs；无换装实现——
  vectorized512/256/128/vectorized_n（bit_op.rs:336/:349/:362/:373）与 u64 字批形态原样在位。

审查（rust_review 口径）
- 单套机制：simd 选型承接声明收敛为一处实现单点（wbase::simd）加三处消费侧取舍口径
  （bit_count.rs:8-11 二路分派、bit_op.rs:4-9 不引向量臂、hash_key.rs:87-91 定长不引向量臂），
  同称「本仓 SIMD 单点」，全仓无相反表述。
- 清死代码：三档 vectorizedNNN 非死码（bit_op.rs:271/:277/:283 级联调用），系 C# 三档的形态对位。
  记一句免后人误判——标量 u64 承接下 512/256/128 级联的净效果等价于按 128 对齐前缀消化，
  逐字节结果不变；删档不减少语义，但会断掉与 C# Vectorized512/256/128 的锚点对位，本票按
  「批宽形态保留」不动。
- 复杂度对标 C#：两侧同为单遍 O(n)、常数因子取决于实际向量宽度；本仓以编译期档位换掉 C# 的
  运行时三档探测，无额外遍历、无新增分配。

验收（本轮实测）
- grep「无稳定跨平台向量抽象」：/Users/z/git/db/wedb/wedb 下 0 命中（仅 task/ 档案与拒绝票引原文）。
- 纯注释证明：分支提交的 diff 过滤 `///`/`//!` 后零行；diffstat 2 files changed, 11 insertions(+),
  3 deletions(-)。
- cargo check --workspace --all-targets exit 0，零 error 零 warning（worktree 私有 target
  /tmp/fork/simd-selection-single-claim/target，未与他代理共享）。
- bun js/check.js exit 0，js/check/ignore 语料零改动（worktree 内前后逐字节相同）。

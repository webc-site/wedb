甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 可见性现码亲验：bit_count.rs:142 `pub fn __scalar_popc` 与 :179 `pub fn __simd_popc_x256` 全 pub 在位；lib.rs:6 `pub mod bit_count` 下模块路径可达，两函数不在 lib.rs pub use 再导出清单。
2 零外部消费复跑：全 wedb grep __scalar_popc/__simd_popc_x256 排除 bit_count.rs 后零命中（含全部 tests/）——pub 面纯冗余属实。
3 C# 锚亲验：BitmapManagerBitCount.cs 现读 :64 `public static long BitCountDriver`、:131 `private static long __scalar_popc`、:328 `private static long __simd_popcX256`——private 对位形态坐实。
4 查重：deviations.md popc 命中系 §92 BITCOUNT 中间字节语义（不涉可见性）；四池无同轴票（w2-gate-red 的 popcnt 系 ZPOPCOUNT 命令面，正交）。
5 架构合规与可执行度：删 pub 即同文件驱动与 use super 测试均不破的最小收敛，符合「接口最小暴露/零死代码 pub API 孤儿清理」纪律；验证闭环（cargo build/test -p wbitmap 全绿 + grep 归零 + clippy 无死码警告）。定级 P4：API 面卫生，无运行期危害。

审核结论：通过（审核席 zcode-r22-review-popc，阶段三独立审核）

审核亲验记录：
1. 可见性属实：bit_count.rs:142 __scalar_popc 与 :179 __simd_popc_x256 均为全 pub fn，位于 lib.rs:6 的 pub mod bit_count 之下（原票引 lib.rs:8 系行号笔误，实际 :6，不影响结论），外部路径 wbitmap::bit_count::__scalar_popc / __simd_popc_x256 可达；两函数不在 lib.rs pub use 再导出清单。
2. 零外部消费属实：全仓 grep（含全部 crate 与全部 tests/）命中仅 bit_count.rs 单文件内 7 处——驱动分派 :132/:134、注释 :141、测试 use super :280 与断言 :319/:320，无任何跨文件跨 crate 消费。
3. C# 对位属实：BitmapManagerBitCount.cs 全文件仅 :64 BitCountDriver 为 public static，:131 __scalar_popc、:186 __simd_popcX128、:328 __simd_popcX256 均 private static，行号与票面一致。
4. 同 crate 惯例属实：manager.rs 内部助手 pub(crate)/私有混用（index/process_negative_offset/try_validate_bitfield_offset 为 pub(crate)，try_validate_length_in_bytes 为私有 fn，均非全 pub），bit_op.rs 测试 oracle 全部 #[cfg(test)]，bit_count.rs 内 bit_index_count 为 pub(crate)、其余助手私有；此两函数确系 crate 内唯一全 pub 非再导出内部实现函数。
5. 查重：deviations.md 无可见性相关登记（§92 系 BITCOUNT 中间整字节计数语义，不涉可见性），r15-r22 历史档无同题在票，todo/ing 无重复。
6. 方案最小性：删 pub 改私有后同模块驱动调用与同文件 #[cfg(test)] mod tests 的 use super 均可访问私有项，零行为零测试改动，为最小闭环形态。

优化执行方案（供 task/fix.md 直接消费）：
1. wedb/wbitmap/src/bit_count.rs:142 将 pub fn __scalar_popc 改为 fn __scalar_popc
2. wedb/wbitmap/src/bit_count.rs:179 将 pub fn __simd_popc_x256 改为 fn __simd_popc_x256
3. lib.rs 零改动（两函数不在 pub use 清单，仅模块路径可达性收敛）
4. 验证：cargo build -p wbitmap 与 cargo test -p wbitmap 全绿（零测试改动）；全仓 grep __scalar_popc 与 __simd_popc_x256 确认除 wedb/wbitmap/src/bit_count.rs 外零引用；跑 ./sh/clippy.sh 确认无死代码或未使用警告

BITCOUNT 计数内核 __scalar_popc 与 __simd_popc_x256 全 pub 暴露（内部实现函数零外部消费，裸索引契约直达公共 API 面）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 侧 __scalar_popc（BitmapManagerBitCount.cs:131）与 __simd_popcX128（:186）、__simd_popcX256（:328）均为 BitmapManager partial 类的 private static 方法，仅由同类 BitCountDriver（:113-120）内部分派调用，不构成任何公开契约面；双下划线前缀即上游私有约定。SIMD 档位的 ISA 探测分派也全部封闭在类内部。Rust 转写对位可见性应为 crate 内部（私有或 pub(crate)），不应对外暴露。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wbitmap/src/bit_count.rs 的 __scalar_popc（:142 pub fn）与 __simd_popc_x256（:179 pub fn）以全 pub 暴露于 lib.rs:8 的 pub mod bit_count 之下，对外路径 wbitmap::bit_count::__scalar_popc / __simd_popc_x256 可达。全仓穷举 grep（wnode/wkv/wcol/wbase/wedb 全部 crate 及全部 tests/，含 wnode/tests/garnet_bitmap.rs 等 21 个位图集成测试）除本文件内 bit_count_driver 调用与同文件 tests use super 自证外零外部消费。对照同 crate 惯例：manager.rs 内部函数一律 pub(crate)（index/process_negative_offset/reverse/try_validate_bitfield_offset/try_validate_length_in_bytes），bit_op.rs 测试 oracle（invoke_bit_operation_unsafe/invoke_nary_bitwise_operation/vectorized_n 及四个算子结构）全部 #[cfg(test)]，此两函数是 wbitmap crate 内唯一以全 pub 暴露的内部实现函数，违 review.md 板块 1「接口最小暴露」与 rust_review「crate 的暴露接口要设计，低耦合高内聚」「零死代码：pub API 孤儿定期清理」。wbitmap 为带完整发布元数据的独立 crate，公共 API 即对外承诺；两函数携带裸索引契约（bitmap[start..=end] 须界内且 start <= end，违约即下溢回绕或切片越界 panic），该无契约保障的 panic 面不应出现在公共 API 上。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
纯治理面缺陷，无运行时数据危害。危害一：公共 API 面污染——外部消费者一旦误用（start > end 时 len = end - start + 1 在 usize 域下溢，release 回绕巨值后 u64_read 读越界、debug 直接 panic；end 越界同理），panic 面经 pub 路径直达。危害二：契约固化风险——后续轮次若按「已导出即公共契约」口径视为稳定 API，内部重构（如分派收口调整、popc_tail 合并）即构成破坏性变更，锁死实现自由度。危害三：与同 crate 既有可见性纪律（pub(crate)/#[cfg(test)]）不一致，属纪律破口样板。

涉及代码：
rust 文件与函数：
wedb/wbitmap/src/bit_count.rs:__scalar_popc
wedb/wbitmap/src/bit_count.rs:__simd_popc_x256

对应 c# 文件与函数：
garnet/libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__scalar_popc
garnet/libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX256
（参照系：C# private static，Rust 侧无对应导出面即为对位形态）

精炼执行方案：
1. 删除两函数的 pub 关键字改为私有（当前仅同文件 bit_count_driver 与同文件 tests 消费，私有即最优；若未来需跨模块再加 pub(crate)，不预支）
2. lib.rs 无需改动（两函数本就不在 pub use 再导出清单，仅模块路径可达性收敛）
3. 测试验证点：cargo build -p wbitmap 与 cargo test -p wbitmap 全绿（同文件 tests use super 可访问私有函数，零测试改动）；全仓 grep __scalar_popc 与 __simd_popc_x256 确认除 wbitmap/src/bit_count.rs 外零残留引用

合入哈希：1e6d84f 收口形态：bit_count.rs 两 popc 内部函数删 pub 改私有，lib.rs 零改动，cargo check 零告＋test -p wbitmap 24 绿，全仓 grep 除本文件外零残留

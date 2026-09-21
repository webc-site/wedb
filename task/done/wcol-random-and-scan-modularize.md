# 细化方案：wcol-random-and-scan-modularize

## 目标
将通用随机函数（`pick_k_random_indexes`, `pick_random_index`）和 `scan_operate_shared` 从 `hash/hash_object.rs` 抽离到 `types/` 下的公共模块，以解除 `set` 和 `zset` 模块对 `hash` 的依赖。

## 步骤
1. 在 `wedb/wcol/src/types/` 下新建 `random_utils.rs`，并将 `pick_k_random_indexes`, `pick_random_index` 及其测试移动至此。
2. 在 `wedb/wcol/src/types/` 下新建 `scan_utils.rs` (或者放到现有的相关文件如 `scan_input.rs` 等)，并将 `scan_operate_shared` 移动至此。
3. 在 `wedb/wcol/src/types/mod.rs` 中声明这些新模块。
4. 修改 `hash_object.rs`, `hash_object_impl.rs`, `set_object_impl.rs`, `set_object.rs`, `sorted_set_object_impl.rs` 等处的 import 语句，将引用指向新的模块。
5. 运行 `cargo check` 确保无误。

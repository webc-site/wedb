# wcol 跨集合通用随机抽取与扫描逻辑模块化抽离

来源：next/zcode.design.md 问题 12

## 问题

通用随机无偏采样函数 pick_k_random_indexes、pick_random_index 以及跨集合共用的扫描求值内核 scan_operate_shared，
全部存放在 hash_object.rs 文件内，导致 set_object 与 sorted_set_object 必须跨模块依赖 hash。

## 涉及路径

- wedb/wcol/src/hash/hash_object.rs
- wedb/wcol/src/set/set_object.rs
- wedb/wcol/src/zset/sorted_set_object.rs
- wedb/wcol/src/types/

## 解决建议

1. 将随机抽取函数抽离至独立的公共模块（如 wcol::types::random_utils）。
2. 将 scan_operate_shared 抽离至 wcol::types::scan_input 或类似公共位置。
3. 解除 set 和 zset 对 hash_object 的不当依赖。

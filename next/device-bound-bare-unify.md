# wnode 中 Device 泛型约束规范为 bare 导入形式

来源：next/zcode-r5-api.md 问题 8

## 问题

wnode 内部存在 `D: wdev::Device` 与 `D: Device` 两种泛型声明混用的情况（各约 200 处）。
而 waof、wcpr、whlog、wcompact、wkv、wedb 统一采用 use wdev::Device 加 bare `D: Device`。

## 涉及路径

- wedb/wnode/src/resp/
- wedb/wnode/src/storage/
- wedb/wnode/src/aof/

## 解决建议

1. 在相关模块头统一 `use wdev::Device;`。
2. 将函数签名中的 `D: wdev::Device` 机械式规整为 `D: Device`。

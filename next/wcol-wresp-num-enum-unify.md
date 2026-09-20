# wcol 与 wresp 统一枚举转数值派生为 strum::FromRepr

来源：next/zcode.design.md 问题 9

## 问题

全局已引入 strum 实现无分支安全的 FromRepr 转换。
而 wcol 与 wresp 各自在 Cargo.toml 引入了 num_enum 依赖，
并在对象 operation 枚举与 RespCommand 上使用 TryFromPrimitive，导致两套宏机制并存。

## 涉及路径

- wedb/wcol/Cargo.toml
- wedb/wresp/Cargo.toml
- wedb/wcol/src/
- wedb/wresp/src/

## 解决建议

1. 移除 wcol 和 wresp 对 num_enum 的私有依赖。
2. 将相关枚举统一变更为 derive(strum::FromRepr)。

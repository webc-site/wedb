方案细化
1. 查找并清理 wcol 和 wresp 中的 num_enum 依赖，统一使用 strum。
2. 移除相关文件中的 TryFromPrimitive 派生，替换为 strum 的 FromRepr 派生。
3. 如果使用 TryFrom 转换的地方，修改为从 from_repr 获取并处理 Option，或者实现相关的 TryFrom trait。
4. 确保所有修改通过 cargo check。

# wkv 物理键读路径中间结果枚举精简

来源：next/zcode.db.md 问题 9

## 问题

wkv/src/session/raw/read.rs 堆叠了 5 套中间枚举（MemRead、RcWalk、MemBack、StoreResult、ReadProbeResult），
在模式匹配与转换间产生较多分支与胶水代码。

## 涉及路径

- wedb/wkv/src/session/raw/read.rs

## 解决建议

1. 精简 read.rs 内部状态机，减少胶水枚举转换层级。
2. 保持对外返回类型不变，提升单点读代码整洁度与内联效率。

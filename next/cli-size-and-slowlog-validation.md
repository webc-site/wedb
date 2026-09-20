# 启动参数尺寸串与 slow_log_threshold 严格校验

来源：next/zcode-r6-cli.md 问题 2 与问题 9

## 问题

1. aof-size-limit 与 index-max-size 在尺寸解析失败时返回 None 并静默跳过后台任务，未报错拒启。
2. slow_log_threshold 在 (0, 100) 开区间时未按规范阻断启动（C# 要求至少 100 微秒）。

## 涉及路径

- wedb/wconf/src/node_options.rs
- wedb/wnode/src/service.rs

## 解决建议

1. 在 NodeArgs::validate 中对 size 字符串进行预校验，格式非法或小于 64B 下限直接抛出 NodeOptionsError 拒启。
2. 对 slow_log_threshold 增加大于 0 且小于 100 微秒时的校验阻断。

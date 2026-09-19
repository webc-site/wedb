优先级：中（功能缺口，非死码）

wconf 的页大小下限常量 MIN_PAGE_SIZE_BYTES 定义后无任何生产读者，C# 侧承担该职责的是
「校验并换算页大小」这一步，rust 侧换算在位、下限校验缺席。

rust：wedb/wconf/src/size.rs:14 `pub const MIN_PAGE_SIZE_BYTES: i64 = 512;`
全仓（含 tests/、js/、sh/）仅两处命中，且都在自己的断言里：
wedb/wconf/tests/garnet_server_config_tests.rs:12（import）、:221（`assert_eq!(MIN_PAGE_SIZE_BYTES, 512)`）
零生产读者。同文件的 `next_power_of_2` / `log2_exact` / `parse_size_bytes` 有真实消费者
（wedb/wnode/src/aof/aof_settings.rs:61-86 经 garnet_log/mod.rs:72 装配），说明缺的不是这套工具
而是「下限」这条规则本身。

c#：garnet/libs/server/Servers/ServerOptions.cs:152 `public const long MinPageSizeBytes = 512L;`
由 :155-164 的 `ValidateAndConvertPageSize(value, propName, ...)` 消费：向下取幂之后
`adjustedSize < MinPageSizeBytes` 即抛异常，文案点名「must be at least 512 bytes to ensure a
worst-case record fits within a single page」；garnet/libs/server/Servers/GarnetServerOptions.cs:964
的 read-cache page size 走同一入口复用该下限。

判据：本票不接受「删常量了事」——C# 该常量有真实语义，属未接线的功能缺口；也不得把它当
重复常量并入别处。要做的是在页大小投影点补 C# 同款下限校验（含取幂后判定与逐属性名文案），
让 MIN_PAGE_SIZE_BYTES 成为唯一真源，并加一条确定性回归用例证伪「256 页大小被静默接受」。
接线点需自行按符号复核（候选：wedb/wconf/src/node_options.rs 的页尺寸投影段与
wnode/src/aof/aof_settings.rs 的消费侧），注意别与 host 配置校验门重复实现两套。

来源：第 12 轮 wconf-pretty-size-zero-consumer 票（已判拒，见 task/reject/
wconf-pretty-size-zero-consumer.md）收口时新暴露的残余项。

拒绝：wconf pretty_size 生产消费者已在场（票据陈旧）；MIN_PAGE_SIZE_BYTES 属未接线功能缺口，不得当死码删

结论：票据判据不成立，rm task/ing 本票，不改码、不建分支、不合并。

== 拒绝原因 ==

1. 立论前提「pretty_size 生产视图零引用」在当前 dev 已为假。
   - 生产读点 10 处：wedb/wnode/src/aof/aof_settings.rs:61/:62/:63（校验一 常驻窗口须容两页）、
     :72/:73（校验二 页不大于段）、:83/:84/:85/:86（校验三 页容主存记录镜像）、:132（尺寸 Display
     投影），另有同文件 :149/:208 测试面复用；定义 wedb/wconf/src/size.rs:112。
   - 可达性：AofSettings::from_options（锚点 C# GarnetServerOptions.cs:GetAofSettings）由
     wedb/wnode/src/aof/garnet_log/mod.rs:72 装配期真调（GarnetLog::new 链），非用例面。
   - 对位即本票修法：C# GarnetServerOptions.cs:1065-1100 正是用 PrettySize 拼三旋钮体检文案，
     rust 同位同形已落地——票据自述的读者清单（wconf/tests/garnet_server_config_tests.rs:12/
     :161-:165/:179）与现状（同文件 :12、:203-:207）已漂移，属并行会话（aof-settings 尺寸单点）
     先落地后的陈旧快照，无本票可做的事。

2. 残余点 MIN_PAGE_SIZE_BYTES（wedb/wconf/src/size.rs:14）不构成删除依据：
   - C# 侧确有该常量 ServerOptions.cs:152，真消费者为同文件 :157 ValidatedPageSizeBits
     （:163-:164 校验页容量下限并抛错拒启），GarnetServerOptions.cs:966 ReadCachePageSizeBits 转调。
   - rust 主存页校验口 wedb/wconf/src/node_options.rs:156-160 现仅校验「2 的幂 + 扇区对齐」，
     未接 512 下限：写侧常量在场、读侧校验待接线，按甄别准则属功能缺口，禁当死码删。
   - 本票两条修法对此常量均无适用面：收 #[cfg(test)] 会把 C# 真实存在的常量抹成测试专用，
     ignore 登记 Utility.cs:PrettySize 不转写更与事实相悖（该函数已转写且已投产消费）。

3. 处置：本票「零生产消费者 + C# 无对位」判据双双失效，全单不成立。MIN_PAGE_SIZE_BYTES 的
   512 下限接线（对位 C# ServerOptions.cs:ValidatedPageSizeBits，落点 wconf hlog 页容量校验）
   属另一命题，另立新题，不并入本「死表面」票。

== 原文 ==

优先级：低
分拣注记（qw.design 第 11 轮条 7 拆出；浅核 2026-09-19：pretty_size wconf/src/size.rs:112、MIN_PAGE_SIZE_BYTES :14 在场；台账无同题票）

尺寸格式化单点在场零生产消费者：wconf pretty_size 的 C# 对位正是启动日志装配真调
问题：wconf/src/size.rs:112 pub pretty_size（锚点 libs/client/Utility.cs:PrettySize）与同文件 :14
MIN_PAGE_SIZE_BYTES 生产视图零引用，读者仅 wconf/tests/garnet_server_config_tests.rs:12/:161-:165/:179；
C# 侧该格式化器真用于装配期日志（GarnetServerOptions.cs:803 "[Store] Using disk segment size of {SegmentSize}"），
rust 装配段（wnode/src/service.rs 设备/日志规划处）不打尺寸行，函数沦为纯用例面。
修法：装配期尺寸/页容量日志行走该单点，或收 #[cfg(test)] 并在 ignore 登记 Utility.cs:PrettySize 不转写。
c#：garnet/libs/client/Utility.cs:PrettySize；消费者 garnet/libs/server/Servers/GarnetServerOptions.cs:803

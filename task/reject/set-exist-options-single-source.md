set 存在性解析单点悬空：ExistOptions 死组与命令层手写双轨（甄别拒绝）

来源：next/set-exist-options-single-source.md（观点为 AI 生成，经对照 HEAD 代码与 garnet C# 甄别后拒绝）。

结论：不成立，拒绝。票据的两个事实前提（wresp 单点零生产消费、SET 存在性参数由命令层逐 token 手写）
在 HEAD 均已不存在，其修订方向「方案 a」已是仓库现状，属已实现，不另开票。


拒绝原因（HEAD 取证，dev a7402c4；git diff HEAD 对 wedb/wnode/src/resp/basic_commands/set.rs 与
wedb/wresp/src/options.rs 零输出，即下述为已提交状态，非他人在途脏改）

1. 单点已在产，非死组。wresp::ExistOptions（wedb/wresp/src/options.rs:228）与
   try_get_exist_options（同文件 :237）被生产代码导入并消费：
   wedb/wnode/src/resp/basic_commands/set.rs:10 导入，:660 初始化 exist_options，
   :709 解析 token，:710 判重复 NX/XX，:730/:731/:738 组合派发。
   票据「全仓生产零消费（仅 options.rs 自身测试）」与事实不符。

2. 命令层手写第二套不存在。HEAD 的 set.rs:706-721 是
   `if let Some(opt) = try_get_exist_options(next_opt)`，票据所引 :707-725 以
   `next_opt.eq_ignore_ascii_case(b"NX"/b"XX")` 逐 token 判定 exist_nx/exist_xx 的形态在 HEAD 查无；
   全仓非测试代码中 NX/XX 的 eq_ignore_ascii_case 命中仅剩 wext_json（见「越界面」）与 geo 的
   STORE 选项（wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:447/:449/:781/:783，
   C# GeoRadiusStore 同为就地解析，非本票对象）。

3. 「同一命令两个参数族解析路径分叉」不成立。set.rs 过期档 :672 走
   wresp::try_get_expiration_option，存在性档 :709 走 wresp::try_get_exist_options，
   两族同轨同源。set.rs:715 保留的 `eq_ignore_ascii_case(b"GET")` 不是第二套存在性解析：
   C# 亦单独跟踪 getValue（garnet/libs/server/Resp/BasicCommands.cs:686-689），
   票据原文 :15 自己亦注明「GET 不属 ExistOptions」。

4. SET 选项解析只有一个入口。parse_set_options（set.rs:650）为唯一实现，
   两个调用点共用：set.rs:398（network_setexnx）与
   wedb/wnode/src/resp/garnet_api/raw.rs:84。不存在绕过单点的旁路解析。

5. 复杂度不高于 C#，反而已达对标。garnet 全仓 ExistOptions 仅一处定义
   （garnet/libs/server/Resp/RespEnums.cs:16），SET 消费同一枚举但 NX/XX 判定就地
   SequenceEqual（BasicCommands.cs:665-684）；TryGetExistOption
   （garnet/libs/server/Custom/ObjectInputExtensions.cs:20）在 C# 只有 GarnetJSON 一处消费
   （garnet/modules/GarnetJSON/JsonCommands.cs:59）。即 C# 本无「SET 必须经共享解析器」这一层，
   rust 把解析器也收进 wresp 属对齐 transpile SKILL.md :65「一处定义」的正当形态，
   不存在需要回退的双轨，也没有「C# 有、rust 缺」的单点。

6. 票据自身验收已过（原文 :30）：「grep ExistOptions 全仓要么有 wnode 生产消费、要么 wresp 侧归零」
   ——wnode 生产消费成立（第 1 条）；「SET NX/XX/KEEPTTL 组合回归全绿」——组合派发
   set.rs:726-745 未改动。方案 b（删 wresp 单点 + 登记 ignore）与现状相反，会制造倒退。


越界面（本票未主张，另议，勿在本票下动手）

- wedb/wext_json/src/json_object.rs:18 另立 ExistOptions：C# GarnetJSON 复用 Garnet.server
  的同一枚举（GarnetJsonObject.cs:351 参数类型即 RespEnums.cs:16），rust 属跨 crate 重复定义。
  但它是活链（json_object.rs:258-326、json_commands.rs:579-622 生产消费），且票据原文 :12-13
  已自判「不同 crate 的类型，非本单点」，不在本票范围。
- 命令层裸内联 token 字面量的跨命令族问题由 next/glm.design.md:59 一条管，
  该条（:59 末）已声明与本票的边界。


原文（照录）

wresp SET 存在性解析单点悬空：ExistOptions 死组与命令层手写双轨

来源：设计审查轮次甄别（原 qcode 条 16，票据已核销删除）。

结论
wresp::ExistOptions + try_get_exist_options 全仓生产零消费（仅 options.rs 自身测试），SET 的存在性参数改由
命令层逐 token 手写字符串比对；而同文件的过期参数解析走 wresp 单点，同一命令两个参数族解析路径分叉，构成协议
解析双轨。判定成立且待做。

现状（HEAD 取证）
- 死单点：wresp/src/options.rs:234 pub enum ExistOptions、:243 pub fn try_get_exist_options（消费仅同文件
  测试 :375-380）；全仓其它 ExistOptions 命中为 wext_json::json_object::ExistOptions（不同 crate 的类型，
  非本单点），以及 set.rs:729 的注释文字。
- 命令层手写：wnode/src/resp/basic_commands/set.rs:707-725 以 next_opt.eq_ignore_ascii_case(b"NX"/b"XX"/b"GET")
  逐 token 判定 exist_nx/exist_xx/get_value，:734+ 再手拼 SetCmd::SetExNx/SetExXx 组合派发。
- 同文件对照活链：ExpirationOption/try_get_expiration_option 被 basic_etag_commands.rs:22、
  session_parse_state_extensions.rs:298 真实消费，证明同族单点已接线、唯 NX/XX 档悬空。

C# 参考
- garnet/libs/server/Resp/RespEnums.cs:16 ExistOptions；garnet/libs/server/Resp/BasicCommands.cs:613-678
  （SET 解析消费同一 ExistOptions）；garnet/libs/server/Custom/ObjectInputExtensions.cs:20 TryGetExistOption。

修订方向
二选一禁止双轨。
方案 a：set.rs NX/XX 解析改走 wresp::try_get_exist_options 单点（对齐同文件 TTL 档已接线的现状）。
方案 b：判定不收敛则删 wresp ExistOptions/try_get_exist_options，并在 js/check/ignore 登记
RespEnums.cs:ExistOptions（理由：命令层就地解析）。

验收
grep ExistOptions 全仓要么有 wnode 生产消费、要么 wresp 侧归零；SET NX/XX/KEEPTTL 组合回归全绿。

优先级
死代码（悬空单点）+ 重复/多套架构（同命令两套参数解析路径）。

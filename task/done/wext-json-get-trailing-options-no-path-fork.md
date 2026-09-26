甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P4
核验记录：C# 亲验——JsonCommands.cs:131-161 选项循环三选项匹配均附 offset < parseState.Count 门，收尾抵尾落 :150-154 `offset > parseState.Count` → AbortWithWrongNumberOfArguments（现树逐行见）。rust 亲验——set_get.rs 选项循环 `while let [opt, val, ..]` 消费完切片耗尽自然退出、paths 空切片、json_object.rs:184-190 try_get 零路径臂回全量文档——分叉现码仍存。查重：§15 a)-e) 逐款核对（正则崩溃/引号 operand/键名转义/decode 防御帧/过滤器臂澄记）无一覆盖该形态；四池零命中。架构：登记级零代码改动、范围严格限定（悬空单选项带不入本案）防文案扩写，测试锁两形态+参照用例闭环。格式：纯文本、双侧路径齐全。定级 P4：修复 C# 收口缺陷方向的在册化补登，零行为改动。

审核结论：通过（登记级，零代码改动方向获裁；C# :131-161 循环与 offset 越界推进、rust 零路径臂回全量美化文档均亲验属实；§15 a)-e) 确不覆盖该形）

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. deviations.md §15 增补 f) 款：登记 JSON.GET 选项带值收尾无路径形态（C# JsonCommands.cs:151-153 wrong-num-args 对 rust set_get.rs:172-186+json_object.rs:170-179 全量美化文档），注明修复 C# 收口缺陷、对齐 RedisJSON、严禁按 C# 缺陷形态回改。
2. 登记范围严格限定为「选项对完整消费后参数耗尽」形；悬空单选项带（JSON.GET k INDENT）rust 按路径处理与 C# 同落路径错误臂，不入本案，勿扩写文案。
3. 测试锁：json_deviation_locks 族补两形态命令级断言——JSON.GET k INDENT "  " 与 JSON.GET k INDENT i NEWLINE n 回全量美化 bulk string；保留 JSON.GET k INDENT i $.a 正常路径用例作参照。

JSON.GET 选项尾随无路径形态分叉：C# 报 wrong number of arguments，rust 回全量文档

问题分析：
1. Garnet 契约对齐：C# JsonGET.Reader 选项循环（JsonCommands.cs:131-161）每轮
   先 GetNextArg 取 token；选项与值成对消费后若抵达参数尾（GetNextArg 越界回空
   span，CustomCommandUtils.cs:38-45），空 token 不匹配三选项名，落入
   `offset > parseState.Count` 判定（:151）即 AbortWithWrongNumberOfArguments
   回 "ERR wrong number of arguments for 'json.get' command"。故
   `JSON.GET key INDENT "  "`（选项带值收尾、无路径）C# 回参数错误帧。
2. 工程现状确证：rust set_get.rs:172-186 选项循环用 `while let [opt, val, ..]`
   匹配剩余切片，消费完 INDENT+值后切片耗尽自然退出循环，paths 取空切片，
   try_get 零路径臂（json_object.rs:170-179）回全量美化文档，正常成功应答。
3. 逻辑危害确证：同一命令形态一侧错误帧一侧成功文档，参数收口契约分叉且无
   登记（§15 五组偏离不含此项；无测试锁该形态——双侧均无）。rust 形态恰与
   RedisJSON 标准一致（路径缺省合法），属修复 C# 收口缺陷的偏离，按本仓惯例
   （§15 族）应登记台账而非留作未申报分叉，防后续审查席误判转写缺陷或误回改。

涉及代码：
rust 文件与函数：
wedb/wext_json/src/json_commands/set_get.rs:json_get_reader
wedb/wext_json/src/json_object.rs:GarnetJsonObject::try_get（零路径臂）

对应 c# 文件与函数：
garnet/modules/GarnetJSON/JsonCommands.cs:JsonGET.Reader（:131-161 选项循环与 :150-154 错误臂）
garnet/libs/server/Custom/CustomCommandUtils.cs:CustomCommandUtils.GetNextArg

精炼执行方案：
1. 裁定保 rust 形态（对齐 RedisJSON）则在 doc/zh/deviations.md §15 增补 f) 款
   登记该收口分叉，注明 C# 锚与保留理由；如裁定回改，则选项循环尾补
   「选项消费后参数耗尽且无路径」错误臂复刻 wrong-num-args 文案。
2. 测试验证点：`JSON.GET k INDENT "  "`、`JSON.GET k INDENT i NEWLINE n` 两形
   态行为锁死（按裁定方向断言），并保留 `JSON.GET k INDENT i $.a` 正常路径用例。

收口注记：合入哈希：096f287 收口形态：§15 增补 f) 款登记 JSON.GET 选项带值收尾无路径收口分叉（保 rust 全量美化文档形、注明 C# :151-153 锚与严禁回改），命令级 reader 漏斗三形锁落 json_deviation_locks 族，零行为改动

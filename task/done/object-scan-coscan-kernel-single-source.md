object_scan / coscan 同步段与慢路径校验整段镜像复抄，慢段 cmd_name 已漂移缺 All 臂

来源：next/glm.design.md 第 7 轮条 1。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev，行号
按当下代码重取。

结论
C# 的 ObjectScan 是一个泛化于 TGarnetApi 的会话方法，同步执行与 pending 重放走同一函数体，校验
逻辑全仓一份；rust 把它切成同步段（&mut self 方法）与慢路径（自由函数 + StorageSession）两份，
其中不含 IO 的纯校验四段逐字双抄，且慢段已经和同步段漂移（少一个 All 臂），注释却自称「单源同
口径」。判定成立且待做。

现状
文件 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/shared_object_commands.rs（全文件 534 行）。

同步段 object_scan :32 的四段纯校验
- cmd_name match :41-47（Hash → HSCAN / Set → SSCAN / SortedSet → ZSCAN / All → "COSCAN" / _ →
  "NONE"）
- 参数计数门 :49-51（parse_state.len() < 2 → abort_with_wrong_number_of_arguments(cmd_name)）
- 光标非负校验 :54-56（try_parse_i64 且 >= 0，否则 RESP_ERR_GENERIC_INVALIDCURSOR）
- sub_id match :60-68（按 object_type 取各族 sscan/hscan/zset 操作码）

慢段 slow::object_scan :360 的同四段
- cmd_name match :369-373 —— 已漂移：只有 Hash / Set / SortedSet，`All` 落到 `_ => "NONE"`，
  而同步段 :45 有 `GarnetObjectType::All => "COSCAN"`。当前 COSCAN 慢路径经 slow::coscan :505
  以解出的内层三型转调，不触达 All，故暂不出错；任何直传 All 的新调用会回 "NONE" 的错误文案。
- 参数计数门 :376-378、光标校验 :381-383、sub_id match :386-391，逻辑与同步段逐字同构，仅错误
  输出通道从 self.abort_with_* 换成 cs::abort_with_wrong_number_of_arguments /
  cs::write_error_raw。
- 慢段模块头注释 :335 自称「校验与 operate 切片与同步段 object_scan 单源同口径」，实现是复抄，
  声明失真（正是 SKILL:65 禁止的「以注释代替单源」形态）。
- 同文件第三处同谓词字面复抄：scan_object_typed 的参数形态预检 :245
  （`parse_state.len() < 2 || !parse_state[1].try_parse_i64().is_some_and(|v| v >= 0)`），该处
  走 self.object_scan 出错误，属薄壳，收口时应一并吃进内核谓词。

三域判定链同样双写
- 同步段 network_coscan :146-225：String 域命中 → WRONGTYPE（:163-166）→ 信封域值首字节内层
  标签（:167-170）→ Meta 域 collection_type 三型门（:172-186）→ 三域皆缺回
  `[0, 空数组]`（:206-211）→ 按标签转调各族 network_*（:213-222）。
- 慢段 slow::coscan :459-533：同一判定链以 storage.read_tag_with 异步原语重写，注释 :458 自称
  「对位同步段 network_coscan」。两侧的差异只在探测原语（同步 read_user_sync / read_envelope_sync
  / read_tag_sync 对异步 read_tag_with）与降级口径（同步段 Ok(false) 让位慢路径、慢段 Err(())
  交 exec_slow 统一应答），判定骨架与输出字节完全同构。

仓内既有惯例支持本修法
同域同步/慢双写的正解是「内核单源 + 双域薄壳」：
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/set_commands.rs:853 慢段模块头自述「装载/折叠核
与同步段单源复用」，/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1070
同形态。本文件是逆例。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Resp/Objects/SharedObjectCommands.cs:18
  `ObjectScan<TGarnetApi>(GarnetObjectType objectType, ref TGarnetApi storageApi)`：参数计数门
  :22-34（内含 cmdName switch :24-32，含 `GarnetObjectType.All => nameof(RespCommand.COSCAN)`）、
  光标校验 :40-43、子命令 switch :49-66、NOTFOUND → [0, 空数组] :79 起。单一函数同时服务同步与
  重放路径，异步性由 TGarnetApi 泛型承接，不在会话层复制校验。

修法
1. 抽无 IO 的共享校验内核（建议同文件私有 fn 或 object_store_utils 里的 pub(crate) fn），入参
   object_type + parse_state，返回校验结果枚举（成功携带 cmd_name / sub_id / args 切片，失败携带
   失败类别 WrongNumArgs / InvalidCursor），同步段与慢段各按自己的错误输出通道落帧；All 臂只在
   内核出现一份，漂移面消失。
2. network_coscan 与 slow::coscan 的三域判定抽纯决策函数：把三域探测结果（命中/缺失/降级）作
   为入参、把探测原语作为闭包或 async trait 注入，返回域分类枚举（StringHit / InnerTag(u8) /
   PromotedType(GarnetObjectType) / Absent / Deferred / IoError），两侧各自把枚举映射到
   WRONGTYPE、[0, 空数组]、转调目标与降级信号，保持现降级语义不变。
3. 若内核注入 async 探测代价过高（compio 下 async 闭包对象安全约束），至少完成步 1，并在步 2 退
   而求其次：把三域判定的骨架写成一宏 + 两份实例化（同 C# 单函数两形态），杜绝第二份文案。
4. 收口后在两个入口的文档注释补 `libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan`
   映射，使 js/check.js 的重复定义口径认账。

优先级
重复/多套架构（纯逻辑四段逐字双抄 + 已发生的臂漂移 + 与仓内既有惯例相反）。

边界
garnet-api-slow-path-command-split（现仍在 next/ 分拣中）管 exec_slow_impl 与各族慢分派的函数体规
模拆分，不动校验逻辑归属；本单只收 object_scan / coscan 的校验与域判定内核。COSCAN 慢段的
tiered 装载与双域取口面不在本单射程（分别归 garnet-api-slow-path-command-split 与
provider-store-single-accessor，二者现仍在 next/ 分拣中）。

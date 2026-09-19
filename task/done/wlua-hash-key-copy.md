任务：wlua ScriptHashKey::copy_to 零消费清理（qcode10.design 条 16 漏网符号收口）

结论
票面核心论断经复核成立：rust 侧 copy_to 的读者仅 wedb/wlua/src/hash_key.rs 同文件的
#[cfg(test)] 用例，生产视图零消费；ScriptHashKey 的全部消费点都在 wlua crate 内
（cache.rs 的会话缓存、commands.rs 的命令面），无任何「把摘要写入外部定长缓冲」的
生产需求点，票面修法一不成立，走修法二。落地取最彻底形态：方法与喂它的断言一并
删除，而不是降 pub(crate) 或收 #[cfg(test)]——只喂用例的出口留着仍是死面。

C# 参考
已逐点核实 C# ScriptHashKey.CopyTo（libs/server/Lua/ScriptHashKey.cs:CopyTo）的
生产调用点共三处：libs/server/Lua/SessionScriptCache.cs:TryLoad（digest.CopyTo(into)，
into 为 GC.AllocateUninitializedArray 现建的 pinned 40 字节数组）、
libs/server/Lua/LuaCommands.cs:TryEVAL 与 LuaCommands.cs:NetworkScriptLoad
（digest.CopyTo(newAlloc)）。三处全部同一形态：C# 的 ScriptHashKey 是裸指针视图
（long* ptr 字段），栈上以 new ScriptHashKey(digest.Span) 构造的键出作用域即悬空，
存入 scriptCache / storeScriptCache 这类长生命周期字典前必须把字节复制进 pinned
堆数组再 new ScriptHashKey(into) 重包一层键；其 out 参 digestOnHeap 也是同一
生命周期弥合的配套形态。这是 C# 指针形态的特有机制：rust 的 ScriptHashKey 为内嵌
[u8; 40] 定长缓冲的 Copy 值类型，cache.rs:try_load_runner 直接 insert(*digest)、
commands.rs:try_add 按值登记，值复制自带缓冲、无外部生命周期，故 rust 无对应消费
机制，属「C# 特有机制不移植」。在 js/check/ignore/server.yml 为
libs/server/Lua/ScriptHashKey.cs:CopyTo 登记函数级豁免（理由同上），与该文件既有的
libs/server/InputHeader.cs:CopyTo 豁免形态区分开：那是输入结构体语料收敛，这是
指针视图生命周期特例。

改动
- wedb/wlua/src/hash_key.rs：删 copy_to 方法；用例 digest_to_hex_and_copy 收缩为
  digest_to_hex，仅保留 new / as_str 的摘要转 hex 断言，魔数 40 收 SHA1_HEX_LEN。
- js/check/ignore/server.yml：登记 ScriptHashKey.cs:CopyTo 豁免条。
- 删除 next/wlua-hash-key-copy-to-zero-consumer.md。

边界
本单只处理条 16 唯一漏网符号 copy_to。hash_key.rs 的 as_bytes 具体方法（现全仓
零读者）与 equals 的测试侧消费归属其他票轨，此处不扩面；lib.rs 对
SHA1_HEX_LEN / ScriptHashKey 类型本身的导出不受影响。

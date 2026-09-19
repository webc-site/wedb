拒件：MSET 批量折叠 collect 进堆 Vec 打破零拷贝准则，主张小批量栈上排序

来源：next/agy.my.md 条 16。判定：不成立（既定设计决策 + 准则射程误置）。

拒绝原因
wkv/src/session/mod.rs:899-931 try_upsert_batch_sync 的 `let mut sorted: Vec<(K, V)> = pairs.into_iter().collect();` 是单次指针对收集（元素为切片引用，零数据拷贝），:907 注释明文「栈外单次借用对收集（批量路径允许一次分配，先例 ri_set_batch）」——单次分配是声明的先例决策。SKILL 零拷贝工程准则的射程是读路径 *_with/*_callback 借用消除点查 Vec 分配（SKILL 性能优化条款），不覆盖排序所需的连续缓冲。小批量（<=16 对）栈上固定容量属微优化：一次小 Vec 分配对 MSET 命令（本身过网络与协议解析）量级可忽略，无正确性缺口。另该函数已做前缀单次外提（:905-906 prefix_slice 透传），muse.my 条 10 对 MSET 的「每键一次 varint 重算」指控同不成立。

引证
wedb/wkv/src/session/mod.rs:905-910。C# 对标 libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional（全键锁内批量，C# 亦先物化键集合）。

# zset 与成员级 TTL 账本多处独立复制成员字节导致记账低报

来源：next/zcode-r6-mem.md 问题 1。

## 问题

wcol/src/zset/sorted_set_object.rs 的 SortedSetObject 同时持有
sorted_set: BTreeSet<SortedSetEntry> 与 sorted_set_dict: HashMap，
两个容器各自持有独立的 Vec<u8> 成员字节，装载与插入点
（sorted_set_object_impl.rs 约 355、358）执行 member.clone() 两份；
过期账本 types/expiry_ledger.rs 的 insert（约 57）再复制一份 Vec<u8> 作字典键，
且只记 SLOT*4 不计键字节。

heap_memory_size 侧 account_entry（约 751）只按一份 round_up(member) + 2 槽计账。
真实驻留最多约 2 到 3 倍于记账值，后果是 MEMORY USAGE 低报，
以及升阶/降阶的体积阈值按记账值放行，内存维门限实际放宽同倍数。

hash 域同型：wcol/src/hash/hash_object.rs 的 insert_expiration 不记第二份字段字节。

C# 对位 SortedSetObject.cs / HashObject.cs 的 UpdateSize：
.NET 的 byte[] 是引用共享，字典、SortedSet 视图、expirationTimes 指向同一份数组，
HeapMemorySize 计一份即有效。

## 方案

优先对齐 C# 的引用共享语义（同时消掉插入/装载热路径的 clone）：

1. 成员键字节改为共享句柄承载（Arc<[u8]> 或 hipstr，SKILL 已列 hipstr），
   贯通字典、有序视图与过期账本三处，一处分配多处持有。
2. account_entry 随之回归「计一份」的正确口径；
   expiry_ledger 的键计入实际持有的字节数（不再是固定 SLOT*4）。
3. hash 域按同法处理 insert_expiration 的字段字节。
4. 禁止只加账不改共享：加账只是把低报变成高估的常数，
   真正的收益（少两次 clone 与两份额外驻留）拿不到。

若共享句柄改造在现有比较/借用接口下改动面失控（需大范围改 BTreeSet/HashMap 的
泛型键与借用查找），则退为按真实份数加账，并在 task/reject/ 或本票归档处
登记该取舍与遗留的 clone 热点。

## 验收

1. cargo check -p wcol --tests 零 error 零 warning。
2. 单元测试：构造含 N 个大成员（>64B）的 zset，断言 heap_memory_size 与
   实测驻留同量级（不再低报 2-3 倍）；hash 域同测。
3. 升阶阈值测试：成员数不变、体积超阈值时按真实体积触发升阶。

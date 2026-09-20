# 过期清退双检与清退之间缺键闩导致并发 SET 新值被整键误删

来源：next/zcode-r6-del.md 问题 2。

## 问题

wkv/src/ttl.rs 的 purge_expired 链是 check_expired 读 TTL 判过期后
claim 检查、del_ttl、delete 无条件删数据，全程不取 try_lock_key_exclusive；
gc/ttl_sweep.rs 的 sweep_expired 删除循环与读路径惰删（probe_alive →
check_expired）同走此链。

并发 SET k v 若落在 ttl_of 读取与 del_ttl/delete 之间：SET 已清 TTL 并写入新值
并回 ACK，随后 purge 的 del_ttl 落空、delete 把新值整键墓碑。
即 DELIFFEXPIM 式过期删除退化为无条件删除，用户已确认写入的值丢失。

C# 同一判定在记录 X 锁内对当前记录重判（RMWMethods.cs 的 NeedCopyUpdate 与
InPlaceUpdaterWorker，未过期即 no-op），ArrayKeyIterationFunctions.cs 的
ExpiredKeyDeletionScan.Reader 仅做候选初筛，无此窗口。
rust 侧 expire_at/persist 的持闩注释自述的正是本族竞态，但 purge 链未纳入。

## 方案

1. purge_expired 全程持 try_lock_key_exclusive，并在闩内重读 TTL，
   仍判定过期才执行 del_ttl + delete；失闩即放弃本轮清退（候选留给下一轮扫描），
   与 C# 记录锁内重判同效。
2. check_expired 的调用点不改判定，只把「判过期」到「落删除」的临界区收进同一闩。
3. 同步 SET 的 String 域 del_ttl 清退臂若不纳入同一键闩，闩只挡 expire/persist
   挡不住 SET，窗口只是收窄而不闭合；须一并处理（失闩沿既有 Ok(Err(u64::MAX))
   降级异步收口）。

若核实 rust 已在其他层持闩（例如存储会话层统一持键闩），则整票驳回并登记 task/reject/。

## 验收

1. cargo check -p wkv --tests 零 error 零 warning。
2. 并发测试：会话 A 对带过去 TTL 的键执行 SET，会话 B 同时触发惰删/扫描清退，
   断言 A 回 OK 后键值仍为 v 且无 TTL；ttl_sweep 与惰删两条入口同测。

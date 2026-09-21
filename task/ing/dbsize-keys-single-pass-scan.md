# dbsize-keys-single-pass-scan 开工方案

## 判定

票面在当前树成立（dev @ f1ceba2 核实）：wedb/wnode/src/storage/session/common/
array_key_iteration_functions.rs 中 string_keys_snapshot（:426，逐键 to_vec + GxHashMap
写胜折叠 + 二次 probe_ttl 过滤 + sort_unstable）仍是 db_size（:407）、db_keys（:381）、
delete_slot_keys（:255）三臂唯一遍历内核；live_key_at（:70）为 SCAN /
count_keys_in_slot（:283）/ get_keys_in_slot_with（:325）的链首判定单点；
C# 对位段逐字核对无误（garnet libs/server/Storage/Session/Common/
ArrayKeyIterationFunctions.cs 的 DbSize / DBKeys / DeleteSlotKeys 与内嵌
UnifiedStoreGetDBSize.Reader、UnifiedStoreGetDBKeys.Reader、DeleteSlotKeysScan.Reader，
三者均为扫描回调内判定的零分配迭代器，且 DeleteSlotKeysScan 明言不过滤过期）。

## 活键判定收口选型（票面修法 1 二选一，取后者）

db_size 采用「以 live_key_at 判定单点重写扫描主体」。不复用 keyspace_stats 内核，理由：
wkv 侧 keyspace_stats（wedb/wkv/src/store/keyspace.rs:474）为引擎级全命名空间两阶段
扫描，第一阶段收集全库候选键集（逐候选 Box::from 堆分配）、第二阶段逐候选异步读探针，
不满足本票「DBSIZE 全程累计堆分配常数级」验收判据，且其形态无法承接 KEYS 逐键枚举过滤
与槽删除就地删除两臂；它是 INFO KEYSPACE 单一消费的引擎侧同族内核（对标 C#
UnifiedStoreGetKeyspaceStats，活键判定与 EXISTS 口径同源），属 wkv 层，不在本票会话层
收口面内。选型后会面三层（wnode StorageSession）键空间遍历判定只剩 live_key_at 一处，
string_keys_snapshot 写胜折叠臂整体删除。

## 改动明细（全部在 array_key_iteration_functions.rs，除注明外）

1. live_key_at 收口为带过期门控的内核 live_key_at_gated（新增私有函数，参数
   skip_expired: bool），live_key_at 薄封装为 skip_expired=true 的现行语义，其余判定
   步骤（extract_live_user_key、链首校验、对侧域新者胜、墓碑剔除）原样迁入内核。
   skip_expired=false 供槽删除臂使用，对应 C# DeleteSlotKeysScan.Reader 无
   CheckExpiry 分支与 UnifiedStoreGetDBKeys/DBSize Reader 有 CheckExpiry 的同文件
   两型分化。不做 :83 前缀外提（归 next/live-key-probe-prefix-hoist.md 另棒）。
2. 新增私有单趟活键计数内核 count_live_keys：hlog().scan 回调内 live_key_at 判定
   放行即 ++count，零分配零排序；db_size 改为直调本函数（C# DbSize 的
   UnifiedStoreGetDBSize.Reader 回调内 ++info.count 对位）；count_keys_in_slot 的
   槽匹配早退分支之后改为复用 count_live_keys，消除同体扫描重复。
   slow.rs C::Dbsize 的向量登记表域内合并计数臂原样保留，不碰。
3. db_keys 重写为单趟扫描：glob 判定与命中收集下沉扫描回调内，未命中键零分配；
   一致读 ctx.with_consistent_read（同步 API，wkv consistent_read.rs:141）原样保留
   在回调内逐键包裹匹配+收集动作，与 C# ConsistentUnifiedStoreGetDBKeys.Reader 在
   Reader 前后 Pre/PostSingleKeyConsistentRead 同构；with_consistent_read 的上抛错误
   经捕获首错 + Ok(false) 早停 + 扫描后重投（回调签名是 whlog::Result，wkv 错误不可
   直接 ? 穿越）。删除排序：C# DBKeys 无排序口径，Redis 不承诺 KEYS 输出序，
   命中键按扫描序（hlog 地址升序）输出。
4. delete_slot_keys 重写为扫描内就地删除（C# DeleteSlotKeys 的
   DeleteSlotKeysScan.Reader 回调内 storageSession.DELETE 对位）：槽匹配早退分支保留；
   候选判定用 live_key_at_gated（skip_expired=false，过期未清键一并删除，修复旧快照
   跳过 Due 键在槽位退役后遗留孤儿域的口径分叉；墓碑链不删）；热路径在回调内经
   self.batch.try_delete_sync 同步删除臂就地删并计数，同步臂降级项（环形页翻转、
   复合对象元数据、迁移 claim）仅收键名进延后小集合、扫描结束后逐键 await
   delete_string 异步闭环（delete_string 内部自带双域删除与 Meta 排空路由，与旧
   快照臂同调用面，不扩面处理 zcode.net.md 问题2 的漏删段）；禁止预物化全库键名。
   消费面 slot_mgmt.rs 与 RESP 层不动。
5. 删除 string_keys_snapshot、仅供其使用的辅助函数 live_value_key 与
   wbase::map::HashMap as GxHashMap 导入；删除后本文件内该符号零消费者
   （已全仓 grep 核实：db_size / db_keys / delete_slot_keys 之外无调用点）。

## 测试

1. 存量修正：wedb/wnode/tests/resp_slow_path.rs 的 dbsize_and_keys_via_slow_path
   两用例断言 KEYS 输出字节序（order:1 先于 user:* 系排序产物），改为解析 RESP
   数组为集合比较；vector_key_domain_ops.rs 已是集合式断言不动；
   storage_api.rs / range_index_tests.rs / consistent_read_session.rs 断言均为
   单命中或计数，不受序影响，逐一跑绿核实。
2. 新增集成测试 wedb/wnode/tests/dbsize_keys_zero_alloc.rs（global allocator 为
   进程级，全文件单一 #[test] 函数内顺序执行，避免并发测试互扰）：
   - 预置一万键键空间后，计数分配器置零，DBSIZE 调用增量分配次数与字节数设常数
     上限（修复前快照逐键 to_vec + 哈希折叠 ≥ 2 万次分配必红）；
   - 置零后 KEYS 用仅命中 1 键的 pattern，增量分配与命中数同阶（远小于全库键数，
     修复前线性必红）；
   - 口径合一断言：db_size == count_keys_in_slot(会话库槽) == 全量 scan_cursor
     续扫去重计数 == db_keys("*") 集合等于写入键集合（逐元素集合比较防退化）；
   - 过期未清键用例：SET 后过绝对过期点置过去，断言 DBSIZE 不计、db_keys 不列，
     而 delete_slot_keys 将其一并删除（返回计数含之、删后库内该键物理不可见），
     对齐 C#「expired-but-not-yet-tombstoned 也删」。
3. CLUSTER DELKEYSINSLOT RESP 用例（wedb/tests/cluster_resp_session.rs 既有）行为
   面不回归，定向跑绿。

## 验证判据

cargo check -p wnode --tests 零警告；定向测试：
cargo test -p wnode --test dbsize_keys_zero_alloc --test resp_slow_path
--test storage_api --test range_index_tests --test consistent_read_session，
以及 cargo test -p wedb --test cluster_resp_session。
严禁 ./test.sh 与 ./sh/clippy.sh（主代理负责）。分段提交，合并走锁协议，
合并后按 rust_review 复审改动面。

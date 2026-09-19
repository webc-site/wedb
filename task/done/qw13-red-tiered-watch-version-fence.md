优先级：高（dev 测试基线红，计数轮前置）

单题：分层存储写臂的对象版本号推进次数与冷键降级/弹出判定在当前 dev 上偏离 C# 对位。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1 wnode::tiered_watch_fence::tiered_hash_write_arms_invalidate_watch —
  wnode/tests/tiered_watch_fence.rs:224
  「分层 HINCRBY：HINCRBY 版本应恰推进一次，应答=[-ERR hash value is not an integer...]」
  left: 4 / right: 3（多推一次）
2 wnode::tiered_watch_fence::tiered_set_zset_list_write_arms_invalidate_watch —
  wnode/tests/tiered_watch_fence.rs:241 「分层 SADD 重复成员：SADD 不得推进版本」
  left: 2 / right: 3（少推一次）
  注意 1/2 方向相反，必须分别定性，不得并成一条「版本推进不对」糊过。
3 wnode::tiered_background_demote::zset_count_dim_deadzone_excluded_and_cold_key_demoted —
  wnode/tests/tiered_background_demote.rs:253 「条目数越 65536 应升阶」
4 wnode::resp_blocking_commands::blpop_on_tiered_key_pops_immediately —
  wnode/tests/resp_blocking_commands.rs:120 「应答不匹配」left: "" / right: ":65540\r\n"
  冷键上的 BLPOP 直接空应答（未弹出也未阻塞登记）。
5 wnode::ttl_rmw_semantics::expire_gt_same_value_rejected_same_domain —
  wnode/tests/ttl_rmw_semantics.rs:602 「第 1 轮同值 GT 必须按严格大于拒绝」left: ":1" / right: ":0"

判读方向（须自行核实）
对象版本号只在真实变更时推进（C# 侧由存储层返回的 RecordMetadata 版本/WriteReason 决定）：
HINCRBY 报错路径不该推版、SADD 重复成员该推版与否按 C# 源码定锚，先读 garnet 对应
ObjectStore 类型的 Set/Increment 实现再判谁对，不要照抄用例。3 的 65536 升阶与降级计数维度同域；
4 的冷键阻塞命令应先走 promote 再判；5 的 EXPIRE GT 是「严格大于才接受」的判定域，属同一批
RMW/TTL 面。

避让
wnode/src/storage/session/storage_session.rs 有他人暂存在途（pending-lat 计时点），改前先
git diff --cached 看清，只做本票最小改；向量登记键域（\0\0 前缀）、RESP null 形态、wkv dbmeta
各属另票。

改动域
wedb/wnode/src/storage/**（分层读写臂、promote/demote 活动、版本推进点）、阻塞命令冷键处理、
TTL/RMW 判定，以及上述五个测试文件。禁止触碰 wkv/src/vdb.rs、waof、wresp。

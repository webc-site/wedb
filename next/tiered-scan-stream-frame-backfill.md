# tiered-scan-stream-frame-backfill

来源 next/zcode-r3-perf.md 问题 3（认领棒：zcode-r3-perf / zcode-r3-txn 甄别票）。

问题一句话
分层态 HSCAN / ZSCAN / SSCAN 的扫描臂把每个输出项 to_vec 攒进中间 Vec<Option<Vec<u8>>>，扫完再二遍遍历重写进 output，是同仓已裁决「预留-回填」流式输出单点的漏网第二形态。

rust 现状
- wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:295 exec_tiered_scan 声明 items: Vec<Option<Vec<u8>>>；:316-:336 在树扫描回调内逐成员 k.to_vec push（zset 分值经 format_double 文本化后再 to_vec 一次，hash 值再 to_vec 一次），每页至多 2×COUNT 次堆分配；:353-:366 扫完全量二遍历重写进 output。
- 同目录 tiered_collection_ops/set.rs:137 SetOperation::Smembers 与 hash.rs 分层 HGETALL / HKEYS / HVALS 四臂已按上轮裁决改为预留-回填流式直写：帧头按上界预留、回调内 write_resp_bulk_string 直写 output、扫完以实际出帧计数回填、错误路径 output.truncate(base) 撤帧。单点在 wedb/wresp/src/ext.rs:233/:239/:253 的 resp_frame_head_len / reserve_resp_frame_head / backfill_resp_frame_head。
- 上轮 task/done/fix-tiered-output-frame-scratch-elimination.md 逐臂收编时按 scratch 型 extend_from_slice 双缓冲形态 grep 全目录，本臂的 items 中间物化形态未在命中面内（或后于该轮引入），现状为同族唯一残留双形态。

C# 证据
无直接 C# 对位（分层态为本仓自定义面，见 SKILL 集合分层存储裁决与 doc/zh/collection.md）；本票的基准是仓内既成单点：ext.rs 三函数的文注不变式（头按上界预留、严禁落 meta.size 计头、错误撤帧）与 set.rs:137 的流式消费样例。分层 SCAN 与分层 SMEMBERS 是同一 RESP 帧族（数组头 + bulk 实体）的两个臂，一臂流式一臂物化即多套架构并存。

修法
1. exec_tiered_scan 照 Smembers 同款改造：先落 2 元数组头与游标 bulk 的预留位（游标十进制宽上界 20 位定长预留）、成员数组头按 want 上界预留，扫描回调内直接 write_resp_bulk_string / write_resp_null_ver 写 output 并计实际条数，扫完按计数回填两处头部；游标值依赖 scanned / expired 汇总，扫完后回填（回填的字节移动逻辑必须走 wresp::ext.rs 单点，若现签名不覆盖 bulk 变体，就在 ext.rs 内扩该单点，严禁在 scan.rs 手搓第二套 copy_within）。
2. want 截断判据由 items.len() != want 改为输出条数 != want，逐字节出帧语义不变（含 RESP2/RESP3 null 项、空页 RESP_EMPTYLIST、C# 等值比较保留负 COUNT 全遍历怪癖）。
3. 错误路径：扫描 Err 上抛前 output.truncate(base) 撤帧（连同预留头回到臂进入点），与 ext.rs 文注不变式 2 及 Smembers 臂同形。
4. 删 items 声明与二遍重写段，函数内不留 Vec<Option<Vec<u8>>>。

验收（修复前必须能红的判据）
- 分配计数门槛测试（tests/，计数 global allocator）：分层键执行一次 COUNT 达到上限的 HSCAN/ZSCAN/SSCAN 页，累计堆分配不随 COUNT 线性增长（修复前每页 2×COUNT 次必红），与同规模 SMEMBERS 流式臂分配同阶。
- 逐字节回归：修复前后三层 SCAN 应答（含游标推进到终态 0、过期成员占游标基数、±inf/NaN 分值 null 项、错误注入下错误帧）逐字节全等；既有分层错误帧用例（tiered_scan_err_propagate 族）全绿。
- 结构性门槛：rg 该目录无 Vec<Option<Vec<u8>>> 型输出中转残留。
- ./test.sh 与 ./clippy.sh 全绿。

为什么这不是自造优化
不改任何协议语义、不改复杂度类，是把上轮已裁决并落了四臂的输出形态收编漏网臂，消除同一帧族「预留-回填流式」与「中间 Vec 二遍重写」两套机制并存（规程最高优先级：重复逻辑 / 多套架构并存 / 后续同族臂继续照抄坏形态的污染扩散面）。方向恰恰是去复杂度：删一个中间缓冲，加零。

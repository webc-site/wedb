审核结论：通过（2026-09-27 审核席逐锚点亲验：主探针 array_key_iteration_functions.rs:125 与跨域探针 :138-141 均裸比 find_tag_cooperative 采回地址；mod.rs:722-726 单点不剥 RC；read.rs:1012-1022 冷读回填挂链；addr.rs:25 READ_CACHE_BIT=1<<47（ADDRESS_BITS=48）；C# FindRecord.cs:84-85 SkipReadCache 属实、AllocatorScan.cs:302 调用链属实；hlog_scan.rs:226/292、compact.rs:218、inplace.rs:293/513/728、cpr_host.rs:494、stats.rs:92、resize.rs:288 消费面清单全数在场；deviations.md 仅 §69/§111 页容量旋钮族无此面登记，判转写遗漏成立）。方案增补裁定：方案 1 的 skip 返回 None 竞态窗折保守判 Live——主探针 None 不折 Dead（视作链首校验通过），跨域探针 None 折非新者胜；禁用 skip_read_cache_with_wait 自旋与重探环：扫描批纪元守卫内自旋等待驱逐清洗会阻塞纪元排空，结构性卡死风险；最坏后果仅瞬态重复计报（SCAN 重复发射/DBSIZE 瞬态多计/delete_slot_keys 幂等重删/复制快照重复键无害），符合「重复可容忍、漏键不可容忍」硬契约，与 hlog_scan.rs:226 None 折 0 转复核同精神（该面有回溯臂，本面无回溯臂故折 Live），不新增走查机制；None 分支须注释锚定 read_cache/window.rs:217-222 契约与本案裁定理由。RC 关闭时新增开销仅一次 is_read_cache 位测试（位不置位不进走查环），数据面零开销承诺成立。

ReadCache 开启时 SCAN 族活键链首探针不剥 RC 位，预热冷键整批误判死漏键

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 扫描条件判定链 AllocatorScan.ConditionalScanPush 经 FindRecord.TryFindRecordInMainLogForConditionalOperation（garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:38-85），在将哈希链头地址与主日志当前地址比较前显式 if (stackCtx.hei.IsReadCache) SkipReadCache(ref stackCtx, out _)（:84-85），RC 链头先剥回主日志真实地址再判链首一致，扫描不因 ReadCache 挂链丢键。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
SCAN / KEYS / DBSIZE / COUNTKEYSINSLOT / GETKEYSINSLOT 活键判定单点 active_user_key_at（wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:115-150）经 find_tag_cooperative（wedb/wkv/src/session/mod.rs:722-726）→ find_tag_by_hash 直接采回含 READ_CACHE_BIT（wbase/src/addr.rs:25，1<<47）的链头地址：ReadCache 开启时冷键点读回填（wedb/wkv/src/session/raw/read.rs:1012-1022 → wedb/wkv/src/read_cache/append.rs:55 append 以链首 CAS 挂载 with_read_cache(curr_tail)）后，槽位链头即 RC 虚拟地址，与主日志记录地址 i.addr 比较必然不等，活键整批误判 Dead。同仓纪律已在位（hlog_scan.rs:226/292、compact.rs:218、session/raw/write/inplace.rs:293/513/728、cpr_host.rs:494、store/stats.rs:92、store/resize.rs:288 均先 skip_read_cache 再比较），唯 SCAN 族探针单点缺失，属转写遗漏非既定改良，deviations.md 无该面登记（仅 §69 RC 页容量旋钮）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
--read-cache 开启部署（默认 false，wedb/wkv/src/config.rs:398；CLI 暴露 wedb/wconf/src/node_options.rs:168）下：
a GET 预热过的冷键在 RC 条目存续期间 SCAN/KEYS/DBSIZE 全程漏键（游标地址推进固化，不可恢复），违反游标遍历「重复可容忍、漏键不可容忍」硬契约；
b 无盘复制快照（wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs 经 get_keys_in_slot，array_key_iteration_functions.rs:622-626）漏掉 RC 预热冷键，副本永久缺键发散；
c delete_slot_keys（array_key_iteration_functions.rs:396）同判定漏判，槽删除/迁移漏删残留；
d 跨域互探 :138-141 的 a > addr 对 RC 地址恒真（1<<47 远超主日志地址），String/信封双域新者胜裁决同形误判。

涉及代码：
rust 文件与函数：
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:active_user_key_at
wedb/wkv/src/session/mod.rs:find_tag_cooperative
wedb/wkv/src/read_cache/window.rs:skip_read_cache

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TryFindRecordInMainLogForConditionalOperation

精炼执行方案：
1 active_user_key_at 主探针（:125）与跨域探针（:138-141）对 find_tag_cooperative 返回地址先判 is_read_cache：是则经 read_cache.skip_read_cache 剥回主日志地址再与记录地址比较（与 hlog_scan.rs:226 消费形态同构）；skip 返回 None（RC 条目关闭竞态窗）时不得折 Dead——保守判 Live 或重探链头，审核席定夺单点形态
2 不动 find_tag_cooperative 单点本体（点读 read_probe 依赖 RC 地址走 RC 快路径直读，单点剥会破坏快路径，剥 RC 仅限扫描判定消费面）
3 测试验证点：--read-cache 开启下 GET 预热冷键后 SCAN/KEYS/DBSIZE 不漏键不漏计、无盘复制快照键集与主库逐键全等、delete_slot_keys 删净无残留、String/信封双域键 SCAN 计数正确

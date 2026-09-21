# 紧缩物理删段对检查点恢复窗钳制（核实属实，接受，选路径 A）

## 裁决与双侧证据

票面 P0 属实，机制链全部核实。

rust 侧：

- whlog/src/hlog/shift.rs:139 shift_begin_address 无条件
  device.truncate_begin_until（:184-192）物理删段，链上零检查点感知
- wkv/src/gc/compact.rs:89 熔断旁路 None 短路；:110 熔断档 n=max，
  until 推进到 safe_ro 全速紧缩
- 默认可达链：wkv/src/gc/reclaim.rs:26 RELEASE_POLL_MS=200、:36
  open_shared 冷启动挂载；wkv/src/config.rs:82 高水位 1024；
  gc.enabled 默认 false（config.rs:168）时 reclaim_when_scan_idle
  （reclaim.rs:117-122）反而兜底驱动回收内核
- 恢复窗 = 检查点 meta 地址视图：wcpr/src/manager/recover.rs:121-123
  begin/tail 取自 meta.hlog_meta，:334 scan_iter(begin, tail)；
  扫已删段报 SegmentNotFound（wdev/src/segmented_device/handle.rs:322）
- FLUSHALL 同穿此面：wkv/src/store/keyspace.rs:409-411
  flush_all_databases → shift_begin_address(tail)

票面两处小误（不改结论）：

- purge_unrecovered_checkpoints 在 wnode/src/database/database_manager_base.rs:214
  （按身份清：恢复成功后删其余全部），另有 wcpr/src/manager/mod.rs:552
  purge_outdated（按数留 CHECKPOINT_RETAIN_GENERATIONS=2，
  database_manager_base.rs:47）；「删光更早检查点」仅对恢复后成立。
  但更早代的重放窗是最新窗的低地址子集，段删后同样不可恢复，结论不变
- C# DoCompactionAsync 物理删段还受 CompactionForceDelete 第三闸
  （truncateLog 形参），比票面「双闸」更保守

c# 侧（「拍检查点才真正删文件」的实现点与契约原文）：

- libs/storage/Tsavorite/cs/src/core/Index/Recovery/Checkpoint.cs:54-59
  CleanupLogCheckpoint：检查点状态机 REST 阶段
  （Index/Checkpointing/HybridLogCheckpointSMTask.cs:66-67）发布后
  Log.ShiftBeginAddress(info.beginAddress, truncateLog: true)——
  只删已发布检查点重放窗之下的段，即 C# 唯一常规物理删段点
- libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:133-134
  契约原文：truncateLog=false 时 "log will be truncated after the next
  checkpoint"；:143-147 Truncate 文档明示「要数据安全就拍检查点」
- 默认闸：libs/server/Servers/GarnetServerOptions.cs:211/:225/:231
  （FrequencySecs=0、Type=None、ForceDelete=false）；
  libs/server/StoreWrapper.cs:967-969 注册条件；:702-705 ForceDelete
  警示原文 "Compaction will delete files, make sure checkpoint/recovery
  is not being used"

为什么必须路径 A 而非照抄 C# 默认零紧缩：rust 内置换号物理回收是
doc/zh/db.md 明文承诺（死亡账本摘除依赖 begin 推进，不受
compaction_type 关停），无 C# 「默认不删」可搭。C# 靠默认不删成立的
恢复安全，rust 必须靠显式检查点窗钳制成立。路径 B（给熔断旁路加
「距上一检查点」闸门）只堵熔断一臂，常规档 Shift/Lookup 紧缩与
FLUSHALL 移位照样越窗删段，需三处闸且仍漏发布点补收——不取。

## 方案：检查点发布基线 + 移位链物理删段钳制（单一机制）

whlog 新增「物理删段地板」delete_floor（AtomicU64，初值 0 =
未发布检查点全禁删）。语义分离：逻辑 begin 照常全量推进（Shift 丢弃、
gc_dead 账本摘除、FLUSH 语义不变），物理删段目标 =
min(new_begin, delete_floor)。

改动点：

1. whlog/src/address.rs：Addresses 增 delete_floor 原子位与
   getter / fetch_max setter；不进 AddressSnapshot（恢复期按
   恢复检查点重设，见 4）
2. whlog/src/hlog/shift.rs：shift_begin_address 尾段由
   Device::truncate_begin_until 内核调用改为显式三步——
   begin fetch_max(new_begin) → wait_safe_read_only_drained(new_begin)
   （纪元屏障照旧，排空在途磁盘读者）→
   device.truncate_until_address(new_begin.min(delete_floor()))。
   同文件新增 pub release_history_until(target)：抬地板 + 屏障 +
   truncate_until_address，发布点补收延后段的唯一入口
3. wcpr/src/manager/create.rs：create_checkpoint_inner 在 meta 发布
   （sync_checkpoint_dir 成功）后调
   store.hlog().release_history_until(meta.hlog_meta.begin_address)
   （对标 C# CleanupLogCheckpoint；失败仅 warn：发布已完成，延后段
   下一轮紧缩按新地板补收，正确性不受影响）
4. wkv/src/store/cpr_host.rs：CprRecover::from_recovered 装配时
   raise_delete_floor(recovered.meta.hlog_meta.begin_address)
   （启动恢复与副本在线导入 recover_from_token 同路覆盖；只抬地板
   不补删，对标 C# OnRecovery 后待下一发布点收）

不扩面：

- wkv/src/store/addr.rs:190 truncate()（C# LogAccessor.Truncate 显式
  破坏性逃生口，调用点 wnode/src/resp/garnet_api/slow.rs:1062
  unsafe_truncate_log 默认关）不加钳制——C# 同为文档警示的显式
  破坏操作，1:1 保留
- waof 截断链（AOF 独立设备与恢复语义）不动
- 熔断判定、在途单飞闸、队列门控原样（与 bftree-release-gate、
  gc-compact-store-flight-gate 正交）

安全性核对：

- 磁盘卫生：延后段由生产检查点链（SAVE/BGSAVE/AOF 体积超限）发布点
  补收，与 C# "Take a checkpoint in order to actually delete" 同
- 单文件设备 truncate_until_address 恒空转（wdev/src/device.rs:296），
  钳制自然空转无害
- 设备截断幂等（wdev/src/segmented_device/truncate.rs:146-148
  fetch_max 快路径、:175 NotFound 容忍），补收重入安全
- 发布点屏障快路径：发布前检查点流程已等 safe_ro >= tail >=
  cp.begin，不变式 begin <= safe_head 保证谓词已真，零挂起

## 测试

1. 改写 wkv/tests/compact/basic.rs multi_segment_physical_truncation
   （:16-89）：现断言「无检查点紧缩即删段」正是票面 P0 行为，改为
   紧缩后段保留（钳制生效）→ 拍检查点（地板抬升并补收）→ 段删除
2. 新增定向测试（复用 wkv/tests/checkpoint/recovery.rs 夹具形态）：
   拍检查点 → 紧缩/移位越窗 → 断言重放窗 [meta.begin, meta.tail)
   段在盘 → recover_latest 恢复成功

## 验收

cargo check -p wdev -p whlog -p wkv -p wcpr -p wnode -p wedb
零 error 零 warning；定向测试通过。严禁 ./test.sh 与 ./sh/clippy.sh。

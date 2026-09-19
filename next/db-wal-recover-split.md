优先级：中
来源：next/agy.db.md 条 9 立项（原档 C# 引证 TsavoriteLogRecovery.cs 有误，已修正：
garnet 无该文件，恢复体在 TsavoriteLog.cs 内）。取证基线：主仓 dev 当下代码。

问题
waof WalLog::recover 单函数约 180 行（log.rs:190-:372），揉合设备段元数据恢复、帧
同步、滑动窗口批量读、伪头与 CRC 过滤、截断点推导、Commit 元数据恢复、环形缓冲
预热七个阶段；log.rs 膨胀至 734 行。

取证
- wedb/waof/src/wal/log.rs:190 pub async fn recover（下一方法 :374 dropped_bytes_after，
  主体约 180 行）；文件共 734 行。
- 函数内阶段地标：device.recover 段恢复 :202、begin_address 装载 :206-:209、
  start_seg > 0 帧同步分支 :215 起、last_commit 提交元数据推导、尾部环形缓冲预热。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:623
  RecoverAsync（恢复主体）与 :2852 RecoverReadOnlyAsync——C# 将恢复编排独立成
  RecoverAsync 专法并按阶段分私有步骤；garnet/libs/storage/Tsavorite/cs/src/core/
  TsavoriteLog/TsavoriteLogRecoveryInfo.cs 承接恢复元数据。

修法建议
抽取 waof/src/wal/recover.rs 独立模块（阶段函数：frame_sync / scan_windows /
derive_truncation / recover_commit_meta / warmup_ring），WalLog::recover 保留为门面
入口串接阶段；阶段间经结构化中间态传递，禁 pub 泄漏。纯搬运重构，恢复语义与
截断判定零改动。

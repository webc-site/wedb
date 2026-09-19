# wkv 检查点/ReadCache 注释锚点收口

来源票：next/wkv-stale-checkpoint-read-cache-anchor.md（qcode10.db 条 5）。

## 立项核实

票单属实。恢复面六处注释以自由文本锚定了两个不存在的文件名 wkv/src/checkpoint.rs 与
wkv/src/read_cache.rs，不参与编译、无门可拦，读者按锚点一跳找不到文件。实测真身：检查点
宿主实现在 wedb/wkv/src/store/cpr_host.rs（fn cpr_err、impl wcpr::CprRecover 的 async fn
from_recovered，其内单次有序扫描内核 run_recovery_pass 承接 RI 桩自愈注册与
recover_all_trees_from_dir 批量预置调用）；ReadCache 引擎为目录形态
wedb/wkv/src/read_cache/（append.rs / cleanse.rs / mod.rs / window.rs），append 真身在
read_cache/append.rs。C# 侧对照属实：garnet 的交叉引用锚点是符号级、可被文档构建期校验，
如 TsavoriteLog.cs 以 see cref="RefreshSafeTailAddress" 锚符号，AllocatorBase.cs 同形锚
ClosedUntilAddress，不存在这类静默失效的文件名锚。

## 处置

六处统一改为现存的「模块路径 + 符号名」形态，不留第二份文件名级口径：

- wkv/src/config.rs enable_read_cache 文档三处：read_cache.rs append 改为
  read_cache/append.rs `append`；checkpoint.rs from_recovered 改为 store/cpr_host.rs
  `from_recovered`；引擎本体改为 read_cache/ 目录（票单外同族的 raw/read.rs 为真实简写
  路径 wkv/src/session/raw/read.rs，不在本单口径，未动）。
- wkv/src/range_index/mod.rs encode_meta_stub_record 文档：checkpoint.rs 改为
  store/cpr_host.rs `run_recovery_pass`（自愈回写的真实宿主，而非泛指的 from_recovered）。
- wcpr/src/error.rs Host 变体文档：wkv/src/checkpoint.rs 改为
  wkv/src/store/cpr_host.rs 的 cpr_err 映射。
- wbftree/src/manager/replication.rs recover_all_trees_from_dir 文档：wkv checkpoint.rs
  改为 wkv store/cpr_host.rs `run_recovery_pass`（其 cpr_host.rs:161 为唯一调用方）。

纯注释改动，零 .rs 行为变更。验收：全仓 .rs 注释中 checkpoint.rs / read_cache.rs
文件名级命中归零；cargo check 零错误零警告。

## 对应关系

Rust：wkv/src/store/cpr_host.rs: cpr_err / from_recovered / run_recovery_pass，
wkv/src/read_cache/append.rs: append。
C# 参考锚点风格：libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:
SafeTailAddress 文档、libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:
SafeHeadAddress 文档。

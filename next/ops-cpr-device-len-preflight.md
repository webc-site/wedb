检查点恢复设备长度预检防止静默空库

1. 问题背景
在恢复检查点时，wcpr/src/manager/recover.rs 和 wkv/src/store/cpr_host.rs 从检查点元数据中恢复状态。
元数据明确包含 tail_address 与 flushed_until 地址。
若运维备份或迁移时漏拷数据文件，或者数据文件被截断，当前实现会因为 EOF 读空页而静默起服，产生伪空库。

2. 修复方案
在恢复检查点并校验元数据时：
检查目标设备的物理大小（如 device.len() 或可用物理大小）。
若设备大小小于元数据声明的 flushed_until（或 tail_address），立即具名报错拒绝启动，明确提示数据文件缺失或与检查点不配套。
补充恢复校验单元测试。

3. 验证准则
子代理在 worktree 中仅运行 cargo check 验证编译。
严禁运行 ./test.sh 或 ./clippy.sh。
遵循 rust_review 规范自查优化。
完成后合并回 dev 分支，删除 worktree，移动本任务卡至 task/done/ops-cpr-device-len-preflight.md。

优先级：中
来源：next/agy.db.md 条 8 与 next/muse.db.md 条 13 两轮同题合并。取证基线：主仓 dev 当下代码。

问题
wdev segmented_device.rs 1437 行巨石：段映射、句柄池、DirectIO 扇区探测、跨段拆分
读写、刷盘同步、物理截断删除、目录恢复扫描与 Device trait 实现同文件，远超同 crate
其余文件总量（chunk.rs 189 / device.rs / sys.rs）。

取证
- wedb/wdev/src/segmented_device.rs 全 1437 行：get_segment_and_offset :373、
  within_single_segment :643、读写切片循环 :1022 / :1206（复用 SegmentChunks）、
  恢复扫描 device recover（对标 LocalStorageDevice.RecoverFiles）、BufferPool 装配
  :220-:278。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs、
  ManagedLocalStorageDevice.cs、StorageDeviceBase.cs——C# 侧设备族按职责分文件
  （基类对齐校验 / 本地设备读写 / 托管包装），无千行单文件形态。
- 与现有票 next/wdev-segmented-device-mutator-knobs.md 不同面：该票删旋钮死面
（set_capacity 等九口），本票拆文件结构；认领顺序建议先拆后删或同分支一体处理，
避免对同一文件两轮冲突。

修法建议
按 handle.rs（句柄表与路径、single_file/with_pool 构造）、io.rs（对齐读写切片与
快慢路径）、truncate.rs（物理截断与段删除）、recover.rs（目录恢复扫描与元数据
校验）拆子模块，mod.rs 保留 SegmentedDevice 定义与 Device trait impl 统一对外；
pub API 路径不变。纯搬运，禁止夹带行为改动（对齐校验单点 validate_aligned_io 已在
chunk.rs，保持复用不动）。

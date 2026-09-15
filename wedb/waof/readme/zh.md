# waof : AOF 预写日志

## 项目介绍

waof 提供基于环形内存写缓冲 + 分段块设备的 WAL 预写日志引擎（`WalLog`）；落盘经 wdev 的 `Device` 设备抽象完成，wram 提供对齐内存与缓冲池（`AlignedBuf` / `BufferPool`）。`AofLog` / `AofRecord` 等为 `Wal*` 类型的别名。

记录头仅 8B（`entry_len: u32` + `crc32: u32`，小端）；空负载以 `EMPTY_PAYLOAD_CRC = 0xFFFF_FFFF` 哨兵保证已提交空记录头不全零，可与崩溃残缺尾区分。

## 模块组成

- `config`：`WalConfig`（buffer_size 默认 16MiB、inflight_slots 默认 256）；扇区对齐不进配置，以 `Device::sector_size()` 为单一真源
- `header` / `record`：`RecordHeader`、`WalRecord`（address / next_address / header / payload，Deref 到 [u8]）
- `log`：`WalLog` / `WalLogInner` 核心引擎，持 begin / tail / flushed_until / committed_until 四个原子位点
- `disk_window`：`DiskWindow` 恢复与扫描共用的磁盘滑动预读窗（内部）
- `iterator`：`WalScanIterator` 滑动窗口分块读，透明跨内存与磁盘段
- `ring_buffer`：`RingBuffer` 内存环形写缓冲，容量为 2 的幂时走位掩码快路径

## 核心 API

- `WalLog<D>`：open（打开并自动 recover）、enqueue（无锁 CAS 预占地址并注册在途槽位）、commit（批量刷盘至 safe_tail）、enqueue_raw（原样写入完整记录帧——复制从节点保真落盘，对标 C# UnsafeTryEnqueueRaw）、enqueue_and_wait_for_commit（写入并等待提交持久化，目标地址为记录末端）、wait_for_commit、scan / scan_all / scan_committed、total_size、recover、truncate、reset
- `WalScanIterator<D>`：跨内存 / 磁盘透明扫描
- `WalConfig`、`WalRecord`、`RecordHeader`（`RECORD_HEADER_LEN = 8`）、`RingBuffer`
- 别名：`AofConfig` / `AofLog<D>` / `AofLogInner<D>` / `AofRecord` / `AofScanIterator<D>`

## 设计要点

- 刷盘语义：enqueue 无锁；commit 持提交锁把 [flushed, safe_tail) 刷盘，safe_tail = tail 与全部在途槽位最小值；wait_for_commit 高速栅栏——已提交无锁返回，否则双重检查 + try_lock 协同提交或监听广播，避免惊群
- 覆写语义：环形缓冲区会覆写未刷盘数据。已落盘记录的内存副本被覆写时，扫描回退磁盘权威数据继续；未落盘记录被环形覆写挤出内存窗时计入 `overwritten_skips` 并以 `Ok(None)` 提前终止——该区间磁盘无权威副本、无法回读恢复，计数非零即代表"扫完"实为中途覆写丢失
- 恢复：要求日志静默；EOF / 残缺头 / 校验和失败 / 全零填充一律保守截断到最后完整记录，其他 I/O 错误显式上抛；无检查点依赖，靠 CRC 记录链自同步定位尾部；truncate 后段首落在跨段残缺负载中部时，恢复先以 frame_sync 逐字节探测重同步并前移 begin_address
- truncate 推进 begin 并物理删段，与 commit 互斥防幽灵段；reset 仅回退内存位点不请磁盘，存在旧记录复活窗口（注释明示）
- RingBuffer 定位：提交前的内存驻留区，enqueue 零系统调用，commit 批量顺序落盘
- 提交边界：无独立 commit 元数据记录，恢复以最后一条完整记录为已提交。已 enqueue 未 commit 的记录若被并发 commit 的刷盘区间覆盖，崩溃后会被视为已提交复活——要求精确提交持久性边界的调用方须自行处理（见 `WalLog` 文档）

## 测试覆盖

tests/ 覆盖：RecordHeader 编解码与破坏鲁棒性、RingBuffer 大地址读写；端到端冒烟（写-扫-提交-截断-重启-追加）；满缓冲、payload 限制、raw 帧保真与从节点重放地址一致、并发有界增长、快速提交并发等待、短写防护；子区间扫描、未提交内存、落后 begin 跳转、物理截断停止、慢读者逐出回退磁盘、内存覆写回退磁盘、大记录、磁盘预取边界；多阶段恢复、残缺尾、空记录持久性、全零填充不复活、中段损坏保守停止、跨段残缺 frame_sync、海量记录；truncate 与文件删除、精确段边界、周期截断、reset 复用。

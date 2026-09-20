# live-key-probe-prefix-hoist

来源 next/zcode-r3-perf.md 问题 4（认领棒：zcode-r3-perf / zcode-r3-txn 甄别票）。

问题一句话
SCAN / COUNTKEYSINSLOT / GETKEYSINSLOT 共用的逐记录存活判定 live_key_at，其双域互斥的第二探针仍走会话绑定版 session_tag_key，在批量循环里逐条重读 ns/db 原子变量并重算 Varint，是全库扫描链路上唯一漏掉前缀外提的判定点。

rust 现状
- wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:70 live_key_at 签名已收 prefix: &[u8]，调用方（:140 / :224 / :287 / :334）已单次外提 session_prefix() 传切片；但 :83 对侧域探针仍调 self.batch.session_tag_key(tag, user_key) —— 其内部（wedb/wkv/src/session/keys.rs:17）每次 self.session_prefix() 重读会话 ns/db 原子变量并重算 Varint 编码。
- 显式前缀内核已在位：wedb/wkv/src/session/keys.rs:26 StoreSession::session_tag_key_with_prefix，文注自述「循环前缀外提内核，transpile SKILL 工程准则」，与 :17 会话版逐字节同编码。

C# 证据
live_key_at 文档自引对位 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorScan.cs:ConditionalScanPush（该文件 :287）——C# 单键单物理链，链首校验天然收敛，无第二域探针；对侧探针是 rust 双物理域（String / ObjectEnvelope）裁决的固有补件，其编码构造就该用仓内既定的外提内核，不引入任何新机制。

修法
一处一行：:83 改为 StoreSession::session_tag_key_with_prefix(prefix, tag, user_key)（prefix 即函数参数手边已外提的切片），函数体其余判定不动。

验收（修复前必须能红的判据）
- 开发过程判据：临时在 wkv session_tag_key 加 cfg(test) 计数观察（不入仓），一万键库 GETKEYSINSLOT 全扫，修复前调用计数与扫描记录数同阶（红），修复后为 0。
- 入仓判据：SCAN / COUNTKEYSINSLOT / GETKEYSINSLOT 既有逐字节回归全绿（行为零变化，本条是准则收口而非语义改动）；rg 确认该扫描文件循环判定位不再出现会话绑定版 session_tag_key。
- ./test.sh 与 ./clippy.sh 全绿。

为什么这不是自造优化
SKILL「性能优化与零拷贝工程准则」明文列出循环前缀外提（Prefix Hoisting）：批量遍历中单次获取并外提 session_prefix()，经 *_with_prefix 系列消除逐字段重复读取原子变量与重算 Varint。本条只是把已在位、已文档化的既定内核接到漏网调用点，属准则合规修复，非新机制非新抽象。

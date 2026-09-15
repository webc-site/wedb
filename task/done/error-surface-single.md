# error-surface-single 甄别与执行

来源：next/design.md 条 15、next/glm.md 条 35（主代理预清理）

对标：garnet/libs/server/Resp/CmdStrings.cs 文案单点、GarnetStatus 错误单点

## 条一：跨文件重复错误文案（成立，范围比意见更大）

GEO 校验文案族两份，逐字节核实为 7 个常量重复（意见只列 4 个）：

RESP_ERR_NOT_VALID_RADIUS（ERR need numeric radius）
RESP_ERR_RADIUS_IS_NEGATIVE（ERR radius cannot be negative）
RESP_ERR_NOT_VALID_WIDTH（ERR need numeric width）
RESP_ERR_NOT_VALID_HEIGHT（ERR need numeric height）
RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE（ERR height or width cannot be negative）
RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT（ERR unsupported unit provided. please use M, KM, FT, MI）
RESP_ERR_COUNT_IS_NOT_POSITIVE（ERR COUNT must be > 0）

重复位置：wnode/src/session_parse_state_extensions.rs（&str 形态）与
wnode/src/resp/objects/sorted_set_geo_commands.rs（&[u8] 形态）

七个文案与 C# CmdStrings.cs 268-286 行权威文本逐字一致，无拼写偏差。

落点修正：意见建议「wnode 域内单点模块」，拒绝该落点。
C# 的单点是 CmdStrings.cs，其 rust 1:1 对应物已存在，即 wresp/src/cmd_strings.rs；
两处使用方均已 import cmd_strings（别名 cs）。再建 wnode 本地模块等于第三个家。
改法：七常量以 &str 形态落入 wresp::cmd_strings（同文件既有 RESP_ERR_NOT_VALID_FLOAT
等 &str 先例），两文件删除本地副本改引单点。sorted_set_geo_commands.rs 需要
&'static [u8] 的位置用 as_bytes() 收敛，顺带消掉 from_utf8(...).unwrap_or("") 样板。

附带修正：session_parse_state_extensions.rs 旧注释「cmd_strings 域由并行代理扩表，
此处本地对齐」已失效，随删除。

slow path 文案一份重复（非 C# 文案，wedb 自有降级兜底）：

RESP_ERR_SLOW_PATH_IO（wnode/src/resp/garnet_api.rs）
ERR_SLOW_PATH_STORAGE（wedb/src/server/cluster_session.rs）

两处字节一致，均为「ERR slow path storage error」。应答字节路径核实等价：
wnode 走 write_error_raw 原样写出，wedb 走 write_resp_error，其前缀判定
（首词全大写 >= 3 字符）命中 ERR 分支，同样原样写出，无加工。
改法：落 wresp::cmd_strings，命名 RESP_ERR_SLOW_PATH_STORAGE，两处改引单点。

范围外记录（不修改）：
RESP_ERR_INVALID_LON_LAT 两处形态不同（sorted_set_geo_commands.rs 静态无坐标，
session_parse_state_extensions.rs 格式化坐标）。C# 单路径（SessionParseStateExtensions.cs:799
TryGetGeoLonLat）为范围失败时格式化坐标、解析失败时 NOT_VALID_FLOAT；rust
wcol::parse_utils::try_get_geo_lon_lat 把两种失败合并为 None，静态文案是对该
API 形状的妥协。修正需改 wcol API 返回可区分错误，越界，仅记录。

TIMEOUT_IS_NEGATIVE / TIMEOUT_IS_OUT_OF_RANGE 仅单文件使用，无重复，不动。

## 条二：错误枚举收敛（两项成立，一项拒绝）

1. wnode 双 Error（成立）

wnode/src/error.rs::Error（8 变体，lib.rs 导出）与 wnode/src/service.rs::Error
（Store/RangeIndex/Wal/Aof 四透传变体）。service 版全仓零引用（grep 证实无
service::Error / service::Result 消费点，调用方均经 aok 泛型 ? 传播）。
改法：RangeIndex（wkv::RangeIndexError）、Wal（waof::Error）、Aof（AofReplayError）
三变体并入根 error.rs（#[error(transparent)] + #[from]，Store 已存在），
删 service::Error / service::Result，service.rs 内部改用 crate::Result。
顺带简化 service.rs 的 WalLog::new 手工 map_err（并入 Wal 变体后直接 ?）。

2. wvector 五枚举（部分成立）

真实使用面核实：
StoreError（store.rs 定义；fsm.rs 消费）—— 跨模块
FsmError（fsm.rs 定义；provider.rs 消费）—— 跨模块
QuantizerError（quantization.rs 定义；provider.rs 消费）—— 跨模块
WedbProviderError（provider.rs 定义；service.rs 消费）—— 跨模块
PrepareError（element_data.rs 定义并使用，lib.rs 仅 re-export，外部零消费）—— 单模块

改法：建 wvector/src/error.rs，收前四个跨模块枚举（WedbProviderError 为
crate 顶层聚合错误，随迁；diskann::convert_error! / always_escalate! 宏随移）。
PrepareError 留 element_data.rs 不动（leaf 留本地是对的）。
不上收 wbase，意见与甄别一致。

3. wcpr 双变体合一（拒绝）

ChecksumMismatch（index_ckpt.rs，索引快照数据 CRC32 校验）与
MetaChecksumMismatch（manager.rs，Checkpoint 元数据 v2 完整性封签）：
语义不同（数据页位翻转 vs 元数据文本篡改）、Display 文案不同、各仅一处构造，
且 wkv/tests/checkpoint/fault_defense.rs 分别 match 两个变体断言分流行为。
非重复定义，合一将丢失可区分语义并破坏既有测试。拒绝。

## 红线自查

不改语义：所有文案逐字保留，RESP 应答字节级等价（两写出路径已核实）。
不动并发面：aof/、key_admin_commands.rs、wresp catalog 表、wconf 均未触碰；
仅 wresp/src/cmd_strings.rs 追加常量（该文件不在 catalog 面）。
wedb/src/server/cluster_session.rs 只删一个本地常量、改一行 import 面。

## 验证结果

分支 w3-error-single 四次小步提交（f4045ad GEO 文案族单点、a1e7244 slow path
单点、83d61d7 wnode 双 Error 并根、685cd05 wvector 中心 error.rs），
已并入 dev（e810c4c），worktree 与分支已清理。

./clippy.sh：3 任务完成，零警告（禁 allow，无新增 allow）。
./test.sh：2025 tests run: 2025 passed, 1 skipped；regress 2 passed。
bun ./js/check.js：exit 0，无输出，无新增缺失/重复（未新增任何 ignore 登记）。

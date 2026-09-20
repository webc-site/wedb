# MSET 批量写入接入 RangeIndex 写门（细化方案）

来源：next/mset-rangeindex-write-gate.md 与 next/zcode.data.md 问题 3（已清理）。

## 判定：成立

rust 侧核实：单键写入口 network_set、network_setex_impl、apply_set_with_expiry、
network_set_conditional（Meta 域臂）与 BITOP dest 均已前置 ri_write_gate
（wnode/src/resp/basic_commands/set.rs，判据单点 meta_is_range_index）；
但 network_mset 快路径（wnode/src/resp/array_commands.rs）直接
try_upsert_batch_sync_with_prefix 折叠写入，slow::mset 慢路径
（同文件 slow 模块）直接 try_upsert_batch_sync 加逐键 upsert_string 兜底，
两路均无门。MSET ri_key value 会盲写 String 记录覆盖 RangeIndex 存根，
磁盘 wbftree 树文件留存成孤儿，元数据双域不一致。票面成立。

## C# 语义核实

- RespCommand.cs:IsLegalOnRangeIndex 是命令白名单（DEL/UNLINK/TYPE/RENAME/
  RENAMENX/DEBUG 与 RI 族），MSET、MSETNX、SET 均不在白名单内。
- 判别挂在存储函数层：RMWMethods.cs:InPlaceUpdater 与 UpsertMethods 双臂对
  RecordType == RangeIndexRecordType 且命令不在白名单的记录置
  RMWAction.WrongType。即 C# 对 MSET 的键是逐键判定（NetworkMSET 循环逐键
  调 storageApi.SET，每键独立命中该门），不是整命令预检。
- C# NetworkMSET 对 WRONGTYPE 的响应是 promote 事务后 DELETE 再 SET 覆写
  （与 NetworkSET 同一臂）。该臂是对象存储专用（RecordType 0 与 object 位
  共用一记录）；对 RI 记录会连树文件一起清退。ri_write_gate 的既有裁决
  （set.rs 文档注释）已明确：rust 不照抄该覆写通道，盲写会留下字符串值与
  孤儿树文件的双域残留，字符串写入口一律 WRONGTYPE 拒。rust 既有门语义
  （tests/range_index_wrongtype_gate.rs 方向 2 已断言 SET/SETEX/GETSET 打
  RI 键回 WRONGTYPE）是本票对齐基准，不另起炉灶。

## 语义选择：整命令预检拒绝

C# 逐键判定的可观察出口（DELETE 覆写）在 rust 已被裁决不采用；rust 单键
出口为 WRONGTYPE。批量写入门控只能落在折叠之前：try_upsert_batch_sync 无
逐键 WrongType 通道。因此采用写前逐键预检、任一 RI 键整命令拒绝：预检循
环在任何写入之前，遇 Blocked 直接应答 WRONGTYPE 返回（零键写入，无半提交
残留，天然满足 MSET 的原子性观感）；遇 Deferred（元记录有磁盘候选）沿用
既有出口整体降级慢路径，慢路径执行臂用异步对偶门 ri_write_gate_async 复
裁决后同样整命令拒绝。这与票面解决建议 1、2 一致，也复用既有三态出口，
不新增判据。

## MSETNX 说明（相邻票边界）

MSETNX 与 MSET 共用 try_upsert_batch_sync 折叠入口，但其前置判定
probe_alive_with_prefix / probe_alive_domain_async 是三域存活探针，存活
RangeIndex 的 Meta 元记录同计存在，NX 语义下任一键存在即整命令回 :0 且零
写入——RI 键在 MSETNX 下已被存在性判定天然挡住，不存在覆写通道，故不需要
也不应再叠加写门（叠加即第二套判据散落）。MSETNX 降级慢路径重放 NX 判定
是相邻票 next/msetnx-page-flip-replay-nx.md 的范围，本票不动。

## 改动面

1. array_commands.rs network_mset：arity 校验后、折叠前，逐键调 ri_write_gate
   （复用 basic_commands 单点，Blocked 已帧内应答，Deferred 降级）。
2. array_commands.rs slow::mset：折叠前逐键 await ri_write_gate_async
   （storage_session.rs 既有异步对偶，与 slow.rs 字符串写臂同款），任一存活
   RI 写 WRONGTYPE 帧返回。
3. tests/range_index_wrongtype_gate.rs：方向 2 新增用例或扩展现有循环，
   断言 MSET 打 RI 键回 WRONGTYPE 且 RI 记录与树内字段不受损、普通键不受
   波及（预检拒绝为整命令，MSET a ri_key b 全部不写）。

不动 wkv 批量写原语（门挂 RESP 写入口是既有设计），不动 EXPIRE 族、
SET 解析宽度、MSETNX 重放语义。

## 验证

cargo check -p wnode；定向 cargo test -p wnode --test range_index_wrongtype_gate。

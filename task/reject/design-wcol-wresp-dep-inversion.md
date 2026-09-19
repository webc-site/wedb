裁决：不成立（依赖方向与 C# 同构：Garnet.server.Objects → Garnet.common.RespWriteUtils；非倒置；「仅用于两类型」取证不实）
来源：next/agy.design.md 条 21 + next/muse.design.md 条 19（两轮同题）。核销 2026-09-19。

一句话结论：wcol → wresp 对应 C#「Objects/ 引用 RespWriteUtils（Garnet.common 底层库）」，
是上层用底层协议原语的正向依赖，无环；wcol 对 wresp 的用途远不止 ObjectOutput 与
RespInputFlags 两类型，把两类型上移也消除不了依赖边，反而与在排票
next/object-output-payload-direct-write.md 的 ObjectOutput 形态改造冲突。

逐条核销
1. C# 对标原貌：ObjectOutput 在 garnet/libs/server/Objects/Types/ObjectOutput.cs（Garnet.server
   库），RESP 写出原语 RespWriteUtils 在 garnet/libs/common/RespWriteUtils.cs（Garnet.common
   底层库）——C# 对象层引用底层协议库是原生形态；muse 条 19 所称「C# Garnet.server 不引用
   modules 的分层被倒置」对比对象错位（RespWriteUtils 不在 modules，在 common 最底层）。
   rust wresp 定位即 common 层协议原语，wcol → wresp 与 C# 方向一致，无环无倒置。
2. 用途取证（不止两类型）：wedb/wcol/src/resp/output.rs 头部 use wresp::{cmd_strings,
   ext::RespVecExt, resp_memory_writer::{Resp3, RespWriter}}；除 ObjectOutput（对标
   ObjectOutput.cs）与 ObjectOutputFlags 外，hash_object_impl 等对象实现就地构造 RespWriter
   写 RESP（对标 C# GarnetObjectBase.Scan 就地构造写法，output.rs:1-6 模块头自述）；
   将 ObjectOutput/RespInputFlags 移到 wresp 或 wnode 后依赖边仍在，主张的收益不成立。
3. 一致性旁证：next/resp-frame-literal-single-source.md 的修法第 4 条正是让 wcol 内手写帧
   改走 wresp::RespWriter 单点（wcol 依赖 wresp 是该票修法的前提）；本条主张拆依赖与其相悖。
4. 冲突风险：next/object-output-payload-direct-write.md 已细化 ObjectOutput 挂载形态改造
   （含 wcol/src/resp/output.rs 改造方案），若再移动其 crate 归属，两票互相踩改动面。

# HyperLogLog 损坏载荷拒绝与 PFMERGE 方向矩阵集成测试补充

来源：next/zcode-r2-test.md 缺口 4。

## 问题
C# 在 API / RESP 层对标覆盖了畸形 HLL dump/sparse 载荷拒绝与 sparse→sparse / sparse→dense / dense→dense PFMERGE 方向矩阵（Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogRestoreCorruptedDumpPayloadIsRejected、HyperLogLogValidatorRejectsMalformedSparsePayload、HyperLogLogTestPFMERGE_SparseToDenseV2）。
rust 目前仅在 whyperlog 内部有单元测试，RESP 命令层（`PFMERGE`、畸形载荷经会话写入被拒）缺少端到端用例。

## 目标
在 `wedb/wnode/tests/hyperloglog.rs` 中补充：
1. `PFMERGE` sparse 到 dense 及多源合并后 `PFCOUNT` 正确性测试；
2. 畸形 HLL 载荷（非法的 HLL 头、错误的魔数、超长或截断的稀疏字节）写入时返回 RESP 错误帧的拒绝测试。

## 验收
1. cargo check -p wnode --tests 0 error 0 warning。
2. wedb/wnode/tests/hyperloglog.rs 测试通过。

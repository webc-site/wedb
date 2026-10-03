# r438 阶段一甄别记录：分层树索引面（wbftree / windex / wkv range_index）空手

- 状态：登记档（非待执行任务，勿审勿分流）
- 席位：只读发现席（纯静态实读，未跑任何 cargo，未改任何跟踪文件）
- 结论：0 立案。三面全文实读并对读 garnet 原文与 bf-tree-0.5.6 引擎源码，三条候选线索逐一证否。

## 一、证否清单（后续席勿再开同面）

1. chunk 零长不对称（序列化侧容忍 keyLen=0 / fileBytes=0，反序列化侧判 Corrupted）
   - 判否依据：C# garnet RangeIndexChunkedDeserializer.cs:109 与 :149 同形即 `<= 0` 走 Error 分支，
     rust 侧 keyLen 上限判据系附加防御，非行为发散。纯对位，不立案。

2. 发布臂 rebind_stub 未清 serialization_phase（C# RangeIndexManager.Migration.cs:187 有
   `SerializationPhase = 0` 复位）
   - 判否依据：rust 全域 grep 该字段仅命中 encode/decode 与测试探针，零生产消费者；C# 侧同族字段
     亦只写不读。「漏清死字节」危害面不可证，按严禁凑数写伪案不立案。

3. 扫描迭代界与引擎长度界
   - ri_count_by_scan 起点取 `[0]` / usize::MAX，对位 C# BfTreeService.cs:496-506 的
     ScanAllStartKey = [0] / int.MaxValue；count=0 返 0、start > end 返 0、闭区间含端、
     空 start_key 由引擎报 Err（bf-tree-0.5.6 src/range_scan.rs:141/:155/:189），与 rust 侧
     「1:1 对标原生层」注释实证一致。
   - 引擎 +1 长度界（wbftree lifecycle.rs:123/:126 对 C# CreateBfTree 直传）被
     wkv/src/range_index/ops.rs validate_bftree_record 单点契约闸全遮蔽：全部写口
     （RI.SET / 批量 / 升阶建树 / AOF 回放）先过闸后入引擎，用户不可观测。不立案。

4. heal / drain / migration / promote / ops 各面逐函数核完：claim 配对表、drain_guard_ok 单点判据、
   四态落地折叠、双域墓碑序，均自带在册注释或已结票锚（本域 task/done 数十票），无新缺陷面。

## 二、查重经过

grep 范围 task/done|reject|ing|issue + doc/zh/deviations.md，符号面 serialization_phase、
publish_migrated、key_len、trailer、validate_bftree_record、扫描界关键词，无在途同面案。

## 三、并入已扫净缝

「分层树索引内核面（wbftree 编解码与长度界、windex 内存索引、wkv range_index ops/heal/drain/migration
的扫描界与发布链）」自本轮并入续轮勿重派清单；唯 ZSCAN 出分值臂损坏载荷面属本波在途票
（task/ing/wnode-tiered-zscan-corrupt-score-silently-zero-vs-materialize-failfast.md），非本面。

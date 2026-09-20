# 紧缩物理删段对检查点恢复窗钳制(P0:熔断紧缩可毁最近检查点)

来源:next/zcode-r11-gc.md 问题 1(已随本票认领从 next 移除)。

## 问题(P0,恢复破洞)

rust 检查点非自含:wcpr 只落索引快照+meta(hlog 地址视图),数据页依赖
flush_all 后驻留主设备;恢复由 run_recovery_pass 扫 [begin, tail) 重放。
而紧缩/移位推进 begin 后经 truncate_begin_until 无条件物理删段
(wdev 分段设备 remove_file),截断链无任何检查点感知钳制。

时序:拍检查点 T_c → 换号风暴使 gc_dead 积压 > 1024 → 熔断旁路
(gc/compact.rs try_compact L89 旁路,until=safe_ro 全速紧缩,默认配置经
spawn_bftree_reclaimer 200ms 自动可达)→ 段删除覆盖 T_c 检查点重放窗 →
崩溃 → recover_latest 选中 T_c → 扫已删段报 SegmentNotFound(拒启)或
容错续跑但 DbMeta 映射丢失→冷装载盲分配→旧数据整体不可达。
purge_unrecovered_checkpoints 删光更早检查点,无回退余地。

C# 对位:DoCompactionAsync 同款物理删段,但双闸:CompactionTask 仅在
CompactionFrequencySecs>0 且 CompactionType!=None 注册(默认零紧缩),
且 StoreWrapper.cs:702-705 明文警示 "Compaction will delete files, make sure
checkpoint/recovery is not being used";另有检查点完成臂补跑紧缩耦合。
rust 熔断旁路为自引入机制,默认可达且无警示。

## 修法(二选一,证据入回报)

1. 路径 A(推荐,C# 「拍检查点才真正删文件」语义落地):
   shift_begin_address/truncate 链对「最近已发布检查点的 hlog_meta.begin/tail
   重放窗」钳制——越窗段延后到下一检查点发布后回收。
   检查点发布点(wcpr create 完成)记录基线;truncate 侧读基线钳制。
2. 路径 B:熔断旁路与常规紧缩统一受「距上一检查点发布」闸门。

FLUSHALL 的 flush_all_databases→shift_begin_address(tail) 同穿此面,一并处理
(C# unsafeTruncateLog 语义共享契约,但 rust 检查点保留策略须与 destructive
截断联动)。

## 纪律

- 与 bftree-release-gate(安全纪元门控)、gc-compact-store-flight-gate(在途
  单飞闸)正交:本票只加检查点窗钳制,不动飞行闸与队列门控。
- r9-soak 发现1(单文件装配下 truncate 空操作)与本票同根不同面:
  单文件下本票钳制自然空转无害;分段化物理回收另票,本票不做。

## 验收

1. cargo check -p wdev -p whlog -p wkv -p wcpr -p wnode -p wedb 零 error 零 warning。
2. 定向测试:拍检查点 → 制造积压触发熔断紧缩 → 断言重放窗内段未被删 →
   kill -9 → recover_latest 恢复成功且 DbMeta 映射完整(复用 wkv/tests
   checkpoint 夹具;无夹具不硬造,回报说明)。

## 门禁

只跑 cargo check(-p 收窄)与定向测试。严禁 ./test.sh 与 ./sh/clippy.sh。

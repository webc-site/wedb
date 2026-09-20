# 后台懒降阶候选扫描复用活跃 bftree_domains 登记表

来源：next/zcode.my.md 问题三

## 问题

collect_demote_candidates 目前直接线性扫描 HybridLog 从 begin_address 到 tail_address，
日志较大时后台评估产生高 IO 与 CPU 负载。
系统在 bftree_release.rs 中已维护活跃 bftree_domains 登记表，未复用该内存表进行候选直读。

## 涉及路径

- wedb/wnode/src/resp/objects/tiered_demote.rs
- wedb/wkv/src/vdb/bftree_release.rs

## 解决建议

1. 将降阶候选收集从日志全扫改为直接遍历 bftree_domains 登记表。
2. 结合访问时间与热度策略评估候选分层对象，大幅降低后台扫描开销。

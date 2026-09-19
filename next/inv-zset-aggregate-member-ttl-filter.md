优先级：高（P1 数据正确性）

问题
ZSET 聚合族（ZDIFF/ZINTER/ZUNION/ZINTERCARD）的计算内核裸遍历字典，不滤已过期成员，
过期成员混入聚合结果；成员级 TTL 语义在聚合面失效。

取证（dev e75716e，按当下代码）
- wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:902 diff_sets：
  filter 只查 `rest.iter().any(|o| o.sorted_set_dict.contains_key(&e.member))`，
  无 is_expired 谓词；
- 同文件 combine_sets（ZINTER/ZUNION 内核，:920 起）与 sorted_set_intersect_length
  （:362，ZINTERCARD）同样零过期裁决；
- 全仓 grep live_dictionary 零命中 —— wcol 无存活视图口；
- 过期结构在 wedb/wcol/src/zset/sorted_set_object.rs:161（expiration_times 字典）与
  :165（expiration_queue 最小堆），is_expired 在 :590；
- 命令层 sorted_set_commands/ 目录 grep is_expired 零命中。

C# 对标（garnet 相对路径:符号）
- libs/server/Objects/SortedSet/SortedSetObject.cs:Dictionary getter（:237-256）：
  堆顶未到期直接返回 sortedSetDict（零成本视图），有到期项则逐成员 !IsExpired 过滤
  建新字典 —— 即「存活视图口」的原始形态；
- 同文件 CopyDiff（:537-571，ZDIFF 内核）：两集合成员均判 !IsExpired 才入结果；
  InPlaceDiff / 聚合族同口径。

修法建议
wcol/src/zset 补存活视图口（对位 Dictionary getter：expiration_queue 堆顶未到期即
零成本返回全集引用或迭代器，有到期项才过滤），聚合内核 diff_sets/combine_sets/
sorted_set_intersect_length 的成员枚举改走该口，一处定义禁两套口径；命令层不重复
实现成员级裁决。注意与 zset-o1 计数面（sorted_set_object.rs count() 已走
delete_expired_items 堆序 purge + len）保持「堆序判断」同一套谓词来源，勿各写一份。

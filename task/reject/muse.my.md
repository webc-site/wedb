本档为 next/muse.my.md（24 条，主题＝自定义优化：分层存储／升阶降阶／原子换域／计数口径）的独立复核记录。

结案形态：0 行剪除。现刻主仓 `git status --porcelain next/muse.my.md` 为 `D  next/muse.my.md`（并发分拣代理已整档消费该来源文件，工作树中已不存在），按规程不重建来源文件、不重裁已落位条目，转独立复核：逐条按现刻 HEAD 代码重取证据核其载体与判词，本档只记复核结论与两处订正，不重复转录原文（原文与拒绝理由已在各 my-* 拒档与载体票内）。

一、复核一致（22 条，载体与判词均与现刻代码事实相符）

条 1 拒：task/reject/my-promote-atomicity.md。复核 wkv/src/range_index/stub.rs:67-72（删键臂先写信封域幂等墓碑）、:212-216（升阶尾段 delete_raw(&env_k).await? 硬错上抛，注释明禁 let _ =），双态残留由墓碑兜底已在位；重灌臂 tree->tree 先拆后建是另一题，在册载体 next/tiered-reflush-atomic-swap.md。
条 2 拒：task/reject/my-demote-volume-prefilter.md。复核 wnode/src/resp/objects/tiered_demote.rs:11-16（头注如实写明预筛只走条目维、体积维经物化后复用 should_demote 单点）、:163、:248；wcol/src/lib.rs:56-58（count AND heap_bytes）；wval/src/meta.rs:81-92（MetaValue 确无体积标量，补水位预筛属设计变更）。原条「头注自称与实现矛盾」不实，判拒正确。
条 3 拒：task/reject/my-ripromote-rirestore-enum.md。复核 wresp/src/command.rs:83-86（编号 63/64 空缺处已注明由 wkv 承接并点名两函数）、wkv/src/range_index/stub.rs:423 promote_range_index_to_tail（doc :421 点名 RIPROMOTE）、:572 restore_range_index_stub（doc :281/:553 点名 RIRESTORE）、wkv/tests/store/range_index.rs:290-291/:756-757 用例在测。「原语名全仓零实现」不实。
条 4 成立已有载体：next/my-wcol-count-name-split.md。复核 wcol/src/types/garnet_object.rs:55（trait count(&self) 只读）、:317-321（zset 侧注明「升阶判定用未剔过期 raw len」）、wcol/src/hash/hash_object.rs:571-574 与 wcol/src/zset/sorted_set_object.rs:571-574（内建 count(&mut self) 先 delete_expired_items 再 len）、wnode/src/resp/objects/object_store_utils.rs:398-431（命令面走 MetaValue.size / count_of_blob O(1) 快道，不经内存口径）。命名分裂为真、票面范围恰当。
条 5 拒：task/reject/my-ricount-range-guard.md。复核 wkv/src/range_index/ops.rs:393-403（唯一计数出口注释）、wnode/src/resp/rangeindex/resp_server_session_range_index.rs:10-12/:677-681（RI.LEN 解析期归一、注释「这里不存在第二套计数入口」）。断言无对象（区间臂从不调用 range_index_count），注释约束即 SKILL:50 钦定机制。
条 6 拒：task/reject/my-rangeindex-type-fork.md（自述来源含 muse.my 条 6），注释增量另立 task/ing/data-rangeindex-enum-comment.md。复核 garnet/libs/server/Objects/Types/GarnetObjectType.cs:18-35（C# 确无 RangeIndex）、.agents/skills/transpile/SKILL.md:52（RangeIndex=5 系本仓规范明示）、wnode/src/resp/array_commands.rs:500-553（TYPE 对升阶集合键回显 MetaValue.collection_type 标准名，无 rangeindex 分叉）。
条 7 成立已有载体：task/ing/custom-obj-multikey-metagate-and-tag-u8.md（worktree /tmp/fork/fix-custom-obj-multikey 与分支 fix-custom-obj-multikey 实存在跑）。原条「custom_object_type_name 运行时比串」不实：wnode/src/resp/custom_objects.rs:33-42 是 const fn 内 u8 标签等值扫描；票面射程为 object_tag 型别擦除与 obj_decode_custom(want: u8)（custom_objects.rs:22-27 清单单点）。
条 8 成立已有载体：next/object-output-payload-direct-write.md（含 f61-obj-output 单轨挂载细化方案）。
条 9 拒：task/reject/my-write-batch-funnel.md。
条 10 拒：task/reject/my-prefix-hoisting-write.md。复核 wnode/src/resp/vector/vector_manager_locking.rs:47-68（registry_key 以 prefix 为入参、STACK_KEY_CAP 栈缓冲拼装，无逐次 varint 重算），原条取证确不实。
条 11 拒：task/reject/my-watch-ttl-gc-bump.md（C# IncrementVersion 只挂写路径完成钩子，惰性过期与紧缩丢弃均不推进，补推进属自造优化）。
条 12 拒：task/reject/my-replay-double-mapping.md。复核 wnode/src/aof/aof_processor.rs:1207/:1219（KeyContextGuard 经 set_virtual_context 直设物理域）、:594/:614（FlushDb/FlushNs 回放走 flush_virtual_database / flush_virtual_namespace 物理域原语）。原条描述的二次映射形态已不存在；残余真缺口在册 next/my-flush-replica-virtual-id-divergence.md。
条 13 拒：task/reject/my-sketch-key-hash-comment.md。复核 wedb/wedb/src/server/migration/sketch.rs:8-13（模块注已声明其为「迁移门控 bloom bitmap、键级可访问性」，其 slot 命名为 bitmap 位序）、wbase/src/hash_slot.rs:54-62（slot_of 库级定槽单点，与 sketch 无调用交集）；「改槽位单元素传入」违 garnet/libs/cluster/Server/Migration/Sketch.cs 的键哈希同形对标。
条 14 成立已有载体：前半 next/acl-setuser-live-connection-propagation.md，后半 next/my-acl-list-two-pass-snapshot.md。复核 wnode/src/resp/acl_commands.rs:112-125（LIST 第一遍仅计数写长度、第二遍重扫实存）。
条 16 拒：task/reject/my-gxhash-deterministic.md。复核 wedb/Cargo.toml:53-61（deterministic 例外的对标理由注释已在位）、wbase/src/map.rs:6/:21/:53（进程级 OnceLock 随机种子与 42 恒种子的口径注释）。
条 17 拒：task/reject/my-bitcode-borrow-u8.md。复核 wcol/src/hash/hash_object.rs:128（「刻意差异：bitcode 0.6 的零拷贝借用编码仅支持 &str」约束注释已在位，即原条自给备选项）。
条 19／20／21／22 拒（合并档）：task/reject/my-muse-selfkeep-batch.md。复核：Grep 全仓 wedb/ 对 serde_json 零命中，wext_json/src/json_object.rs:8/:61/:70 与 wlua/src/functions/cjson.rs:6/:344 均 sonic_rs；wbase/src/time.rs:3-15 两时钟域分工注释、wkv/src/ttl.rs:70/:129-130 TTL 取 now_ticks；wbitmap/src/bit_count.rs:5-11 与 bit_op.rs:6-7 已互指 wbase::simd 单点并声明差异（即原条「补互指注释」动作已在位）；wconf/src/node_options.rs:4（nested_text 钦定、C# 双格式不转写）与 connection_protection_option.rs:9，无第二配置文件格式依赖。
条 24 成立已有载体：next/design-anchor-remount-batch.md 组 3（复核 wnode/src/resp/vector/vector_manager_cleanup.rs:221 与 wnode/src/service.rs:1419 两处 VectorManager.cs:VectorManager 锚点实存）；后半 HashSet 对判否复核：wnode/src/resp/objects/tiered_collection_ops.rs:691 为函数体内行内 // 注释，不入 check.js 文档注释扫描，真实现锚点唯一在 wcol/src/hash/hash_object_impl.rs:247。

二、订正一（条 15，结论采信、取证面过窄）

task/reject/my-gossip-hex-base32.md 拒绝原因 1 称 hex_str_u128 的「生产命中仅 cluster_manager_slot_state.rs 各错误臂」——此说过窄，现刻另有文字渲染与日志命中面：wedb/wedb/src/server/cluster_config/serializer.rs:206-235（get_cluster_info → append_node_info，:234 注释自述「节点 id 仅在 RESP 渲染点转 32 字符小写 hex（对标 C# 40 hex 字符串形态）」）、同文件 :265/:311/:314/:452/:573、cluster_config/mod.rs:172（短 id 切片）、cluster_provider.rs:600/:611/:614/:1392 与 server/cluster_manager_slot_state.rs 错误臂。

结论仍不成立，但正确依据是：C# 的 Guid hex 串同样只出现在文字与日志面（garnet/libs/cluster/Server/ClusterConfig.cs:519 GetClusterInfo、garnet/libs/cluster/Session/RespClusterBasicCommands.cs:277 CLUSTER NODES），gossip/复制线格式在 rust 侧为 bitcode 二进制载荷（serializer.rs:19-21 线格式版本注记），内部 id 全程 u128 二进制，落盘文件名走 base32（wcpr/src/meta.rs:61-65 token_to_base32、wbftree/src/manager/mod.rs:129-131 hash_prefix）——正是 SKILL「内部纯二进制不转字符串，落盘文件名转 base32」的分工；且原条自给的备选动作「注明 hex 仅为 Guid 兼容」已由 serializer.rs:234 注释承接。后续审查勿再以「心跳 hex 编解码」复报，亦勿照抄该拒档的命中面清单。

三、订正二（条 23，旧拒档已过期，勿据其判「无事可做」）

task/reject/design-anchor-dup-false-positives.md 条 12 曾以「wlua 两快路径 doc 与 .cs 之间是空格、不匹配 CS_REF_REGEX」核销本条。现刻 HEAD 不符：wedb/wlua/src/functions/redis.rs:401（try_fast_path_set）与 :478（try_fast_path_get）的函数文档注释均已是 `libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting SET/GET 分支` 冒号形态，与 :584 总入口锚点合计三挂同一符号，check.js 判重必报该组——独立佐证见 task/ing/net-checkjs-dup-anchor-trio.md:14（现刻实测 15 组重复中点名 LuaRunner.Functions.cs:ProcessCommandFromScripting 一组，并划归 design/my/lua 域不由其处理）。

该条已由并发分拣转录进在册载体 next/design-anchor-remount-batch.md 第七组（:62-67，处置＝两快道去锚点改散文、锚点只留总入口），故不再另立新票；本订正的作用是把旧拒档条 12 标为已失效，避免下一轮审查代理拿它撤销第七组。

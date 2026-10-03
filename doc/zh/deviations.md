# doc/zh/deviations.md 架构设计偏差裁决登记册（2026-09-28 重置后回收重建索引册）

## 册头声明

1. 本册系 2026-09-28 本仓历史压缩为单个 init 提交后，从存活载体回收重建的裁决单源索引册。原册（100+ 节）未随重置迁移，doc/zh/ 当日仅存 collection.md 与 db.md；重建票与主控审核结论见 task/done/doc-deviations-registry-lost-in-init-reset-migration.md。
2. 本册 § 号与「第 N 条」系历史编号，本册为 09-28 重建索引，节号可对上者即在册裁决；沿用原编号，缺失节留空缺位不重编。原册在册期新条按「册尾顺编、先入库者得号、撞号让位不写死」规则占号（规则本体见 task/done/b2-screening-recheck-notes-20260925.md），故历史号密集非稀疏；带字母尾注（§15a-f、§32b、§117a-d、§124d、§165a-c、§166a-c 等）系同节族下分目。号位时间锚：2026-09-25 册尾为 §98（b2 注记），2026-09-27 册尾为 §162（慢日志票甄别注记），2026-09-27 至 09-28 又落 §164-§176、§178、§181。
3. 新裁决的真值源是票体：task/done 常驻可 grep。本册只登已回收的在册裁决；台账未及者以 task/done|reject|issue 票体 grep 为旁证真值源。
4. 条目口径：每节只登判据加符号名锚（函数/类型/常量/文件名），严禁钉行号。教训实证：§32 族曾因行锚六连漂消耗一整张纯订正票（task/done/doc-deviations-sec32-list-anchors-drift.md）。
5. 硬规则：每节必须带一手来源指针（task/done/<票>.md 或 task/reject/<票>.md 路径，或 doc/zh/collection.md 行内 § 批注锚，或代码注释引用符号锚）。无实证来源者一律不录正文，在册尾列出「号位空缺清单」，严禁为凑册编造判据。
6. 代码内现存约六百余处历史 § 引用保持原样，不批量改写（避免与在途改动撞车）；代码内引用号位能与本册对上者即在册裁决，对不上者见册尾号位空缺清单。

---

## 一、在册裁决（已回收判据面，按号位升序）

### [§1（第 1 条）] 最短往返裁决与浮点溢出格式化
- 判据：最短往返裁决；浮点格式化求和溢出（HINCRBYFLOAT 等）和逾 DBL_MAX 恒经 format_double 单源落 "inf"/"-inf"（严格对标 Redis 而非 C# Infinity 词形），尾回指注在册；非 0 租户会话点查租户内 default 参与门禁。
- 符号锚：format_double、HINCRBYFLOAT、hash_increment_float
- 来源：wedb/wresp/src/resp_memory_writer.rs、wedb/wcol/src/hash/hash_object_impl.rs 注释锚

### [§2] RESP_ERR_GENERIC_SCORE_NAN 文案与 NaN 拒收
- 判据：分值解析遇到 NaN 恒拒收且文案逐字节相同（RESP_ERR_GENERIC_SCORE_NAN 同串）；集合运算 NaN 分值恒归 +0。
- 符号锚：RESP_ERR_GENERIC_SCORE_NAN、parse_double
- 来源：wedb/wbase/src/num.rs、wedb/wnode/tests/resp_sorted_set.rs 注释锚

### [§4（含 §4a、§4b）] EXPIREAT/PEXPIREAT 绝对面钳制与饱和算式
- 判据：a 部裁秒/毫秒绝对面到期时间戳换算饱和算式，严禁回改；b 部裁与 EXPIREAT/PEXPIREAT 绝对面钳制同源，远端大值不再掐连接。
- 符号锚：convert.rs（expire_after_to_ticks、expire_at_seconds_to_ticks、expire_at_milliseconds_to_ticks、compute_expiration_ticks）
- 来源：wedb/wbase/src/convert.rs 注释锚

### [§6] wpubsub 分片订阅三表分离
- 判据：分片订阅（shard）与频道、模式订阅采三表分离形态，系对齐声明在册节；redis unstable 的槽级退订推 SUNSUBSCRIBE 机制在该对齐声明下成立。
- 来源：task/done/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration.md

### [§7] wpubsub SUNSUBSCRIBE 补齐
- 判据：SUNSUBSCRIBE 语义按上游补齐在册；该节与 §6 均不覆盖订阅生命周期对槽事件面（该面由来源票补全）。
- 来源：task/done/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration.md

### [§11（第 11 条）] 数据目录独占排他锁 datadir_lock
- 判据：SO_REUSEPORT 偏差下防多实例并发双写的唯一防线：数据目录单点独占排他锁（datadir_lock），在网络设备打开前单点获取，使网络层无法像 C# 随意重用。
- 符号锚：datadir_lock、acquire_datadir_lock
- 来源：wedb/wnode/src/datadir_lock.rs、wedb/wnode/src/server.rs 注释锚

### [§12] 随机源独立与单物理多回放拓扑恢复
- 判据：SRANDMEMBER / HRANDFIELD 两态随机源独立，不承诺同 seed，避免复刻上游安全缺陷；单物理多回放拓扑 AOF 恢复 boot 装配门半扇（同时校验物理数与回放任务数）。
- 符号锚：aof_boot_gate_violation、random_field
- 来源：task/done/wnode-aof-multi-replay-single-physical-silent-zero-replay.md、wedb/wnode/tests/sorted_set_random_member_live_zero.rs 注释锚

### [§14] PubSub 慢订阅者邮箱容量限制与挂起期探测保全水位
- 判据：慢订阅者内存水位刚性封顶 capacity，拒收丢弃量经 PubSubMailbox 承接（修复性分叉）；挂起期探测保全臂增设保全水位门（PROBE_PRESERVE_MAX_BYTES），累计保全量超门即停止重建探测读，防止全会话缓冲无界 OOM。数据面满水位丢尾策略本条所辖恒一字不动（publish/pattern_publish/shard 投递臂）；服务端主动状态通知（ShardUnsubscribe 槽迁出强制退订帧）入列独走有界等待重试兜底臂（自旋+yield 让出、逐邮箱预算、入列即止；超界 warn 留痕放弃并递增 dropped_shard_notify 可观测计数，悬挂定性为连接同 ns 生存期内、至 AUTH 换租/会话释放整体清退）——C# 广播直写 throttle.Wait 零丢弃形态的兜底投影，不落入亦不改动本条数据面判据。
- 符号锚：PubSubMailbox、preserve_probe、PROBE_PRESERVE_WATERMARK、try_publish_control、dropped_shard_notify
- 来源：task/done/wnode-probe-preserve-unbounded-recv-buffer-oom.md、task/done/wpubsub-shard-forced-unsubscribe-frame-loss-stuck-subscription-flag.md、wedb/wpubsub/src/subscriber.rs 注释锚

### [§15 族（§15a-f）] wext_json JSONPath 偏差臂级澄记
- 判据：主条澄记面仅覆盖 MatchTokens 容器臂与 In 不可达臂；§15a-f 分目对应形名：NX/XX、$ 根条件写、正则崩溃、键转义、解码防御帧、选项收尾。各分目均不涉交错臂（交错臂补全见来源票，向 C# 收敛、无新登记）。
- 符号锚：MatchTokens、In（JSONPath 谓词臂）
- 来源：task/done/wext-json-wildcard-container-arms-missing.md

### [§18] 修复型惯例家族（C# 缺陷 rust 防御先例谱）
- 判据：属于修复型惯例家族（与 §12/§16/§17/§20/§21/§82/§92 同谱）：上游 C# 存在潜在缺陷或未定义行为时，rust 侧实施严格边界收口与防御性处理。
- 来源：task/done/wnode-aof-multi-replay-single-physical-silent-zero-replay.md、task/done/wmetric-slowlog-id-width-and-incrit-numbering-unregistered-deviation.md

### [§20（含 a/b 分目）] TYPE 命令对空串及表外值归未知回空
- 判据：a 部裁 TYPE 命令解析已知类型；b 部裁空串 TYPE 归本标志及混合大小写、表外值归未知臂回空，系刻意偏离，边界不变。
- 符号锚：array_commands.rs（parse_scan_filter）
- 来源：wedb/wnode/src/resp/array_commands.rs 注释锚

### [§21] DUMP / RESTORE 校验和从类型字节起算
- 判据：DUMP / RESTORE 序列化载荷的 CRC64 校验和从类型字节起算（含类型字节的 rust 口径，与 types.rs 单源同构），保证合法载荷正常往返。
- 符号锚：network_dump、restore、crc64
- 来源：wedb/wnode/tests/restore_10byte_payload_slice.rs、wedb/wnode/tests/nx_conditional_ttl_degrade_replay.rs 注释锚

### [§22] wnode 向量族错误文案括注字节面
- 判据：向量族 WRONGTYPE 族错误文案的括注字节面在册节；与 §164（统一无句点刻意偏差）同域互指。
- 来源：task/done/wnode-vector-wrongtype-dotted-five-command-frame-text-divergence.md

### [§23] 向量族量化 mismatch 误导文案统一收敛
- 判据：向量族量化类型不匹配等错误文案统一收敛，不跟随 C# 多分支混乱文案。
- 来源：task/done/wnode-vector-wrongtype-dotted-five-command-frame-text-divergence.md

### [§25] wmetric 慢采样 pending 零样本条目
- 判据：pending 零样本系在册条目，非 tick 域选择面（tick 域另见 §166b）。
- 来源：task/done/wmetric-info-observable-divergence-registry-trio.md

### [§26] waof/wedb 复制 AofAddress 逗号串解析门禁
- 判据：AofAddress 逗号串解析门禁节；与 repl-history 合法性门（来源票所修面）正交在册。
- 来源：task/done/wedb-repl-repl-history-recover-legality-gate-missing.md

### [§27] whlog committed 前缀恢复面
- 判据：原册文宣称前缀中段截断由页装载报错承接；whlog 席现码核验为静默清零，该不符由来源票收口（页界装载闸向），册内原文措辞不可回收。
- 符号锚：HybridLog::recover（裸名 recover 以类型名伴锚消歧，whlog 域）、flushed_until 覆盖验
- 来源：task/done/whlog-recover-committed-prefix-eof-silent-zero-page.md、task/done/wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak.md

### [§32（含 §32 b) 清单）] 全仓严格整数文法消费点单源台账
- 判据：a 部裁前导零 strict 文法（TryGetInt 系整数参数面）；b) 清单系严格整数文法消费点单源台账（≥25 目）。已回收消费点目（符号锚口径）：
  - 条目 12 LTRIM：parse_i32_pair_args（快慢径共用单源，快臂 write.rs、慢臂 slow.rs 消费）
  - 条目 13 LINDEX/LSET：slow.rs 分层 Lindex 臂与慢物化臂；LSET 实解析在物层 strict_i32
  - 条目 14 LPOP count：write.rs 快臂门 / slow.rs 慢臂门
  - 条目 15 LMPOP/BLMPOP：parse_mpop_args 内 numkeys/count strict_i32，代理 parse_lmpop_args，消费点 list_mpop / list_blocking_mpop
  - 条目 16 LPOS RANK/COUNT：物层 read_list_position_input
  - 条目 24 MEMORY USAGE samples
  - 条目 25 SLOWLOG GET count
  - 其余条目（1-11、17-23、26 及后）判据与锚不可回收，见号位空缺清单注。
- 符号锚：wcol/src/list/list_object_impl.rs（list_set、read_list_position_input）、list_commands/mod.rs（parse_i32_pair_args、parse_lmpop_args）、shared_object_commands.rs（parse_mpop_args）
- 来源：task/done/doc-deviations-sec32-list-anchors-drift.md（条目 12-16 订锚实录）、task/done/wedb-cluster-mlog-key-time-frontier-parse-divergence-comment.md（文法轴划界）、task/done/wkv-bftree-release-queue-defers-engine-dispose-to-delay-window.md（条目 24）、task/done/wmetric-slowlog-id-width-and-incrit-numbering-unregistered-deviation.md（条目 25）

### [§35] 键空间 Rehash 期间同探针错误折叠
- 判据：键空间 Rehash 期间同探针错误折叠，Dead/None 假剔除折叠慢路径存储错误帧，统一错误分流。
- 符号锚：StoreSession::check_expired、Dead/None
- 来源：wedb/wkv/src/session/mod.rs 注释锚

### [§36] wtls 证书校验五分面
- 判据：TLS 校验面五分形态在册节。
- 来源：task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md

### [§43/§60] wconn SETNAME 收端解析面裁决
- 判据：SETNAME 收端解析面两条裁决在册（§43/§60）；与复制出站诊断名对账无涉（该差异本票不触碰之裁定见来源票）。
- 来源：task/done/replication-egress-client-name-observability.md

### [§51/§65] wpubsub 集群 PUBLISH 先发后订投递窗与单点直投
- 判据：两条目各钉集群 PUBLISH 先发后订投递窗一面；Rust 无磁盘日志介质，双路径收敛至单点直投；均不覆盖订阅生命周期对槽事件面。
- 符号锚：subscribe_broker.rs（publish_shard_now）
- 来源：task/done/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration.md、wedb/wpubsub/src/subscribe_broker.rs 注释锚

### [§53] 成员级 TTL 读侧单次 now 采样剔除
- 判据：成员级 TTL 读侧单次 now 采样惰性剔除，抽样域收敛至存活集，剔除不固化；声明数恒等实写数，到期账随摘除一并结清。
- 符号锚：types::member_ttl（format_member_ttl、decode_member、member_expired_at）、SortedSetObject::sorted_set_expire、tiered_collection_ops/hash
- 来源：wedb/wcol/src/zset/sorted_set_object_impl.rs、wedb/wnode/tests/sorted_set_pop_ttl_reply_header.rs 注释锚

### [§54] wnode UDS 父目录自创建
- 判据：Unix domain socket 父目录自创建形态在册。
- 来源：task/done/wnode-uds-tls-combo-silent-plaintext-endpoint.md

### [§55] wtls PEM-only
- 判据：证书装载 PEM-only 面在册。
- 来源：task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md

### [§56] wtls cert-subject-name 删员互拒门
- 判据：cert-subject-name 删员互拒门在册（两票同引）。
- 来源：task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md、task/done/wnode-uds-tls-combo-silent-plaintext-endpoint.md

### [§57] wtls fail-fast 双向
- 判据：TLS 装载 fail-fast 双向形态在册。
- 来源：task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md

### [§58（含 a/b/d 分目）] 事务内禁 HELLO 认证/换租与排队期分流
- 判据：事务内禁 HELLO 认证/换租（wedb 自有面，对标 SELECT_IN_TXN_UNSUPPORTED）；§58a 排队期可解析出合法 AUTH 选项组则中止；§58b 事务窗禁停泊围栏；§58d 无 AUTH 合法形回 +QUEUED 并由同步快臂直出应答 map。
- 符号锚：cmd_strings.rs、parse_hello_args（AUTH 选项解析已并入 parse_hello_args，parse_auth_args 独立符号已不存在）
- 来源：wedb/wresp/src/cmd_strings.rs、wedb/wnode/tests/transaction_tests.rs 注释锚

### [§63] wconn UNBLOCK 负数 ID
- 判据：裁 UNBLOCK 负数 ID 形态在册；不涉年龄时基面（该面由来源票收口）。
- 来源：task/done/wnode-client-age-dual-clock-list-info-divergence.md

### [§68] wvector RI 预览门恒开
- 判据：RI 面在册三条之一：预览门恒开形。
- 来源：task/done/wnode-rimetrics-is-live-is-flushed-probe-warmup-divergence.md

### [§69] wkv RC 页容量旋钮
- 判据：页容量旋钮族条目（与 §111 同谱）；SCAN 族探针面不在其内（来源票判转写遗漏成立）。
- 符号锚：skip_read_cache
- 来源：task/done/wkv-scan-live-key-probe-missing-read-cache-skip.md

### [§74] wdev 设备段三面
- 判据：设备段杂散文件、删段吞错、段尺寸三面在册；非 wcpr 检查点目录面（该面由来源票另案收口）。
- 来源：task/done/wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak.md、task/done/wcpr-latest-checkpoint-meta-unsealed-rescan.md

### [§75] wvector 向量登记表域
- 判据：向量登记表域条（VectorManager::key_index_registry，不驻 wkv 值域）；RI 升阶/创建链不在其域钉面内（来源票划界）。
- 符号锚：VectorManager::key_index_registry
- 来源：task/done/wkv-promote-ri-chain-mid-command-generation-tear.md、task/done/wedb-migrate-source-vector-set-delete-outside-aof-replica-diverge.md；doc/zh/db.md 向量登记表域节回指「第 75 条」系旁证

### [§76（第 76 条）] wconf 导出生效配置到 TOML 文件与全量字段差异
- 判据：导出生效配置到 TOML 文件（对标 Options.cs:501 ConfigExportPath；导出全量字段差异见本条）。TOML 序列化支持及配置导出格式与字段集差异裁决在册（node_options.rs:1723 首个校验锚）。
- 符号锚：wedb/wconf/src/node_options.rs（NodeOptions::export_config、to_toml_string）、Options.cs:ConfigExportPath
- 来源：wedb/wconf/src/node_options.rs 注释锚

### [§78] wdev 新建目录形 fsync 双屏障
- 判据：wdev 段新建形目录屏障口径在册节；与 rename 形（wbftree detach 票）、删除形（wcpr purge 票）四点异形不并案之裁定见来源两票。
- 符号锚：wdev sync_dir 双屏障口径
- 来源：task/done/wcpr-purge-checkpoint-unlink-dir-entry-missing-fsync-resurrection.md、task/done/wbftree-detach-tree-heal-rename-missing-dir-fsync.md

### [§79] wvector 域登记表既有口径
- 判据：向量索引记录驻 VectorManager 域登记表、wkv 值域外，系 §79 前后既有口径；迁移源端向量集删除链不在其内（来源票所修面）。
- 来源：task/done/wedb-migrate-source-vector-set-delete-outside-aof-replica-diverge.md

### [§80] 浮点格式化单源落 "inf"/"-inf"
- 判据：和逾 DBL_MAX 恒经 format_double 单源落 "inf"/"-inf"，与 ZSCAN/ZRANGE/HINCRBYFLOAT 单源收口（每成员恒发 2 项：成员 + 分值）。
- 符号锚：format_double、ZRANGE、ZSCAN
- 来源：wedb/wcol/src/zset/sorted_set_object.rs、wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs 注释锚

### [§82] SETRANGE 空写与间隙填零语义
- 判据：SETRANGE 超过当前字符串长度时，不对齐 C# zeroInit:false 陈旧间隙缺陷，rust 刻意填零。
- 符号锚：basic_commands/set.rs、string_in_place_grow.rs
- 来源：wedb/wnode/src/resp/basic_commands/set.rs、wedb/wnode/tests/string_in_place_grow.rs 注释锚

### [§83] wvector RI.CREATE CACHESIZE 守卫
- 判据：RI 面在册三条之二：RI.CREATE CACHESIZE 守卫形。
- 来源：task/done/wnode-rimetrics-is-live-is-flushed-probe-warmup-divergence.md

### [§84] wvector 关停排空 5 秒强收
- 判据：关停排空 5 秒强收后残余任务交 Runtime 析构兜底取消；丢弃路径真实在册。
- 来源：task/done/wvector-ensure-index-ready-startpoint-cancel-stuck.md

### [§89] wcol LPOS 词元面分叉
- 判据：LPOS 词元解析面分叉在册；命令族覆盖抽查视其为设计内不另报。
- 符号锚：read_list_position_params、read_list_position_input（单源词元解析加三门校验拆两函数）
- 来源：task/done/js-checkjs-session-api-wrapper-registry-gap.md

### [§96] wreviv 域钉族：复制快照域钉已修形
- 判据：域钉族三条之一（§96/§99/§118）：复制快照域钉已修形；命令降级续跑尾参面不在其内（来源票确证后另案收口）。
- 来源：task/done/wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap.md、task/done/wkv-promote-ri-chain-mid-command-generation-tear.md

### [§98] wacl ACL SAVE/LOAD 装配期不落盘
- 判据：ACL 认证器装配期不落盘裁决；回落臂系登记内保留面，其安全前提「记录在场即存储唯一真源」——FLUSHALL 物理拆除记录时 NoRecord 臂成为绕过 §98 收口的新入口，两节互恰，该前提破坏形由来源票收口。
- 来源：task/done/wnode-flushall-destroys-acl-user-records-auth-lockout.md、task/done/wcpr-latest-checkpoint-meta-unsealed-rescan.md

### [§99] wkv 快照装载读值域钉
- 判据：域钉族之二：快照装载读值域钉已修形（换号族各管一面口径见来源票）。
- 来源：task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md、task/done/wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap.md

### [§100] wtxn 无事务打包机制裁决（双键移动写回序）
- 判据：裁本仓无事务打包机制，以「写回序先目标后源」五臂补偿纪律替代，并容忍「元素双份」残态；「dst 已持久则目标唤醒必发」的贴发面纪律由来源票另半收口。双键移动写回序条，勿与域钉族（§96/§99/§118）混引。
- 来源：task/done/wcol-blmove-broker-half-commit-dst-wake-lost.md、task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md、task/done/wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap.md

### [§101] wconn 容量计数
- 判据：容量计数裁决在册；不涉年龄时基。
- 来源：task/done/wnode-client-age-dual-clock-list-info-divergence.md

### [§102] wtls 握手超时条
- 判据：TLS 握手超时条（缺省十秒量级口径）在册；§102 收口注曾以行号形被误引为「§1343」，已由来源票落册修锚。
- 来源：task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md、task/done/wnode-client-age-dual-clock-list-info-divergence.md

### [§103] wmetric SLOWLOG 随行锁面
- 判据：全册 SLOWLOG 面除 §32 条目 25 外文上仅 §103 随行锁面一笔；慢日志 id 位宽与取号序列化不在册（该面后落 §167）。
- 来源：task/done/wmetric-slowlog-id-width-and-incrit-numbering-unregistered-deviation.md

### [§104（含 a/b 分目）] ZSET 聚合族语义锁与交集收缩异常面消除
- 判据：宗一（§104 a）nan0 全产点归一：ZADD k inf m 后 ZUNION 1 k WEIGHTS 0 结果归零而非 NaN，严禁按 C# 回改；宗二（§104 b）交集真收缩异常面消除：ZINTER 2 a b（a 含 b 外成员 x，真收缩）正常回成员表且连接存活（C# 抛异常掐连接面不复刻）；ZDIFF 负 numkeys 翻案面（双侧同文非偏差）。
- 符号锚：sorted_set_commands/write.rs、zset_aggregate_nan0_disjoint_locks.rs
- 来源：task/done/b2-screening-recheck-notes-20260925.md:75、wedb/wnode/tests/zset_aggregate_nan0_disjoint_locks.rs

### [§105] wacl Enum.TryParse 怪癖族第二形（空白修剪）
- 判据：怪癖族谱系在册二形之一：空白修剪第二形。
- 来源：task/done/wacl-acl-setuser-enum-only-name-custom-fallback-registry.md

### [§106] wacl Enum.TryParse 怪癖族第一形（数字回退拒形）
- 判据：怪癖族第一形：数字回退拒形；MODULE 族数值回退拒形不动之边界见来源票。
- 符号锚：is_valid_custom_command_name（回落臂非本条面）
- 来源：task/done/wacl-acl-setuser-enum-only-name-custom-fallback-registry.md、task/done/wacl-acl-setuser-enum-only-name-custom-fallback.md

### [§108] wbase glob 单引擎裁决
- 判据：glob 消费面无第二引擎（单引擎成立）；rust 优侧纯登记型先例；「码面原有自述系真锚但非登记锚，本票补登互引」口径出处；util.c nesting>1000 守卫恰证上游自认病理形；措辞收紧与 util.c 外仓锚按内容引之订正在册。
- 符号锚：wbase glob.rs（判定不改一字）、wpubsub channel_ns.rs（消费点既有臂）
- 来源：task/done/wbase-glob-single-engine-adjudicated-clauses-zero-lock-tests.md

### [§109] wacl 自定义命令事务形态在案
- 判据：自定义命令名解析的事务形态在案登记；对照 C# 失败先于 store.write 零残留口径。
- 符号锚：lookup_command、RespCommand::from_cs_name
- 来源：task/done/wacl-acl-setuser-enum-only-name-custom-fallback-registry.md

### [§110] BITFIELD 未知子命令错误回显编码现状语义锁
- 判据：BITFIELD 未知子命令错误回显编码维持仓内现状，向量族统一错误前缀。
- 来源：task/done/b2-screening-recheck-notes-20260925.md:23、task/done/wnode-vector-wrongtype-dotted-five-command-frame-text-divergence.md

### [§111] wkv 页容量旋钮族（含 c 目 reviv 几何旋钮缺失）
- 判据：与 §69 同谱页容量旋钮族；c 目登 reviv 几何旋钮缺失形；bftree 引擎释放时长面不在其内（另案）。
- 来源：task/done/wkv-bftree-release-queue-defers-engine-dispose-to-delay-window.md、task/done/wkv-scan-live-key-probe-missing-read-cache-skip.md

### [§113] wcol SINTERCARD 在册分叉
- 判据：SINTERCARD 应答面分叉在册（与 §158 尾注合引）；命令族覆盖抽查视其为设计内不另报。应答面判据已归拢入 §184（五命令族首源缺失臂修复型登记）。
- 来源：task/done/js-checkjs-session-api-wrapper-registry-gap.md

### [§114（含 a/b 分目）] RESTORE 空值键 (+OK) 降级与 length 编码收口
- 判据：宗 a 裁 RESTORE 空值键 (+OK) 错误降级应答，不复刻上游崩溃；宗 b 裁 length 编码收口。与 §108/§155 同谱 rust 优侧纯登记。
- 符号锚：key_admin_commands/types.rs、wresp/src/length.rs
- 来源：task/done/wbase-glob-single-engine-adjudicated-clauses-zero-lock-tests.md:3、wedb/wnode/src/resp/key_admin_commands/types.rs

### [§115] wtxn WATCH 版本双轨与 EXEC 锁面重展开
- 判据：换号族之一：SELECT watch 保全/版本双轨面；EXEC 时刻以入参 lock_prefix 物理前缀经 save_lock_hashes 对裸键重展开并入 key_entries 臂已登记本条（锁面仅覆 WATCH 键一臂，该臂由来源票另案收口）。
- 符号锚：watch_container.save_lock_hashes
- 来源：task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md、task/done/wtxn-multi-queued-lock-hash-stale-across-generation.md

### [§116] wedb repl_offset2 钳位判据
- 判据：repl_offset2 钳位判据本身在册（negotiate_resync 逐子日志钳位源）；PartialResync 钳制往返缺失、[applied,tail) 残留段漏应用面不在册（来源票另案），且「在码激活的 repl_offset2 钳」表述随该票沿用。
- 符号锚：negotiate_resync
- 来源：task/done/wedb-repl-repl-history-recover-legality-gate-missing.md、task/done/wedb-repl-diskbased-partial-resync-skips-replica-recover-clamp.md

### [§117 族（§117a-d）] 值域轴宽向登记谱系
- 判据：d 目裁 TIMEOUT int32 到 i64 值域轴宽向，系「rust 宽而结果正确且对齐加热路径成本」形态的宽向登记先例；a/b/c 目判据不可回收（仅代码引用证其存在）。bool 词法面分叉非本轴外延（划界见 §168）。
- 来源：task/done/wedb-cluster-mlog-key-time-frontier-parse-divergence-comment.md、task/done/wext-json-slice-step-zero-error-phase-divergence.md

### [§118] wreviv 域钉族：迁移驱动探针窗域钉已修形
- 判据：域钉族之三。
- 来源：task/done/wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap.md、task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md

### [§119] wconf 节点 id 渲染 32hex 身份形状条
- 判据：身份形状条=节点 id 渲染 32hex；曾被他票误指为身份形状条的号位由该票甄别注记订正为此号。
- 来源：task/done/wedb-cluster-init-local-missing-flush-config.md

### [§120] wnode 降阶轮寄生 expired-object-collection-freq
- 判据：旋钮兼职降阶轮启动门之登记：后台降阶评估轮由周期对象收集后台任务承载，依赖显式置 expired-object-collection-freq > 0；缺省 0 即禁用——升阶后再无前台写入的冷分层键永无后台降阶评估点（前台懒降阶臂只覆被写触碰键）。另系文档分叉注释订正先例之一。本条只登降阶轮寄生面，wkv 冷恢复饥饿面不在册（来源票另案）。
- 符号锚：wnode/src/primary_tasks.rs object_collect_loop、expired-object-collection-freq、tiered_demote_round
- 来源：doc/zh/collection.md 行内批注（3.3 降阶触发条、后台降阶评估轮条、8 运维建议条）、task/done/wkv-bg-demote-claim-selflock-cold-restore-starvation.md、task/done/wbase-group-commit-broken-broadcast-watermark-doc-fork.md

### [§121] MULTI/EXEC 排队期锁集登记跨事务零残留
- 判据：排队期锁集登记跨事务零残留语义锁，DISCARD 或 EXEC 提交后锁集彻底排空。
- 符号锚：txn_queue_lockset_residual.rs
- 来源：wedb/wnode/tests/txn_queue_lockset_residual.rs 注释锚

### [§122] wext_json 深度上限宽向纯登记
- 判据：JSON 深度上限宽向形，「rust 宽而结果正确且对齐加热路径成本」登记先例之一；step==0 语义错形不在册（来源票裁对齐改错另案）。
- 来源：task/done/wext-json-slice-step-zero-error-phase-divergence.md、task/done/wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak.md

### [§124（含 124d 分目）] wtls 残面四形
- 判据：票据/链深/EKU/吊销四旋钮残面在册（代码侧引 §124d 分目，其判据不可回收）；UDS+TLS 静默明文形不在册（来源票另案）。
- 来源：task/done/wnode-uds-tls-combo-silent-plaintext-endpoint.md、task/done/wtls-config-set-cert-rebuild-missing-client-ca-verifier.md、wedb/wconf/src/node_options.rs（§124d 引用证存在）

### [§127] wvector 量化收敛序与 SeqCst 握手
- 判据：裁量化「序」：置位、排空、快照、再排空收敛序与 SeqCst 握手；并钉原生 next_id 参照锚（复用臂契约 mark 置位成功即返回该 id 不丢弃）。计数取消安全洞不在本条（后续票归并 ReuseGuard 单机制族、不回改本裁决）。
- 符号锚：ReuseGuard、next_id（diskann-garnet 原生 fsm 复用臂）
- 来源：task/done/wvector-fsm-refill-reuse-arm-drops-marked-id.md、task/done/wvector-fsm-next-id-inflight-count-cancel-leak-quantization-hang.md、task/done/wvector-fsm-visit-used-empty-sentinel-overflow-panic.md

### [§128] wvector 屏障序在册面
- 判据：本条在册面为屏障序；量化 worker 落域问题正交（来源票另案收口）。
- 来源：task/done/wnode-vector-quantization-worker-session-domain-miss.md、task/done/wvector-fsm-visit-used-empty-sentinel-overflow-panic.md

### [§130] wmetric 观测面超集行立案族
- 判据：案一系「生产面空表虚报对外假数据」属生产实害级定标先例；bg_task_health、aof_flush_failures 超集行立案同款先例；与 §120/§162 并列为文档分叉注释订正先例。gc_* 三形另落 §165。
- 来源：task/done/wmetric-info-observable-divergence-registry-trio.md、task/done/wmetric-info-memory-gc-zero-and-proc-sentinel-registry.md、task/done/wvector-fsm-visit-used-empty-sentinel-overflow-panic.md、task/done/wbase-group-commit-broken-broadcast-watermark-doc-fork.md

### [§131] wkv SELECT watch 保全
- 判据：换号族之六：SELECT watch 保全面在册。
- 来源：task/done/wkv-flush-firstmap-sentinel-replay-retires-live-domain.md

### [§132] HyperLogLog PFMERGE / PFADD 的 C# :0 臂仍推版本落 AOF
- 判据：HyperLogLog 命令中，同形 C# :0 臂非全静默，Succeeded 派发推版本落 AOF。
- 符号锚：hyper_log_log_commands.rs
- 来源：wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs 注释锚

### [§133] 对象键过期判定与判型先行次序
- 判据：键与对象过期判定与判型先行次序：先判过期后判类型，消除 UnifiedStore 内部反序产生的过期幽灵形；GET 族读漏斗「已过期未清退对象键」恒缺失形。
- 符号锚：storage/session/common/ttl_sync.rs、expired_object_key_get_funnel_missing_shape.rs
- 来源：wedb/wnode/src/storage/session/common/ttl_sync.rs、wedb/wnode/tests/rmw_rebuild_side_domain_retire.rs 注释锚

### [§134] wacl byte.Parse HexNumber 族（族题）与全枚举成员对照形
- 判据：族题谱系：byte.Parse HexNumber 族形在册；后落票将「全枚举成员对照形」（目录缺席内名坠自定义名轨）补登并归本族题谱系，钉两形态实测文案（默认 features 回 Unknown custom command 且零残留；双侧命令名对照集差异即本条）。
- 符号锚：lookup_command 文档注释回指锚
- 来源：task/done/wacl-acl-setuser-enum-only-name-custom-fallback-registry.md、task/done/wacl-acl-setuser-enum-only-name-custom-fallback.md

### [§136] wcol RESP 应答成员序非契约
- 判据：分层态与内存态对同一成员集回「帧头与成员集合等价」应答即契约；成员序非契约——set 型读应答 SMEMBERS/SINTER/SUNION/SDIFF 双态成员序分别随 gxhash 布局序与树扫描序，且 wbase 进程级随机种子致跨重启序漂移，跨态逐字节全等不可达亦非要求。
- 来源：doc/zh/collection.md 行内批注（4 RESP 应答透明条）

### [§139] 事务锁覆盖判定与降级慢体事务锁模式跨段界延伸
- 判据：RespServerSession::txn_locks_cover_cmd 事务锁覆盖判定；脚本内触碰事务锁集外的键时按 Basic ephemeral 模式执行，无让闩判据；事务重放段命令自同步快臂降级进慢臂时，已判定的 Transactional 锁器模式随挂起体 SlowWait 快照跨段界延伸至 exec_slow_impl，整个慢体 await 段保持让闩，杜绝慢臂以 Basic 重取已持排他桶闩引发 LockTimeout 自撞（对标 C# TransactionalSessionLocker.TryLockEphemeralExclusive 恒回 true 与 RespServerSession 慢路径重投仍在 transactionalApi 视图内）。
- 符号锚：txn_locks_cover_cmd、TxnKeyEntry、SlowWait::for_command、exec_slow_impl、push_session_locking
- 来源：wedb/wnode/src/resp/resp_server_session/txn.rs、wedb/wtxn/src/txn_key_entry.rs、wedb/wnode/src/resp/slow_path.rs、wedb/wnode/src/resp/garnet_api/mod.rs、wedb/wnode/src/resp/garnet_api/slow.rs 注释锚

### [§140] wvector 副本侧回建失败即拒
- 判据：副本侧「回建失败即本轮全量收口失败拒授予位点」条在册；主侧半截恢复静默放行与其口径相抵之修复由来源票收口（归 None 复用既有拒启通道同口径）。
- 符号锚：reconcile sweep、in_use 滞留判据
- 来源：task/done/wnode-vector-registry-recovery-put-fail-half-recovery-context-leak.md

### [§142] ZADD 批量输入错误整体丢弃与原子性收口
- 判据：ZADD 批量参数解析时，家族回写门按载荷首字节 '-' 整臂拒写；中段错臂整体丢弃（原子性收口，禁按 C# 部分提交形回改）；错臂上成员级 TTL 剔除不固化。
- 符号锚：sorted_set_commands/mod.rs、sorted_set_object_impl.rs
- 来源：task/done/wcol-zset-zadd-fullpath-anchor-missing-checkjs-residue.md、wedb/wnode/tests/zset_r15_parity.rs 注释锚

### [§143] wedb 键级 TTL 粗化与值域 gate 单源
- 判据：键级 TTL 粗化与值域 gate 单源：历史粗化门主副 ≤15 ticks 恒早偏移形态收口，全链路禁止粗化，put_ttl 裸写内核逐位相等，值域裁决唯发生在命令入口 network_expire 单点。
- 符号锚：network_expire、put_ttl、expire_at
- 来源：task/done/wedb-cluster-init-local-missing-flush-config.md、wedb/wkv/src/ttl.rs 注释锚

### [§149] EVAL numkeys=0 空键区双闸放行形
- 判据：EVAL numkeys=0 空键区过集群槽位门双闸放行形（保留无键放行形，同一机制双保险，严禁按 C# 摘除闸口）。
- 符号锚：catalog/simplified.rs、simplified_spec_folding.rs
- 来源：wedb/wresp/src/catalog/simplified.rs、wedb/wresp/tests/simplified_spec_folding.rs 注释锚

### [§150] wvector RI 判死异构键吸收形
- 判据：RI 面在册三条之三：判死异构键吸收形。
- 来源：task/done/wnode-rimetrics-is-live-is-flushed-probe-warmup-divergence.md

### [§151] ZADD 数据段奇数尾巴防御性截断
- 判据：ZADD 数据段奇数尾巴防御性截断（首 token 即分值形），杜绝 C# GetArgSliceByRef 越界读 UB，禁按 C# 形改写为越界读。
- 符号锚：sorted_set_object_impl.rs、tiered_collection_ops/zset.rs
- 来源：task/done/wcol-zset-zadd-fullpath-anchor-missing-checkjs-residue.md、wedb/wnode/tests/resp_sorted_set.rs 注释锚

### [§154] wconn CLIENT KILL ID 非正值
- 判据：裁 KILL ID 非正值形态在册；不涉年龄时基。
- 来源：task/done/wnode-client-age-dual-clock-list-info-divergence.md

### [§158] wedb cluster 副本漂移双旋钮条
- 判据：漂移双旋钮登记条在册；第 7 项将读一致性消费链记为在役闭环，系多日志在线重放面已有登记在案的取舍条目（无裁决封存 multi_log 恒关）；拓扑旋钮与换代面不及（来源票另案），SINTERCARD 尾注亦挂本条（见 §113 合引）。
- 符号锚：multi_log、log_enabled
- 来源：task/done/wedb-cluster-replicate-missing-sequence-manager-generation.md、task/done/js-checkjs-session-api-wrapper-registry-gap.md

### [§162] 册尾条（09-25 至 09-27 间册尾）
- 判据：判据全文不可回收。可证残片：其一，系文档分叉注释订正先例（与 §120/§130 并列，见 wbase-group-commit 票）；其二，册内含「无新增数据危害面」措辞，diskbased 票裁定 fix 时同步订正该措辞回指其重放窗；其三，2026-09-25 至 09-27 间为现册册尾（慢日志票候号起点注记）。
- 来源：task/done/wbase-group-commit-broken-broadcast-watermark-doc-fork.md、task/done/wedb-repl-diskbased-partial-resync-skips-replica-recover-clamp.md、task/done/wmetric-slowlog-id-width-and-incrit-numbering-unregistered-deviation.md

### [§164] wnode 向量族错误文案统一无句点
- 判据：向量族错误文案统一无句点系刻意偏差，严禁按 C# 分流回写；随册 parity 注释订正与 12 命令×7 键型全帧等值断言收紧（零行为改动）。与 §22 括注面同域。
- 来源：task/done/wnode-vector-wrongtype-dotted-five-command-frame-text-divergence.md

### [§165（含 a/b/c 分目）] wmetric gc_* 三形
- 判据：gc_* 四行恒 0、峰值同源复制、非 Linux /proc 探针全落 -1 哨兵三形，纯台账零行为；分目 §165a 载「禁第二真值源（InfoProvider 语义混接禁）」回指锚，§165b/§165c 判据仅存代码引用证存在，不可回收。
- 来源：task/done/wmetric-info-memory-gc-zero-and-proc-sentinel-registry.md、wedb 代码侧 §165a-c 引用

### [§166（含 a/b/c 分目）] wmetric 可观测三宗
- 判据：a page_size::get() 真值化（探针同源同版）；b tick 域恒 10MHz 登记严禁回改（限缩为直方图量程/值域面；历史 size 行按数组全长推论作废，实口径以 distinct_values 为唯一真值源，见 §194）；c TreeCache 超集行登记（§130 同款先例续）。
- 符号锚：page_size::get、wbase/src/time.rs 恒 10 MHz 断言、garnet_info_metrics.rs TreeCache.ReservedBytes/BudgetBytes 两行
- 来源：task/done/wmetric-info-observable-divergence-registry-trio.md

### [§167] wmetric SLOWLOG id 位宽与临界区取号序列化
- 判据：i64 不回绕加临界区串行取号 vs C# int 回绕与锁外取号，上游缺陷修复型分叉家族（同 §16/§108 口径），严禁按 C# 回改（int 截断与锁外取号均禁接回）；双宗并一条（同容器取号单链路，先入库者得号、撞号不覆写），钉跨 2^31 负 id 形与并发 id/物理序倒置形两可观测量；三头注互引零行为。
- 来源：task/done/wmetric-slowlog-id-width-and-incrit-numbering-unregistered-deviation.md

### [§168] wedb cluster MLOG KEY TIME frontier bool 词法宽向
- 判据：宽向归化维持：C# GetBool(1) 仅收单字节 1/0 其余 NotANumber 断连；rust strict_i64 宽向归化（任意严格整数非零 true、非整数与缺位 false），刻意分叉；非 §32 整数文法轴、非 §117d 值域轴外延，互引划界；五案锁测钉宽向形防回改。
- 符号锚：strict_i64
- 来源：task/done/wedb-cluster-mlog-key-time-frontier-parse-divergence-comment.md

### [§169] wvector RI.METRICS 三宗
- 判据：is_live 与 tree_handle 判据源分叉（注册表实况对促热后存根句柄）、is_flushed 的 C# 结构性恒 false 对 rust 可报 true、恢复失败时 C# 错误帧对 rust 数据帧，三宗并记；裁维持 rust 只读真值形，严禁按 C# 促热形回改（回改即复活诊断命令的 RMW 写放大与预算抢占）。
- 来源：task/done/wnode-rimetrics-is-live-is-flushed-probe-warmup-divergence.md

### [§170] StorageSession::new_readonly 名实归一
- 判据：new_readonly 名实归一：36 处写位点改 new，剩余消费点全为纯读；collect 执行体含 RMW 者不再误用 new_readonly。
- 符号锚：StorageSession::new、StorageSession::new_readonly
- 来源：task/done/wnode-storage-session-new-readonly-contract-doc-mismatch.md

### [§171] GroupCommitPipeline LeaderGuard Broken 广播与伪失败收敛
- 判据：LeaderGuard Drop 收口复用错误臂广播单点：Drop 广播 Err(Broken) 经通道直达 wait() 第一臂 Ok(res) 原样上抛，Err(_) 水位兜底臂仅 tx 未 send 即弃（整管线 drop 等极端窗）可达；target <= 实际已提交水位的 Follower 得伪失败，重试经 enter Done 快路径收敛回成功。
- 符号锚：GroupCommitPipeline::LeaderGuard、Enter::Done
- 来源：task/done/wbase-group-commit-broken-broadcast-watermark-doc-fork.md、wedb/wbase/src/group_commit.rs 注释锚

### [§172] CLIENT 年龄双时钟源单源收口
- 判据：CLIENT 年龄双时钟源单源收口为 accept 预注册条目真源：NetworkHandler::set_session 经 set_creation_ticks 装配期单点回填，CLIENT LIST / CLIENT INFO / CLIENT KILL MAXAGE 三消费点零改动共读同基；哑桩形态回落构造期自取。
- 符号锚：ConsumerEntry.creation_ticks、RespServerSession.creation_ticks、NetworkHandler::set_session
- 来源：task/done/wnode-client-age-dual-clock-list-info-divergence.md

### [§174] wvector ensure_index_ready 取消安全守卫
- 判据：CAS 成功点单挂 StartPointLoadGuard 栈上零分配守卫（Drop 兜底复位 NoStartPoints，两落定臂先解除再 store）+ fsm.claim_start_id 幂等认领位点，杜绝取消残影重铸非零 id 死路。
- 符号锚：StartPointLoadGuard、ensure_index_ready_or_init
- 来源：task/done/wvector-ensure-index-ready-startpoint-cancel-stuck.md

### [§175] whlog 记录长度与值长度边界拒写
- 判据：validate_append_args 共用门补两硬臂：key > MAX_KEY_LEN 拒 KeyLengthOverflow、val_len > u32::MAX 拒 ValueLengthOverflow，置于页锁/tail CAS 前，append/复活两臂单点收口互逆对称。
- 符号锚：validate_append_args、MAX_KEY_LEN
- 来源：task/done/whlog-revivify-arm-missing-max-key-len-gate-rdh-mask-silent-corruption.md

### [§176] wnode purge_unrecovered_checkpoints 孤儿快照穿透回收
- 判据：purge_unrecovered_checkpoints 枚举口由 list_checkpoints 改为 list_all_checkpoint_tokens（物理全量对标 C# ListContents，孤儿快照穿透回收），按代回收轨持 meta 口径并在恢复期禁取检查点。
- 符号锚：purge_unrecovered_checkpoints、list_all_checkpoint_tokens
- 来源：task/done/wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak.md

### [§178] wkv 分层态键侧空/超长成员受理双态分叉
- 判据：分层态键侧空/超长成员 InvalidKV 与内存态受理:N 的双态分叉在册，边界线=引擎受理面；载荷侧空值双态一致受理不入分叉；先例同族（升阶建树契约闸）；validate_bftree_record 头注「两上限不同源」失根引用已改指本条。
- 符号锚：wkv/src/range_index/ops.rs validate_bftree_record、promote.rs 契约闸
- 来源：task/done/wkv-tiered-hash-empty-field-acceptance-dualstate-divergence.md

### [§181] wvector 命令面存储故障三态应答分臂
- 判据：向量命令面存储故障三态应答分臂：存储读失败全数透明化，service.rs enumerate/exists 门族 Err 上抛，VREM/VISMEMBER/VEMB/VSETATTR 命令面存储故障回 ERR 帧不写 AOF，缺元素应答契约不变（偏离 C# 吞错折叠）。
- 符号锚：vector_id_exists、vector_iid_exists、try_remove
- 来源：task/done/wvector-store-read-failure-folded-to-empty-missing.md

### [§182] 集群管理臂有界纪元排空无精确逆件承判 warn 留痕
- 判据：有界纪元排空（bump_and_wait_for_epoch_transition）返 false 的处理按「本臂有无精确逆件 / 干净失败通道」分界，不按「有无返值」一刀切。有精确逆件/干净失败通道（failover 停写位点、迁移族七点、replicate-sync 族）承判判败回滚；无精确逆件的管理臂（SETSLOT 全部四臂、SETSLOTSRANGE 全部四臂、REPLICAOF NO ONE 升主臂）承判留痕（warn 记未静止事实与槽号/区间/臂位），应答按变更已生效照实回 +OK，绝不假报失败回滚，亦不给收口钩加门。
- 符号锚：network_cluster_set_slot、network_cluster_set_slots_range、network_replicaof、unsafe_bump_and_wait_for_epoch_transition、bump_and_wait_for_epoch_transition
- 来源：task/done/wedb-cluster-mgmt-epoch-drain-warn-only-trace.md

### [§183] INFO STORE 堆行别名折 0（对位 C# tracker-null 缺省生产同形）
- 判据：project_db_snapshot 曾将 DbSnapshot.log_heap_size_bytes 与 ReadCacheSnapshot.heap_size_bytes 别名成页环常驻字节，致 INFO STORE 段 Log.CurrentHeapSizeBytes / ReadCache.CurrentHeapSizeBytes 与同行 CurrentMemorySizeBytes 恒等，违背 C# LogAccessor.MemorySizeBytes「not including heap objects」量纲定义与 LogAccessor.HeapSizeBytes 两态值域（真实堆驻留或 0）；本仓无 C# CacheSizeTracker.Initialize 的 store 级堆计账对位物，且 C# GarnetServer 仅在配置 LogMemorySize/ReadCacheMemorySize 时接入 tracker，缺省部署下 C# 生产 HeapSizeBytes 即报 0 ⇒ 折恒 0 系对位 tracker-null 缺省生产同形、非虚标，亦守「观测面禁建第二套堆计账」单机制规矩（同族先例 §165 gc_* 恒 0）；该两字段唯一消费面为 INFO STORE 逐行显示，不入 store_* 求和（求和只取 log_memory_size_bytes 与 rc.memory_size_bytes），max/current 两行不动；未来若真引入目标内存强制，tracker 与堆行一并复活属独立裁决。
- 符号锚：project_db_snapshot、DbSnapshot.log_heap_size_bytes、ReadCacheSnapshot.heap_size_bytes、LogAccessor.HeapSizeBytes、CacheSizeTracker.Initialize
- 来源：task/done/wmetric-info-log-heap-row-aliased-to-ring-bytes.md

### [§184] SINTER/SDIFF 族首源缺失臂修复型登记（rust 全键判型锁形，合引 §113）
- 判据：SINTER/SDIFF/SINTERCARD/SINTERSTORE/SDIFFSTORE 五命令首源缺失臂裁修复型登记——保留 rust 全键判型（load_many 单点逐键 set_load，任一键 WRONGTYPE 整体 -WRONGTYPE），不采 C# 短路收空（SetOps.cs SetIntersect :442 / SetDiff :879 在 GET keys[0] NOTFOUND 时立即 return OK 收空集、后续键不再 GET 不再判型；两 STORE 面 SetIntersectStore :381 / SetDiffStore :822 据此走 members.Count==0 臂 EXPIRE(dst, TimeSpan.Zero) 静默删除目标键回 :0）——rust 现行即 Redis 上游语义（对 WRONGTYPE 源不静默收空、不静默删键），C# 短路面具破坏性；锁形覆盖读臂（SINTER/SDIFF 回 -WRONGTYPE、SINTERCARD 回 -WRONGTYPE 非 :0）与 STORE 面（SINTERSTORE/SDIFFSTORE 目标键不删、原样保留、回 -WRONGTYPE），五形快慢双路径同口径（load_many_async 同漏斗）；SUNION/SUNIONSTORE 两侧一致全键判型不在其内；§113 的 SINTERCARD 应答面归拢入本条（判据面以此为准），§150（判死异构键折叠 NotFound）只覆盖 string 影子/RI 到期判死域、活 string 键判型臂不在其内。
- 符号锚：set_commands/write.rs load_many、set_commands/write.rs set_combine_store、set_commands/read.rs set_combine、set_commands/read.rs set_intersect_length、set_commands/slow.rs load_many_async
- 来源：task/done/wnode-set-combine-first-key-missing-wrongtype-shortcircuit-divergence.md

### [§185] 扫描族 COUNT 截断比较 i64 加宽（不复刻 C# int32 回绕退化形）
- 判据：HSCAN/ZSCAN 扫描族「截断比较」以 i64 承载 count 并按 i64 加宽翻倍（count * 2 恒不回绕），属修复型惯例家族（§18 同谱）：C# SortedSetObject.cs:Scan / HashObject.cs:Scan 以 int32 unchecked 承载，COUNT=-2147483648 翻倍回绕为 0，退化为「首条未命中条目即停」空页爬行（不属负值全量遍历族）；rust 不复刻该回绕退化形，严禁按 C# 回改为 i32 截断回绕——回改才是真回归且破坏扫描族快慢双路径双态全等。负 COUNT 恒不命中 → 全量遍历、count=0 首个未命中条目即停两上游怪癖 1:1 保留，不在本条裁量内。旧册「第 20 条 d)」号位经 2026-09-28 重建后为 TYPE 命令条（现 §20 仅 a/b 分目）占用致五处代码注锚悬空，本条系该在役裁决判据面回收再落册。
- 符号锚：scan_kernel、SortedSetObject::scan、HashObject::scan、tiered_collection_ops/scan.rs（分层扫描臂）、scan_family_dualstate_frames.rs（双态帧锁测）
- 来源：task/done/wcol-zset-zscan-count-i64-widening-anchor-20d-dangling.md（五处代码注释锚：wcol sorted_set_object.rs / scan_input.rs / hash_object.rs、wnode tiered_collection_ops/scan.rs、tests/scan_family_dualstate_frames.rs）

### [§186] 恢复失败恒拒启（FailOnRecoveryError 旗标零消费，无续行门）
- 判据：AOF/检查点恢复面设备错误沿 `?` 全链上抛、装配口恒拒启、无续行门，系对 C# 缺省吞错带已恢复部分数据续行起库的刻意收紧（同谱 §57 wtls fail-fast 双向先例）：C# 恢复链路全程受 FailOnRecoveryError 旗标门控且缺省 false（ServerOptions.cs 声明处），恢复异常 catch 记日志后续行——AofRecover.cs Recover 的 RecoverReplayDriver catch 臂、SingleDatabaseManager.cs RecoverCheckpointAsync catch 段、DatabaseManagerBase.cs ReplayDatabaseAOF catch 段、MultiDatabaseManager.cs 三处同门；rust 侧该旗标零代码消费、全仓无装配层裁决点，恢复失败恒上抛拒启，脏位点下绝不续跑重放，严禁按 C# 回改复活吞错续行形。六处代码注释（恢复装配/驱动口与回归测试头注）旧引「§122」系悬空错指——§122 现为 wext_json 深度上限宽向条与恢复拒启无关，本条落册后六处注锚统一改指本节。
- 符号锚：recover_database_aof_async、GarnetLog::recover_async、SingleLog::recover_async、WaofSublog::recover_async、open_recovered_with_config_and_aof、waof_recover_async_error.rs
- 来源：task/done/wnode-recover-fail-fast-deviation-unregistered-sec122-anchor-drift.md

### [§187] 向量选项关键字 ASCII 折叠字母域收窄（不复刻 C# 二参版全字节位 +32 偏移词法怪癖）
- 判据：向量命令选项/格式关键字的大小写无关比较走 wbase::ascii::eq_ascii_case（u8::eq_ignore_ascii_case，仅 'A'-'Z'/'a'-'z' 字母位折叠、非字母字节要求精确相等），系对 C# 二参版 EqualsUpperCaseSpanIgnoringCase 全字节位偏移缺陷的修复型收窄（同谱 §18/§32 前导零收口谱系），严禁按 C# 病形回改放宽：C# AsciiUtils.cs 二参版（:69-90）逐字节 `b1 == b2 || b1 - 32 == b2` 无字母域限制——`b2 is >= 65 and <= 90` 仅 Debug.Assert（Release 无效），任意 b1 = b2+32 字节命中，含非字母位（数字/连字符/下划线）的关键字全部暴露病形面；三参版（:95-122，allowNonAlphabeticChars: true，XNOQUANT_U8/XBIN_I8 等字面）非字母位要求精确相等、字母位偏移路径即正常 ASCII 大小写折叠，与 rust 行为全等无分叉。二参版消费面 RespServerSessionVectors.cs 选项/取参循环全部命中本怪癖，病形清单（尾位/中段大写字母按 +32 命中非字母位：'X'-32=0x38='8'、'R'-32=0x32='2'、'M'-32=0x2D='-'）：VADD 量化器选项 "QX" 识别为 Q8（:232）、XDISTANCE_METRIC 值参 "LR" 识别为 L2（:392）、VSIM 选项 "FILTERMEF" 识别为 FILTER-EF（:801）、VADD 取参格式 "XUX"/"XBX"/"XIX" 识别为 XU8/XB8/XI8（:128-149）、量化器别名 "XPREQX" 识别为 XPREQ8（:256 二参版含数字位 8，同族活病形；甄别面所拟 "XPREXX" 逐位复核 'X'-32=0x38≠'Q'=0x51 不成立、"XUX8" 4 字节对 3 字节长度即拒，均非活病形）。rust 侧上述病形一律拒收：选项位落 "ERR invalid option after element"、度量位落 "ERR invalid XDISTANCE_METRIC"、VSIM 未知选项落 "Unknown option"、取参格式位落 "ERR invalid vector specification"。消费面核清：wresp::options::equals_ignore_case 其余消费（NX/XX/GT/LT/CH/INCR/EX/PX/EXAT/PXAT/KEEPTTL/SUM/MIN/MAX/RAW/WITHSCORES）与 wbase::eq_ascii_case 全仓其余消费（wbitmap bitfield、wnode 会话类型/方向解析、wcol 选项与地理单位）关键字全为纯大写字母词形——纯字母位上 C# 二参版偏移路径与 ASCII 大小写折叠语义全等，无第二处分叉面，无第二处需认记。
- 符号锚：eq_ascii_case、equals_ignore_case、VALUE_KINDS、QUANT_OPTS、METRIC_OPTS、lookup、Cur::at
- 来源：task/done/wvector-option-keyword-ascii-plus32-lex-quirk-divergence.md

### [§188] 扫描族 COUNT 负/零值规整（负/零钳 0 且扫描层至少扫 1 条，不复刻 C# 未扫描即中断硬写游 0 形）
- 判据：SCAN COUNT 负/零值修复性规整，负值钳 0、扫描层 max(1) 至少扫 1 条，不复刻 C# 底层 acceptedCount(0) >= count(<=0) 立即返回 true 未扫描即中断且外层硬写游标 0 的缺陷。
- 符号锚：parse_scan_filter（array_commands.rs）、array_key_iteration_functions.rs。
- 来源：task/done/wnode-scan-family-legacy-anchor-20ac-subitems-registry-gap.md

### [§189] 分层扫描中间游标纯校验（不对齐一律判 false 终结，不复刻 C# 自页首步进回退对齐后续扫）
- 判据：对落在记录中间的游标不对齐一律判 false 由调用方终结回 (0, 空)，Redis SCAN 本无快照保证，漏扫可容忍，免去三区分派重写。
- 符号锚：validate_cursor（scan.rs）。
- 来源：task/done/wnode-scan-family-legacy-anchor-20ac-subitems-registry-gap.md

### [§190] MGET 缺键/对象键 RESP3 分支返回 _\r\n（不复刻 C# 全链恒写协议恒定 $-1\r\n）
- 判据：Redis 7 / RESP3 标准下数组内 null 元素形制为 _\r\n，wedb 统由 RespWriter 单态化按会话版本输出；C# 原型 MGET 因未感知 respProtocolVersion 恒写 TryWriteNull（$-1\r\n），属上游版本感知未闭合。wedb 维持现代化 RESP3 null 形，与 §108/§114/§184 同谱 rust 优侧纯登记。
- 符号锚：do_network_mget（array_commands.rs）、resp3_null_parity.rs。
- 来源：task/done/wnode-array-mget-resp3-null-frame-divergence.md

### [§191] 向量过滤域非有限数字词法单一解析收口（溢出放行 ±Inf、特殊词形词法拒收）
- 判据：比照 C# Utf8Parser 语义，以单一数字词法解析函数（parse_f64_exact，只认十进制数字/小数点/指数，全量校验，溢出折 ±Inf）收口 compiler/attribute_extractor/runner 三处口径；编译与提取溢出值放行、runner Str 臂非有限归 0 口径消除，nan/inf 词形由词法层统一拒收（不复刻以 is_finite 值域门误拒溢出字面量）。
- 符号锚：parse_f64_exact、parse_number、to_num
- 来源：task/done/wvector-filter-nonfinite-number-literal-divergence-compile-vs-inf.md

### [§192] 向量索引起点崩溃窗半截态就地自愈补写（不复刻 C# 无原子起点写序且硬报损坏致索引永久砖死）
- 判据：比照 C# 原生回调写入起点每笔独立落盘确认（段间无事务保障）、全目录无对位半截态处理，wedb 对「claim 在先 ∧ Vector 0 在场 ∧ Neighbors 0 缺席」（以及量化记录缺失）起点半截态改硬报错为就地自愈补写：起点邻接按构造恒为空表，直接补写空邻接记录；量化记录缺失由全精度起点向量重建补写，消除重启/重试报 StoreError::Read 致索引永久砖死。
- 符号锚：read_start_point_core、maybe_set_start_point
- 来源：task/done/wvector-start-point-partial-write-crash-window-hard-error-no-self-heal.md

### [§193] MULTI 排队期入队失败统一中止整笔事务（不复刻 C# 吞错继续提交）
- 判据：比照 C# ProcessMessages 排队期（TxnState.Started）命令入队阶段的 ACL 拒绝、脚本缺失（NOSCRIPT）与未知命令臂仅写错误行并计入 rejected 统计、全仓无入队失败置 Aborted 动作导致后续 EXEC 照常提交队列内合法命令，wedb 采纳 All-or-Nothing 事务语义加固改良：在未知命令/未知子命令（Invalid）、ACL 拒绝（NOPERM/NOAUTH）、脚本缺失（NOSCRIPT）入队失败点统一触发 abort_pending_transaction，将会话镜像与事务管理器同置 TxnState::Aborted，随后 EXEC 必定返回 -EXECABORT 且丢弃整笔事务不执行任何队列命令；避免半笔提交破坏事务原子性直觉，定性为 wedb 有意加固改良偏离 C#。
- 符号锚：abort_pending_transaction、queue_failure、TxnState::Aborted、process_messages（core.rs）
- 来源：task/done/wtxn-multi-queue-failure-abort-whole-txn-unregistered-vs-csharp.md、wedb/wnode/src/resp/resp_server_session/txn.rs 注释锚

### [§194] wmetric LATENCY HISTOGRAM size 行以 distinct_values 为真值源
- 判据：LATENCY HISTOGRAM 的 size 行手写算式为 `512 + 8 × distinct_values`（已记录不同值个数）。与 C# 原生 `GetEstimatedFootprintInBytes()` 的口径关系未经盘上亲验（HdrHistogram 源码非本仓内置），本仓以 `distinct_values` 为唯一真值源；消除将 `HistogramBase.cs:431`（计数数组全长）视作同源出处的表述，亦解除 §166b 历史上按「数组全长决定 size」推论造成的脱同步。禁止在无亲验判据下臆测桶几何改写算式。
- 符号锚：GarnetLatencyMetrics::GetEstimatedFootprintInBytes、garnet_latency_metrics::FOOTPRINT_HEADER_BYTES、distinct_values
- 来源：task/done/wmetric-latency-histogram-size-row-formula-diverges-from-own-doc-anchor.md

### [§195] 同步慢路径 DbMeta 映射批降级 tolerated 旧域泄漏（登记级承接，无异步回放单点）
- 判据：比照 C# 每逻辑库独立 Tsavorite 实例、清库截断自身实例、不存在可丢的「逻辑→物理映射」面，rust 侧以磁盘 DbMeta 为映射权威，物化/重解析映射批的落盘承诺由本仓自设。上下文映射两处同步批（set_context 物化臂与 virtual_domain 换代重解析慢路径臂）均处 compio 同步域、禁 await，只能做原子批同步尝试；现码无独立异步回放单点（persist_dbmeta_batch 的回放环系调用方异步 future 内逐项 await 驱动，不可在同步域复用；新造后台回放臂违「不新造第二套补偿机制」且超票面允许文件面），故两处降级项（翻页 degraded 与引擎硬错误 Err）统一收敛为 warn 同文即弃登记。最坏崩溃时该映射丢一次落盘 = tolerated 旧域泄漏；绝不旧号复用撞号——同批携带的 0x05 分配水位与 flush/swap 批同源抬升封堵号复用面（本仓自设不变量后半句）。
- 符号锚：StoreSession::set_context、StoreSession::virtual_domain、try_persist_dbmeta_sync、DbMetaRecord::NextId
- 来源：task/ing/wkv-vdb-generation-swap-slow-path-blind-alloc-unpersisted-watermark-reuse.md

### [§196] Scan 模式紧缩阶段 3 冻结判死位图（对标 C# tempKv 快照读，消除同轮伴生记录复查互扰）
- 判据：比照 C# TsavoriteCompaction CompactScan 阶段 3 仅消费 iter1 定稿的 tempKv 快照、绝不回读主库现态判活，且其记录物理头内嵌 TTL 与值一体随迁；rust 侧为省去临时日志分配将 TTL 拆为独立物理键与宿主同列候选表，在 Scan 阶段 3 处置环内边复查边处置会导致后处置者读到前处置者本轮刚造成的删除态（伴生侧车被清退摘槽后宿主复查因「旁路缺席」被翻转判活），使过期记录被回拷尾部永生且分层树已被注销。wedb 引入两子段快照化复查：在阶段 3 入口以新鲜 now 对全体非死候选预跑复查并冻结为判死位图（HashSet<Box<[u8]>>），处置子段只读该冻结位图分派清退/迁移，绝不再读存储判活，保持伴生对偶记录同轮同命运。
- 符号锚：LogCompactor::compact_scan、recheck_dead、judge_dead、drop_dead
- 来源：task/done/wcompact-scan-phase3-recheck-reads-sidecar-dropped-in-same-round-value-immortal-tree-destroyed.md

### [§197] HyperLogLog round_estimate 越界估计恒得负哨兵（复刻 C# 缓存恒失效不变式，消除 rust 饱和转换）
- 判据：比照 C# HyperLogLog CountSparse / CountDenseNCEstimator 出口为 unchecked (long) 转换，x64 架构下浮点越界由 cvttsd2si 得 long.MinValue（负数），写进载荷后 IsValidCard（GetCard >= 0）恒判失效，强制下一轮重新计算，越界估计永不入缓存；而 rust 的 f64 as i64 为饱和转换，+inf 或有限越界均得 i64::MAX（正数），直接令 is_valid_card 转真并将越界垃圾基数固化为合法缓存、甚至写回持久盘。wedb 在 round_estimate 的越界档（!r.is_finite() || r < i64::MIN as f64 || r >= i64::MAX as f64）显式返回 i64::MIN 负哨兵，复刻 C#「越界估计绝不作为合法基数缓存」的可观测不变式；声明 arm64 架构下 C# 同样为饱和的平台差异。
- 符号锚：whyperlog::estimate::round_estimate、whyperlog::frame::is_valid_card、whyperlog::frame::set_card
- 来源：task/done/whyperlog-round-estimate-saturating-cast-caches-overflowed-card-as-valid.md

### [§198] 紧缩 on_dropped 并发安全垫 host_exists 探查错误显式上抛（三值保守裁决）
- 判据：比照 C# TsavoriteCompaction ConditionalCopyToTail NOTFOUND 臂（不确定存活绝不宣告死亡）保守保留补拷，wedb/wkv/src/compact.rs 的 host_exists_cooperative 契约明确「返回 Err 为迁移内核错误，调用方须按存活保守处置本轮，严禁折成孤儿判死丢弃」。同文件 is_deleted 的 TTL/ETag 两臂遇 Err 均保守判活，唯 on_dropped 安全垫此前以 if let Ok(true) 单向折损将 Err（迁移内核错误）误折入孤儿破坏性清退链，导致活宿主被误入账伪 RangeIndexDrop 且注销物理树。wedb 将其改写为三值显式匹配：Ok(true) 保守放行、Ok(false) 确证孤儿清退、Err(e) => return Err(e) 显式上抛，借道调用方既有 drop_dead 跳过摘槽与 retain_record 保守通道延至下轮重试。
- 符号锚：WedbCompactionFunctions::on_dropped、host_exists_cooperative、CompactRun::drop_dead
- 来源：task/done/wkv-compact-on-dropped-host-probe-err-folded-as-orphan-destroys-live-host.md

### [§199] waof 物理 WAL 三项自持机制（C# 依赖 Tsavorite 设备面，无对位）
- 判据：C# garnet 无本地 WAL 实现（AOF 复用 Tsavorite 设备与检查点链，commit 元数据由设备层双写承担），wedb waof 为注册偏离的自持实现：①随批尾 in-log commit 元数据帧单写取代 C# 双写 commit 元数据（waof/src/wal/commit.rs，负载恒 24B / 整帧 32B = 8B 帧头 + 24B CommitMeta），恢复扫至最后 commit 帧收敛提交上界、帧后残段物理擦除（waof/src/wal/recover.rs）；②无检查点依赖的 CRC 自同步恢复——半条（torn tail）记录按帧头 CRC 界定并截断，段边界跨越与段号 32 位域显式拒绝回绕均在恢复扫描内闭环；③FastAofTruncate 上层两态拒绝开关不做：C# FastAofTruncate=false 时对副本数据缺口显式拒绝恢复（GarnetAppendOnlyFile.cs:DataLossCheck 消费面，C# 源 :201 !serverOptions.FastAofTruncate 实证）、=true 时容忍截断；rust 恒取容忍臂（自动保守截断至最后完整记录）+ 截断观测面 recover_truncated_at / recover_dropped_bytes 与 warn 日志，上层拒绝开关不做。
- 符号锚：waof::wal::commit、waof::wal::recover、GarnetLog::recover_async、WalCommitStep
- 来源：wedb/waof/src/wal/commit.rs 与 recover.rs 注释锚（确认 agent 审查轮登记）

### [§200] AofBackpressure 同步慢臂 park 冻结宿主 compio worker（C# Thread.Sleep 轮询不冻结进程）
- 判据：水位判据/解除判据/publish-delta 与 C# libs/server/AOF/AofBackpressure.cs 逐条对位一致（无偏离）；偏离仅在同步慢臂的等待实现——C# Thread.Sleep 轮询仅冻结当前线程，wedb 同步臂 park 冻结宿主 compio worker 执行器：与追加任务同执行器的发布任务冻结期不可推进（残余冻结面，引票 zcode-r41-wakeup）。compio 线程每核运行时纪律下的既有登记面，严禁改回线程内轮询等待（冻结收窄至单核属可接受裁决，异步臂不受影响）。
- 符号锚：aof_backpressure（wnode/src/aof）、GarnetLog::backpressure_wait_key / backpressure_wait_vector、wbase::GroupCommitPipeline
- 来源：wedb/wnode/src/aof/aof_backpressure.rs 模块注释锚（确认 agent 审查轮登记）

### [§201] 集合对象 Scan 收敛游标 `==` 改 `>=`（修复上游过期垫数死锁死角）
- 判据：比照 C# HashObject.cs / SortedSetObject.cs 的 Scan 尾判定 `if (cursor + expiredKeysCount == hash.Count) cursor = 0;`（符号锚 HashObject::Scan / SortedSetObject::Scan）——Scan 系纯只读路径（C# libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation 直入，绝不调用 DeleteExpiredItems），到期成员持续滞留并垫高 Count；当存活数 L < 起始游标 start ≤ 含到期总数 N 时全部存活条目下标恒 < start 被跳过、零产出，cursor 停在 start，尾判定 start + E == L + E 退化为 start == L（恒假），游标无法归零、向客户端回原游标死循环挂死。wedb scan_converge_cursor 以 `>=` 收敛：正常未截断遍历恒 cursor + expired == total 两判等价，分页 COUNT 截断恒 cursor + expired < total 两判均不命中维持续页，唯死锁死角 `>=` 补足归零；内存态与分层态同口径。
- 符号锚：wcol::types::scan_input::scan_converge_cursor、HSCAN/ZSCAN 共享扫描内核（hash_object / sorted_set_object 两 scan）
- 来源：wedb/wcol/src/types/scan_input.rs 注释锚（确认 agent 审查轮登记；task/done/r9-scan-cursor-screening-20261001.md 旁证）

### [§202] ZRANGE byIndex 下标 f64→i64 饱和加宽（不复刻 C# (int) unchecked 截断回绕）
- 判据：比照 C# SortedSetObjectImpl.cs:473 `(int)minValue, (int)maxValue` unchecked 截断——stop 下标 > i32::MAX 时 x86 cvttsd2si 回绕 int.MinValue → maxIndex 负 → minIndex > maxIndex 误答空数组（如 ZRANGE k 0 9999999999 在 C# 误答空集）；wedb 双态（wcol 内存态 sorted_set_object_impl 与 wnode 分层态 tiered_collection_ops/zset）以 f64→i64 饱和加宽 + len-1 钳制承接，越界上界按 Redis 上游语义回 start 起全量，属宽向修复偏离；f64→i64 在 i32::MAX..i64::MAX 区间与 C# (int) 的截断行为分叉即本条在册面。
- 符号锚：wcol::zset::sorted_set_object_impl（ZRANGE byIndex 臂）、wnode::resp::objects::tiered_collection_ops::zset（分层 ZRANGE 臂）
- 来源：wedb/wcol/src/zset/sorted_set_object_impl.rs 与 wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs 注释锚（确认 agent 审查轮登记）

### [§203] 复制面元数据线格式 bitcode 化（CheckpointEntry / SyncMetadata，不复刻 C# BinaryWriter 布局）
- 判据：C# libs/cluster/Server/Replication/CheckpointEntry.cs:ToByteArray/FromByteArray（:104-121 BinaryWriter：8B storeVersion + 双 Guid + AofAddress.Serialize + 4B 旗标 + 7bit-len string）与 SyncMetadata.cs:ToByteArray（:134/:164，9 字段含 currentReplicationOffset）为 C# 集群内部线布局；wedb 复制协议为 rust-internal（无 C# 双端互操作面），两处线格式刻意换 bitcode 紧凑编码：checkpoint_entry.rs to_byte_array/from_byte_array 编 CheckpointMetadata；sync_metadata.rs SyncMetadataWire（8 字段）裁去 C# 线上 currentReplicationOffset 字段（语义面由 AofAddress 位点对承担）。同族先例：cluster_config/serializer.rs「v2 起 .NET BinaryWriter 布局换 bitcode」。字段增减破坏的契约是本仓复制双端自洽，与 C# 线协议无关。
- 符号锚：CheckpointEntry::to_byte_array / from_byte_array、SyncMetadataWire、SyncMetadata::to_byte_array、cluster_config::serializer（同族先例）
- 来源：wedb/wedb/src/server/replication/checkpoint_entry.rs 与 sync_metadata.rs 注释锚（确认 agent 审查轮登记）

### [§204] INFO STATISTICS→STATS 别名接受面（C#/Redis 皆无此段名）
- 判据：C# TryGetInfoMetricsType 对未知段名回 ERR Invalid section，真 Redis 同拒；wedb InfoMetricsType::from_name 首臂对 `STATISTICS`（大小写不敏感）别名匹配到 Stats 段，属自研接受面超集（宽容历史客户端段名拼写），行为面为「多接受不误答」，登记在册严禁删改回 ERR。
- 符号锚：wresp::metrics::info_metrics_type::from_name 首臂、InfoCommand::parse_sections
- 来源：wedb/wresp/src/metrics/info_metrics_type.rs 注释锚（确认 agent 审查轮登记；原误锚 §103 已订正）

### [§205] 事务统计面随 TryTransactionProc 裁剪（INFO STATS 两行删除，较 C# 少输出）
- 判据：C# total_transaction_commands_received / total_transaction_commands_execution_failed 两计数唯一生产消费点 libs/server/Custom/CustomRespCommands.cs:29/:43 TryTransactionProc，wedb 该存储过程面整体清理不转写（wnode/resp/txn_resp_commands.rs 同位自注），两计数生产恒 0——GarnetSessionMetrics 两字段、cs_incr/cs_getters 表项、INFO STATS `total_transaction_commands_received` / `total_transaction_commands_execution_failed` 两行整体删除（C# 输出含此两行，wedb INFO STATS 较 C# 少两行；删除面孤儿访问器与恒 0 假活行一并灭绝）。
- 符号锚：GarnetSessionMetrics（字段/宏表已裁）、garnet_info_metrics INFO 表、txn_resp_commands.rs 裁剪自注
- 来源：wedb/wmetric/src/garnet_session_metrics.rs 与 wmetric/src/info/garnet_info_metrics.rs 注释锚（确认 agent 审查轮登记）

---

## 二、号位空缺清单（存在实证、判据正文不可回收）

下列节号由存活票体交叉引用或代码注释「deviations.md §N/第 N 条」悬空引用证其曾在册；按硬规则注释只证曾存在与编号，不证裁决内容，无实证来源者一律不录正文。语境词系引用方原话，仅供后续票回收时对号。

- §9/§10 — 原始前序节号，存活载体零命中，判据不可回收
- §13/§13h — 原始前序节号，代码仅残余标号，判据不可回收
- §16/§17 — 修复型惯例家族成员，判据不可回收
- §19/第 19 条 — 修复型惯例家族成员，判据不可回收
- §24 — 独立会话域无等待面、维持立即可取语义回空值条
- §28 — 原始节号，仅残余代码注释，判据不可回收
- §29 — 换号域邻族条（两票核对清单点名为邻面且不圈本面）
- §30 — 仅 PANIC 一条，判据不可回收
- §31 — 原始节号，存活载体零命中，判据不可回收
- §33 — 经纪锁内原子出件契约条（itembroker 票与码注双实证，判据不可回收；与 §24 独立会话域无等待面条邻族非同目）
- §34 — 原始节号，存活载体零命中，判据不可回收
- §37/§38/§39 — 原始节号，存活载体零命中，判据不可回收
- §40 — 设备段目录面与 §74 合引「三面在册」，独面目不可辨
- §41/§42 — 原始节号，存活载体零命中，判据不可回收
- §44 — failover 族正交条（与 §88 并引）
- §45/§46 — 原始节号，存活载体零命中，判据不可回收
- §47/第 47 条 — 读臂删空自愈语义锁条
- §47a — 写臂错误防幻键裁决条（zset_r15_parity 码注实证，判据不可回收；与上条 §47 非同目——§47 为读臂面语义锁，本条为写臂错误防幻键面，亦不以 §47 母号含混吸收）
- §48/第 48 条 — 统一清除数据与随键 TTL/ETag 并推进 WATCH 版本、对齐 C# 语义条
- §49 — 单字节对齐与位段图勘误条
- §50/§52 — 原始节号，存活载体零命中，判据不可回收
- §59/§61/§62/§64/§67 — 原始节号，存活载体零命中，判据不可回收
- §66a/§66b — 增量族存期家族内刻意不对称回指条
- §70 — 大小校验语法相关条
- §71 — 原始节号，存活载体零命中，判据不可回收
- §72/§73 — 文档锚刷新条目，判据不可回收
- §77 — 尺寸族区段条目
- §81 — 检查点邻族核对清单点名条
- §85/§86 — 拟号条目（依 b2 注记让位条款失效）
- §87 — 装载型写臂先窗后装纪律（取窗点前移）
- §88/第 88 条 — 差集及其后果登记条（failover 族）
- §90 — 用户名门锁条（双参 AUTH 非 default 异名加正确口令形）
- §91 — .NET NotImplementedException 文案帧收口条
- §92/第 92 条 — 严禁按 C# >= 形态回改之修复型条
- §93 — 留槽条：0.5 与之系未裁决分叉，严禁按任一方径改（wconf/src/node_options.rs 两锚位）
- §94 — stale registry 六锚根正本单源条；task/done/b2-screening-recheck-notes-20260925.md 指认「已落地现册 §94」，原始落册票不在五池存活载体，判据不可回收
- §95 — 纪元栅栏条（迁移族 done 票与检查点票核对清单点名）
- §97 — Lua 脚本快路径 Err 臂 Protocol 折叠面收缩
- §107 — 原始节号，存活载体零命中，判据不可回收
- §112 — 应答文本形态条（与第 1 条最短往返裁决同向）
- §123 — PERSIST 无 TTL 命中 :0 臂观测面防回摆锁面条
- §125 — 错误帧应答（禁静默成功、禁降级重放致值双写）已登台账条
- §126 — 语境不可读，仅代码引用证存在
- §129 — 修复型分叉已登条（claim 回退残余形同条登记语境）
- §135 — 三分配点条（hll_init_payload 建键上探语境）
- §137 — 原始节号，存活载体零命中，判据不可回收
- §138/§144 — 崩溃形不复刻族条目（与 glob 票「单条形制同谱」点名）
- [§141/§146/§147/§148] — 原始节号，存活载体零命中，判据不可回收
- §145 — PFMERGE dest 轨裁决条
- §152 — worker 线程 join 有界上界毫秒 P3 防御档条
- §153 — ZRANGESTORE 空 src/dst 键两形锁条（票 wnode-zrangestore-empty-key-guard 在册语境）
- §155 — 与 §108/§114 同谱 rust 优侧纯登记条，主题不可考
- §156/§157 — 原始节号，存活载体零命中，判据不可回收
- §159 — acl_mount_stale 预门条
- §160 — 迁移源端向量集删除关联条
- §161 — 登记内有意宽向裁量条（严禁按 C# 64 回改语境）
- §163 — config_commands.rs 注释引「§163 台账登记」；doc-deviations-sec32 票 09-27 注记「近 3h deviations 仅动 §163」证其占号成立；落册票不在五池存活，判据不可回收
- §165b/§165c — §165 分目仅代码引用证存在（判据面已登 §165 条目）
- §173/§177 — 原始节号，存活载体零命中，判据不可回收
- §179/§180 — 哈希与分层域登记条（判据正文不可考）
- 另：§32 b) 清单条目 1-11、17-23、26 及后目的判据不可回收（已回收目见 §32 条目）。

---

## 三、伪号面排除（非历史节号，不占空缺位）

- 第 3 节/「deviations §3.4」— task/done/b2-screening-recheck-notes-20260925.md 明注该册无此节
- §85/§86 — 同上票裁定：票面自称 §85/§86/§88/§89/§98 者一律失效（系拟号非在册号；§88/§89/§98 另有独立存在实证，已入空缺位/在册条目）
- §359 — task/done/wconf-size-flag-grammar-missing-memorysizevalidation-tier-tp-accepted.md 审核结论明注「删除假锚 §359」
- §1343 — task/done/wnode-client-age-dual-clock-list-info-divergence.md 订正：实为 §102 行号指涉
- §347/§399/§2094/§1911-1917/§405-416 — 均系「deviations.md 行号面」引用被误写 § 形（§1911-1917 行面即 §140 回建条邻域；§405-416 与「现册尾 §98/§162」时间线冲突）。其中「§405-416」所指判旨可证不可号：network_setex/network_get/network_expiretime 三包装设计内缺席、由命令层唯一实现承接、已在 js/check/ignore 姊妹条在册，见 task/done/js-checkjs-session-api-wrapper-registry-gap.md；因号位不可考，不入正文。

---

## 四、覆盖率与载体统计（截至本册落笔）

- 在册条目：86 节（涵盖存活载体回收之在册裁决，每一节均带一手来源指针）；
- 号位空缺清单：约 55 个号位（含分目与「第 N 条」同型号，实证存在但判据不可回收）；
- 曾存在可证节号合计：约 140 个，与原册「100+ 节」量级吻合。
- 来源载体贡献：
  1. task/done 票体 50+ 张票支撑绝大多数在册条目（其中终态/收口记录行直接给号者含 §164-§172、§174-§176、§178、§181 等）；
  2. doc/zh/collection.md 行内批注 2 节（§120、§136）；
  3. 代码注释引用与符号锚（如 node_options.rs:1723 第 76 条首个校验锚、datadir_lock §11、format_double §1 等）；
  4. task/reject 池 0 节（三张驳回票无 § 引用）；
  5. task/issue 旁证 2 条（b2 注记供 §94 存在证与拟号失效规则；§158/§3 勘误）。
- 未尽面：§32 b) 清单大部、§93 留槽裁决本体、§163 占号者、号位空缺清单全部——待他席从新增存活票或后续 grep 回收；本册严禁为凑册编造判据，严禁钉行号。

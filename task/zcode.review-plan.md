审查轮次调度表(工作副本,非认领票;子代理产出 next/zcode-r{N}-*.md)

规则
每轮 5 个并发子代理,视角 = 横切维度 × 垂直域,与历史轮不重复。
每份产出文件尾一行判定:视角结论:已穷尽 / 有增量。
连续判定「已穷尽」的轮数计入收敛计数,目标连续 99 次。
子代理纪律:只审查不修改代码,不跑 test.sh/clippy.sh/cargo;禁占位函数;格式禁加粗/表格/水平线;每条意见 rust/c# 双侧路径。

已用视角(不可复用)

轮1 2026-09-20(5 文件,无 r 前缀)
- zcode.net.md 网络协议×复制/迁移/gossip
- zcode.data.md 命令完整性×数据类型/TTL
- zcode.db.md 引擎语义×hlog/索引/AOF/checkpoint
- zcode.design.md 死代码重复×全仓拓扑
- zcode.my.md 自定义优化落地×SKILL 清单 9 项

轮2 2026-09-21(5/5 落地,均判有增量)
- zcode-r2-test.md 测试对标×C# garnet.test 族(13 条:覆盖缺口5/错位2/假锚13-31处)
- zcode-r2-concurrency.md 并发安全×锁序/epoch/竞态(2 条:检查点闸门收口、vdb 析构竞态)
- zcode-r2-crash.md 崩溃一致性×checkpoint/AOF/换号 GC(4 条:头号=换号树删除绕安全纪元)
- zcode-r2-error.md 错误处理×panic 面/吞错/错误帧(7 条:头号=panic="abort" 全进程崩)
- zcode-r2-lifecycle.md 启动关停配置×启动序/关停/CONFIG(8 条:aof-commit-freq 热更失效等)

轮3 2026-09-21(5/5 落地,均判有增量)
- zcode-r3-security.md 安全 ACL×权限矩阵/授权门/ns 穿透(6 条:HELLO 认证预门、CLUSTER FLUSHALL-NS 可达面等)
- zcode-r3-perf.md 性能资源×拷贝热点/写放大/锁粒度(4 条:信封写回 3-4 次拷贝、DBSIZE 全量物化等)
- zcode-r3-protocol.md RESP 解析鲁棒×分帧/超大/流水线/RESP3(2 条:单命令应答峰值无界、长度头回显字节)
- zcode-r3-txn.md 事务阻塞×MULTI/WATCH/阻塞族/Lua(3 条:Lua 事务模式空操作、WATCH 版本表无前缀、wtxn 死面)
- zcode-r3-cluster.md 集群路由深水×SETSLOT/failover/gossip(3 条:头号=迁移驱动锚死默认域,数据级)
注:轮1 产出与轮2/3 部分发现已被 fixloop 生态认领核销(task/done/net-zcode-cleanup 等)。

轮4 2026-09-21(5/5 落地,均判有增量)
- zcode-r4-client.md 客户端会话×CLIENT 族/超时/背压(2 条:CLIENT LIST 镜像视图静态化、KILL 对阻塞挂起无效力)
- zcode-r4-lua.md Lua 脚本面×API 覆盖/缓存/错误传播/复制(4 条:EVAL 永久降 RESP2、SELECT 穿透串库等)
- zcode-r4-objimpl.md 类型内部算法×SortedSet/Bitmap/HLL/List/Geo(1 条:BITCOUNT BIT 口径刻意差异未登记)
- zcode-r4-observe.md 观测面×INFO/SLOWLOG/MEMORY/DEBUG(7 条:hit_rate 恒 0 两连口径等)
- zcode-r4-foundation.md 基础件×varint/base32/bitcode/coarsetime(3 条:ms 钳制四处重复、bitcode 无版本域等)

轮5 2026-09-21(5/5 落地;4 有增量,regs 首个已穷尽)
- zcode-r5-restart.md 重启恢复×TTL/脚本缓存/集群/复制断点(2 条硬伤:bftree 常驻回收驱动恢复实例漏挂、Vector Set 登记表恢复链零回建)
- zcode-r5-regs.md 历史票回归×done 28 张行为票核码(零真回归;视角结论:已穷尽)
- zcode-r5-timers.md 后台任务节拍×expiry/紧缩/gossip(3 条:紧缩内核双驱动并发窗口、AOF 首拍相位等)
- zcode-r5-repl.md 复制流深水×offset/backlog/背压(7 条:头号=attach 双泵竞态致数据面死锁)
- zcode-r5-api.md API 卫生×命名/错误/trait/pub/unsafe(9 条:头号=unsafe 无论证)

轮6 2026-09-21(5/5 落地,均判有增量)
- zcode-r6-mem.md 内存治理×记账/预算(3 条:zset 双容器记账低报 2-3 倍、升阶树页环无总闸)
- zcode-r6-del.md 删除矩阵×全路径组合(5 条:P0×2:SET 覆写升阶树键主从发散、过期清退丢已 ACK 值)
- zcode-r6-scan.md 迭代器×SCAN 族游标(4 条:分层 SCAN 到期成员偏移跨页重复;澄清双侧均无反转游标)
- zcode-r6-cli.md 启动参数×CLI/配置文件(13 条:复制域六旋钮零通路、fail-on-recovery-error 默认相反)
- zcode-r6-doc.md 文档对账×doc/readme/SKILL(5 条:README 职责声明与拓扑倒置;SKILL 点名项全部核实一致)

轮7 2026-09-21(5/5 落地;4 有增量,verify34 已穷尽)
- zcode-r7-verify12.md 复核轮1/2:68 条,已修复 56/仍在 2/在途 6/驳回 4/误报 0;复核揭示修复副产锚形残留
- zcode-r7-verify34.md 复核轮3/4:35 条,已修复 18/在途 10/驳回 3/误报 2(视角结论:已穷尽)
- zcode-r7-verify56.md 复核轮5/6:48 条,成立 41/零已修复(票全在途未合并)/过强 2/驳回 2;r5-restart1 证据更硬
- zcode-r7-ops.md 运维者×部署/备份/故障(2 条:双二进制同 --dir 互踩 P0、备份恢复无长度校验)
- zcode-r7-redteam.md 攻击者×渗透链(P0:阻塞族经纪会话钉死 ns0 跨租户读写,转写引入结构性旁路)

轮8 2026-09-21(5/5 落地,均判有增量)
- zcode-r8-sample-a.md 抽样精读×命令臂(4 条:SETEX i64/int32 宽度、DECRBY i64::MIN 符号翻转、PFMERGE 原子性)
- zcode-r8-sample-b.md 抽样精读×存储引擎(3 条:只读区链首 elide 未落地、删除 elision 门控两侧相反、truncate 钳制无信号)
- zcode-r8-sample-c.md 抽样精读×集群/复制/对象(5 条未声明增量:negotiate FullResync 收敛、STORE_RANGEINDEX_FLUSH 拒绝、迁移半失败 +OK 放弃未登记等)
- zcode-r8-const.md 常量审计(6 条:AofReplayDriftCheckFreq 默认 0 vs C# 1、MAX_UNFLUSHED_SEND_BYTES 页位错折半、主存页 16MiB 三处双真源)
- zcode-r8-deps.md 依赖供应链(7 条:wnode 死依赖 enum_dispatch、SKILL 三项逃逸 workspace 单点、webpki-roots 双版本)

轮9 2026-09-21(5/5 落地,均判有增量)
- zcode-r9-load.md 压测者×并发热点(5 条:大对象闩持 O(整对象)、AOF 环满幻影写/INCR 双递增、epoch 槽占连接上限、复制单泵串行扇出;澄清:OBJECT_DELTA 全仓无实现痕迹,SKILL 与 HEAD 不符)
- zcode-r9-soak.md 长稳×缓变(3 条:P0 单文件设备物理回收全空操作约 2.6TB/月、冷键降阶默认不可达、排空墙钟裸减)
- zcode-r9-chaos.md 混沌×故障剧本(5 条:磁盘满×检查点重试环副本静默丢写、aof-commit-wait 失败仍 +OK、时间脉冲墙钟/单调错位、bf-tree 落笔 ENOSPC panic)
- zcode-r9-client-compat.md 客户端兼容(2 条:TIME 微秒未 6 位补零、ASKING 补 arity;PIN G 带消息疑似项已排除)
- zcode-r9-recent.md 近期提交回归(6 条:wip 直合两次短暂回归已自愈、panic=abort 双裁决矛盾在档)

轮10 2026-09-21(5/5 落地,均判有增量)
- zcode-r10-bigo.md 大对象×复杂度界(树态 hash/set/list 穿透臂 O(整对象),修正 r9-load 盲区:删除族升阶后反更贵)
- zcode-r10-sched.md 调度×compio 亲和(4 条:后台任务钉死首会话 worker、darwin SO_REUSEPORT 后绑独收 thread-per-core 名存实亡、同核命令饿死窗)
- zcode-r10-format.md 落盘格式×单点纪律(4 条:AOF 版本域同号异构、RI 复合元记录读侧散点;全格式盘点无双格式)
- zcode-r10-memberttl.md 成员 TTL(纠偏:C# 有 HEXPIRE/ZEXPIRE 族,必须对标;镜像闭环全通;3 条登记缺口)
- zcode-r10-idem.md 幂等性(2 条:副本重放死任务+resync 起点错位丢段、向量迁移 RESERVE 重入泄漏)

轮11 2026-09-21(5/5 落地;4 有增量,reply 已穷尽)
- zcode-r11-boundary.md 边界值字典(7 条:浮点格式化整体分歧 zmij vs 定点 0.1+0.2 落盘分叉、浮点解析基座锚错 ZADD nan 分叉、HINCRBY 拒 007)
- zcode-r11-recheck67.md 复核轮6/7(纠偏:「SET 覆写升阶树发散」P0 系误报已实测驳回;verify34 档案不可恢复;新增 5 条流程缺口)
- zcode-r11-cross.md 跨域交互矩阵(3 条:EXEC×迁移传输窗 TRYAGAIN 队列作废、无盘全量同步停写窗挪用 TRYAGAIN、slot_wait_memo 泄漏毒化误 ASK)
- zcode-r11-gc.md GC 引擎深水(P0:紧缩物理删段×检查点恢复重放窗无屏障,熔断旁路默认可达;备注 4 条)
- zcode-r11-reply.md 回复编码×出网缓冲(五问全核实:缓冲生命周期/背压/零拷贝/RESP3 单点/多会话隔离全干净;视角结论:已穷尽)

轮12 2026-09-21(5/5 落地,均判有增量)
- zcode-r12-recheck810.md 复核轮8-10(43 条:5 修复/32 仍在/P0 六项全成立零在途票;新增 aof-segment-size 假旋钮、71bf987a 接线即修)
- zcode-r12-checkjs.md check.js 纪律(A 类抽 20 条 17 假缺失:多对一归并不补侧锚;重复定义 19 组全真双锚;B 层 3 虚构锚;词元段 891 已过期口径)
- zcode-r12-assert.md 断言强度(40 抽样:rust 普遍强于 C# 对位;失守=2 ignore 悬空遮蔽活缺陷 tiered_envelope_writeback_race/concurrent_vadd_disk_spill)
- zcode-r12-anchor.md 锚全量(4709 锚:310 路径不存在、5 虚构、3 语义错挂、356 裸锚双门禁外;r7 六残留原样两处在涨)
- zcode-r12-hetero.md 异构集群(裁决:混编不支持成立,版本门显式互拒,运行期 bug 零;3 条登记建议)

轮13 2026-09-21(进行中)
- zcode-r13-recheck1112.md 复核轮11/12 意见现状
- zcode-r13-embed.md 嵌入式 API 面×wedb crate 库用形态
- zcode-r13-tools.md 离线工具×redact/verify/dump/restore 对标
- zcode-r13-tls.md 安全传输×TLS/mTLS/Unix socket/多监听
- zcode-r13-log.md 日志面×级别/输出/量控制对标

收敛计数:连续已穷尽 1/99(r11-reply)

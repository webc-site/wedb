客户端延迟直方图零读者链删除（wconn record_latency / sent_at / metrics.rs 整面）

来源：task/ing/client-latency-histogram-unwired.md（fixloop 波次二，qcode10 net 视角条 8 副条）。
落地形态为票面首选修法「删掉这条恒不通电的观测链 + ignore 整文件登记」，反向方案（补 wconf
旋钮 + 另造读者）未走。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev。
合入 dev：34753d93（plumbing commit-tree，父 dev 5599db4a + 分支 client-latency-b2 tip 0b645d8a；
其时主仓 payload 已 checkout+add 归一；至 dev 顶 e5a01a4b 复核，本单 17 个 payload 文件零漂移）。

一棒成果采信
一棒代理（撞 150 轮上限死亡）留有完整分支 rm-client-latency-hist（tip a10eba5），唯一代码提交
568bdba（17 文件 +58/−503），另 3 个提交皆为 merge dev；同一 worktree 里它还留有重放分支
rm-client-hist-v2（tip abdcf38，代码提交 f6aa8b5），两者相对 dev 的净差逐字节相同（仅
context 行与 @@ 偏移因 dev 前进而变）。未跑归档、未回填票面、未合 dev。
二棒开新 worktree /tmp/fork/client-latency-b2（分支 client-latency-b2，fork 基 1fb07d0），
`git cherry-pick -n 568bdba` 零冲突落地：js/check/ignore/client.yml、wconn/src/network/replies.rs、
wedb/src/client.rs、wedb/tests/replication_assembly_e2e.rs 四处走三方自动合并，合并后
client.yml 的 blob（80a06d7→22c3c8c）与一棒 v2 重放版完全一致，即 dev 现状的
`ProcessReplyAsMemoryByteArray` 锚点（client.yml:97）被完整保留，未被 568bdba 旧 context
`ProcessReplyAsMemoryByte` 回退。未采用一棒分支本身，避免越界搬运其 merge 链。

二棒复核点（只查关键项，全部成立）
一、被删 pub 面确为零生产读者。dev 上 `git grep wconn::metrics` 零命中；copy_latency_histogram、
reset_latency_histogram、latency_metrics、outstanding_requests_metrics、
dump_latency_hist_to_console、record_rtt、SharedLatency、new_shared_latency 的命中面只有
wconn/src/metrics.rs 自身（含其 mod tests 自测）与 wconn/tests/latency.rs 两处，测试侧不算
消费者。`record_latency` 的生产写侧唯一入口是 wedb/src/client.rs:105 构造调用末参写死 false，
wconf 无对应旋钮与 CONFIG 槽位，即票面「开关面/消费面双缺」成立。
二、`sent_at` 删除无 trait/泛型界牵连。CommandItem 为 pub(crate)，三个构造口
（new/new_str/new_bytes）的 `sent_at: Option<Instant>` 形参只波及 crate 内调用点；该结构体经
crossfire 有界通道传递，无 AsRef/序列化/比较等对字段有约束的 trait 实现，删字段不改通道语义
（types.rs:25-57、network/mod.rs:41-49、pump.rs:172-183、replies.rs:175-243 同步收缩）。
握手函数 handshake 与 network_loop、read_pump、dispatch_replies 随之各去一个
`Option<&SharedLatency>` 形参，GarnetClientSession 侧原本恒传 None，行为不变。
三、依赖收口合法。删 coarsetime / hdrhistogram / parking_lot / wbase 后，wconn 全域（src + tests）
对这四个 crate 名零命中（仅 wconn/AGENTS.md 的选型散文提及）；dev 上 wbase 在 wconn 内只有
Cargo.toml:28 声明与 metrics.rs:25 一处 use，故随文件删除一并退出，不留空挂依赖。
四、ignore 整文件登记带理由且 YAML 可解析。js/check/ignore/client.yml 把
`libs/client/GarnetClientMetrics.cs: [GetLatencyPercentiles]` 单成员条目删去，改为文件末尾独立
一条整文件登记，理由逐一点名 C# 六成员（CopyLatencyHistogram、ResetLatencyHistogram、
GetPercentiles、GetLatencyMetrics、GetOutstandingRequestsMetrics、DumpLatencyHistToConsole）
与三处开关/读者所在基准树（Resp.benchmark --client-hist、playground/GarnetClientStress、
GarnetClientTests.cs SimpleMetricsTest）及其各自已有的不移植登记位，并指回 rust 侧删除事实。
解析实测：yaml.safe_load 通过，8 条目均含「文件 / 理由」两键，无本仓既往的「解析失败整份静默
失效」形态；check.js 跑后 client.yml 未被回写改写（生效判定项，非失效登记）。
五、wresp/src/metrics/n2_format.rs 的 6 行是纯注释口径订正：模块头与 fmt_n2 文档里的「wconn
客户端面与 wmetric 服务端面同源」改为「wmetric 服务端延迟百分位与 INFO 命中率两面同源」，
并把 wconn 侧写法改述为「收敛前」；未新增任何格式化实现，带千分位定点格式化的单源仍在
wresp/src/metrics/n2_format.rs（fmt_n2），未造第二套。
六、测试面收缩核对：删 wconn/tests/latency.rs 的 4 个 #[test]（百分位/重置、别名口径、默认关闭
语义、RTT 反映服务端延迟）与 metrics.rs 内 3 个自测，其覆盖对象（直方图本体）整体消失，属票面
明文「不留为跑通检查而保留的自测」；其余测试仅随签名收口（client_timeout.rs 两处、tests/main.rs
一处、network/mod.rs 内 3 处测试各去末参 false；wedb 两个测试各去一行实参），零断言削弱、
零无关用例删除。

与并发票的交叠处理
- tick 刻度换算单源票（合入 6fc87518、归档 13fa29d4）在本票飞行期间改了本票要删的
  wconn/src/metrics.rs（两个本地 const → `wbase::convert::stopwatch`），第二次 merge dev 时呈
  修改/删除冲突。按本票交叉引用段既有裁决「本票先落则该票 wconn 侧随文件删除自动消解、禁为待删
  文件做常量收敛」保持删除；其 wbase/src/convert.rs 单点、wmetric 三文件、wkv/keyspace.rs 两处
  射程原样保留，未回退他人成果。
- wconn 域近期另有锚点对调类落地（f15-wconn-anchor / wconn-parse-bytes-anchor-mismatch 族）：
  本轮三次 merge dev（0b26d23d → a92477f5 → 5599db4a）对 replies.rs、network/mod.rs、
  client.yml 的净差均为零冲突或仅本票自身删除，交叠处一律以 dev 现状为基重放删除。
- 同构造调用面的主条（max_outstanding_tasks 被 `.max(CHANNEL_CAP)` 吞掉）在册件已无（判重见
  task/reject/qcode10-net-client-outstanding-dup.md），本票只摘末位 record_latency 实参，
  参数列其余项未动，不存在「各改一半参数列」。

改动
- wedb/wconn/src/metrics.rs：整文件删除；wedb/wconn/src/lib.rs 去 `pub mod metrics`。
- wedb/wconn/src/client.rs：去 record_latency 形参与 latency 字段、:62 初始化、:86 clone、
  :118 泵实参、四处 `Instant::now()` 取时；C# recordLatency 不落地的理由注释改指 ignore 登记位。
- wedb/wconn/src/types.rs：去 CommandItem.sent_at 与三个构造口对应形参。
- wedb/wconn/src/{network/mod.rs,network/pump.rs,network/replies.rs,session.rs}：SharedLatency
  形参链与 record_rtt 结算点随之撤除。
- wedb/wconn/Cargo.toml：coarsetime、hdrhistogram、parking_lot、wbase 退出依赖表。
- wedb/wconn/tests/latency.rs 删除；client_timeout.rs、tests/main.rs 随签名收口。
- wedb/wedb/src/client.rs 构造调用去末参 false；wedb/tests/cluster_flushall_broadcast.rs
  两处、replication_assembly_e2e.rs 五处同步去实参。
- wedb/wresp/src/metrics/n2_format.rs：注释口径订正（6 行）。
- js/check/ignore/client.yml：GarnetClientMetrics.cs 由单成员扩为整文件登记 + 理由。

验证
- cargo check --workspace --all-targets（worktree 内，CARGO_TARGET_DIR=/tmp/fork/client-latency-b2/target）：
  末次 EXIT=0，0 error 0 warning，34 个 crate 复检（覆盖 --all-targets 的 tests 面）；
  单口 `cargo check -p wconn --all-targets` EXIT=0（wconn/tests 仅剩 client_timeout.rs、main.rs）。
  首轮在 fork 基 1fb07d0 上曾报 wnode E0560 `SessionDependencies has no field acl_settings`：
  该基端正处 7a83b8c 回滚夹带的窗口（wacl 单档链半成品），非本票引入，并入 dev 后自愈。
- bun js/check.js：只在 worktree 内跑，EXIT=0，输出 53 行，libs/client/GarnetClientMetrics.cs
  零缺失条目、零「虚构锚点」新增（票面验收两条皆中）；并 dev 前后的两次输出逐行相同
  （libs/client 族残余仅 LightEpoch，属他域存量）。
- 合入 dev（34753d93）后自证：四符号在 wedb/bench/js 代码与配置面仅余 js/check/ignore/client.yml
  理由散文中的 record_latency、sent_at 各一处（登记理由的表述，非引用）；
  copy_latency_histogram、dump_latency_hist_to_console、SharedLatency 全仓零命中；
  reset_latency_histogram、record_rtt、new_shared_latency 的余命中全部落在本票票面文字
  （task/ 文档，随归档转 done 作记录），代码与配置面为零。wconn/src/metrics.rs 与
  wconn/tests/latency.rs 在 dev 树上已不存在。主仓工作树 payload 已随 update-ref 同步 checkout
  并 git add 归一（合并脚本内连续执行，防并发陈旧工作树经 fixrs 的 git add -u 反吞）。
- 未跑 ./test.sh 与 ./sh/clippy.sh（主代理集中回归）。

边界与遗留
- 主仓 js/check/ignore/common.yml 在 check.js 下被自动剪掉一条已实现登记
  `libs/common/Format.cs: [TryParseAddressList]`（endpoint-parse-fail-fast 落地后的 stale 条目）
  并重排一段 folded 标量；与本票无关，二棒未提交该文件（worktree 内已 checkout 还原），
  需中央回归轮单独落该语料回写。
- task/done/tick-scale-conversion-single-source.md 的 wconn 侧射程（:2、:19、:63、:85 四处指向
  wconn/src/metrics.rs）随本票删除失效，其「两份本地派生组归一」现为 wmetric 单份；本票不越界
  改他票 done 件，请中央轮在其件内补一行「wconn 侧随 client-latency-histogram-unwired 删除消解」。
- 反向方案若日后翻回（给集群控制面真加客户端延迟观测），须先撤 client.yml 的整文件登记，并按票面
  「三面齐」补 wconf 旋钮 + 构造投影 + 真实读者对位设计，禁再造写死形参。
- 一棒 worktree /tmp/fork/rm-client-latency-hist 与分支 rm-client-latency-hist、rm-client-hist-v2
  按分工未删未动，由主代理处置；其全部代码内容已进 dev 34753d93，删除无损失。
- GarnetClient::new 形参列现为 6 个（endpoint / auth_username / auth_password / client_name /
  max_outstanding_tasks / timeout_millis），C# recordLatency 不落地的理由已就地写进
  wconn/src/client.rs 的构造器文档注释并指回 client.yml 登记位。

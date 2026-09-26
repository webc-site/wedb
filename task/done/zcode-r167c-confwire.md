甄别结论：通过（案三剔除并案）（甄别席 zc-fix-r16-confwire，2026-09-26）定级 P2
核验记录（票面逐锚现码复跑，非票面背书）：
并案独立复核：案三与 todo/wtls-cert-hotswap-refresh-toctou-revert.md 确系同一竞态同一改法——现码亲验 wedb/wtls/src/server.rs update_cert_file :225 无锁 store、:227-230 路径独立锁段、:232-234 restart_refresh_loop 另段锁自增 epoch；Inner::reload :332 核对于 if 条件内求值即释放、:335 store 在锁外，票面交错序可调度成立；其 C# 描述「原子指针交换与互斥机制」失实（GarnetTlsOptions.cs UpdateCertFile 实测 :100-121 为 :120 TlsServerOptions 整体重建，全无缝合所谓原子交换/互斥），wtls 席判定引述属实——案三并案至 wtls 票，以该票为精案。但 wtls 席「confwire 执行时仅留案一/案二」划界过宽：案四（集群凭据启动接线）与 TLS 无涉，系本票独立增量，不予并案。本票执行范围=案一/案二/案四。
案一成立：C# 锚 GarnetServerOptions.cs:678 声明、Options.cs:716-718 绑 --vector-set-quantization-task-count、:1038 直取投影、VectorManager.cs:226-228 消费（票面「ServerOptions.cs」系文件名微偏，类实存于 GarnetServerOptions.cs；NodeArgs 字段实测为 i32 非 Option<usize>，node_options.rs:651，微偏不翻案）；rust 侧 service.rs:893 冷启动硬编码 node_components(&store,false,0)，(false,true) 臂 open_with_config_and_aof :1371→open_with_config :1193→同落 :893，恢复臂 :1658/:1538-1541 确消费该值，vector_manager.rs:351 零值回退 available_parallelism——假旋钮确证。
案二成立：C# ServerConfig.cs:263 HandleIndexSizeChangeAsync :279-284 确设 auto-grow 拒门（门条件 AdjustedIndexMaxCacheLines>0 即 --index-max-memory-size，GarnetServerOptions.cs:553/:808；错帧 CmdStrings.cs:351 实文 "ERR Cannot adjust index size when auto-grow task is running (option: '{0}')"——票面 IndexSizeAutoGrow 成员名与错帧文案系转述失准，实质门在场）；rust 侧 config_commands.rs:433-435 注释「本仓无 index 自动增长选项」与 service.rs:618 spawn_index_auto_grow_task（:1832-1865 index_auto_grow Some 即常驻拉起）直接矛盾，CONFIG SET 臂 :474 与自动臂 database_manager_base.rs:535 双侧同入 grow_index_blocking，双写竞态真实。
案四成立：C# Options.cs:177-182 注册 --cluster-username/--cluster-password、:999-1000 投影入 serverOptions，garnet/libs/cluster/Server/ClusterProvider.cs 在面；rust 侧 wedb/wedb/src/args.rs:43 ClusterArgs 与 wconf/node_options.rs 均零命中二成员，cluster_provider/mod.rs:210 auth_container 硬初始化 (None,None)，boot 装配 wedb/wedb/src/server/boot.rs:45 ClusterProvider::new() 无参，仅赖 config_commands.rs:321-328 运行期 CONFIG SET 补救——启动脱节确证；票面所引 wedb/wcluster/src/provider.rs、wedb/wnode/src/boot.rs 路径不存在（现树无 wcluster crate），实际落点 wedb/wedb/src/server/cluster_provider/{mod,traits,assets}.rs 与 wedb/wedb/src/server/boot.rs，执行按订正路径。
查重：deviations.md 零登记本案三面（§127 量化条目系数据面屏障，正交）；issue/wconf-defaults-knobs-absence-unregistered.md 明文「handle_index_size_change 注释失真已由 todo/zcode-r167c-confwire.md 案二另立，勿动其 auto-grow 门」互认不重复；各池无案一/案四同题票。
合规与可执行度：三案均为既有机制透传/前置门/构造期投影，单套机制零新抽象、真源单点（NodeArgs→装配链→消费槽），对标 C# 锚齐全，改动点具体、clippy+test.sh 闭环可验，符合 transpile 与 rust_review 纪律；执行时案三门面归 wtls 票，本票勿动。

审核结论：通过，定级 P2。
确证冷启动路径参数 --vector-set-quantization-task-count 未透传硬编码丢弃、CONFIG SET index 缺失 auto-grow 运行门导致双写扩容竞态、TLS 证书热更回滚竞态与集群凭据参数脱节。执行方案清晰，供 task/fix.md 直接消费。

配置项全链路真接线与热更闭环审查提案

案一：冷启动正常路径下 --vector-set-quantization-task-count 命令行参数未透传丢弃（假旋钮）

问题分析：
1. Garnet 契约对齐：C# garnet/libs/server/Servers/ServerOptions.cs 中声明 VectorSetQuantizationTaskCount 属性，并在 garnet/libs/host/Configuration/Options.cs:GetServerOptions 中将命令行参数绑定到 options.VectorSetQuantizationTaskCount。在 garnet/libs/server/Resp/Vector/VectorManager.cs 初始化时，使用传入的 ServerOptions.VectorSetQuantizationTaskCount 实例化后台量化协程池与任务限制。
2. 工程现状确证：wedb/wconf/src/node_options.rs 中 NodeArgs 正确声明了 vector_set_quantization_task_count: Option<usize>，并且在 wedb/wnode/src/service.rs:StorageSessionProvider::open_from_args_with_config 恢复路径（(true, _) 分支）正确使用了 args.vector_set_quantization_task_count。但是在正常冷启动路径（(false, false) 与 (false, true) 分支），均调用了 open_node_with_config(args, config)。在 open_node_with_config 中，node_components 实例化被硬编码为 node_components(&store, false, 0)，硬编码传入 0。在 node_components 内部，当 tasks 为 0 时直接回退为 cpu 核心数（std::thread::available_parallelism），args.vector_set_quantization_task_count 未被传入。
3. 逻辑危害确证：运维在冷启动生产节点时通过 CLI 传入 --vector-set-quantization-task-count 4（希望限制后台量化对 CPU 占用，防止与高频读写争抢核心资源），该参数在冷启动路径被完全忽略静默丢弃，系统强制占用全核（如 64 核机子上开 64 个并发量化任务），导致 CPU 尖刺、查询 P99 时延恶化，成为脱节的假旋钮。

涉及代码：
rust 文件与函数：
wedb/wnode/src/service.rs:StorageSessionProvider::open_from_args_with_config
wedb/wnode/src/service.rs:open_node_with_config
wedb/wnode/src/service.rs:node_components

对应 c# 文件与函数：
garnet/libs/server/Servers/ServerOptions.cs:VectorSetQuantizationTaskCount
garnet/libs/host/Configuration/Options.cs:GetServerOptions
garnet/libs/server/Resp/Vector/VectorManager.cs:VectorManager

精炼执行方案：
1. 改造 open_node_with_config 与 node_components，增加 vector_quant_tasks 入参或直接透传 NodeArgs。
2. 在 open_node_with_config 中将 args.vector_set_quantization_task_count.unwrap_or(0) 传入 node_components。
3. 测试验证点：冷启动指定 --vector-set-quantization-task-count 2，断言 VectorManager 内部量化工作者并发数精准为 2 而不是回退到 host CPU 核心数。


案二：CONFIG SET index 缺失 auto-grow 运行门导致双写扩容竞态

问题分析：
1. Garnet 契约对齐：C# garnet/libs/server/ServerConfig.cs:HandleIndexSizeChangeAsync 中，对 CONFIG SET index 进行了强约束前置校验：若 indexSizeAutoGrow 选项开启，立即短路拒绝，向客户端返回 GenericErrIndexSizeAutoGrow 错误帧（"-ERR Cannot set index size when auto-grow is enabled\r\n"），严禁运行时人工扩容与自动扩容并发碰撞。
2. 工程现状确证：wedb/wnode/src/resp/config_commands.rs:ServerConfig::handle_index_size_change 中，代码第 434 行留下失真注释，声称“判定: 本仓无 index 自动增长选项 (Garnet opts.IndexSizeAutoGrow)，该服务器选项未接，故不设该门亦无对位常量”。然而 wedb/wconf/src/node_options.rs:NodeArgs 已实装 index_max_size 选项，wedb/wnode/src/service.rs:spawn_index_auto_grow_task 也已完全接线并常驻运行：当 index_max_size 大于当前 index_size 时，后台周期任务通过 grow_index_blocking 触发动态扩容。
3. 逻辑危害确证：当节点开启了 index_max_size 自动扩容策略时，客户端仍可通过 CONFIG SET index 并发修改哈希表槽位数。由于 config_commands.rs 缺失 auto-grow 运行门，客户端的 CONFIG SET 与后台 spawn_index_auto_grow_task 同时并发调用 grow_index_blocking。不仅违背 Garnet 规范行为（未返回 GenericErrIndexSizeAutoGrow 错帧），且可能在底层 Fast-Hash 索引扩容状态机中产生并发重入或破坏扩容单调性。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/config_commands.rs:ServerConfig::handle_index_size_change
wedb/wnode/src/service.rs:spawn_index_auto_grow_task
wedb/wconf/src/node_options.rs:NodeArgs

对应 c# 文件与函数：
garnet/libs/server/ServerConfig.cs:HandleIndexSizeChangeAsync
garnet/libs/server/Resp/CmdStrings.cs:GenericErrIndexSizeAutoGrow
garnet/libs/server/Servers/ServerOptions.cs:IndexSizeAutoGrow

精炼执行方案：
1. 在 ServerConfig / RuntimeServerConfig 中引入对当前是否处于 index auto grow 状态（即 index_max_size > index_size 或 auto-grow 开关激活）的检测门。
2. 在 handle_index_size_change 前置校验：若 auto grow 开启，直接写入 RespResponse::Error(b"ERR Cannot set index size when auto-grow is enabled") 并阻断。
3. 测试验证点：启动配置 index_max_size 的节点，发送 CONFIG SET index 2g 命令，断言收到 auto-grow 拒绝错误帧且索引大小未被篡改。


案三：TLS 证书在线热更与后台定时刷新存在无互斥回灌覆写竞态

问题分析：
1. Garnet 契约对齐：C# garnet/libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile 采用原子指针交换与互斥机制，确保证书热更与定时重载（ServerCertificateSelector.cs）单向递增推进，不发生旧证书逆向回灌。
2. 工程现状确证：wedb/wtls/src/server.rs 中，ServerTlsConfig::update_cert_file 在换装新证书时，先无锁调用 self.resolver.0.store(Arc::new(ck))，随后才获取 let mut st = self.state.lock().map_err(...)，并在获取锁后才自增 st.refresh_epoch += 1 并更新 cert_file / cert_pass 等。而后台定时重载函数 Inner::reload 中，先获取 state 锁，读取当前 epoch 与路径；随后释放锁并执行磁盘 IO（load_certs）；再次获取锁，校验 st.refresh_epoch == current_epoch；随后退出锁作用域，才无锁执行 resolver.0.store(Arc::new(ck))。
3. 逻辑危害确证：若后台 reload 正在从磁盘载入旧证书，此时客户端发送 CONFIG SET cert-file 换装新证书并成功执行 update_cert_file，但若 update_cert_file 在 store 之后、尚未获取 state 锁递增 epoch 的极短时间窗口内，reload 协程恰好执行了 epoch 校验（此时读到的仍是旧 epoch），校验通过。随后 update_cert_file 递增 epoch 完成退出，而 reload 紧接着在锁外调用 resolver.0.store(Arc::new(ck))，将已被替换为新证书的 TLS 解析器强制覆盖回旧证书，导致 CONFIG SET 换装成功后证书被幽灵回滚。

涉及代码：
rust 文件与函数：
wedb/wtls/src/server.rs:ServerTlsConfig::update_cert_file
wedb/wtls/src/server.rs:Inner::reload
wedb/wtls/src/server.rs:Inner

对应 c# 文件与函数：
garnet/libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile
garnet/libs/server/TLS/ServerCertificateSelector.cs

精炼执行方案：
1. 将 update_cert_file 内 resolver.0.store 的更新操作移入 state 锁的临界区保护之内，且必须在递增 refresh_epoch 之后执行。
2. 将 Inner::reload 中的 resolver.0.store 同样移入第二次 state 锁保护的临界区内，确保 epoch 校验与指针发布强一致原子化。
3. 测试验证点：模拟高频 reload 与 update_cert_file 并发交织，验证最终 resolver 必定为最新 update_cert_file 的证书，绝无旧证书反向冲刷。


案四：集群互信凭据启动参数脱节未向 NodeArgs/ClusterArgs 暴露

问题分析：
1. Garnet 契约对齐：C# garnet/libs/host/Configuration/Options.cs 中明确声明了 ClusterUsername 与 ClusterPassword 命令行选项（--cluster-username <string> 与 --cluster-password <string>），并在 ClusterProvider 启动构造阶段注入 AuthContainer，使集群节点间能够携带凭据握手通信。
2. 工程现状确证：在 wedb/wcluster/src/provider.rs 中，ClusterProvider 已完整实现了 auth_container: Arc<AuthContainer>，并提供了 update_cluster_auth 运行时更新方法，CONFIG SET cluster-username / cluster-password 能够调停生效。但检查 CLI 启动入口与配置聚合：wedb/wedb/src/args.rs:ClusterArgs 缺失 cluster_username 与 cluster_password 字段；wedb/wconf/src/node_options.rs:NodeArgs 缺失 cluster_username 与 cluster_password 字段；wedb/wnode/src/boot.rs 中初始化 ClusterProvider 时，无法从启动参数中获得初始认证凭据，默认只能初始化为 None（空凭据）。
3. 逻辑危害确证：在启用了集群模式且需要节点互信认证的生产环境中，节点无法通过启动命令行或配置文件直接指定集群认证账号密码。节点启动初始化握手时由于缺乏凭据会被对端集群节点立即拒绝，只能依赖节点启动成功后外部发送 CONFIG SET 补救，破坏集群自动化编排部署与冷启动高可用。

涉及代码：
rust 文件与函数：
wedb/wedb/src/args.rs:ClusterArgs
wedb/wconf/src/node_options.rs:NodeArgs
wedb/wnode/src/boot.rs:open_cluster_provider
wedb/wcluster/src/provider.rs:ClusterProvider

对应 c# 文件与函数：
garnet/libs/host/Configuration/Options.cs:ClusterUsername
garnet/libs/host/Configuration/Options.cs:ClusterPassword
garnet/libs/cluster/Server/ClusterProvider.cs

精炼执行方案：
1. 在 wedb/wedb/src/args.rs 的 ClusterArgs 与 wedb/wconf/src/node_options.rs 的 NodeArgs 中补充 cluster_username 与 cluster_password 参数声明。
2. 在 wedb/wnode/src/boot.rs 中 open_cluster_provider 构造阶段，将启动参数透传至 AuthContainer 初始化，建立开箱即用的互信凭据。
3. 测试验证点：传入 --cluster-username wedb --cluster-password secret 启动集群节点，断言节点内部 ClusterProvider 的 AuthContainer 启动即包含有效鉴权凭据。

视角结论:有增量

合入哈希：9264df8f5ea6a610509090fed9aa2c367af6a978 收口形态：案一/二/四接线闭环，案三并案 wtls

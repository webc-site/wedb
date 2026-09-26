甄别结论：通过（甄别席 zc-fix-r16-certswap，2026-09-26）定级 P2
核验记录（现码逐锚复跑）：
Rust 锚全成立：update_cert_file :208-236（:225 无锁 store、:227-231 路径独立锁段、:232-234 restart）、restart_refresh_loop :252-260（:255 代际自增另段锁）、reload :324-337（:332 核对在 if 条件内求值即释放、:335 store 确在锁外，无锁窗口现码仍在）、循环体 :288-308、try_start :274 自取同一锁（state 为 parking_lot Mutex :61，非重入，锁内嵌套死锁警示属实）；「reload 核对通过→update 全程→:335 回灌」交错在两锁段间隙可调度，成立。
C# 锚成立：GarnetTlsOptions.cs UpdateCertFile :100-120（:118 整体重建）、GetSslServerAuthenticationOptions :122-168（:150 EndTimer、:154 新 selector、:163-166 回调闭包读新字段）；ServerCertificateSelector.cs 构造器实测 :52-66/:71-86、旧 Timer 回调 :105-142（写点 :111/:117 仅写退役旧对象）——「涉及代码」段沿用旧微偏行号，票内行级勘误注记已覆盖，实质全对；ServerConfig.cs cert 臂实测 :173-182（票面 :171-180 微偏），调用点 :177 精确。
生产并发成立：wnode/src/server.rs run_async :170 自建 Runtime :211、block_on :246 内驱动 start :307（:563 ensure_refresh_loop 挂表）；spawn_accept_worker :1111-1144 逐核独立 OS 线程各持自建运行时；CONFIG SET 调用点 config_commands.rs:342（network_config_set :252，cert 臂 :337-348）——跨线程真并发属实。restart_refresh_loop 全仓唯一调用点 :233，随拆随删无壳可留属实。
测试与自陈成立：:581-633/:639-681 两测均为「核对时点代际已变」形态，未覆盖票面交错；:320-323/:204-205 契约承诺未兑现属实；wnode/tests/server_cert_reload.rs :56/:189 两集成测名实存。
查重：deviations.md TLS 条目 §36/§55-§57/§71/§102/§124 均无本 TOCTOU 登记，done/ing/reject/issue 池无同题票；但 todo/zcode-r167c-confwire 案三与本票同轴（同竞态，且其把 C# 误读为「原子指针交换与互斥机制」、锁写法描述失实）——以本票为精案，案三随本票并案，confwire 执行时仅留案一/案二。
合规：单把 parking_lot 锁临界区延展、零新机制零新分配、IO 留锁外、数据面 resolve 零锁不受渗透，符合 transpile 对标 C#「换装即终态」结构与 rust_review 单机制/零死代码纪律；改动点可落、clippy+test.sh 闭环。

审核结论：通过（审核席 zcode-r17-review-cert，2026-09-26）

双侧亲验记录：
Rust 竞态亲验：wedb/wtls/src/server.rs Inner::reload :332 代际核对系 if 条件内瞬时持锁（求值即释放），:335 store 在锁外，核对-换装临界区断裂属实；update_cert_file 三件跨三段——:225 resolver store 锁外、:227-231 路径写一段锁、:252-260 代际自增另一段锁。票面交错序（旧代 reload :332 核对通过 → update_cert_file 全程完成 → :335 回灌旧证）两线程间可调度，成立。
生产并发亲验：wnode/src/server.rs run_async :170 自建 Runtime 驱动 start（start 内 :563 ensure_refresh_loop 拉起刷新循环任务，挂装配运行时线程）；CONFIG SET 在 spawn_accept_worker 独立 OS 线程（wnode-worker-N）会话内执行 config_commands.rs:342 调用点——跨运行时真并发，非同线程串行假象。
C# 结构亲验：GarnetTlsOptions.cs:100-120 UpdateCertFile，:118 整体重建 TlsServerOptions；GetSslServerAuthenticationOptions :122-168 内 :150 旧 selector EndTimer、:154 新 selector（文件形态构造器 :71-86 同步装载新证 + 挂新 Timer）、:163-166 新回调闭包引用新 selector——旧 Timer 回调（ServerCertificateSelector.cs:105-142，写点 :109-120）只写退役旧对象 sslServerCertificate 字段，新 options 无路径可达，结构上无回灌面，票面说法属实。
测试覆盖亲验：server.rs :581-633 / :639-681 两测均为「核对时点代际已变」形态（update 先行完成后才跑 reload(0)），未覆盖「核对通过后 update 才完成」交错；:322-323「根治换证在途重读回灌竞态」注释与 :204「新握手立即用新证书」文档承诺对该交错未兑现，票面指控成立。
查重：doc/zh/deviations.md TLS 在册条目 §36（装载五处分叉）/§55（PEM-only 收窄）/§56（cert-subject-name 删员）/§57（加载失败 fail-fast，失败路径保留旧证——非本并发竞态面）/§102（握手 10s 超时）/§124（会话票据/链深/EKU/吊销），无一登记本 TOCTOU；task/issue、todo、ing、reject 各池无同题票据，非重复立项。
行级勘误（不翻案）：ServerCertificateSelector.cs 构造器实际 :52-66/:71-86、Timer 回调写点 :109-120，票面 :53-65/:65-85/:107-119 微偏，实质全对。

整理优化执行方案（供 task/fix.md 直接消费）：
1 wedb/wtls/src/server.rs Inner::reload :324-337 收口：:332-335 改单段 state.lock() 守卫——锁内读 refresh_epoch 比对 my_epoch，失配即返回 Ok(())，匹配则同守卫内 resolver.0.store；证书文件 IO 读段（:329-331）保持锁外不变。
2 wedb/wtls/src/server.rs update_cert_file :208-236 换装原子化：resolver.0.store、cert_path/key_path 写入、refresh_epoch += 1（保留 refresh_freq_secs > 0 门）并入同一段 state.lock() 守卫；restart_refresh_loop :252-260 解体——代际自增直入 update 临界区，try_start_refresh_loop 拉起保持锁外调用（其内部 :274 自取同一 state 锁，parking_lot 非重入，锁内嵌套即死锁，票面已正确识别）；restart_refresh_loop 现无第二调用者（grep 全仓仅 update_cert_file :233 一处），随拆随删不留壳。两步合账：旧代 reload 的核对-换装与 update 的换装-换代互斥于同一把 state 锁——reload 临界区先落位则随后被新证覆盖，update 临界区先落位则代际核对失配丢弃，回灌路径闭合；并发双 CONFIG SET 亦收敛为后到临界区整体胜出（对位 C# 末次 UpdateCertFile 胜出形）。
3 测试验证点：既有 reload_race_epoch_mismatch_drops_stale_cert / reload_in_flight_stale_cert_dropped_on_epoch_mismatch 维持绿；补第三态用例采并发迭代式而非 reload 手动分段（分段暴露会污染生产 API）：循环 N 轮，每轮 spawn 线程执行匹配代 Inner::reload、主线程并发 update_cert_file 切新证，join 后断言活跃证书恒为新证——修复后两种临界区落位序终态均为新证，逐轮断言即闭环；wnode/tests/server_cert_reload.rs 的 config_set_cert_file_reloads_online 与 cert_refresh_timer_serves_rotated_certificate 维持绿；./sh/clippy.sh 零警告 + ./test.sh 全绿。
4 零开销自证：同一把 state Mutex 临界区延展，零新锁零新代际机制零新分配；ArcSwap::store 无阻塞、临界区内无 IO，数据面 resolver.resolve 握手路径零锁不受渗透；失败路径（load_certs/load_private_key 报错先返）语义不变，禁半态换装契约保留。

wtls 证书热换装（CONFIG SET cert-file-name）与在途刷新循环 reload 的检查-换装间隙回灌竞态（TOCTOU）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 证书热换装按「整体换对象」完成：ServerConfig.cs:171-180（CONFIG SET cert-file-name 臂 :177）调 GarnetTlsOptions.UpdateCertFile（GarnetTlsOptions.cs:100-120），其 :118 以新路径重建整份 TlsOptions（GetSslServerAuthenticationOptions 构造新 ServerCertificateSelector，构造期 :53-65/:65-85 同步装载新证书并按 certRefreshFrequency 重挂 Timer）。旧 selector 连同其 Timer 一起被替换退役：旧 Timer 回调（ServerCertificateSelector.cs:107-119）只写旧 selector 对象的 sslServerCertificate 字段，对新 options 无引用可达——结构上新证书换装后不存在任何旧定时器写者能把活跃证书改回去，换装即终态。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧为单共享 resolver（ArcSwap<CertifiedKey>）+ 代际护栏的刷新循环。ServerTlsConfig::update_cert_file（wedb/wtls/src/server.rs:208-236）的换装序为：resolver.0.store(Arc::new(ck))（:225，无锁）→ state 锁内写 cert_path/key_path（:227-231）→ restart_refresh_loop（:232-234）在另一段 state 锁内做 refresh_epoch += 1（:252-260）。刷新循环体（:288-308）逐周期调 Inner::reload（:324-337）：锁外读路径并装载证书（IO 慢段），随后锁内核对代际（:332），通过即在锁外执行 resolver.0.store（:335）。缺陷在两处临界区错位：其一，reload 的「核对代际」与「换装 store」不在同一临界区，:332 核对通过到 :335 store 之间存在无锁窗口；其二，update_cert_file 的「换装 store（:225）」与「代际自增（restart_refresh_loop）」同样不在同一临界区。交错序：旧代 reload 锁外读旧路径并完成装载 → :332 核对时 refresh_epoch 尚未自增（update 的 restart 还没跑到）→ update_cert_file 全程完成（:225 换新证、:228 写新路径、:233 代际 +1 并拉起 N+1 代循环）→ 旧代 reload 在 :335 把旧路径装载的证书 store 进 resolver——活跃证书被回灌为换装前的旧证书。代码注释自陈「store 前重取 refresh_epoch 与 my_epoch 比对，失配即丢弃……根治换证在途重读回灌竞态」（:320-323）与 update_cert_file 文档承诺「重载成功即原子换装活跃证书，新握手立即用新证书」（:204-205），现实现均未兑现：既有两测（reload_race_epoch_mismatch_drops_stale_cert :581-633 / reload_in_flight_stale_cert_dropped_on_epoch_mismatch :639-681）只覆盖「核对时点代际已变」形态，未覆盖「核对通过后 update 才完成」的交错。生产可达性：CONFIG SET 在会话线程（worker 运行时，config_commands.rs:342 调用），刷新循环任务在装配运行时（boot/run_async block_on 内 spawn），两线程真并发；cert-refresh-freq > 0 即周期性 reload 在场。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
控制面竞态，非数据面。后果：CONFIG SET cert-file-name 已回 +OK（换装成功）后，活跃证书可能在窗口内被回灌为旧文件证书，直到下一刷新周期（refresh_freq_secs，仅刷新开启时存在自愈）才恢复新证。窗口内新握手使用旧证书——对「因旧证泄密/吊销而紧急轮换」场景是身份面缺口；mTLS 客户端证书校验面不受影响（verifier 独立于 resolver），服务端身份证书面受影响。窗口极窄（reload :332 至 :335 数条指令间须塞入 update 全程），低概率、可自愈，但违背函数自身文档契约（「禁半态换装」「新握手立即用新证书」）且 C# 结构上无此面。

涉及代码：
rust 文件与函数：
wedb/wtls/src/server.rs:ServerTlsConfig::update_cert_file（:208-236，resolver store :225 / 路径写 :227-231 / restart_refresh_loop :232-234）
wedb/wtls/src/server.rs:ServerTlsConfig::restart_refresh_loop（:252-260，代际自增与拉起）
wedb/wtls/src/server.rs:Inner::reload（:324-337，代际核对 :332 与 store :335 临界区断裂）
wedb/wtls/src/server.rs:try_start_refresh_loop 内刷新循环任务体（:288-308，周期调 reload）
wedb/wnode/src/resp/config_commands.rs:network_config_set（:337-348，:342 生产调用点，worker 运行时线程）

对应 c# 文件与函数：
garnet/libs/server/ServerConfig.cs:NetworkCONFIGSET（cert-file-name 臂 :171-180，调用点 :177）
garnet/libs/server/TLS/GarnetTlsOptions.cs:UpdateCertFile（:100-120，:118 整体重建 TlsOptions）
garnet/libs/server/TLS/ServerCertificateSelector.cs:ServerCertificateSelector 构造器（:53-65/:65-85，同步装载 + Timer 挂表）与 GetServerCertificate 定时回调（:107-119，旧 Timer 仅写退役旧对象）

精炼执行方案：
1. Inner::reload 收口临界区：将收尾代际核对与 resolver.0.store 并入同一 state.lock() 守卫（锁内读 refresh_epoch 核对，通过即同守卫内 store；证书文件 IO 读段保持在锁外不变）。
2. update_cert_file 换装原子化：resolver store（现 :225）、cert_path/key_path 写入、refresh_epoch 自增三件并入同一段 state.lock() 临界区；restart_refresh_loop 拆为「锁内自增」由 update_cert_file 直做 +「锁外仅 try_start_refresh_loop 拉起」（try_start_refresh_loop 内部自取同一 state 锁，锁内嵌套调用会死锁，必须保持锁外）。两步合并后：旧代 reload 的核对-换装临界区与 update 的换装-换代临界区互斥，先于 update 落位则随后被新证覆盖，后于 update 落位则代际核对失配丢弃，回灌路径闭合。零新机制（同一把 state 锁的临界区延展，ArcSwap store 无阻塞，锁内无 IO）。
3. 测试验证点：既有 reload_race_epoch_mismatch_drops_stale_cert 与 reload_in_flight_stale_cert_dropped_on_epoch_mismatch 保持绿；补第三态锁面用例——模拟「旧代 reload 核对通过后、store 前插入完整 update_cert_file」的交错（可在测试内以手动调 Inner::reload 分段或借 state 锁竞争注入），断言活跃证书终态恒为新证书；wnode/tests/server_cert_reload.rs 的 config_set_cert_file_reloads_online 与 cert_refresh_timer_serves_rotated_certificate 维持绿。

合入哈希：7fe4769 收口形态：update_cert_file 的换装-换代（resolver store、cert/key 路径落位、refresh_epoch 自增）与 Inner::reload 的核对-换装各并入单段 state 锁临界区互斥闭合回灌路径，restart_refresh_loop 随拆随删（唯一调用点代际自增直入 update 临界区、拉起保持锁外），补第三态并发迭代用例 concurrent_reload_update_race_always_lands_new_cert，对位 C# GarnetTlsOptions.UpdateCertFile 整体重建换装即终态、无回灌面（含并案 confwire 案三）。

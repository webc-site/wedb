甄别结论：通过（甄别席 J5，2026-09-27，定级 P2——BLPOP 0 挂起窗逐轮保全无上界，堆驻留随灌入速率×时长线性无界，多连接即 OOM 放大面；无数据丢失有界背压对齐 C#）。亲验：drive.rs 活连接保全臂 Ok(_) => { preserve_probe(...); probe_buf.clear(); continue; } 无水位判据无退出条件，preserve_probe take_recv_scratch+extend_from_slice 无上界沉淀，MIN_READ_SPACE=4096（buffer.rs:11）每轮限额轮数无界；消费段悬停于 probe_race().await，core.rs 收缩面挂起期不可达。C# 对照亲验：ListCommands.cs:283 Must block on network thread + AsyncUtils.BlockingWait 停摆无未决接收、TcpNetworkHandlerBase.cs:214 do/while 内核守护，SO_RCVBUF 有界天然背压。审核方案（竞速局部单阈值水位门、停建探测读退化为两路 select、否决 DeadConn 断连向）亲验认可，与 todo 脚本臂票互补不同轴、先落者留门位锚。勘误维持审核：DEFAULT_RECV_BUFFER_CAPACITY 实为 1<<16（core.rs:70），1<<17 系 :83 OUTPUT_WATERMARK_BYTES。派沙箱席 c01f。

审核结论：通过（P2 维持。挂起期无排空通道经亲验坐实：probe_race 保全臂 drive.rs:769-773 continue 循环重建探测读无任何水位判据与退出条件，preserve_probe :786-801 经 take_recv_scratch（resp_session_consumer.rs:191 mem::take recv_buffer 整体移交直填）extend 沉淀无上界；竞速循环 poll 结构核实 resolve_fut 与 terminate_fut 全程 pinned 于 select、probe_fut 每轮重建，对端持续来字即逐轮回到保全臂，每轮至多 MIN_READ_SPACE(=4096, buffer.rs:11) 字节、轮数无界，挂起窗堆驻留随灌入速率×时长线性无界。挂起期排空通道核验：消费段悬停于 probe_race(...).await，循环内仅 preserve_probe 与 add_net_bytes、零消费点，try_consume_messages 的 shift/收缩收口（core.rs:882-924）挂起期不可达，OUTPUT_WATERMARK_BYTES（core.rs:83）系出向应答门与入向 recv_buffer 无关——票面不收窄，OOM 放大面真实。BLPOP 0 挂起窗可刻意触发、多连接并发即内存放大，但无数据丢失/损坏/panic，正常流水线窗口间即排空，P2 恰当。C# 对照成立：ListCommands.cs:283 网络线程内联 BlockingWait 期间 HandleReceiveWithoutTLS 的 do/while ReceiveAsync 循环（TcpNetworkHandlerBase.cs:214）停摆无未决接收，字节滞留内核 SO_RCVBUF 有界、TCP 零窗自然背压、单连接驻留硬顶，与 todo/wnode-script-suspend-arm-no-peer-liveness-probe-zombie-observer 审核裁定第 6 条订正后的 C# 盲窗叙事自洽。互补不冲突核实：该票补脚本臂探测（活性轴），本票约束保全驻留上界（资源轴）；其裁定第 3 条「竞速期本地累积、收场一次保全」重构不消解本票（本地累积同样无界），水位门随保全点迁移即可，两票落地顺序无关。查重：issue/todo/reject 各池无同轴在册票。票面一处常量值勘误：DEFAULT_RECV_BUFFER_CAPACITY 实为 1<<16（core.rs:70），1<<17 系 OUTPUT_WATERMARK_BYTES（core.rs:83），机制结论不受影响。）

挂起期探测保全臂无界抽干对端来字：preserve_probe 循环无水位判据，BLPOP 0 挂起窗对端持续灌帧全会话缓冲线性沉淀成 OOM 放大面（C# 同窗字节滞留内核 socket 缓冲天然有界背压）

问题分析：
1 Garnet 契约对齐：C# 阻塞/慢命令在网络线程内联阻塞（garnet/libs/server/Resp/Objects/ListCommands.cs:283 自注 Must block as we're on the network thread，AsyncUtils.BlockingWait 裸 GetResult），挂起窗无人在场读套接字，对端持续来字滞留内核 socket 接收缓冲（garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:214 内核事件守护）——SO_RCVBUF 有界、TCP 窗口闭合即天然背压，单连接驻留内存有硬顶。
2 工程现状确证：rust 泵挂起窗为主动抽干——wedb/wnode/src/net/handler/drive.rs:767-772 活连接保全臂 Ok(_) => { preserve_probe(session, entry, &probe_buf); probe_buf.clear(); continue; }，每轮探测读至多 MIN_READ_SPACE(=4096, buffer.rs:11) 字节即经 take_recv_scratch/extend_from_slice/return_recv_scratch（:786-801 preserve_probe）并入会话 recv_buffer 后重建探测读续等执行体；循环无水位判据、无上限、无退出条件。接收缓冲收缩仅存在于消费期 shift 收口（core.rs:892-921，capacity > DEFAULT_RECV_BUFFER_CAPACITY(1<<17) 才换块/收缩），挂起期永不触达。probe_buf 自身每轮 clear 保持 4KB 量级，增量全部沉淀会话缓冲。与 todo/wnode-script-suspend-arm-no-peer-liveness-probe-zombie-observer 票的探测补偿方向互补（该票补脚本臂探测，本票约束探测的驻留上界），互不覆盖。
3 逻辑危害确证：客户端建连 → BLPOP key 0（timeout=0 无限等待观察者）→ 持续 4KB 帧灌入 → 每轮保全入会话缓冲 → 该连接堆内存随挂起时长×灌入速率无界线性增长，多连接并发即 OOM 放大面；入向字节记账（:798-800 add_net_bytes）为真实到达字节无记账错，错在无界驻留。C# 同输入内核缓冲打满后客户端 send 阻塞，内存有界。

涉及代码：
rust 文件与函数：
wedb/wnode/src/net/handler/drive.rs:probe_race 活连接保全臂（:725-781，:767-772 continue 循环）、preserve_probe（:786-801 extend 无界沉淀）
wedb/wnode/src/resp/resp_server_session/core.rs:try_consume_messages_body 收缩面（:892-921 挂起期不可达）、DEFAULT_RECV_BUFFER_CAPACITY（:70）

对应 c# 文件与函数：
garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:OnNetworkReceiveWithoutTLS（:214 内核守护与有界滞留窗）
garnet/libs/server/Resp/Objects/ListCommands.cs:ListBlockingPop（:283 内联阻塞零读取天然背压）

精炼执行方案：
1 保全臂增设挂起期保全水位（复用 §14 邮箱高低水位思路或 OUTPUT_WATERMARK 同款常量门）：preserve_probe 累计保全量超门即停止重建探测读（FIN 盲窗换有界）或判 DeadConn 走 Disposed 收场；水位状态挂会话域与 recv_buffer 同生命周期，preserve_probe 单点判，不新增全局容器
2 测试验证点：BLPOP 0 挂起 + 持续灌帧，断言会话 recv_buffer capacity 有顶、断连收场后资源计数配对归零；短流水线正常保全回归不回退

审核裁定执行方案（审核席修订，覆盖票面方案 1 的两处松散点）：

1 门位与门形：水位计数挂 probe_race 竞速局部（每次竞速窗口独立计数归零，窗口间消费段必然排空，单窗有界即全连接有界），不进会话域——票面「挂会话域与 recv_buffer 同生命周期」可落但更重，且脚本臂（todo 票裁定落地后）挂起窗 recv_buffer 为 lua.rs:121 换出壳，会话域计数反添换出交互面。门判据单阈值即可：probe_race 局部累计保全量（逐轮 preserve 后 probe_buf.len() 累加），超 PROBE_PRESERVE_WATERMARK（新常量，core.rs 紧邻 DEFAULT_RECV_BUFFER_CAPACITY 定义，取 1<<17 与出向 OUTPUT_WATERMARK_BYTES 同量级）即停建探测读，循环退化为终止广播 × 执行体两路 select（ACL 臂同形样板）；严禁高低双水位迟滞——挂起窗内一旦停建即不再重建，无抖动面，双门属过度设计。
2 收敛语义择向：停建探测读即入向停止抽干 → 内核接收缓冲打满 → TCP 零窗客户端 send 阻塞，与 C# 内联阻塞窗行为逐点同构（有界驻留 + FIN 盲窗），契约对齐；票面「判 DeadConn 走 Disposed 收场」备选否决——C# 对灌帧客户端是背压不是断连，杀连接属行为偏离且误伤合法慢消费者。停建窗 FIN 不可达系 C# 同形盲窗（todo 票裁定第 6 条在案），执行体 resolve 后消费段重见流尾即正常收场。
3 与 todo/wnode-script-suspend-arm-no-peer-liveness-probe-zombie-observer 协同：该票裁定第 3 条把保全时点改「竞速期本地累积、收场一次保全」——本票水位门随累积点走（门住本地累积量），门形与常量同一份，两票落地顺序无关，先落者留门位锚注释指向后落票，严禁出现两套计数两套门。
4 测试验证点（维持票面并落点）：wedb/wnode/tests/net_pump_consume_tests.rs 泵夹具（ScratchLineConsumer 同款形制）加桩阻塞消费者（resolve 长挂）三案——a) BLPOP 0 形挂起 + 持续灌帧超水位，断言 recv_buffer capacity 封顶于水位量级且探测读停建；b) 停建后执行体 resolve，断言保全字节照常消费应答、连接存活不误断；c) 短流水线正常保全回归（低于水位逐轮保全行为零变化）+ 断连收场资源计数配对归零。

收口记录（收票席 R4 批次，2026-09-28）：合入 9aa6d4d0（验货 8a8e890a+dev 前进复查零警）。收口形态=probe_race 活连接臂增设保全水位门 Ok(_) if acc.len() < PROBE_PRESERVE_WATERMARK（core.rs 新常量 1<<17，紧邻 DEFAULT_RECV_BUFFER_CAPACITY，mod.rs 单源 re-export）：达界停建探测读退化为终止×执行体两路 select（ACL 停车臂样板），挂起窗单连接入向驻留封顶，对标 C# 网络线程内联阻塞（ListCommands.cs:283）字节滞留内核 SO_RCVBUF 有界背压（TcpNetworkHandlerBase.cs:214）——门与计数单份 acc.len() 无第二套。锁测 net_pump_consume_tests.rs 三案反证敏感（撤门实测 200706>135167 即红；30/30×4 无 flake）。deviations 无需新增。

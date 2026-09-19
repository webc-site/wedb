优先级：高（dev 测试基线红，计数轮前置）

单题：AOF 落盘帧与重放侧键标签解析口径不一致，重开直接判「物理键损坏」。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1 wnode::aof_shutdown_tests::dispose_flushes_uncommitted_frames_for_recovery —
  wnode/tests/aof_shutdown_tests.rs:138
  open recovered: Store(Io(Custom { kind: Other,
    error: "AOF replay: AOF 条目物理键损坏: 非法或未知的键标签字节: 0x75" }))
  0x75 即 ASCII 'u'，是被当作了键标签的键内容字节 —— 写侧帧边界与读侧条目游标两套口径已错位。
2 wnode::aof_replay::test_acl_replay_loop — wnode/tests/aof_replay.rs:789
  「回放恢复的 alice 认证成功」left: "-WRONGPASS Invalid username/password" / right: "+OK"

判读方向（须自行核实）
第 1 条是本次最硬的产线级缺口：dispose 刷出的未提交帧在重放时不可解析。重点查 AOF 帧写入侧近期
改动（会话 dispose/未提交帧刷盘、帧头字段增减、批量帧与单条帧的边界标记）与重放侧逐条目键标签
解析，是否共用同一编码器；若两侧各有一份帧格式定义，按「只留一个机制」收口到单点。
第 2 条须独立定性：可能同因（帧错位导致 ACL 命令条目被跳过），也可能独立（ACL 命令录制/回放射
未接）。两条都判完再回报，若确为同一根因须如实写明并留 C# 锚点，不得只修第 1 条就宣称完成。

避让
wnode/src/aof/aof_settings.rs 有在途分支 fix-min-page-guard 改动（页旋钮走校验核），本票勿动
设置面；wnode/src/storage/**（分层写臂）、wkv dbmeta/NS_MAP 各属另票。

改动域
wedb/waof/**、wedb/wnode/src/aof/**（处理器与回放消费点）、wacl 的 ACL 命令录制/回放射（若第 2
条确为独立），以及上述两个测试文件。禁止触碰 wresp、wkv 存储层核心。

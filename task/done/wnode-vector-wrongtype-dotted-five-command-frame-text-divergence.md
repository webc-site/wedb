终态：已合入 dev（2026-09-27）。e1e05cb §164 登记(册尾顺编):向量族统一无句点系刻意偏差,严禁按C#分流回写;parity 注释订正+12命令×7键型全帧等值断言收紧,零行为改动

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：按审核反转方向执行：纯登记+parity 注释+断言收紧，勿执行票面原方案1，严禁按 C# 分流回写

审核结论：通过·方向反转（真分叉在册，惟勿按 C# 句点对齐。C# 五点位行号属实俱引 RESP_ERR_WRONG_TYPE（CmdStrings.cs:201），但「两句点常量并存」不成立——C# 仅一句点常量，无句点系 AbortVectorSetWrongType:1893 就地字面量，实形为同判据族内 5 带/7 无分叉；rust wrong_type_reply 恒发无句点属实，消费五点（Reject:610/:615、保守拒 mod.rs:738、慢臂:652/:704）全在向量 12 命令域零外溢；parity:41/:266/:253 确锁错侧、wrong_type:405 startswith 松锚；§22 仅裁帧型未裁文案属实。方向裁决：拒改 rust 分流带句点——无句点系 Redis 标准文案（rust:54 自陈），按 C# 分流即回写上游臂间混乱，与 §23/§110/§113 先例同型违单机制红线；5.1 全等条辖数据集响应非错误文案，错误面仅裁前缀且 WRONGTYPE 前缀已合）

整理执行方案（审核席订正版，零行为改动，供 fix 消费）：
1 deviations 补新条：向量族 WRONGTYPE 统一无句点系刻意偏差，严禁按 C# 五点带句点形报分叉
2 parity 注释订为「本仓统一文案，非 C# 同款」，断言零改
3 松锚收紧为 12 命令无句点全帧等值；查重五池唯一

向量族五命令 WRONGTYPE 帧尾句点分叉：C# 回带句点版 RESP_ERR_WRONG_TYPE，rust 守卫单点恒出无句点版

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 向量会话层 WRONGTYPE 臂文案分两族。
   无句点族（AbortVectorSetWrongType 就地字面量，尾无句号）：VADD(:501)、VSIM(:946)、
   VEMB(:1362/:1417)、VDIM(:1491)、VGETATTR(:1536)、VINFO(:1575)、VREM(:1826)。
   带句点族（WriteError(CmdStrings.RESP_ERR_WRONG_TYPE)，即
   "WRONGTYPE Operation against a key holding the wrong kind of value." 尾含句号）：
   VCARD(:1454)、VISMEMBER(:1665)、VLINKS(:1718)、VRANDMEMBER(:1787)、VSETATTR(:1872)。
   RESP 错误帧对外逐字节全等是本仓承诺（wresp/src/cmd_strings.rs 头注自陈），
   尾句点即字节差。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：rust 全部向量命令的 WRONGTYPE 出帧
   收口到单一 wrong_type_reply()（resp_server_session_vectors.rs:627，常量
   ERR_VECTOR_SET_WRONG_TYPE :59-60 无句点版）。快路径守卫 Reject 臂与写命令保守拒臂
   （garnet_api/mod.rs:714-718 守卫统一拒绝、:736-740 Degrade 保守拒）与慢臂真读裁决
   （resp_server_session_vectors.rs:652/:704）对十二命令一概发无句点帧。于是
   VCARD/VISMEMBER/VLINKS/VRANDMEMBER/VSETATTR 五命令存活非向量键场景下，rust 帧
   比 C# 帧少一个尾部句号字节。cmd_strings.rs:54-58 注释已明知两版是「两条不同文案、
   不合并」，但守卫单点未按命令分流。带句点版单点 RESP_ERR_WRONG_TYPE
   （wresp/src/cmd_strings.rs:72）非向量域在用，向量五命令域漏引。
3. 逻辑危害确证：错误文案逐字节对拍的客户端/回归 harness 在五命令形态直接判红；
   测试面已带假锚——vector_cold_key_read_parity.rs:41-42 以单一无句点 WRONGTYPE_FRAME
   断言全部读写臂，:227 注释称只读慢臂「C# res==WRONGTYPE 同文案」、:253 注释称写命令
   三臂「同一文案」，对 VSETATTR 而言与 C# :1872 带句点臂不符，属测试锁错侧，
   修复时需同步订正，防后续轮据此假锚判「已对齐」。与 §22 边界：§22 只裁读不准态
   回 WRONGTYPE 帧型与读写分臂，未裁文案字节；与本条不冲突、不重开。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:ERR_VECTOR_SET_WRONG_TYPE、
wrong_type_reply、network_vector_read_slow/network_vector_write_slow 裁决臂
wedb/wnode/src/resp/garnet_api/mod.rs:exec 向量守卫 Reject 臂与 Degrade 保守拒臂
wedb/wresp/src/cmd_strings.rs:RESP_ERR_WRONG_TYPE（带句点单点，已存在待引）
测试：wedb/wnode/tests/vector_cold_key_read_parity.rs:cold_alive_string_read_slow_wrongtype_write_guard_reject

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVCARD(:1454)、
NetworkVISMEMBER(:1665)、NetworkVLINKS(:1718)、NetworkVRANDMEMBER(:1787)、
NetworkVSETATTR(:1872)、AbortVectorSetWrongType(:1890-1893)
garnet/libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_TYPE(:201)

精炼执行方案：
1. wrong_type_reply 增加按命令分流（或调用侧按 cmd 选帧）：VCARD/VISMEMBER/VLINKS/
   VRANDMEMBER/VSETATTR 五命令的守卫 Reject 臂、保守拒臂与慢臂真读 WRONGTYPE 改发
   带句点 RESP_ERR_WRONG_TYPE；其余七命令维持无句点 ERR_VECTOR_SET_WRONG_TYPE 不变。
   分流判据用既有 is_vector_read_command/命令枚举位，不新增机制。
2. 订正 vector_cold_key_read_parity.rs：WRONGTYPE_FRAME 拆为带/无句点两常量，
   五命令断言改带句点全帧，VSETATTR 写臂同改；resp_vector_set_wrong_type.rs 的
   startswith "-WRONGTYPE " 前缀断言收紧为全帧等值。
3. deviations.md 视裁决走向补注：若维持按 C# 分流补齐句点，§22 括注文案字节面归本条；
   若反向裁「全族收无句点」则须显式登记字节差，二选一不留白。
4. 测试验证点：SET 存活 string 键后逐发 VCARD/VISMEMBER/VLINKS/VRANDMEMBER/VSETATTR
   与 VADD/VREM 对照，前者帧尾含 "value.\r\n"、后者 "value\r\n"，与 C# 逐字节一致。

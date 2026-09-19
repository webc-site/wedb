裁决：拒绝 repl-send-bytecap 票测试节原案的一条测试构造，实施时已换等价可行形态。
核销 2026-09-20。取证基线：dev 当下 replica_wire.rs / wconn session.rs。

拒绝条目：「replica_wire.rs 单测仿 tcp_wire_not_connected 构造 TcpSessionWire
（pump_alive 关，直发失败入溢流），连续 append_log 大 payload 帧断言字节触顶断连」

理由：
1. send_or_enqueue 首行即 is_connected 闸（pump_alive && client.is_connected），
   pump_alive 关直接返回 NotConnected，根本走不到溢流入队臂，原案构造不成立。
2. 未 connect 的 GarnetClientSession tx 通道为空，is_connected 恒假，同样只能测得
   NotConnected 错路。
3. 常量 byte 顶为 128MiB（4 × 2<<24），以真实推帧灌到触顶需 ~8000 帧 × 16KiB、
   瞬时驻留逾 128MiB，单测代价过重。

替代实施：连静默 loopback 端点使会话通道在位（无凭证握手零往返）、不启常驻泵、
in_flight 置位封死直发臂——溢流只积不排、确定性触顶；TcpSessionWire 增 byte_cap
字段（产线 connect 取 MAX_UNFLUSHED_SEND_BYTES 唯一数值源，单测注入小值），与写泵
flush_threshold_bytes 注入分片阈值同形，非新增配置项。字节/条数两判据各出一枚单测。

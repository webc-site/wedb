优先级：低

7 [LOW] encode_cluster_append_log_frame 是零生产消费的第三套 APPENDLOG 编码器，C# 侧本为两个独立方法
具体问题：wconn 已有两个真编码器 encode_append_log_init_frame（:280，7 元素）与
encode_append_log_frame（:299，8 元素），生产链经 GarnetClientSession 方法直调它们（:120 初始化
帧、:153/:184 记录帧）；:323-348 又加了一个按 payload Option 分派到二者的门面口。全仓读者仅
replica_wire.rs:680 那条 cfg(test) 别名与 :728/:735 用例，生产零命中，但该口 pub、且被四处门面
文档当作对外能力宣示（wconn/README.md:22、:64 与 wconn/readme/zh.md:10、readme/en.md:10）。
C# 无此形态：GarnetClientSession.cs:473 ExecuteClusterAppendLog 与
GarnetClientSessionReplicationExtensions.cs:612 ExecuteClusterAppendLogInit 是两个并列方法，
调用方按语义各自选口，从不靠 Option 参数分派。与 task/ing/zero-consumer-dead-surfaces-batch-
five.md 第 9 条同族（该条处理 wconn 客户端应答门面的零读者臂），可并批。
rust：wedb/wconn/src/session.rs:319-348 encode_cluster_append_log_frame（对照 :280、:299 两个真
编码器与 :120/:153/:184 生产调用点）
c#：garnet/libs/client/ClientSession/GarnetClientSession.cs:473 ExecuteClusterAppendLog；
garnet/libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:612
ExecuteClusterAppendLogInit
修法：删该口（连同四处文档条目），第 4 条的 cfg 别名对一并删；若保留收敛门面，则让生产侧
GarnetClientSession 的初始化/记录两方法经它转调、使两个真编码器降级为私有实现，消除「三套并存 +
门面零读者」。

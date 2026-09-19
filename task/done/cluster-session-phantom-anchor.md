cluster-session-phantom-anchor 已完成并合并

来源 ./js/check.js 的「# 虚构锚点」项（非 next 票据）。取证基线 dev HEAD 67dfad8a；合并后 HEAD e89780c2。

## 问题
wedb/wnode/src/cluster_session.rs:121 的文档注释把 fn remote_node_id() 挂到
libs/cluster/Session/ClusterSession.cs:RemoteNodeId，但 ClusterSession.cs 无此符号（虚构锚点）。

## 正确对位（子代理实读核验）
真正对应的是接口属性 IClusterSession.RemoteNodeId（garnet/libs/server/Cluster/IClusterSession.cs:18，
注释「the id last presented during a GOSSIP message」）。写入点 RespClusterBasicCommands.cs:414
NetworkClusterGossip 内（「Node Id shouldn't change once set for a connection」，对标本仓「唯一写入点
CLUSTER GOSSIP 建链」）。CLIENT 类型用途 ClientCommands.cs:66-80 经 IsReplica(RemoteNodeId) 派生
REPLICA/MASTER 否则 PUBSUB/NORMAL，与注释逐字吻合。同 trait 兄弟方法 dispose /
process_cluster_commands 早已锚到 IClusterSession.cs，本修正与之一致。确认 AofSyncDriver.cs:36 的
RemoteNodeId 是另一概念（同步任务的副本目标），非本处对位。

## 处置
仅改注释一行：ClusterSession.cs:RemoteNodeId → IClusterSession.cs:RemoteNodeId。合并后 check.js
「# 虚构锚点」段落清空（子代理在其分支实测 exit 0）。无代码/行为变化。

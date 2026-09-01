# WeDb 待支持指令与模块清单 (TODO)

以下指令因 WeDb 暂未实现对应的底层数据结构或高级运行时（如 Lua/Function 脚本引擎、原生 Redis Cluster 槽管理、Sentinel 哨兵架构、Array 数据类型等），已收录至此清单供后续规划支持。

## CLUSTER 模块 (22 指令)
- **`CLUSTER MYID`**: Returns the ID of a node. *(since: 3.0.0)*
- **`CLUSTER ADDSLOTS`**: Assigns new hash slots to a node. *(since: 3.0.0)*
- **`CLUSTER KEYSLOT`**: Returns the hash slot for a key. *(since: 3.0.0)*
- **`CLUSTER REPLICATE`**: Configure a node as replica of a master node. *(since: 3.0.0)*
- **`CLUSTER LINKS`**: Returns a list of all TCP links to and from peer nodes. *(since: 7.0.0)*
- **`CLUSTER MIGRATION`**: Start, monitor and cancel slot migration. *(since: 8.4.0)*
- **`CLUSTER DELSLOTS`**: Sets hash slots as unbound for a node. *(since: 3.0.0)*
- **`CLUSTER SYNCSLOTS`**: Internal cmd for atomic slot migration protocol between cluster nodes. *(since: 8.4.0)*
- **`CLUSTER COUNTKEYSINSLOT`**: Returns the number of keys in a hash slot. *(since: 3.0.0)*
- **`CLUSTER BUMPEPOCH`**: Advances the cluster config epoch. *(since: 3.0.0)*
- **`CLUSTER COUNT-FAILURE-REPORTS`**: Returns the number of active failure reports active for a node. *(since: 3.0.0)*
- **`CLUSTER MYSHARDID`**: Returns the shard ID of a node. *(since: 7.2.0)*
- **`CLUSTER SLAVES`**: Lists the replica nodes of a master node. *(since: 3.0.0)*
- **`CLUSTER ADDSLOTSRANGE`**: Assigns new hash slot ranges to a node. *(since: 7.0.0)*
- **`CLUSTER GETKEYSINSLOT`**: Returns the key names in a hash slot. *(since: 3.0.0)*
- **`CLUSTER SETSLOT`**: Binds a hash slot to a node. *(since: 3.0.0)*
- **`CLUSTER DELSLOTSRANGE`**: Sets hash slot ranges as unbound for a node. *(since: 7.0.0)*
- **`CLUSTER FAILOVER`**: Forces a replica to perform a manual failover of its master. *(since: 3.0.0)*
- **`CLUSTER SAVECONFIG`**: Forces a node to save the cluster configuration to disk. *(since: 3.0.0)*
- **`CLUSTER FLUSHSLOTS`**: Deletes all slots information from a node. *(since: 3.0.0)*
- **`CLUSTER SET-CONFIG-EPOCH`**: Sets the configuration epoch for a new node. *(since: 3.0.0)*
- **`CLUSTER REPLICAS`**: Lists the replica nodes of a master node. *(since: 5.0.0)*

## SCRIPTING 模块 (6 指令)
- **`FUNCTION FLUSH`**: Deletes all libraries and functions. *(since: 7.0.0)*
- **`FUNCTION LOAD`**: Creates a library. *(since: 7.0.0)*
- **`SCRIPT FLUSH`**: Removes all server-side Lua scripts from the script cache. *(since: 2.6.0)*
- **`FCALL`**: Invokes a function. *(since: 7.0.0)*
- **`SCRIPT LOAD`**: Loads a server-side Lua script to the script cache. *(since: 2.6.0)*
- **`FCALL_RO`**: Invokes a read-only function. *(since: 7.0.0)*

## SENTINEL 模块 (15 指令)
- **`SENTINEL SIMULATE-FAILURE`**: Simulates failover scenarios. *(since: 3.2.0)*
- **`SENTINEL FAILOVER`**: Forces a Redis Sentinel failover. *(since: 2.8.4)*
- **`SENTINEL REPLICAS`**: Returns a list of the monitored Redis replicas. *(since: 5.0.0)*
- **`SENTINEL MASTERS`**: Returns a list of monitored Redis masters. *(since: 2.8.4)*
- **`SENTINEL MYID`**: Returns the Redis Sentinel instance ID. *(since: 6.2.0)*
- **`SENTINEL PENDING-SCRIPTS`**: Returns information about pending scripts for Redis Sentinel. *(since: 2.8.4)*
- **`SENTINEL INFO-CACHE`**: Returns the cached `INFO` replies from the deployment's instances. *(since: 3.2.0)*
- **`SENTINEL REMOVE`**: Stops monitoring. *(since: 2.8.4)*
- **`SENTINEL IS-MASTER-DOWN-BY-ADDR`**: Determines whether a master Redis instance is down. *(since: 2.8.4)*
- **`SENTINEL FLUSHCONFIG`**: Rewrites the Redis Sentinel configuration file. *(since: 2.8.4)*
- **`SENTINEL GET-MASTER-ADDR-BY-NAME`**: Returns the port and address of a master Redis instance. *(since: 2.8.4)*
- **`SENTINEL SENTINELS`**: Returns a list of Sentinel instances. *(since: 2.8.4)*
- **`SENTINEL CKQUORUM`**: Checks for a Redis Sentinel quorum. *(since: 2.8.4)*
- **`SENTINEL SLAVES`**: Returns a list of the monitored replicas. *(since: 2.8.0)*
- **`SENTINEL`**: A container for Redis Sentinel cmds. *(since: 2.8.4)*

## ARRAY 模块 (18 指令)
- **`AROP`**: Performs aggregate operations on array elements in a range. *(since: 8.8.0)*
- **`ARMGET`**: Gets values at multiple indices in an array. *(since: 8.8.0)*
- **`ARSEEK`**: Sets the ARINSERT / ARRING cursor to a specific index. *(since: 8.8.0)*
- **`ARCOUNT`**: Returns the number of non-empty elements in an array. *(since: 8.8.0)*
- **`ARMSET`**: Sets multiple index-value pairs in an array. *(since: 8.8.0)*
- **`ARDEL`**: Deletes elements at the specified indices in an array. *(since: 8.8.0)*
- **`ARNEXT`**: Returns the next index ARINSERT would use. *(since: 8.8.0)*
- **`ARGREP`**: Searches array elements in a range using textual predicates. *(since: 8.8.0)*
- **`ARLASTITEMS`**: Returns the most recently inserted elements. *(since: 8.8.0)*
- **`ARRING`**: Inserts values into a ring buffer of specified size, wrapping and truncating as needed. *(since: 8.8.0)*
- **`ARINFO`**: Returns metadata about an array. *(since: 8.8.0)*
- **`ARGETRANGE`**: Gets values in a range of indices. *(since: 8.8.0)*
- **`ARINSERT`**: Inserts one or more values at consecutive indices. *(since: 8.8.0)*
- **`ARGET`**: Gets the value at an index in an array. *(since: 8.8.0)*
- **`ARSET`**: Sets one or more contiguous values starting at an index in an array. *(since: 8.8.0)*
- **`ARSCAN`**: Iterates existing elements in a range, returning index-value pairs. *(since: 8.8.0)*
- **`ARDELRANGE`**: Deletes elements in one or more ranges. *(since: 8.8.0)*
- **`ARLEN`**: Returns the length of an array (max index + 1). *(since: 8.8.0)*

## HASH 模块 (3 指令)
- **`HIMPORT PREPARE`**: Defines a session-local fieldset that maps a name to a sorted set of field names. *(since: 8.10.0)*
- **`HIMPORT DISCARDALL`**: Removes all session-local fieldsets for the connection. *(since: 8.10.0)*
- **`HIMPORT`**: A container for session-based hash import cmds using fieldsets. *(since: 8.10.0)*

## GENERIC 模块 (2 指令)
- **`MIGRATE`**: Atomically transfers a key from one Redis instance to another. *(since: 2.6.0)*
- **`WAITAOF`**: Blocks until all of the preceding write cmds sent by the connection are written to the append-only file of the master and/or replicas. *(since: 7.2.0)*

## CONNECTION 模块 (1 指令)
- **`CLIENT CACHING`**: Instructs the server whether to track the keys in the next request. *(since: 6.0.0)*

## STREAM 模块 (1 指令)
- **`XCFGSET`**: Sets the IDMP configuration parameters for a stream. *(since: 8.6.0)*

## HYPERLOGLOG 模块 (1 指令)
- **`PFDEBUG`**: Internal cmds for debugging HyperLogLog values. *(since: 2.8.9)*

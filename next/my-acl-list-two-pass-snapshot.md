优先级：低

问题
ACL LIST 与 ACL USERS 两遍扫描无快照，并发增删时应答数组头与实际元素数漂移破坏客户端协议解析。第一遍全 hlog 阻塞扫描仅计数并写数组长度，第二遍重新全扫描逐条写正文；两遍之间同命名空间并发 SETUSER/DELUSER 即长度与条数背离。代码注释自称「与 C# 侧同一非原子窗口口径一致」，实际 C# 是 GetUserHandles() 单快照先写 Count 再遍历同一快照，窗口仅 ConcurrentDictionary 弱一致枚举（微秒级）；rust 窗口是两次全日志阻塞扫描（大日志可达秒级），宽度远超 C#，等价性主张不成立。

取证（dev 当下代码重取）
wedb/wnode/src/resp/acl_commands.rs:112-155 network_acl_list（第一遍 :118-138 计数 + 写长度 :146，第二遍 :151 起重扫直写）；:151-155 注释自述非原子窗口与 C# 对齐。:178-227 network_acl_users 同型。扫描内核 wedb/wnode/src/resp/acl_store.rs:122-152 for_each_user（全 hlog scan + 链首地址校验去重）。

C# 对标
garnet/libs/server/Resp/ACLCommands.cs:54-77 NetworkAclList（:66 GetUserHandles() 取快照后 :67 写 Count、:70 遍历同一快照）；:83-106 NetworkAclUsers 同。

修法建议
第一遍在栈上或小 Vec 收集用户名快照（ACL 用户量级小，与零大字典设计不冲突），或单遍收集后统一整形输出，保证数组头与元素数强一致；default 兜底单例（in_memory_default）与解码失败关闭语义保持。来源 next/agy.my.md 条 14 与 next/muse.my.md 条 14 后半（前半 SETUSER 活连接传播已由 next/acl-setuser-live-connection-propagation.md 承接）合并处理。

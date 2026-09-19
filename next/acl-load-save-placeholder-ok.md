优先级：中
分拣注记（qw.my 第 11 轮条 1 拆出；浅核 2026-09-19：network_acl_load acl_commands.rs:440/:450、network_acl_save :457/:467 各 write_raw RESP_OK 在场（行号小漂移）；台账无同题票，ing/acl-setuser-live-connection-propagation.md 仅顺带提及 ACL SAVE 非本题；与 SKILL 一致无冲突）

ACL LOAD / ACL SAVE 是回 +OK 的虚设实现，删掉文件层后没换成「不适用」应答
问题：wnode/src/resp/acl_commands.rs:446 network_acl_load 与 :463 network_acl_save 在
:455 / :472 各自 write_raw(output, cs::RESP_OK) 收尾，函数体无任何装载/落盘动作，模块注释
:443 / :460 自述「内存形态兼容应答，未接存储落盘」；两臂由同文件 :726 / :727 挂在
RespCommand::AclLoad / AclSave 上，即客户端真能打到。我方按 SKILL:34 把 ACL 改为
KeyTag::Acl (0x0D) 入存储、SETUSER/DELUSER 同步写穿（wnode/src/resp/acl_store.rs:122 write 与
:143 delete 是唯一出口），于是 SAVE 无事可做、LOAD 无从重载，但代码仍保留了「做了」的应答：
运维据此以为权限已落盘或已从外部文件重载，属声明与实现相反。
C#：garnet/libs/server/Resp/ACLCommands.cs:321 NetworkAclLoad / :362 NetworkAclSave 先过
同文件 :30 ValidateACLFileUse，未配 acl-file 即回 RESP_ERR_ACL_AUTH_FILE_DISABLED
（garnet/libs/server/Resp/CmdStrings.cs:300）并直接返回，配了才真调
AccessControlList.Load/Save，异常走 TryWriteError；即 C# 从不为「什么都没做」回 +OK。
rust 侧该错误常量整体未移植（wresp/src/cmd_strings.rs grep ACL_AUTH_FILE_DISABLED 零命中）。
修法：两函数改走 cs::write_error_raw 回一条「ACL 直接持久化于存储，SAVE/LOAD 不适用」错误帧
（形态对位 C# FILE_DISABLED），或从命令目录与 ACL 子命令表整体摘除并在
js/check/ignore/garnet/libs/server/Resp/ACLCommands.cs.yml 登记；不得维持伪 +OK。
条款：SKILL.md:79（严禁占位函数或虚设实现）、SKILL.md:34（ACL 存储化）、SKILL.md:12（不需要旧版
形态，直接删除相关代码）。

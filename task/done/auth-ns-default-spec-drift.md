认证与 ACL 管理命令 `#` 缺省命名空间：规范文档措辞与实现对表（纯文档修订）

来源：glm.my 第 1 条（分拣判定成立）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状（声明与实现的口径差）
- 声明侧：/Users/z/git/db/wedb/doc/zh/db.md:227（2.2 节）「无 `#` 字符（如 alice、default）：默认命名空间为 0」，
  /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:33 同款措辞「认证格式 `<ns>#用户名`（无 `#` 默认 ns 0）」。
- 实现侧：缺省一律取会话当前绑定租户，而非硬 0。
  - AUTH 存储点查 /Users/z/git/db/wedb/wedb/wnode/src/resp/acl_commands.rs:581
    `parse_user_namespace_with_default(&uname, self.namespace)`；
  - ACL SETUSER 同文件 :227、ACL DELUSER :335、ACL GETUSER :495 均以 `ctx.caller_namespace` 为缺省；
  - 解析口 /Users/z/git/db/wedb/wedb/wacl/src/user.rs:60 parse_user_namespace_with_default；
    硬 0 版是同文件 :53 parse_user_namespace，仅内存 default 路径
    /Users/z/git/db/wedb/wedb/wacl/src/auth/garnet_acl_authenticator.rs:53 使用（只服务 default 用户，不受影响）。
- 实现形态是 3.5 节的必然推论而非偏离：/Users/z/git/db/wedb/doc/zh/db.md:282 规定 namespace != 0 的连接
  严禁携带 `#`、仅允许管理所属本地 Namespace 的用户；代码门禁在 acl_commands.rs:234、:342、:502
  （`caller_namespace != 0 && (target_ns != caller_namespace || raw_name.contains('#'))` →
  RESP_ERR_ACL_FOREIGN_NAMESPACE）。缺省若为硬 0，非 ns0 连接既不能写裸名（会指到 ns 0）也不能写 `1#bob`
  （被门禁拒），本租户用户无从指名。首次认证时两会话同为 ns 0，恰好掩盖了这处措辞漂移。

判定
纯文档对表项，零代码触点：实现正确且唯一自洽，规范措辞未随架构收窄。先例为 spec-doc-drift-gcbarrier-ri-promote
票（同族「文档声明与已落地机制漂移」，含 SKILL.md 括注订正）。

修法
1. /Users/z/git/db/wedb/doc/zh/db.md:227 该行改为「无 `#` 时缺省为会话当前绑定的命名空间（未认证会话即 ns 0）」，
   并加一句与 3.5 门禁的闭环说明（非 ns0 连接不得携带 `#`，故缺省必为当前租户）。
2. /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:33 同句同步收窄（同一口径，禁两处各写一套）。
3. 不改 acl_commands.rs / wacl 任何代码；不改 garnet_acl_authenticator 内存路径。

C# 参考
- 无对位：/Users/z/git/db/wedb/garnet/libs/server/Resp/ACLCommands.cs 全文无 namespace 概念，
  多租户命名空间为 wedb 自有架构（SKILL.md:31-36 条），本票只对齐本仓规范文档。

优先级
打磨（规范文档与实现措辞漂移，误导后续按「硬 0」检索与改动）。

验收
- grep db.md 与 SKILL.md 无「无 `#` 默认 ns 0」硬口径残留。
- db.md 2.2 / 2.2 会话绑定段 / 3.5 三处与 acl_commands.rs:234、:342、:502、:581 口径互检一致。
- 零代码 diff（git diff 只含 doc/ 与 SKILL.md）。

细化方案（f38 实施版，甄别核实后追加）
- 行号勘误（相对票据取证基线有漂移，语义不变）：
  db.md 实际 230 行；acl_commands.rs 实际 :266/:273（SETUSER+门禁）、:374/:381（DELUSER）、
  :534/:541（GETUSER）、:620（AUTH 存储点）、:686（SETUSER 自改刷新预判，同以 self.namespace 缺省）；
  garnet_acl_authenticator.rs 实际 :57（硬 0 版唯一调用，其后 target_ns != 0 直接 false，只服务 default 用户）。
- 甄别结论：成立。非 ns0 连接裸名（缺省 caller_namespace→本租户，门禁放行）、显式 `1#bob` 被门禁拒；
  若缺省为硬 0 则非 ns0 连接裸名也被门禁拒（target_ns=0 != caller_namespace），本租户用户无从指名，
  与 3.5 门禁矛盾。实现自洽，文档措辞漂移。C# ACLCommands.cs 仅含 `namespace Garnet.server` 关键字，
  无多租户概念，无对位。
- 改动清单（零代码触点）：
  1. doc/zh/db.md 2.2 节「无 `#` 字符（如 alice、default）：默认命名空间为 0」改为缺省取会话当前绑定
     命名空间（未认证即 ns 0），并补一句与 3.5 门禁的闭环说明（非 ns0 连接不得携带 `#`，裸名必落当前租户）。
  2. .agents/skills/transpile/SKILL.md:33 括注同步收窄为同一口径。
- 验收：grep 无硬口径残留；零代码 diff；不跑 test.sh（未改代码）。

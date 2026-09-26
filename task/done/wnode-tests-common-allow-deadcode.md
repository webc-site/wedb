甄别结论：通过（甄别席 zc-fix-r16-testsallow，2026-09-26）定级 P2
核验记录：
1 wedb/wnode/tests/common/mod.rs:10 #![allow(dead_code)] 亲验成立；实跑 ./allow.check.sh 全仓唯一命中该行、exit 1，门禁红灯在位属实。
2 四函数锚亲验成立：:28 test_server_tls、:34 server_tls_config、:49 start_tls_server、:63 handshake_refused；订正一（票面漏计第四函数）经 server_mtls.rs:23/76/80/136/186 五处消费核实必要。
3 五消费点锚亲验成立：client_laddr.rs:28-29、handshake_timeout.rs:31-32、push.rs:33-34、server_lifecycle.rs:27-28、server_mtls.rs:22-23；server_cert_reload.rs 确无 mod common，名单封闭。
4 依赖底座亲验：wnode/Cargo.toml:85 dev-dependencies 含 wnode_tls_test 成立；wnode_tls_test/Cargo.toml:17 已依赖 wnode(features=["tls"])、[dependencies] 确未列 wtls，订正二（补 wtls.workspace = true）成立；dev 循环为 cargo 合法形态且现存。
5 仓法锚亲验：rust_review/SKILL.md 第 11 条「禁用 allow 绕过」在位；allow.check.sh 头注自陈全仓检测无 tests/ 豁免；deviations.md §146 系扩 #[expect] 同禁登记，恰证无豁免条目。
6 非重复：deviations.md 无本文件豁免；task/todo、ing、reject、issue 各池无同轴票；task/done 归档与 review_history 仅为历史记录非在册覆盖。现状在现码仍存在。
7 N.A. 锚先例 task/issue/process-commit-discipline.md 在位，工程纯洁面无 C# 对位属实。方案与「禁写 allow」规范同向无冲突；执行票若跑 ./sh/clippy.sh 应归主代理门禁段执行。

审核结论：通过（审核席 zcode-r15-review-deadcode，双侧亲验）

审核确证记录：
1 红灯亲验。实跑 ./allow.check.sh 报 ERROR 且 exit 1，rg 全仓唯一命中即 wedb/wnode/tests/common/mod.rs:10，门禁红灯在位属实，非幻觉。
2 消费面亲验。五个 mod common 声明点核实无误：client_laddr.rs:28-29、handshake_timeout.rs:31-32、push.rs:33-34（三者仅用 test_server_tls）、server_lifecycle.rs:27-28（用 start_tls_server + test_server_tls）、server_mtls.rs:22-23（用 handshake_refused + server_tls_config + start_tls_server）。server_cert_reload.rs 确无 mod common，五文件名单封闭。
3 偏差册查重。doc/zh/deviations.md 无本文件豁免条目；§146 系 allow.check.sh 扩 #[expect] 检测面的登记，恰证无 tests/ 豁免，与本票不重复。
4 架构亲验。wnode_tls_test 已依赖 wnode（features=["tls"]）、compio、compio-tls、aok，wnode 的 dev-dependencies 已含 wnode_tls_test（wedb/wnode/Cargo.toml:85），dev 循环为 cargo 合法形态且现存，迁移不引入新循环；测试夹具面，数据面零开销无关。
5 票面两处疏漏，执行方案已订正（见下）。

票面勘误（不构成拒绝，执行时必须按订正方案走）：
其一，模块实含四个函数而非三个：票面漏计 :63 handshake_refused（被 server_mtls.rs:23/76/80/136/186 五处消费）。若照原方案「三函数迁移」执行，server_mtls.rs 将编译断链。
其二，wnode_tls_test 的 [dependencies] 现未列 wtls，而 common/mod.rs 依赖 wtls::ServerTlsConfig；票面「现有依赖底座」在 wtls 一项不成立，需补 wtls.workspace = true 一行（wtls 为 workspace 成员，wnode 对其 optional 门控于 feature "tls"，wnode_tls_test 的 wnode 依赖已开该 feature，直接补列即可）。

问题分析：
1 Garnet 契约对齐。本票为工程纯洁面（板块1「零死代码与假桩清退」与审查红线5「用 allow 压制警告」），无 C# 行为契约对位；流程先例见 task/issue/process-commit-discipline.md（纯流程票 N.A. 锚先例）。仓法依据三点：其一 .agents/skills/rust_review/SKILL.md 第 11 条「禁用 allow 绕过，按 rust 最佳实践改写代码」；其二 allow.check.sh 头注自陈检测全仓 Rust 源码 `\[allow` 形态且无 tests/ 豁免面；其三 doc/zh/deviations.md §146 明文将该检测面扩至 #[expect] 同禁、未开任何目录豁免。
2 工程现状确证。wedb/wnode/tests/common/mod.rs:10 `#![allow(dead_code)] // 各测试二进制仅引用其中子集`，模块含三个函数（:28 test_server_tls、:34 server_tls_config、:49 start_tls_server，共 73 行），被 wnode/tests/ 下 client_laddr.rs、handshake_timeout.rs、push.rs、server_lifecycle.rs、server_mtls.rs 五个测试二进制以 `mod common;` 各自独立编译引用；因各二进制仅消费子集（client_laddr/handshake_timeout/push 仅用 test_server_tls），未引用函数逐二进制触发 dead_code 告警，遂以模块级 allow 全量压制。实测 ./allow.check.sh 顶层门禁脚本现报 ERROR（rg 命中该行，exit 1），门禁红灯在位。而 wnode 的 [dev-dependencies] 已含 wnode_tls_test（wedb/wnode/Cargo.toml dev-dependencies 段），且五测试文件已大量 `use wnode_tls_test::{...}`（如 handshake_timeout.rs:29、server_cert_reload.rs:19），夹具单源通道现成未用足。
3 逻辑危害确证。其一，红线5 直接命中：以注释申辩 + allow 属性绕过编译器死代码检测，使该模块后续新增孤儿函数静默免检。其二，门禁脚本报红意味着 allow.check.sh 巡检结果失真：要么该脚本被长期忽视（检测面空转），要么任何跑它的席位都会撞上存量红灯污染增量判定。其三，测试装配逻辑（服务端 TLS 配置构造 + GarnetServer 装配）与 wnode_tls_test 既有夹具（自签证书、wait_entries、test_connector）分居两处，同一 TLS 测试基础设施双入口，违背「同类功能只保留一套」与夹具单源惯例。

涉及代码：
rust 文件与函数：
wedb/wnode/tests/common/mod.rs:模块级 #![allow(dead_code)] 与 test_server_tls / server_tls_config / start_tls_server
wedb/wnode/tests/server_lifecycle.rs:28 与 server_mtls.rs:23:start_tls_server / server_tls_config 消费点
wedb/wnode/Cargo.toml:dev-dependencies（wnode_tls_test 在位）

对应 c# 文件与函数：
N.A.（工程纯洁面无 C# 原型对位；流程票 N.A. 锚先例 task/issue/process-commit-discipline.md）

精炼执行方案（审核订正版，共四函数全量迁移）：
1 wedb/wnode_tls_test/Cargo.toml [dependencies] 补 wtls.workspace = true（规范要求依赖用 cargo add 添加，等价一步）。
2 将 wedb/wnode/tests/common/mod.rs 四个函数整体迁入 wedb/wnode_tls_test/src/lib.rs（:28 test_server_tls、:34 server_tls_config、:49 start_tls_server、:63 handshake_refused），保持函数名与签名不变；证书底座 test_cert_der/test_key_der 即同文件在位项，迁移后 use 路径同文件直取。
3 五个测试文件删除 `mod common;` 声明与 `use common::xxx` 行，改为并入既有 `use wnode_tls_test::{...}` 导入（client_laddr.rs:28-29、handshake_timeout.rs:31-32、push.rs:33-34、server_lifecycle.rs:27-28、server_mtls.rs:22-23）；随后删除 wedb/wnode/tests/common/ 目录整体。
4 验证点：全仓 rg "(\[allow|#!?\[expect)" -t rust 零命中；./allow.check.sh 输出「allow 检测通过」exit 0；wnode 五个 TLS 测试二进制 cargo nextest 定向全绿；./sh/clippy.sh 无新告警。

合入哈希：c8314a1（ff 合并）收口形态：tests/common 四 TLS 夹具整体迁入 wnode_tls_test 单源，五测试文件改用既有导入，allow.check.sh 归零，产品码零改动。

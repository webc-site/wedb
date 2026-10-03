甄别结论：通过（甄别席 J7，2026-09-27，定级 P2——宽档 t/p 穿门静默面，补门侧窄闸）。C# 正则 :387 仅 k/m/g 亲验；rust validate aof 三面 :1335-1345、lua :1403-1417、aof_settings parse_knob_size :152-159、check_pow2_size :1062-1076 全走宽档收，size.rs :66-69 自陈漏计旗标闸，亲验坐实；1t/2t 穿门 2TiB 装配与 4t 截断守护失效为静默面非 fail-fast，故 P2；补门侧窄闸不动折算核，与 aof-size-limit 票同字段异面不并案。派沙箱席 c01o。

审核结论：通过（锚点亲验：C# OptionsValidators.cs:385-413 正则仅 k/m/g，T/P 旗标面即拒；rust 六臂单层穿门属实，aof_settings 组合校验使 1t/1t/2t 三档全穿；deviations 与五池零同题。订正已并入：去除 Markdown 加粗与标题前缀、删除假锚 §359、check_pow2_size 现行号 :1062-1076）

尺寸旗标启动门缺 C# [MemorySizeValidation] 正则档（t/p 后缀）：--aof-* 与 --aof-size-limit 接受 C# CLI 一律拒发的 1t/1p 档，fail-fast 门退化为荒谬巨值放行

问题分析：
1. Garnet 契约对齐（C# 原型文法分层）
C# 尺寸面实为两套文法两层闸：① 内部折算核 `GarnetServerOptions.ParseSize`/`TryParseSize`（garnet/libs/server/Servers/ServerOptions.cs:220-263）后缀表 k/m/g/t/p（1024^1..5）；② 旗标启动门 `MemorySizeValidationAttribute`（garnet/libs/host/Configuration/OptionsValidators.cs:385-413）正则 `^\d+([KkMmGg][Bb]?)?$`（:387）——仅 k/m/g 三档，T/P 在 CLI 面一律拒，不符即启动报错 "Expected string in memory size format (e.g. 1k, 1kb, 10m, 10mb, 50g, 50gb etc)"。挂该闸的在物旗标：`--aof-memory`/`--aof-page-size`/`--aof-segment-size`（Options.cs:211/215/219）、`--aof-size-limit`（:255）、`--index-max-size`（:86）、`--lua-script-memory-limit`（:644）等。CONFIG SET 运行面（ServerConfig.cs:220/265）则直走 TryParseSize 宽档——C# 自身「旗标严、运行宽」两档分层系一手设计。
2. 工程现状确证（rust 单层化后文法漂移）
rust 侧六字段解析入口已汇一单点（wconf/src/size.rs `parse_size_bytes` :80-108 对位 C# 折算核逐字等义：同早返单后缀形、同 `b` 尾容忍、同 ASCII 大小写折叠、MUL_K..MUL_P=1024^1..5 对 `Math.Pow(1024,s+1)` 逐档核对恒等；wrapping 溢出同 C# unchecked 回绕），node_options.rs（aof 三面 :1335-1345、aof-size-limit :1314、index-max-size :1322、lua :1403-1417）、aof_settings.rs `parse_knob_size` :152-159、config_commands.rs :417/:456（对位 C# CONFIG SET 同宽，非案）全部同走此口——无第二套解析，收单点属实。但 size.rs :66-69 自陈「C# 两处同体实现…同走此口」漏计第三套：旗标闸正则。后果即启动门只设折算核宽档，t/p 后缀穿门而过：`--aof-page-size 1t --aof-memory 2t` 三面体检全过（页 2^40 高于下限、内存恰等两倍界内、段默认 1g≥页? 注：页>段即拒，配 `--aof-segment-size 1t` 三 t 俱进）→ 装配期按 2TiB 窗口巨量分配；`--aof-size-limit 4t` 穿 check_pow2_size [1,i64::MAX] 闸成 4TiB 截断线（等效永不截断）。C# 同输入在 CLI 闸即报错拒起。index-max-size/lua 两旋钮 t/p 档虽被下游值域闸拦（index [64,1<<37]、lua [1K,2GB]），错误面亦从「格式不符」漂移为「越界」类，属同根文法缺档的次生面。
3. 逻辑危害确证
启动臂退化面：C# fail-fast 配置错误 → rust 静默接受后进程级 OOM/崩溃或守护语义静默失效（aof-size-limit 巨值=截断守护名存实亡）。宽严方向单向（rust ⊃ C#），既有合法配置零回改影响；属门缺档非核错档。
已核验非案（勿重开）：内核 1:1（含 ""→0、"-x" 全拒、wrapping 溢出同 C#，§32/convert.rs 在途勿碰）；回显面 CONFIG GET aof-size-limit/aof-* 原样字符串（runtime_server_config.rs:86-88/67-75 对位 C# RuntimeServerConfig.cs:153-154 SetReadOnly 同形）；空串哨兵三门面已由 todo/wconf-aof-size-limit-empty-string-boot-guard-sentinel-divergence 在册；buffer-pool/pagecount/reviv 等旋钮缺席系 §111/§69/§70/§77 已裁；负值回绕臂各消费点 range/页下限全拒（check_pow2_size :1062-1076、validated_page_size_bits）。五池 grep `MemorySizeValidation|1t|t/p` 零同题。

涉及代码：
rust 文件与函数：
wedb/wconf/src/size.rs:parse_size_bytes/try_parse_size（:80-125，:66-69 单点自陈注）
wedb/wconf/src/node_options.rs:validate 尺寸臂（:1314-1345、:1403-1417）、check_pow2_size（:1062-1076）
wedb/wnode/src/aof/aof_settings.rs:parse_knob_size/knob_bits/page_knob_bits（:134-159）

对应 c# 文件与函数：
garnet/libs/host/Configuration/OptionsValidators.cs:MemorySizeValidationAttribute（:385-413，正则 :387）
garnet/libs/host/Configuration/Options.cs:211/215/219/255/86/644（挂闸旗标清单）
garnet/libs/server/Servers/ServerOptions.cs:ParseSize/TryParseSize（:220-263，宽档内核，rust 已对位勿动）

精炼执行方案：
1 补旗标层窄闸，不动折算核：wconf 增设 `is_flag_size_str`（对位 C# 正则：全 ASCII 数字 + 至多一枚 k/m/g(+可选 b) 后缀、大小写不敏感、无空格），validate 六尺寸臂（aof 三面对位 Options.cs:211-219 挂默认闸、aof-size-limit/index-max-size/lua 对位挂 isRequired:false 闸——空串豁免沿用现 filter）以窄闸判格式，穿门 t/p 档即 InvalidSizeStr 拒启；折算仍单点走 try_parse_size，禁第二套解析。
2 CONFIG SET mainlog/memory-size、index 两臂维持宽档（C# ServerConfig 即 TryParseSize），不收口、勿顺手改。
3 size.rs :66-69 注释订正：C# 系「两内核 + 一旗标闸」三层，rust 收敛内核 + 补门侧窄闸方为全对位。
4 测试验证点：validate 面锁 `--aof-page-size 1t`、`--aof-size-limit 4t`、`--aof-memory 2t` 拒启（错误点名旗标与格式示例），`128m/1g/1GB/64kb` 全档绿；`"1 GB"`/`"-1k"`/`"x16"` 两文法同拒回归；CONFIG SET `1t` 宽档绿测锁不回退；aof_size_limit="" 哨兵臂按在途票口径不动。

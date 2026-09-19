错误帧清洗下沉到 write_error_bytes 单点：wlua/wresp 六处裸写仍可被脚本文本注入帧

来源：error-frame-sanitize-bypass-trio 落地后该代理的遗留建议（票已归档
task/done/error-frame-sanitize-bypass-trio.md，载荷 899c767b，归档 da86e8e8）。
取证基线：主仓 /Users/z/git/db/wedb，分支 dev，HEAD 3146533e 之前的当前 tip
（本档全部行号按 dev 当下代码复核）。

结论

上一轮把三处输入回显（INFO 段名、LATENCY 池名、EVAL 脚本错误回显）先过
sanitize_error_str 再成帧，但那是在调用侧打补丁。成帧机制本身仍是两套：
/Users/z/git/db/wedb/wedb/wresp/src/resp_memory_writer.rs:691
`write_error_bytes(&[u8])` 是裸写 `-<bytes>\r\n`，不做任何 CRLF 处理；
:701 `write_error(&str)` 与 :707 `write_error_with_prefix` 才在内部调
ext.rs:35 `sanitize_error_str`。于是所有以字节切片入参写错误应答的站点重新成为
旁路，其中输入是客户端可控的：

- /Users/z/git/db/wedb/wedb/wlua/src/runner/resp_convert.rs:155（TryWriteSingleItem
  出错分支，err_buff 来自 Lua 栈上的错误对象文本）
- 同文件 :482 `try_write_error`（LuaRunner.cs:TryWriteError 对应面，同样是栈上错误文本）
- 同文件 :533 `resp_out_error`（compile 路径直接写 msg）
- /Users/z/git/db/wedb/wedb/wlua/src/commands.rs:422 `abort_with_error_message`
  （message 为 &[u8]，:410 的调用点把命令名拼进模板后裸写）
- /Users/z/git/db/wedb/wedb/wlua/src/runner/executor.rs:133（err 入参原样成帧）

`error("x\r\ny")` 这一类脚本可控文本即可以换行提前结束错误帧，后续字节被客户端
解析成新应答，属响应缓存/帧注入面，与上一轮修的是同一类缺陷、不同层级。

修法

一套机制：把清洗下沉到唯一的裸成帧口 `write_error_bytes`，让它先按字节做
CRLF→空格替换 + MAX_ERROR_MSG_LEN 截断再写 `-`/`\r\n`；`write_error` 与
`write_error_with_prefix` 改为复用同一下沉后的入口，去掉各自重复的
sanitize 预处理，避免出现「清洗函数两套」。utf-8 截断按字节边界处理即可，
错误应答本身是行分隔帧，不要求客户端可解码为合法 utf-8；若为复用
sanitize_error_str 而必须 `from_utf8_lossy`，则须在注释锚点里写明这层转换的
取舍，不许静默改语义。

要求

1. 先对照 C# 定语义：/Users/z/git/db/wedb/garnet/libs/common/RespMemoryWriter.cs
   的 WriteError / WriteDirect 家族，以及 Lua 错误写出
   libs/server/Lua/ 侧（RespWriteError 系）是否清洗。若 C# 明确不清洗任何
   错误文本，则本票退化为「只在 wlua 五个站点前加清洗」并说明为何 rust 需要；
   不许为规避而引入可选开关。
2. 收敛后全仓 grep 判据：`write_error_bytes(` 的调用点行为单源可控，wresp 内
   除 write_error/write_error_with_prefix 外不得再有第二处自拼 `-`+`\r\n`
   的错误成帧（`write_error_raw` 若为独立实现须并入同点）。
3. 回归：至少一条 EVAL/lua 脚本内 `error()` 文本含 CRLF 的用例，断言输出帧数
   与清洗后字节逐字节一致；命令名回显面复用上一轮 info/latency 的断言口径。
4. 不改 wlua 的错误文案本身，不新增第二套清洗函数，不动 resp_server_session.rs
   与 garnet_api（他票在跑）。

门禁：子代理只运行 `cargo check --workspace --all-targets`（在
/Users/z/git/db/wedb/wedb 这一层，exit 0 且 0 warning）与 `bun js/check.js`
（报告前后逐字节相同）。不运行 ./test.sh、./sh/clippy.sh。

实际落地事实（dev-errframesan，载荷 6f7833fa，merge a13925cb）

一、C# 取证（要求 1 的裁决）

garnet 确无任何错误文本清洗层：libs/common/RespMemoryWriter.cs:210 与 :220 两个
WriteError 重载（byte 与 char 两型）全部直落 libs/common/RespWriteUtils.cs:228/:245/
:266 的 TryWriteError，三型均为 `*curr++ = '-'` + `CopyTo` + newline 的裸拷贝；
Lua 侧 libs/server/Lua/LuaRunner.cs:1657（TryWriteSingleItem 出错）、:2520
（TryWriteError）、:1525（pcall 已带 ERR 前缀直接放行）与 :1536-1542（pcall Lua
错误，TryWriteError 前缀 + TryWriteError(errBuf) + TryWriteDirect("\r\n") 三段裸写）
同样原样成帧。全仓 `grep -rn Sanitize libs/ main/` 只命中向量面的
SanitizeAndTrackIngestedRecordIfApplicable（与错误帧无关）；C# 靠 9 处 XML 注释
“The string mustn't contain a CR (\r) or LF (\n) bytes” 把责任推给调用方。

据此不采「退化为调用侧清洗」分支：调用侧打补丁正是本票要终结的层级（上一轮
899c767b 在 executor.rs 落的 `from_utf8_lossy` + `write_error` 即该形态，且 lossy
会把脚本原始错误字节改写成 U+FFFD，属票面禁止的静默改语义）。rust 需要本层的理由
沿上一轮取证结论：sanitize_error_str 是我方输出门面自立的机制，字节切片入参口即该
机制的旁路断链，收口方式只能是清洗与成帧同点。故落地为唯一裸成帧口下沉，未引入
任何开关。

二、改到的单点

1. wedb/wresp/src/ext.rs:44 新增字节域净化单点 `sanitize_error_bytes`（首个 CR/LF
   处切断 + max_len 按字节边界帽），:56 `sanitize_error_str` 改为其 `&str` 门面
   （同一套清洗，只额外把长度帽回退到 UTF-8 字符边界，既有 &str 调用点字节不变），
   清洗函数仍只有一套。
2. wedb/wresp/src/resp_memory_writer.rs:103 新增 `write_error_frame_to`（wresp 内唯一
   自拼 `-` 与 `\r\n` 的错误成帧实现，内部先净化），四处入口并入：
   `write_error_bytes`（:721）、`write_error`（:727，去掉 sanitize 预处理）、
   `write_error_with_prefix`（:735，去掉重复 sanitize）、`Resp2::write_bulk_error`
   （:228，RESP2 下 bulk error 即简单错误帧，原为独立裸成帧点，票面未列但属同判据）。
   `Resp3::write_bulk_error`（:311）为 `!<len>\r\n<正文>\r\n` 长度前缀帧，正文内 CRLF
   不构成第二帧，保持原样并在门面注释里写明。
3. 票面六站点的取证与落地：wlua 侧不再有任何 `sanitize_error*` 调用，清洗全部由成帧点
   承担 —— resp_convert.rs:157（TryWriteSingleItem 出错分支，实为
   `ConstantStrings::*` 常量文案）、:487 try_write_error（`return {err=...}` 面，
   脚本可控文本，本票真正的注入面）、:541 resp_out_error（compile 面）、
   commands.rs:425 abort_with_error_message（:413 命令名模板）、
   executor.rs:135 run_for_session（preamble `Err` 实为 `&'static [u8]` 常量，
   票面「err 入参原样成帧」按落地后代码订正）、executor.rs:419-436 run_common
   （删 `from_utf8_lossy` 预处理，两分支改走字节入参口 :425/:433，文案一字未改，
   仅长度帽由字符边界改为字节边界）。
4. 票面「CRLF→空格替换」按要求 4「不新增第二套清洗函数」落地为既有的 CRLF 切断语义
   （与上一轮 info/latency 回归口径一致），未另立替换式。

三、grep 判据（落地后复核）

- `grep -rn "push(b'-')" wedb/wresp/src` 只剩 2 处：resp_memory_writer.rs:106（唯一
  成帧点）与 cluster_cmd_strings.rs:95（残留，见下）。
- `grep -rn "write_error_bytes(" wedb/` 共 55 处命中：定义 1（:721）+ 门面内部调用 1
  （:728）+ 调用点 53（wcol 36、wlua 12、wresp/tests 5），全部经单点清洗；调用侧净化
  调用（`grep -rn "sanitize_error" wedb/wlua`）为 0。
- 残留（越界事实，本票不吞）：wresp 内仍有 6 处 `-ERR …` 模板裸写 ——
  cmd_strings.rs:387/:406/:416/:432（客户可控段先过 sanitize_error_str 的
  MAX_PARAM_NAME_LEN=128 帽，模板字面量无 CRLF）与 cluster_cmd_strings.rs:65/:77
  （纯数值），另加 cluster_cmd_strings.rs:95 write_redirect_error（MOVED/ASK，入参为
  常量 kind + slot 数值 + 集群配置面 endpoint，调用点
  wedb/src/server/slot_verify.rs:79/:83，非命令输入）。这类「前缀+中缀变量+后缀」
  模板要并入单点需另立第二组成帧签名，与本票「一处清洗、不新增第二套」冲突，故留作
  后续票。

四、回归（要求 3）

- wedb/wnode/tests/lua_script_tests.rs:280 lua_error_crlf_cannot_inject_frame 扩为两面
  逐字节断言：`error('boom\r\n:4242\r\n')` → `-ERR Lua encountered an error:
  user_script:1: boom\r\n`、`return {err='boom\r\n:4242\r\n'}` → `-boom\r\n`，各断言
  `\n` 计数为 1（沿用上一轮 info_invalid_section_crlf_cannot_inject_frame 的口径）。
- wedb/wresp/tests/writer.rs:273 test_error_frame_single_point_sanitization：字节入参
  口 CRLF/LF 切断、非 UTF-8 字节原样保留（不 lossy）、`&str`/前缀/RESP2 bulk error
  同源、512 字节帽后帧长恒为 1+512+2 且单帧、RESP3 bulk error 保持长度前缀原样。
- wedb/wresp/src/ext.rs:225 sanitize_error_bytes_and_str_share_one_mechanism：核心与
  门面同源、门面的字符边界回退。

五、门禁与合入

`cargo check --workspace --all-targets` 在合入前最后一次 merge dev 之后重跑：exit 0、
0 warning（/tmp/fork/check6.txt）。`bun js/check.js` 以「dev-only 树 vs dev+载荷树」
对照跑两次，报告逐字节相同（diff 为空，exit 0），check.js 顺带改写的
js/check/ignore/common.yml 系 dev 存量漂移、与本载荷无关，已 `git checkout --` 还原
未提交。附带跑的定向用例（wresp/wlua/wcol 的 lib+writer、wnode 的 lua_script_tests 与
resp_admin）全绿。合入 a13925cb 的 `git diff 65dc01c1 a13925cb --stat` 恰为上述 7 个
文件，无夹带。

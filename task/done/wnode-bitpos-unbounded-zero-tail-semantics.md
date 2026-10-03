终态:闭环(2026-09-29)。甄别 adcd275 → 沙箱 52a2af9 → 并 dev 011206b(测试册冲突双保留)。bit_pos_driver 增 has_end_offset 形参两生产点透传,BYTE 臂唯一出口 search_for==0&&!has_end_offset → input_len*8,:243 锁测翻 24+显式 end 对照锁,naive 神谕加 end_given 维度;金样经主控 redis 8.10.1 亲测(24/24/-1/0/缺键 0)。终验 test.sh 5205/5205+clippy 0。
甄别结论:通过(P2 级,2026-09-29 主控席)。亲验:bit_pos_driver 签名无 has_end_offset、:243 锁测钉 -1 精确命中、快臂解构 has_end_offset 在手未透传、parse 文法 count>4 蕴含 count>3(unit 恒带 end)、BYTE 臂早退保证区间非空与 input_len>0(审核席点 1 成立);本机 redis-server 8.10.1 实测 24/24/-1/0/缺键 0 全吻合。

审核结论：通过（2026-09-29 独立审核席，P2 级协议语义分叉）。rust 缺形逐点亲验（:243 锁测钉住 -1 坐实）、
C# hasEnd 仅入校验不进驱动确证、Redis 标准锚经本机 redis-server 8.10.1 实测逐金样吻合（全 1 无 end 找 0
回 24、仅 start 回 24、显式 end/-1 回 -1、负 start 钳后外延、start 越界无 end 回 -1、空串找 0 回 -1、
找 1 永不外延）+ bitops.c 承重引文 raw 源核验（1726/1753/1851 精确命中）。文法前提（unit 须与 start、end
同现，BIT 臂恒 has_end_offset=true）经 parse 层 643-649 与 Redis 实测双侧确证。
审核席整理四点（执行席遵照）：
1. 承重出口实际条件仅 search_for==0 且 !has_end_offset：「区间非空且 input_len>0」两守卫被驱动既有
   早退（bit_pos.rs:30-36、55-61）恒真保证，留防御性注释即可，不写运行时条件。
2. driver 签名加参后 wbitmap 既有 driver 测试全部补新实参：既有用例 end 全按显式 end 传 true 语义
   原值保持（含 :246/:248），仅 :243 翻为 24；matches_naive_on_random_buffers 神谕同步加 end_given 维度。
3. 落注释行号以 unstable 实测为准：虚拟字节臂 1764-1767、空区间前判 1808-1811、缺键臂 1800-1806
   （票面 1770-1774/1818/1801-1805 微漂，语义不变）。
4. 快臂调用点（bitmap_commands.rs:195-206 解构）已具备 has_end_offset 绑定、慢臂字段直取，透传零阻力；
   生产调用点全仓仅两处（快 223/慢 1064），加形参即闭环。

原票面：
BITPOS 找 0 时「右侧无显式 end 即零尾无限延伸」Redis 标准语义未落地：全 1 值无 end 找 0 本仓回 -1，Redis 回 strlen*8

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。Redis 官方标准（src/bitops.c:bitposCommand，
unstable 与 7.x 同构；redis.io/docs/latest/commands/bitpos 文档原文逐字确认）：
a) 键缺失：找 0 回 0、找 1 回 -1（bitops.c:1801-1805「infinite array of 0 bits」注释臂）——
本仓与 C# 现形均等，不在本票。
b) 键存在、找 0 且无显式 end（argc==3 全串形或 argc==4 仅 start 形，二者 end_given=0，
bitops.c:1726/1753）：字符串右侧视为零填充延伸，区间内（含负化与钳制后的有效区间）无 0 时
返回「首个不属于串的位」即 strlen*8（bitops.c:1851-1854 只在 end_given 时才把越界 pos 折回 -1）。
文档金样：三字 0xff → BITPOS key 0 回 24；BITPOS key 0 <start> 同回 24（仅 start 不闭合区间）。
c) 显式给 end（含 end=-1）即闭合区间，无 0 回 -1。
C# 原型为上游缺形：BitmapCommands.cs:NetworkStringBitPosition（267-357）解析
hasStartOffset/hasEndOffset（289-326）仅喂 TryValidateBitPosOffsets（328-333）做校验，该状态
不进驱动；BitmapManagerBitPos.cs:BitPosDriver（21-58）将 end 钳到 inputLen-1 后无匹配一律回 -1，
strlen*8 分支不可达。即本票是「双侧同形但均背离 Redis 标准」，按本仓先例（smessage 帧词、
vector wrongtype 文案按 Redis 标准回改）以 Redis 为准回改，非「按 C# 回改」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）。rust 转写忠实复刻了 C# 缺形：
bitmap_commands.rs:network_string_bit_position（189-237）经 parse_bit_pos_args（613-661）已产出
BitPosArgs.has_end_offset（字段 608、赋值 641、回填 659），但调用 bit_pos_driver（221-234）不传
该状态；wbitmap/src/bit_pos.rs:bit_pos_driver（14-70）BYTE 臂 end 钳 input_len-1（38-40），
bit_pos_byte_search 无匹配回 -1（171）、bit_pos_bit_search 无匹配回 -1（111）。慢臂
slow.rs:C::Bitpos（1042-1086）共用同一 parse 与同一 driver，同形。缺失键臂（快 233、
慢 1076-1082 找 0 回 0 找 1 回 -1）与 Redis 等形，不动。
现状锁测钉住错误形：wbitmap/src/bit_pos.rs:243
`assert_eq!(bit_pos_driver(&ones, 3, 0, -1, 0, 0x0), -1);`（无 end 全 1 找 0 锁 -1）。
C# 测试侧自带 Redis 形神谕 helper 却未接驱动：GarnetBitmapTests.cs:597-633 邻近的本地参考
Bitpos() 在未找到时返回「越界位」；既有的越界锁测
BitmapBitPosOutOfBoundsEndOffset{Byte,Bit}ModeTest（GarnetBitmapTests.cs:2570-2625）全部使用
显式 end，与本修复兼容（显式 end 形维持 -1 不变）。
3. 逻辑危害确证。默认配置、redis.io 官方教程用法「BITPOS key 0 找首个未占用位」（位图空闲槽
扫描标准习语）在本仓对全 1 值回 -1，Redis 回 strlen*8（下一个可追加位）：客户端把「尚有空位」
误判为「位图耗尽」，或被迫改发 STRLEN 再乘 8 绕行——线上协议面可观测、金样可锁定的行为差分。
补充文法事实（界定修复面）：unit 参数只在同时给出 start 与 end 时合法（bitops.c:1739-1756，
argc==6 才读 BIT|BYTE），故「无显式 end」仅 BYTE 全串/仅 start 两形，返回值恒为 strlen*8，
修复无需处理 BIT 形虚拟字节臂（bitops.c:1770-1774 的 end=(totlen<<3)+7 仅在带 end 的 BIT 形
被钳回 totlen*8-1，两侧现形已与 Redis 等）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:network_string_bit_position（189-237，驱动调用
221-234，缺失键臂 233 不动）
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:parse_bit_pos_args / BitPosArgs（598-661，
has_end_offset 已在手 608/641/659）
wedb/wbitmap/src/bit_pos.rs:bit_pos_driver（14-70，BYTE 钳制 38-40）、bit_pos_byte_search
（117-172，尾 -1 于 171）、bit_pos_bit_search（75-112，尾 -1 于 111）、tests（232-254，:243 锁测）
wedb/wnode/src/resp/basic_commands/slow.rs:C::Bitpos 臂（1042-1086，driver 调用 1063-1071）

对应 c# 文件与函数：
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitPosition（267-357；
hasStart/hasEnd 289-326 仅入校验 328-333；NOTFOUND 臂 349-354 等形不动）
garnet/libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitPosOffsets（66-81，校验面不动）
garnet/libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosDriver（21-58）

Redis 标准锚（非本仓文件）：src/bitops.c:bitposCommand（1720-1860；end_given 定义 1726/1753；
无界零尾 1851-1854；缺键臂 1801-1805）；redis.io/docs/latest/commands/bitpos 原文。

精炼执行方案：
1. bit_pos_driver 增一个 has_end_offset: bool 形参（语义即 Redis end_given），快慢两调用点
透传 BitPosArgs.has_end_offset——唯一真源，禁开第二个查找入口或复制搜索体。BIT 臂恒
has_end_offset=true（parse 层文法保证「unit 须与 end 同现」，落注释钉死该不变式；BIT 形行为零改动）。
2. BYTE 臂改动仅此一处出口：逐段搜索全部命中「全 1」走完区间（现回 -1 的出口 171）时，若
search_for==0 且 !has_end_offset 且区间非空且 input_len>0 → 回 input_len*8；其余维持 -1。
空串（input_len==0）保持 -1（Redis 空区间 start>end 回 -1，bitops.c:1818 前判，两侧现形已等）。
找 1 形永不外延（bitops.c:1851 条件 bit==0）。
3. 测试验证点（交执行席落地）：
a) wbitmap 单测：订正 :243 锁测为「无 end 全 1 找 0 → 24」并新增显式 end -1 → -1 对照锁；
matches_naive_on_random_buffers 的 naive 神谕同步加 end_given 维度（与 Redis 形对齐）。
b) wnode 集成 garnet_bitmap.rs BITPOS 段：新增金样锁 SET key 0xff*3 → BITPOS key 0 回 :24、
BITPOS key 0 1 回 :24、BITPOS key 0 0 2 回 -1（显式 end）、BITPOS key 1（全 1 找 1）回 0、
BITPOS missingkey 0 回 0（回归既有 233 臂）；慢臂同字节复核（parse/driver 单源，两臂各测）。
c) 既存锁 :458-476（负索引 BIT 形）、:546-551（越界 start/end 回 -1，均显式 end 形）零改动通过；
C# 对照面 GarnetBitmapTests.cs:2570-2625（显式 end → -1 锁）与新行为不冲突，无需订正。
4. 禁触线：try_validate_bit_pos_offsets 校验面（manager.rs:65-88 与 C# 金样等形）不动；
process_negative_offset 负索引折算不动；缺失键 0/-1 臂不动；§92（BITCOUNT 区间钳修复防回改锁）
不触碰；found/notfound 簿记面（票 wnode-string-bitmap-found-notfound-accounting-matrix 收口形）
零改动；本票为 Redis 标准回改，deviations.md 不登记（分叉消除非新偏差）。

查重结论：
task/ 全树 grep -ril「bitpos|bitop|bitcount|setbit|getbit|bitfield」仅命中五件：
task/ing/wnode-command-family-extension-manifest-unwired.md（R.BITPOS 系 COMMAND 自省面接线，
非执行语义，不重叠）、task/done/wrecord-modified-bit-dead-marker-retire.md（位标记无关）、
task/done/r435-a-list-inline-test-screening-20260928.md 与 task/done/b2-screening-recheck-notes-20260925.md、
task/done/checkjs-r1-anchor-ignore-registration.md（锚注册/测试审阅注记，无语义裁决）。
「unbounded|padded」扫 task/ 命中件均无关域。doc/zh/deviations.md grep「BITPOS/位图/bitmap」零命中，
无在册裁决；§110 裁 BITFIELD 未知子命令回显文本、§92 裁 BITCOUNT 钳形，均不覆盖 BITPOS 尾部语义。
无在途或已结票裁「保持双侧 -1 现形」；C# 测试神谕 helper 反证该 Redis 形为已知标准。

未尽面：
1. BIT 口径 unit 须与 end 同现的文法（两侧与 Redis 等形）若未来扩「BIT 无 end」形态，
bitops.c:1770-1774 虚拟零字节臂需另案。
2. GEO 族双侧均有实装，本轮预算内未完成逐命令（GEORADIUS 修正/精度/错误帧族）对拍，留待后续审计。
3. bitmap 域其余项（SETBIT/GETBIT 扩容零填充、BITCOUNT 裁剪与默认 BYTE、BITFIELD 解析/OVERFLOW
全局末值/64 位溢出臂、nil 帧形、错误帧字节族 vs C# 逐字节）本轮逐项对拍已确认双侧等形且等 Redis
标准形，无立案；BITFIELD INCRBY OVERFLOW FAIL 溢出仍落 0（Redis 不落盘）系既有刻意保持 C# 形
（bitfield/execute.rs:689-705 注释自证），未登 deviations，属在册缺口，是否补册或回改另案，勿在本票扩面。
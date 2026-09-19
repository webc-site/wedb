优先级：低

512MB 负载上限常量跨 crate 双定义无单点：wbitmap 与 wnode 各持一份同值 pub 常量

来源：qcode.rounds.md 第 8 轮 glm 系列 design 条（LOW 批次，2026-09-19 收口，产物仅存
台账；本轮清账甄别：network_*/initialize_if/iterate_store/wresp 原语/装配口等同类已被
zero-consumer 批二~批五收口，唯本条无票仍成立）。

现状（主仓 dev 实测）
- wedb/wbitmap/src/manager.rs:9 `pub const MAX_BITMAP_PAYLOAD_BYTES: i64 = 512 * 1024 * 1024`
  （对位 garnet/libs/server/Resp/Bitmap/BitmapManager.cs:19
  `internal const int MaxBitmapPayloadBytes = 512 * 1024 * 1024`，1:1 正确）。
- wedb/wnode/src/resp/basic_commands/set.rs:25
  `pub(crate) const MAX_STRING_PAYLOAD_BYTES: usize = 512 * 1024 * 1024`，
  注释自认「libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes，Bitmap 域共用」
  ——即明知同源仍就地再写一份数值，靠散文互指，无单一来源。
- C# 侧该上限只在 BitmapManager.cs 一处定义；字符串命令域没有第二个 512MB 常量
  （grep libs/server/Resp 仅 BitmapManager.cs:19 命中）。

问题
- 同一协议上限两个 crate 各持一份常量：任一侧调整（如未来跟随 C# 调上限）不会传导，
  静默分叉为「bitmap 上限 ≠ string 上限」；且类型不一（i64 vs usize），比较/换算处
  需隐式转换，进一步掩盖两常量同源的事实。

修法
- 单点化：wnode 侧删除 set.rs:25，改 `use wbitmap::MAX_BITMAP_PAYLOAD_BYTES`
  （或经 wbase 统一再导出，与「物理键编码四处单点」同形态）；保留 BitmapManager.cs
  锚点在 wbitmap 一处。
- 验收：全仓 512MB 负载上限常量定义唯一；set.rs 各消费点（:243 注释域）行为不变。

细化方案（2026-09-19 实测补全，比票面多一处派生重复）
- 消费链全景：
  - wbitmap/src/manager.rs:9 MAX_BITMAP_PAYLOAD_BYTES(i64) 与 :12
    MAX_OFFSET_FOR_BITMAP_LENGTH = (MAX_BITMAP_PAYLOAD_BYTES*8)-1，lib.rs 已 pub use，
    wnode Cargo.toml:40 已依赖 wbitmap —— 引用路径现成。
  - wnode/src/resp/basic_commands/set.rs:25 MAX_STRING_PAYLOAD_BYTES(usize)：
    set.rs:253 SETRANGE 上限校验唯一数值消费点。
  - wnode/src/resp/basic_commands/mod.rs:31 pub(crate) use 再导出。
  - wnode/src/resp/bitmap/bitmap_commands.rs:26 引入后 :33
    `MAX_BIT_OFFSET = (MAX_STRING_PAYLOAD_BYTES as i64 * 8) - 1` —— 与
    MAX_OFFSET_FOR_BITMAP_LENGTH 同公式重复（C# BitmapManager.cs:20 单点）；
    :37 parse_bit_offset 与 wbitmap::is_valid_bit_offset 同为
    BitmapManager.cs:IsValidBitOffset 的 1:1 对标（函数级重复）。
- 改法：
  1. set.rs：删 :23-25 定义，use wbitmap::MAX_BITMAP_PAYLOAD_BYTES，:253 比较处
     `> MAX_BITMAP_PAYLOAD_BYTES as u64`（i64 正常量 cast 安全）。
  2. mod.rs:31：再导出列表去掉 MAX_STRING_PAYLOAD_BYTES（rust_review 禁二次导出，
     且不再存在）。
  3. bitmap_commands.rs：删 :33 MAX_BIT_OFFSET，parse_bit_offset 内改调
     wbitmap::is_valid_bit_offset（公式派生与函数对标双双收敛到 wbitmap 单点）。
- C# 锚点形态同构验证：BasicCommands.cs:463 SETRANGE 即直接引用
  BitmapManager.MaxBitmapPayloadBytes，跨文件直接 use 与 C# 一致；不经 wbase 中转
  （rust_review SKILL 禁止二次导出）。
- 验收：全仓 grep 512*1024*1024 负载上限常量定义唯一（wbitmap/manager.rs:9）；
  SETRANGE/SETBIT 行为不变；cargo check 过。

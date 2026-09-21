# zcode-r8-my 自定义优化与上下游打通专项审查

视角: 自定义优化与上下游打通深度审查（轮 8）。
对照 ./.agents/skills/transpile/SKILL.md 与 ./task/refine.md 中声明的自定义优化设计（物理键格式、双维度判定升降阶、单日志多库隔离、无锁流式响应、Base32 编码等），全链路追踪其在 wval、wkv、wcol、wnode 等模块中的上下游打通情况，排查是否存在设计未落地、半途断链、性能倒退或正确性隐患。

按优先级排列的问题清单:

4. read_user_async 缺失 with_prefix 变体导致异步多键批命令前缀重复重读并强制堆分配

具体问题:
在 wnode/storage/session 中，已为同步读提供了 read_user_sync_with_prefix 优化，允许外提会话前缀避免循环内重复解析。但在异步读体系中，StorageSession 仅暴露了 read_user_async(key, f)，未提供带前缀的变体。
read_user_async 内部需要依次探查 String 域、ObjectEnvelope 域和 Meta 域，其内部调用的 read_tag_with 每次都会重新调用 batch.session_prefix()，通过原子读获取 ns/db 并重新执行 Varint 编解码。在 basic_commands/slow.rs 的 bitop_command_slow 循环中，针对每个源键不仅在外部重复调用 session_prefix() 检查向量索引，还在内部通过 read_user_async 触发多达 3 次重复前缀计算。同时，bitop_command_slow 传入了 |v| v.to_vec()，强制为每个源键分配新的堆内存，而下游累加器 acc.fold 实际仅需要 &[u8] 切片借用。
此外，bitmap_commands.rs 中的同步命令 string_bit_operation 在遍历源键时也漏掉了外提前缀，在循环内部反复调用 store.session_prefix() 并调用了无前缀的 read_user_sync。
rust 相对路径:
wedb/wnode/src/storage/session/storage_session.rs 的 read_user_async 函数
wedb/wnode/src/resp/basic_commands/slow.rs 的 bitop_command_slow 函数（第 980 至 995 行）
wedb/wnode/src/resp/bitmap/bitmap_commands.rs 的 string_bit_operation 函数（第 317 至 326 行）
c# 相对路径:
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs 的 StringBitOperation 方法
建议:
在 StorageSession 中补齐 read_user_async_with_prefix 变体，将内部探针的三次前缀开销降为零；在 bitop_command_slow 和 string_bit_operation 循环外部单次外提 session_prefix()；消除 bitop_command_slow 闭包中的 to_vec() 强制分配，对齐零拷贝借用规范。

5. rename_slow 显式声明前缀外提却未使用，rename_sync 存在多次散落重读

具体问题:
在 key_admin_commands/slow.rs 的 rename_slow 函数中，入口处第 77 行明确写了 let prefix = storage.batch.session_prefix();，注释宣称会话前缀单次外提，但在紧随其后的旧键物理域三探中，调用的却是无前缀版本的 storage.read_tag_with，使得入参 prefix 完全未被使用，内部白白重新计算了 3 次前缀。
与此同时，在 key_admin_commands/keys.rs 的 rename_sync 函数中，前缀获取散落在各处分支（第 465 行 let prefix = store.session_prefix()、第 513 行 let nx_prefix = store.session_prefix()、第 532 行行内再次调用 store.session_prefix()），且中间的 ttl_of_sync 与 etag_of_sync 也各自在底层重新提取前缀。SKILL 声明的循环前缀单次外提优化在键重命名链路未形成规范闭环。
rust 相对路径:
wedb/wnode/src/resp/key_admin_commands/slow.rs 的 rename_slow 函数（第 76 至 105 行）
wedb/wnode/src/resp/key_admin_commands/keys.rs 的 rename_sync 函数（第 465、513、532 行）
c# 相对路径:
garnet/libs/server/Resp/KeyAdminCommands.cs 的 Rename 方法
建议:
在 rename_slow 中改用已有的 read_tag_with_prefix，消除未使用的 prefix 变量和重复计算；在 rename_sync 函数入口处单次提取 prefix 并在全链路传参复用。

审查通过项（已核实无增量、实现优雅的面）:

1. 物理键前缀与 NamespaceDbCodec:
wval/src/ns_codec.rs 与 wkv/src/session/keys.rs 的物理键格式已完全上下游打通。租户命名空间、逻辑数据库与 KeyTag 通过 Varint 和单字节紧凑编码，实现了零歧义、零内存越界，且会话一致性读与前缀匹配在上游 wnode 各命令中广泛正确接入。

2. Base32 编码与 Checkpoint 元数据映射:
wbase/src/base32.rs 实现了 RFC 4648 规范的高性能 Base32 编解码，与 wcpr/src/meta.rs 中的 Guid、检查点目录以及段设备恢复无缝打通，不存在旧 GUID 格式泄露或编解码不匹配问题。

3. 分层集合流式响应设计:
wcol/src/types/garnet_object.rs 与 wnode/src/resp/objects/tiered_collection_ops/* 在大集合读取时，采用游标与流式迭代器避免了将整树全部物化到堆内存中，RESP 序列化直接写入输出缓冲，架构方向正确。

视角结论: 有增量

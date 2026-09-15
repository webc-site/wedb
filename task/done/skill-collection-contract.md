skill-collection-contract：清理「集合按大小拆分」未采纳契约（SKILL 文档 + 死枚举 + 死链）

来源
- next/ds.data.md 原条目 1（已删）：SKILL 集合类型设计口径冲突，遗留死枚举
- next/glm.md 原条目 13（已删）：集合载体按大小拆分契约未落地
- 主代理拍板：不落地该契约。refine.md 最高原则 1:1 对标 C#，garnet 集合是内存对象
  （garnet/libs/server/Objects/Hash/HashObject.cs、Set/SetObject.cs、List/ListObject.cs，
  均为内存字典/列表，无打平/紧凑分层概念），该节是早期设计残留未采纳

核实结论（2026-09-15，主仓库 dev 分支）
1. KeyTag::Hash(0x02)/KeyTag::Set(0x03)：生产 src 零引用，仅 wval/tests、
   wkv/tests/compact/more_log_compaction.rs 引用。属死枚举
2. StorageEncoding::Flattened(1)：生产 src 零写入（活值为 FlattenedTree=2，
   wkv/src/range_index.rs:243/754/883、compact.rs:72、checkpoint.rs:107 使用）。属死枚举
3. SubKey 编解码死链（删 Hash/Set 后 is_subkey() 恒 false，链路整体失效）：
   - wval/src/meta.rs: SubKeyRef/SubKeyBuf/SubKeyCodec/kv_prefix/ensure_subkey、
     SUBKEY_HEADER_SIZE/SUBKEY_STACK_CAP
   - wval/src/ns_codec.rs: encode_sub_key/encode_sub_key_with_prefix/encode_chunk_key_with_prefix/
     decode_sub_key/decode_chunk_key/decode_subkey_id_version/DecodedSubKey、
     SUBKEY_META_HEADER_LEN/MIN_CHUNK_KEY_LEN/CHUNK_ID_LEN/MIN_SUBKEY_LEN
   - 上述生产 src 零调用，仅 wval 内部定义 + 3 个测试文件
4. wcompact host trait is_stale_subkey：唯一数据来源 decode_subkey_id_version
   生产恒 None（无子键写入），现状即恒 false。删除链路后必须一并摘除，
   否则只能写恒 false 占位实现（违反 SKILL「严禁占位函数」）
5. key_id 元数据表（update_key_id_meta 等）与 MetaDeathScope 是活机制
   （RangeIndex 树生命周期、session/collection.rs、session/swap.rs、
   wcompact/src/compactor/scope.rs 死条目回收），保留
6. 集合实际载体：wcol ObjectEnvelope 信封（1:1 对标 C#），保留
7. 数值保序映射（wbase/src/float.rs encode_f64 等）生产 src 零使用（仅测试引用），
   i64 符号翻转 (x ^ (1<<63)) 全仓无实现。按拍板第 4 点条件不成立，SKILL 条目删除；
   wbase/float.rs 模块本身不在本待办范围，不动
8. STACK_KEY_CAP=62 存在（wval/src/ns_codec.rs:46），SKILL「会话完整物理键 <=62B
   零堆分配」条目属实，保留

修订点

一 .agents/skills/transpile/SKILL.md
1. 「集合类型，按大小拆分」bullet（26-41 行区域）压缩为一句：
   集合类型对标 C# 内存对象（wcol 信封 + ObjectEnvelope 标签），不做按大小分层；
   RangeIndex 例外走 wbftree（对齐 C# RangeIndexManager/RangeIndexStub，
   单树单文件生命周期，ri_set/ri_get_callback/ri_del/ri_exists/ri_len/ri_scan/ri_range）
2. 「二进制 Key 刚性防穿透与保序公理」节逐条核对：
   - 删「定长刚性帧隔离公理」（17B 打平子键帧，SubKey 链已删）
   - 保留「会话完整物理键 [NsVarint]+[DbVarint]+[KeyTag]+[Payload]」（NamespaceDbCodec 实现存在）
   - 删「单树多前缀物理隔离公理」（TreePrefix 枚举 ZSetScore/ZSetMember/SetMember/
     ListIndex/RangeIndexKey/HashField 全仓无实现；RangeIndex 键布局由 wbftree 侧自有约定）
   - 删「数值保序双射映射」（f64/i64 保序映射生产零使用）
   - 删「载荷与键严格分离」（打平存储语境，随节收缩）
3. 「严格删空生命周期与原子墓碑」节中「旧子键逻辑失效，物理垃圾交由后台
   Compaction 异步回收」措辞失真（无子键），微调为墓碑/幽灵 meta 语义
4. 「O(1) 复杂度计数规约」节删 LLEN ListStub 条（ListStub 未实现，
   LLEN 实际走 wcol 内存对象 O(1)）；HLEN/SCARD/ZCARD 直读计数保留
5. 其他节不动

二 死枚举删除
1. wedb/wval/src/tag.rs: 删 KeyTag::Hash/KeyTag::Set 枚举项与 as_str 分支、
   删 is_subkey()（唯一消费者随链删除）、枚举 doc 注释同步修订
2. wedb/wval/src/meta.rs: 删 StorageEncoding::Flattened；from_u8/try_from_u8
   折叠语义同步修订（1 不再合法，与 0xff 一致显式拒绝）；is_flattened() 收窄为
   FlattenedTree；修订 72-74 行附近 from_u8 注释
3. from_u8 修订注意：from_u8 是「本构建写入字节的回读面」，删除 Flattened 后
   match 仅剩 2 => FlattenedTree，其余折叠 Compact——语义不变仅分支减少

三 SubKey 死链清理（删枚举的必然完备闭包，否则链路恒错）
1. wedb/wval/src/meta.rs: 删 SubKeyRef/SubKeyBuf/SubKeyCodec/kv_prefix、
   SUBKEY_HEADER_SIZE/SUBKEY_STACK_CAP、stack_heap_buf! 实例化
2. wedb/wval/src/ns_codec.rs: 删 encode_sub_key 系/decode_sub_key 系/DecodedSubKey/
   decode_subkey_id_version 与 SUBKEY_META_HEADER_LEN/MIN_CHUNK_KEY_LEN/
   CHUNK_ID_LEN/MIN_SUBKEY_LEN，修 import 与相关 doc 注释
3. wedb/wval/src/lib.rs: 同步修订再导出列表
4. wedb/wkv/src/compact.rs: 删 is_stale_subkey 实现与 decode_subkey_id_version import
5. wedb/wcompact/src/host.rs: 删 trait 方法 is_stale_subkey
6. wedb/wcompact/src/compactor/judge.rs: 删 wrapper，judge_dead 四通道改三通道
   （墓碑 → 用户谓词 → TTL），注释同步
7. wedb/wcompact/src/compactor/run.rs: 阶段 3 快速清理通道 cand.is_dead ||
   is_stale_subkey 收窄为 cand.is_dead，注释同步
8. wedb/wcompact/src/compactor/scope.rs: 修订引用 is_stale_subkey 的论证注释
   （meta 死亡登记机制本身保留）
9. wedb/wcompact/src/compactor/mod.rs:30、wedb/wkv/src/store/mod.rs:293 注释措辞修订
10. wcompact/tests/compact/support.rs:425 删桩实现

四 测试处置（测试一并改，对标 C# 无此机制的测试删除）
1. wedb/wval/tests/meta_and_subkey.rs: 删 SubKeyCodec/SubKeyRef/SubKeyBuf/
   Flattened 相关测试段，保留 MetaValue/CompactMetaValue 测试；文件改名为
   meta_value.rs（内容以 meta 为主）；Flattened 删除后非法编码字节测试补 1
2. wedb/wval/tests/ns_codec.rs: 删 encode_sub_key/decode_sub_key 测试段
3. wedb/wval/tests/tag.rs: 删 Hash/Set 枚举表行与 is_subkey 相关断言
4. wedb/wval/tests/zset_codec.rs: 删 KeyTag::Hash/Set 断言段（含 wbase float
   引用若仅此处使用则同步清理该测试文件相关段）
5. wedb/wkv/tests/compact/more_log_compaction.rs:440/460: 按上下文改写或删除
   以 KeyTag::Hash 编子键的测试用例
6. wcompact 既有紧缩测试跑通即可（is_stale_subkey 摘除后行为等价：恒 false 分支移除）

五 check.js
- 预期无新增缺失（删的是 rust 侧符号，非 C# 映射）；若报缺失在
  js/check/ignore 相应 yml 登记理由

风险与边界
- 不动：CompactMetaValue（独立结构，不依赖被删值）、wbase/float.rs（独立模块）、
  key_id 元数据表、MetaDeathScope、RangeIndex/wbftree 全链、ObjectEnvelope 载体
- wcompact 判死链改动为行为等价变换（生产恒 false 分支移除），非逻辑变更
- 验收：bun ./js/check.js 0 缺失 0 重复、./clippy.sh 0 警告、./test.sh 全过

验证结果（2026-09-15，分支 skill-collection-contract，基线 9631a1d）
1. bun ./js/check.js：exit 0，0 缺失 0 重复，无需新增 ignore
2. clippy（cargo +nightly clippy -q --tests --all-targets --all-features
   -- -D warnings -W clippy::absolute_paths）：exit 0，0 警告
3. ./test.sh：wedb 2028 tests passed (1 skipped) + regress 2 passed，全过
4. 主仓库合并（677bd4e，基线推进到 1533d75 后三方合并）后
   cargo check --workspace --all-targets 通过

执行摘要
- SKILL.md：「集合类型，按大小拆分」16 行压缩为 2 行（内存对象信封 + RangeIndex
  wbftree 例外）；「二进制 Key 公理」节收缩为会话物理键一条（17B 定长帧 /
  TreePrefix 枚举 / 数值保序映射 / 载荷键分离四条未采纳全删）；
  前缀外提条目去 sub_key_with_prefix 措辞；秒删条目改幽灵 meta 语义；
  计数规约删 LLEN ListStub 条、ZCOUNT 归内存对象
- 代码：KeyTag::Hash/Set 与 is_subkey 删除；StorageEncoding::Flattened 删除
  （from_u8/try_from_u8/is_flattened 同步收窄）；SubKey 死链
  （SubKeyCodec/SubKeyRef/SubKeyBuf/SUBKEY_*/ns_codec 的 encode_sub_key 系/
  decode_sub_key 系/DecodedSubKey）删除；wcompact trait is_stale_subkey 与
  wkv 实现摘除（生产恒 false 分支，行为等价）；MetaDeathScope/meta 水位回放/
  key_id_versions 死条目回收等活机制保留
- 测试：meta_and_subkey.rs 改名 meta_value.rs（删 6 个 SubKey 测试，
  Flattened 断言改 1 拒绝）；ns_codec.rs 测试删 sub_key/chunk 段，
  KeyTag::Hash 引用改 Ttl；tag.rs 测试删 Hash/Set 行；
  zset_codec.rs 整文件删除（KeyTag 死值测试 + float 测试与
  wbase/tests/main.rs::test_float_primitives 重复错位）；
  wkv more_log_compaction.rs 删子键淘汰专用测试函数；
  wcompact support.rs 删桩实现
- 合并处理：fmt 对 5 个无关测试文件的空行副作用在合并时回退为 HEAD 版本
- 遗留：wbase/src/float.rs 保序编码生产 src 零使用（SKILL 条目已删，
  模块与 wbase 自身测试保留，可另行评估去留）；StorageEncoding::FlattenedTree
  命名仍含 Flattened 字样（活枚举值，改名波及 3 文件，未动）


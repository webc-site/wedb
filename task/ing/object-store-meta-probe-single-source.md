对象装载三连域探针四份手写：收敛为 Meta 探针 + 域瀑布单点

来源：next/agy.design.md 条 11。
取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev 当下工作树，行号按符号重取（票面行号已弃）。

现状
- 四个函数各写一遍「Meta 分层态探针 → ObjectEnvelope 信封探针 → String 域反探」瀑布：
  /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:254 obj_load_custom_sync、
  :330 obj_load_custom_async、:392 obj_length_sync、:455 obj_length_async
  （:224 obj_load_typed_sync 与 :319 obj_load_typed_async 已是转调薄壳，不计重复）。
- Meta 探针闭包四处逐字节同形：`if raw.len() >= wval::META_VALUE_SIZE {
  MetaValue::from_slice(raw).ok() } else { None }`，见 :263、:340、:400、:464。
- 紧随其后的类型闸臂同形：`is_alive` 判定 + `meta.collection_type == tag` 分流 +
  不等即 `write_error_raw(output, RESP_ERR_WRONG_TYPE)`，见 :269-:282、:347-:357、:406-:425、
  :471-:485。全文件 `write_error_raw(output, RESP_ERR_WRONG_TYPE)` 出现 12 次。
- String 反探臂同形（信封 NotFound 后探 String 域判 WRONGTYPE/缺失/降级）：
  :299-:308、:376-:386、:439-:448、:503-:513。
- 同形度取证：四处差异仅在（a）同步 try_read_tag_sync / 异步 read_tag_with 的调用形态，
  （b）信封段解码器（obj_decode_custom + deserialize 对 count_of_blob 直读），
  （c）obj_length_* 多一条 `now_ticks() < meta.next_expiry` 的 size 短路（:413、:476）。
  探针骨架本身无差异 → 真重复。
- 已有相邻单点不可混用：键存活三域探针在
  /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/ttl_sync.rs:425 probe_alive_with_prefix
  与 :484 probe_alive_domain_with_prefix，只回答「活不活」，不产出 MetaValue/类型闸，
  与本单「装载取类型」是两件事；next/msetnx-slow-path-meta-domain-probe.md 管的是那条链缺 Meta 臂，
  两单不得互相顶替。
- 后果：任何分层态语义修订（如新增域、改 collection_type 闸口径、改降级条件）需同步四处，
  已经出现口径漂移面（length 族有 next_expiry 水位短路、load 族无，注释各自表述）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs
  （统一存储读方法单次 GET 出类型，判型在方法内一条 switch），
  以及 /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/ObjectStore/Common.cs 的装载入口。
  C# 无「三物理域瀑布」概念（分层是本项目自定义，见 SKILL 集合自适应混合分层存储架构），
  故该瀑布必须本项目自证一处定义，现状四份属失控。

修法
1. 在 object_store_utils.rs 立两个私有单点，不改对外四函数签名：
   - `fn meta_probe(raw: &[u8]) -> Option<MetaValue>`（承接四处同形闭包：长度门 + from_slice）。
   - `enum MetaGate { Absent, Degrade, SizeShortCircuit(u64), WrongType }`
     加 `fn meta_gate(meta: Option<MetaValue>, tag: u8, want_size: bool) -> MetaGate`，
     把「is_alive + collection_type == tag + next_expiry 水位」三段判定收一处；
     WRONGTYPE 应答仍由调用侧统一走单点写出（把 12 处 write_error_raw 的 WRONGTYPE 帧
     收敛到 meta_gate 的 WrongType 出口一次写）。
   - `fn string_domain_wrong_type(...)`：信封 NotFound 后的 String 反探臂，同步/异步各一入口，
     与 meta_gate 同文件同段。
2. 同步侧（try_read_tag_sync 回 `StoreResult`）与异步侧（read_tag_with 回 `Option`）
   只做「读形态适配」，判定核共用第 1 步的三函数；禁出现第二份 is_alive/collection_type 表达式。
3. 分层语义零改动：obj_load_* 的 Degrade 触发条件、obj_length_* 的 size 短路与
   RESP 输出字节必须逐位不变；水位短路仍只在 length 族（把 want_size 参数作为唯一分叉）。
4. 若第 1 步使 object_store_utils.rs 单文件继续膨胀，目录化拆分见
   task/ing/object-store-utils-file-split.md（该票落地时按本单核为切割线，先落本单）。

验收判据
- `grep -rn "raw.len() >= wval::META_VALUE_SIZE" wedb/wnode/src` 命中数 = 1（定义点）。
- `grep -c "write_error_raw(output, RESP_ERR_WRONG_TYPE)" wedb/wnode/src/resp/objects/object_store_utils.rs`
  由当前 12 降为 1（或 2，若异步侧错误通道必须分开），且 `meta_gate`、`meta_probe` 为该文件内
  唯一持有 collection_type 判定的函数（`grep -n "collection_type" object_store_utils.rs` 命中集中在核内）。
- 四函数 obj_load_custom_sync / obj_load_custom_async / obj_length_sync / obj_length_async
  的调用面零签名变化（`grep -rn "obj_length_sync\|obj_load_custom_sync" wedb/wnode/src` 的位点数不变）。
- 既有 resp 对象族测试与 wnode/tests/consistent_read_session.rs 全绿，期望字节不改。

优先级
重复/多套机制（同域瀑布四份手写，且判定核含分层语义，漂移代价高）。

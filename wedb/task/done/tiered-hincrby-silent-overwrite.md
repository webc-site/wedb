主题：分层态 HINCRBY/HINCRBYFLOAT 现存非数字值静默按 0 覆写、缺 infinity 门、新字段两态漂移

问题
分层臂在 read_callback 中读到 Found 但值解析失败时，cur_val 留 0、is_new 已置 false，
随后仍无条件 tree_put 用增量覆写原非数字值并回应答成功，用户现存数据被无声改写。
Hincrbyfloat 另缺对象层的现存值无穷门（现存 inf 被 strict_f64(..,false) 判为不可解析后按 0 覆写）。
两态新字段的存储与应答口径漂移：对象层新字段存/回增量原文，分层臂存/回格式化结果（"0.10" 存 "0.1"）。
整型溢出语义漂移：分层用 saturating_add，对象层用 wrapping_add。

C# 对位文件:函数
- libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement —— 现存值 TryParse 失败
  WriteError(RESP_ERR_HASH_VALUE_IS_NOT_INTEGER) 并 return（不落库）；新字段
  hashValueRef = incrSlice.ToArray()（存原文）；result += incr（unchecked 环绕）。
- libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat —— 入参 incr
  IsInfinity → RESP_ERR_GENERIC_NAN_INFINITY；现存值 TryParseWithInfinity 失败
  → RESP_ERR_HASH_VALUE_IS_NOT_FLOAT；现存值 IsInfinity → RESP_ERR_GENERIC_NAN_INFINITY_INCR；
  新字段存 incrSlice 原文。
分层臂为 rust 自定义（C# 无分层），须与 rust 对象层单源求值等价，不得自创第二套语义。

rust 现状文件:函数
- wedb/wnode/src/resp/objects/tiered_collection_ops.rs:tiered_hash_arm（Hincrby / Hincrbyfloat 两臂）
- 对象层单源：wedb/wcol/src/hash/hash_object_impl.rs:hash_increment / hash_increment_float
- 错误常量：wedb/wresp/src/cmd_strings.rs RESP_ERR_HASH_VALUE_IS_NOT_INTEGER /
  RESP_ERR_HASH_VALUE_IS_NOT_FLOAT / RESP_ERR_GENERIC_NAN_INFINITY_INCR
- 解析单源：wedb/wbase/src/num.rs strict_f64(can_be_infinite=true) 即 try_parse_with_infinity 口径

改造步骤
1. Hincrby：闭包内解析失败置 bad_value；闭包后 bad_value → cs::write_error_raw
   (RESP_ERR_HASH_VALUE_IS_NOT_INTEGER) 且 return Ok(true)，跳过 tree_put、不动 size、不推进 dirty；
   新字段存/回增量原文（write_integer_from_bytes(incr_slice)），现存字段用 wrapping_add 对齐对象层。
2. Hincrbyfloat：现存值改用 strict_f64(payload, true)（允许 inf 词形）匹配对象层 try_parse_with_infinity，
   据此区分两态错误（非浮点 → NOT_FLOAT；现存 inf → NAN_INFINITY_INCR），回错且不写；
   新字段存/回增量原文（write_resp_bulk_string(incr_slice)）。
3. 错误/被拒出口均在任何 tree_put 之前返回，ctx.dirty 恒假，finish_tiered_arm 不推进 WATCH 版本，
   与 HSETNX 命中已存在同属未走写漏斗出口（对齐 C# 纯读不 IncrementVersion）。

涉及上下游链路
- 上游 RESP 会话 HINCRBY/HINCRBYFLOAT 分派（exec_tiered_hash）不变；Err(()) 仍表 arity/入参语法
  降级出口，新增的回错走 Ok(true)+已写错误帧，语义清晰不冲突。
- 下游 tree_put / save_bftree_meta_stub / finish_tiered_arm 复用现有单点，无新增机制。

验收
- 分层键预置非数字字段值，HINCRBY/HINCRBYFLOAT 回错且原值不变、WATCH 版本不推进；
  现存 inf 字段 HINCRBYFLOAT 回 NAN_INFINITY_INCR；
- 同数据内存态与分层态应答逐字节一致（含新字段 "0.10"、整型环绕溢出场景）；
- 回归 wedb/wnode/tests/tiered_cmds_align.rs、tiered_watch_fence.rs；cargo check 零错误。

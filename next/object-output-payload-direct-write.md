ObjectOutput 载荷改挂载会话输出尾段单步直写（去中转缓冲二次拷贝）

来源：next/qcode-my-r4.md 第 4 轮条 #7（该档本轮清账删除）。按主仓 HEAD 复核：成立且待做，
原计划自留「站点审计超预算则拆登 task/ing 留后续」，本单即该后续。
同主题的薄单 task/ing/qcode-my-r4-object-payload-direct-write.md（同一 #7 的第二张单，
审计成本与冲突风险两点已在下方方案内承接）已并入本单并删除，本单为该条唯一载体。
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 5b4e8fe7。

现状
- wedb/wcol/src/resp/output.rs:25-35 `pub struct ObjectOutput { payload: Vec<u8>, result1, result2,
  output_flags }`，对象命令一律自带中转 Vec；同文件 :47-59 的 null/协议版本分派单点亦写向 &mut output.payload。
- 构造点 `ObjectOutput::new()` 全仓 20 处（如 wedb/wnode/src/resp/admin_commands.rs:221、:281，
  wedb/wnode/src/resp/objects/hash_commands.rs:41，list_commands/mod.rs:42-43，
  sorted_set_geo_commands.rs:493、:565、:631、:638、:793、:836、:885、:891），命令产出后由调用侧
  整段拷进会话输出缓冲，形成「对象缓冲 → 会话缓冲」双写。

C# 参考
- garnet/libs/server/Objects/Types/ObjectOutput.cs:36 结构体内 :41 `public SpanByteAndMemory
  SpanByteAndMemory;`、:46 `public IGarnetObject GarnetObject;`，即输出缓冲由会话侧直接挂载进对象
  应答结构，无中转向量。

方案
- ObjectOutput 增挂载/回交口（payload 借用会话输出尾段，commit 时 O(1) 指针换回），
  payload_written 判定改按挂载起点偏移量比较。
- 改动收敛在 wnode 对象命令构造点与 object_store_utils 消费点；逐站点核查「构造 → 消费」区间内
  不得有对同一 output 的旁路写入，不满足者保留自带缓冲（同一大应答/异步路径口径），禁双轨。
- 协议字节零改动；分派单点（output.rs:47-59）继续唯一，不得在站点各自展开 RESP2/RESP3 臂。

优先级
打磨（拷贝收敛，无功能变化）。若站点审计超预算，按「保留自带缓冲」收小范围，不留半吊子双轨。

交叉引用
- 文件级职责切分见 task/ing/object-store-utils-file-split.md；本条只动输出缓冲形态。

细化方案（f61-obj-output，基线 dev e89780c2 复核后）
- 形态：单轨挂载。wcol/src/resp/output.rs 的 ObjectOutput 改
  `ObjectOutput<'a> { payload: &'a mut Vec<u8>, base: usize, result1, result2, output_flags }`，
  删 Vec 中转与 Clone/Default。一处定义：
  mount(&'a mut Vec<u8>)（记 base = len，对标 ObjectOutput.cs:FromPinnedPointer）、
  payload_view() -> &[u8]（base..）、written()（len > base，payload_written 判定改此）、
  reset()（truncate(base)，对标 C# writer.ResetPosition）。
- wcol 内部：operate 族签名 `&mut ObjectOutput` → `&mut ObjectOutput<'_>`（trait
  types/garnet_object.rs 4 处 + hash/set/list/zset/geo 实现转发，机械替换）；
  body 零改动（&mut Vec<u8> deref 与 RespWriter::new_ref coercion 兼容）；
  唯一语义点 sorted_set_object_impl.rs:650 payload.clear() → reset()。
- wnode 构造点：四个 run_operate（hash/set/list/zset）签名加 output 参数返回
  ObjectOutput<'o>；rmw_helpers 的 RunOp bound 改
  `for<'o> FnOnce(&mut Obj, Op, &[&[u8]], &'o mut Vec<u8>) -> ObjectOutput<'o>`，
  sync/async 两臂 extend_from_slice 删除，payload_written 改 obj_out.written()。
- 站点分型（逐点核查结论）：
  - 会话直写：装载直评站点（HGETALL 等）与 RMW 骨架臂挂 mount(output)；
  - 解析消费非回显（GEOSTORE/ZRANGESTORE 的 parse_pairs_payload 臂）：挂本地
    sink Vec（保留自带缓冲口径，无类型双轨）；
  - 丢弃站点（garnet_api/admin HCOLLECT、aof 回放）：挂本地 sink。
- 旁路写入治理：obj_out 存活期间对 output 的读写一律经 obj_out.payload /
  payload_view / reset（geo_add 存储错误臂整体丢弃重写 → reset + 经路写），
  禁止裸 output 并用。
- 协议字节零改动；RESP2/RESP3 分派单点（output.rs 适配口）不动。

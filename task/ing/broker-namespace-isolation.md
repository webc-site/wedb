# 阻塞族 CollectionItemBroker 跨租户命名空间与库隔离

来源：next/zcode-r7-redteam.md 问题 P0-1

## 裁决

成立，接受，且范围大于票面：ns 与 db 两维都串扰。属转写引入的结构性旁路：C# 取件随
观察者自身会话执行域（CollectionItemBroker.cs 的 TryGetResult(key, observer.Session.
storageSession, ...)），rust 转写把该执行域换成经纪进程单例存储会话后丢失了域绑定，
叠加 wedb 自有多租户维度即成漏洞。

## 根因与证据

1. 注册用裸用户键：list_commands/blocking.rs 四处 park_broker_wait 的 keys 闭包
   map(|k| k.to_vec())、sorted_set_commands/blocking.rs 两处，均产出裸键。
   pump.rs:71-89 park_broker_wait 原样转交 broker.start_wait。
2. 取件会话是装配期派生的进程单例，恒在根域：service.rs:802 store.new_session()，
   wkv/src/session/mod.rs:184-195 初值 ns0/db0 且执行期从不切换上下文。
3. 取件源直用裸键装载，物理键=会话前缀+裸键：collection_item_source.rs 的
   list_outcome、zset_outcome 把裸 key 交给 self.session.enter_batch() 后的
   obj_load_typed_sync 与 save_list，前缀恒为根域，故任意租户的阻塞取件都在 ns0/db0
   读对象信封，越权消费或搬移 ns0 队列，BLMOVE 目标键通知同理。
4. 观察表进程级单 map、键为裸键：collection_item_broker.rs:41、:187；不同租户同名键
   共用同一 FIFO，互相唤醒串扰。
5. 写后唤醒通知也是裸键：pump.rs:24 notify_collection_update、garnet_api/mod.rs:406
   StoreGarnetApi::notify_collection_update（调用点 list/write.rs、sorted_set/write.rs、
   garnet_api/slow.rs:832、:884）。

后果链：ns5 BLPOP q → 观察者挂裸键 q → 经纪在 ns0/db0 试取弹出 → ns0 同名键被越权消费
回给 ns5；任意租户 LPUSH q 唤醒他域观察者。db 维度同理完全未隔离。

## 为何推翻前一版方案（ChannelNsPrefix 复用）

前一版拟把 pubsub 的 ChannelNsPrefix 上移到 wbase 更名 NsPrefix，以十进制 ns、db 加
冒号定界折叠成 [ns:db:裸键]，并让 wcol 观察者携带 prefix、给 CollectionItemStore trait
增 (ns,db) 入参、取件时 set_context。否决，三条硬伤：

其一，违禁第二套编码。存储物理键布局 [VirtualNsVarint][VirtualDbVarint][KeyTag][裸键]
已是全仓 (域→字节) 的唯一真源（wval::ns_codec，SessionPrefixBuf、NamespaceDbCodec、
wkv session_tag_key），再引入 [ns:db:裸键] 是与之并行的第二套隔离编码。纪律要求隔离前缀
构造必须复用既有单点、禁止手搓第二套编码。

其二，定界十进制编码是为 pubsub 的 glob 模式匹配安全而设计，与经纪不透明观察键毫无关系，
生搬属于错配单点。

其三，改动面过宽且更重：侵入 wbase 新模块、wpubsub 迁移、wcol 观察者与 trait 签名；取件
用 set_context 会对经纪单例会话反复做逻辑到虚号解析、路由快照钉住乃至 DbMeta 落盘，产生
本不该有的副作用，且需把裸 ns、db 一路透传进不感知租户的 wcol。

## 方案：复用存储物理键单点，经纪零改动

隔离键即「对象信封在该键所属虚拟域下的存储物理键」，由存储编码单点构造、由存储解码单点
还原，全程不修改 wcol 经纪（它对键保持不透明）。折叠与解折叠各只用一个既有单点：

- 折叠：wkv::StoreSession::session_tag_key(KeyTag::ObjectEnvelope, 裸用户键)，产出
  TaggedKeyBuf。所有隔离点共用此唯一函数，杜绝第二套编码。
- 解折叠：wval::NamespaceDbCodec::decode_tagged_key(隔离键) → (vns, vdb, tag, 裸键)。
  vns、vdb 即物理键自带虚拟域，裸键即载荷。

三处落地：

1. 注册折叠。pump.rs park_broker_wait 增 store 入参，在确认已注入经纪后、调用
   broker.start_wait 前，把 keys 闭包产出的每个裸键 session_tag_key 折叠为隔离键再登记。
   六个注册闭包保持原样返回裸键。cmd_args（BLMOVE 目标裸键、方向字节、count 标量）不折叠。
   方法对 D 泛型化。

2. 通知折叠。两个 notify_collection_update 各自就地折叠：
   - RespServerSession::notify_collection_update（快路径，pump.rs）增 store 入参，内部
     session_tag_key 折叠后转 handle_collection_update。十处命令层调用点本就持有 store，
     仅补传（含 sorted_set/write.rs 两处 notify_dst 闭包，闭包内捕获 store）。方法对 D 泛型化。
   - StoreGarnetApi::notify_collection_update（慢路径，garnet_api/mod.rs）持有 self.session，
     就地折叠，两 slow.rs 调用点签名不变。service.rs:1790 collection_notify 闭包收到已折叠键，
     无需改动。

3. 取件解折叠与回裸键。collection_item_source.rs try_get_result 入口先 decode_tagged_key
   还原 (vns, vdb, 裸键)，再 self.session.set_virtual_context(vns, vdb) 把经纪专属会话切到该
   隔离键所属虚拟域，然后以裸键驱动 list_outcome、zset_outcome 的装载、弹出、回写；出件
   结果 CollectionItemResult 的 key 一律回裸键，客户端应答无感。BLMOVE 的 notify_key 由原来
   直接回传 cmd_args 内裸目标键，改为在同一已切换虚拟域下 session_tag_key(ObjectEnvelope,
   裸目标键) 重新折叠再入队，使其与注册、通知三处折叠键同源可比。解码失败或标签非
   ObjectEnvelope 一律按不可取处理返回 none（生产路径恒为折叠键，此为防御）。

单写者纪律：set_virtual_context 走 &self 的 Relaxed 原子标量赋值，零解析、零分配、零落盘，
与 set_context 不同，绝不触发租户映射物化。经纪取件会话为装配专属（service.rs:802 派生、
仅注入 CollectionItemSource，无第二持有者），try_get_result 只在经纪主循环单任务串行臂
（initialize_observer、try_assign_item_from_key 均在 handle_broker_event 内）被调用，无跨任务
并发，符合会话上下文单写者约束。附带修正一项潜在缺陷：此前经纪取件写回的 AOF 信封记录键
也固定在根域，切换虚拟域后 AOF 落回条目真实域。

## 涉及文件（全部在 wnode，wcol/wkv/wval/wbase 零改动、仅复用其单点）

- resp/resp_server_session/pump.rs：park_broker_wait 与 notify_collection_update 折叠并泛型化。
- resp/objects/list_commands/blocking.rs、resp/objects/sorted_set_commands/blocking.rs：
  六处 park_broker_wait 调用补传 store。
- resp/objects/list_commands/write.rs、resp/objects/sorted_set_commands/write.rs：十处
  notify_collection_update 调用补传 store。
- resp/garnet_api/mod.rs：StoreGarnetApi::notify_collection_update 就地折叠。
- resp/objects/collection_item_source.rs：try_get_result 解折叠、切虚拟域、以裸键驱动出件、
  结果回裸键、BLMOVE notify_key 重折叠。
- 测试：resp_blocking_commands.rs 客户端句柄暴露执行域，追加跨租户、跨库回归。

## 测试计划

经 GarnetApiFace::set_context 把测试连接的执行域置于指定 (ns, db)：
- 跨租户不越权：ns1 写入队列值后，ns2 阻塞同名键到期空回，ns1 阻塞同名键正常取到值。
- 跨库不越权：db1 写入、db2 同名键阻塞空回、db1 阻塞取到值。
- 跨租户唤醒定向：ns1、ns2 两阻塞者同挂同名键，仅 ns1 侧推入唤醒 ns1，ns2 到期空回。
既有端到端用例全走根域，折叠加解折叠对其透明，无回归。

## 剩余风险

- 同 (ns,db) 内以同一裸键名混用列表队列与有序集合队列，二者折叠键相同、共用一条观察者
  队列：与改动前及 C# 原生按裸键索引行为一致，非本票扩大范围。
- set_virtual_context 依赖经纪主循环单任务调用 try_get_result 的既有前提，已在代码注释标注；
  若日后出现并发第二取件点须改回按调用传入独立会话。

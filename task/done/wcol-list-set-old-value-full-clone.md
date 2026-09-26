归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 4a36395（P3），收口形态：list_set 三行零拷贝——element 局部先记账、std::mem::replace 整包移出旧值免 clone、后减旧账（i64 加减换序有注释防修回），生产净 −1、内联测试三例（四类应答帧矩阵／heap 差值恰等记账差／probe 覆写除 to_vec 外零分配，clone 回苏即红）。续排注：票面「tests/ 既有 list 套件」锚不实，实为文件内联 lib 测试（probe 分配器约束），按现码就地扩例已注提交信息。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P3
核验记录（现码复跑，非票面背书）：
1 rust 锚亲验：list_object_impl.rs list_set 现读 `let old = self.list[index as usize].clone()` 后仅以 &old 调 update_size(false) 即弃——缺陷在位未灭失；update_size（list_object.rs:224-236）现读只消费 item.len() 计 round_up_ptr+SLOT*2 不读字节，clone 纯为绕借用冲突坐实。
2 C# 锚亲验：ListObjectImpl.cs ListSet 现读 :332-334 `UpdateSize(targetNode.Value, false); targetNode.Value = element; UpdateSize(targetNode.Value);`——引用直传零字节复制对位属实。
3 查重：deviations.md LSET 命中区系 §19 缺键幻键/解析面（正交）；task/{done,ing,issue,reject} 无 list_set 记账面同轴票。
4 架构合规与可执行度：mem::replace 三行零拷贝形态不新增 API 不动 update_size 签名、记账公式保持单点，「len 记账变体」已裁为可选非必要不引入——最小改动；测试点（四类应答帧回归 + heap_memory_size 差值断言 + 分配探针先例）闭环，单套机制合规。定级 P3：热路径效率缺陷，无数据面/应答分叉。

审核结论：通过（审核席 zcode-r23-review-wcol，2026-09-26）
逐锚亲验：rust 侧 list_set（list_object_impl.rs:326-329）clone 后仅 :327 以 &old 调 update_size，update_size（list_object.rs:224-236）只消费 item.len() 计 round_up_ptr(len) + SLOT*2、不读字节内容，clone 纯系绕开 update_size(&mut self) 与 &self.list[idx] 的借用冲突、非数据需要，mem::replace 可解，clone 非必要成立；C# ListSet（ListObjectImpl.cs:329-332）亲验为 UpdateSize(targetNode.Value, false) → targetNode.Value = element 引用赋值 → UpdateSize(targetNode.Value)，节点值引用交接零字节复制，「引用直传零拷贝」说法属实；记账本身无误（clone 的 len 与原值一致、即刻释放），危害定性为热路径效率缺陷，准确；deviations.md 无同面登记（§19/§406 LSET 相关系缺键幻键与解析点订锚，正交），task/ 全目录无重复票。

优化执行方案（供 task/fix.md 直接消费，较原案精简为单点最小改动）：
1. list_set 记账与换值改零拷贝三行形态：let element = args[1].to_vec() 后先 self.update_size(&element, true)（element 尚在局部，无借用冲突），再 let old = std::mem::replace(&mut self.list[index as usize], element) 旧值整体移出零复制，末尾 self.update_size(&old, false)（old 已移出 self，&old 与 &mut self 无冲突）——不新增任何 API、不动 update_size 签名，记账公式仍单点，i64 加减可换序、false 分支 debug_assert 底线不减穿
2. 原案「按 len 记账单点变体」降级为可选：仅当执行席认为持有移出旧值不雅时才抽 account_len(len, add) 私有助手并令 update_size 转调（公式仍一处定义），非必要不引入
3. 测试验证点：list_set 行为回归（四类应答帧与 result1 不变，对位 r19-listset L8 矩阵）；补断言 LSET 覆写大元素前后 heap_memory_size 差值恰为 (round_up_ptr(new_len) + SLOT*2) - (round_up_ptr(old_len) + SLOT*2)；有条件时以分配计数探针（同文件 LPOS 零分配测试 probe 先例）断言 LSET 路径零额外分配

ListObject::list_set 为记账读长度对旧值整包 clone，LSET 热路径每次多付一次 O(元素长) 分配与拷贝（C# 以节点引用直传零拷贝）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ListSet（garnet/libs/server/Objects/List/ListObjectImpl.cs:300-338）对被覆写节点的旧值以引用直传记账：UpdateSize(targetNode.Value, false) 与 targetNode.Value = element 后 UpdateSize(targetNode.Value)（:329-332），全路径零字节复制（LinkedList 节点值引用交接，element 数组直接入节点）。应答与记账语义均不复制旧元素。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧 list_set（wedb/wcol/src/list/list_object_impl.rs:326-329）执行 let old = self.list[index as usize].clone() 后以 &old 调 update_size(false)，再以 &element 调 update_size(true)，最后赋值覆盖。update_size（list_object.rs:224-236）只读 item.len() 计 round_up_ptr(len) + SLOT*2，不消费字节内容；clone 系整包堆分配加逐字节复制，纯粹为绕开 update_size(&mut self, &[u8]) 与 &self.list[idx] 的借用冲突，而非数据需要。C# 对位路径零拷贝，rust 侧多付一次分配与拷贝属转写自加重负。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
LSET 为热路径写命令，每次执行多一次 O(旧元素长) 的堆分配与 memcpy：大元素列表（如数百 KB 元素的队列类业务）单命令放大为同等量级的无效内存带宽与分配器压力；记账本身无误（clone 的 len 与原值一致，旧 clone 即刻释放）。无 panic 面、无应答分叉，属效率缺陷而非语义缺陷，量级随元素长度线性放大。

涉及代码：
rust 文件与函数：
wedb/wcol/src/list/list_object_impl.rs:ListObject::list_set（:326-329，:326 clone 仅为 :327 读 len）

对应 c# 文件与函数：
garnet/libs/server/Objects/List/ListObjectImpl.cs:ListSet（:329-332，targetNode.Value 引用直传 UpdateSize 零拷贝）

精炼执行方案：
1. list_set 改零拷贝形态：先记 let idx = index as usize 与 let new_len = element.len()，以 std::mem::replace(&mut self.list[idx], element) 移动换入新值（旧值移出无复制），随即 drop 旧值
2. 记账单点保持：为 ListObject 增按 len 记账的单点变体（update_size 转调该变体，条目构成公式仍一处定义），list_set 以旧值 len 与 new_len 各调一次，消除借用冲突根源而非以 clone 绕行
3. 测试验证点：list_set 行为回归（四类应答帧与 result1 不变，对位 r19-listset L8 矩阵）；补一条断言 LSET 覆写大元素前后 heap_memory_size 差值恰为 round_up_ptr(new_len) - round_up_ptr(old_len)；有条件时以分配计数探针（同文件 LPOS 零分配测试 probe 先例）断言 LSET 路径零额外分配

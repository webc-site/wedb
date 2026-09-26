归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 8b2bd79（P2），收口形态：四分值更新臂（add/sorted_set_add/increment/geo_add）弃 to_vec+Arc::from 新建，改经 dict_handle/get_key_value 单查取回字典存活句柄复用，双索引真 Arc 共享；判定改入参切片免中间 Vec；src 净 +18（机制口径入提交信息）、测试 +47 钉账（heap 恒等＋ptr_eq 全配对，回退即红）。续排注：零 deviations 登记。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P2
核验记录（现码复跑，非票面背书）：
1 四更新臂现码亲验：sorted_set_object.rs:334 get_mut 更新臂 `let m = Arc::<[u8]>::from(member.to_vec())` 新建句柄进树；sorted_set_object_impl.rs:424/:586 与 geo_impl.rs:96 更新臂同形新建 Arc——字典保旧键仅值更新、树收新 Arc 的双份驻留现状成立，未灭失。
2 机制面复验：wbase/src/map.rs:47 `pub type HashMap<K,V> = collections::HashMap<K,V,GxBuildHasher>` 现读在位（std insert 既有键保旧键仅更值语义）；dict_handle（sorted_set_object.rs:736 现读 `fn dict_handle(&self, key:&[u8]) -> Option<Arc<[u8]>>`）正确工具在位未用属实；lib.rs should_promote/should_demote 体积维直吃 heap_memory_size——MEMORY USAGE 虚低与升降阶失真链成立。
3 C# 锚亲验：SortedSetObjectImpl.cs:184-190 更新臂 sortedSet.Remove/Add 形态现读在位；票面如实披露 C# 同形双份系上游既有、本案定性为 rust 自有契约（「一处分配多处持有」注释与 account_entry 单份口径）未兑现——非要求对齐 C#，定性正确不违对标纪律。
4 查重：deviations.md 无同面登记；四池无同轴票；zset_heap_accounting.rs 仅锁新增臂 per_entry（审核席亲验），更新臂缺口未暴露属实。
5 架构合规与可执行度：取回字典既有句柄复用为唯一收敛方向（get_key_value/get_mut 两形态等价任选），不动记账公式（单份口径即法定形态），消除更新臂 to_vec+Arc::from 两段分配符合零拷贝准则；测试点（反复更新 heap 恒不变 + Arc::ptr_eq 同句柄 + 应答回归）闭环。定级 P2：记账失真与双份驻留属资源防护面（无 RESP 行为分叉）。

审核结论：通过（审核席 zcode-r23-review-wcol，2026-09-26）
逐锚亲验：四臂新建 Arc 现码确认（sorted_set_object.rs:334 / sorted_set_object_impl.rs:424 / :586 / geo_impl.rs:96）；wbase/src/map.rs:47 即 std::collections::HashMap 换 GxBuildHasher，insert 对既有键保留旧键仅更新值，双份驻留成立；SortedSetEntry::cmp 委托 SortedSetComparer 按 (score, 成员字节) 判序，remove 新句柄按字节等值可移除旧条目，零 RESP 行为分叉；dict_handle（:736-741）在位、set_expiration 在用，更新臂未用；account_entry（:775-782）按单份实计而实际双份，漏报成立；lib.rs should_promote/should_demote 体积维直吃 heap_memory_size（:39 注释自证），TIERED 失真面成立；C# 侧同形亲验（SortedSetObjectImpl.cs SortedSetAdd :186-190 sortedSetDict[member]=score 保旧键 + sortedSet.Add 新数组、SortedSetIncrement :347-351、SortedSetGeoObjectImpl.cs :76-84，UpdateSize 仅 :206/:322/:697/:722 调用，更新臂不调）——票面如实披露 C# 双份驻留系上游既有形态，本案定性为 rust 自有契约（字段注释 :158-159、account_entry 注释 :771-773、四处臂内注释、单份记账口径）未兑现，非要求对齐 C#，定性正确；deviations.md 无同面登记，task/ 全目录无重复票；zset_heap_accounting.rs 仅锁新增臂 per_entry 构成，更新臂缺口确未被测试暴露。

优化执行方案（供 task/fix.md 直接消费）：
1. 四个更新臂统一改取字典共享句柄：判定入口的 get(member) 换 dict_handle 等价的 get_key_value 单次查找（句柄 None 即落新增臂，选项门判定序不变），树 remove 旧条目 + insert 新条目共用取回的同一 m；字典分值改写两形态任一——get_mut 原地改写（SortedSetObject::add 现形即正确骨架，仅 :334 的 Arc::from(member.to_vec()) 换句柄）或 insert(m.clone(), score)（m 为旧句柄时 insert 保旧键仅更新值，与 get_mut 等价，sorted_set_add/sorted_set_increment/geo_add 现形只需替换 Arc 来源即回归真共享）
2. 消除更新臂中间分配：sorted_set_add/sorted_set_increment/geo_add 更新臂不再执行 member to_vec 后 Arc::from 两段分配复制，判定与守卫用入参切片、句柄经字典取回（引用计数自增，零字节复制）；SortedSetObject::add 同步去 to_vec
3. 订正 account_entry 与四处「一处分配多处持有」注释为实现事实陈述（修复后为真），字段注释 :158-159 核对无失实残留
4. 测试验证点：zset_heap_accounting.rs 补更新臂断言——同成员反复 ZINCRBY/ZADD/GEOADD 更新分值 N 次后 heap_memory_size 恒不变、Arc::ptr_eq 断言字典键句柄与 sorted_set 迭代取出的成员句柄指针同一；既有 wnode/tests/tiered_cmds_align.rs 与 zset 命令族测试回归确认应答零变化；禁止改动记账公式本身（round_up_ptr + SLOT*4 单份口径即法定形态，修复的是实现兑现而非口径）

zset/geo 分值更新臂新建成员 Arc 未复用字典共享句柄，成员字节双份驻留致记账漏报与热路径全量复制，四处「一处分配多处持有」声明失实

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# SortedSetAdd 更新臂（garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:150-157，sortedSetDict[member] = score 后 sortedSet.Remove/Add 新元组）中 Dictionary 索引器同样保留旧键数组、SortedSet 收新数组，C# 侧即存在成员字节双数组驻留且更新臂不调 UpdateSize（SortedSetObject.cs:812 仅 :206 Add 新增臂与 :322 反序列化调用）；SortedSetIncrement、SortedSetGeoObjectImpl.cs GeoAdd 更新臂（:76-84）同形。即 C# 原型对更新臂无共享句柄承诺，双份驻留系 C# 既有形态。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧 zset 以 HashMap<Arc<[u8]>, f64> + BTreeSet<SortedSetEntry> 双索引共享同一 Arc 为明定设计：sorted_set_object.rs:157-165 字段注释、account_entry（:774-782「member 字节全容器仅一份……此处计一份即足额」）与四处更新臂内注释（sorted_set_object_impl.rs:423、:585「一处分配多处持有：散列与有序视图共享同一 Arc」、sorted_set_object.rs:343、geo_impl.rs:81）均声明单份驻留。但四个分值更新臂实际执行 let m = Arc::<[u8]>::from(member) 新建分配（sorted_set_object.rs:334 SortedSetObject::add 更新臂、sorted_set_object_impl.rs:424 sorted_set_add 更新臂、:586 sorted_set_increment 存量臂、geo_impl.rs:96 geo_add 更新臂），随后 sorted_set_dict.insert(m.clone(), score)——std HashMap（wbase/src/map.rs:47 即 std::collections::HashMap 换哈希器）insert 遇既有键仅更新值、保留旧键分配，新 Arc 只进 BTreeSet：字典持旧 Arc、有序视图持新 Arc，两份成员字节并存，注释声明与 account_entry 记账口径（按单份实计）双双落空。新增臂（sorted_set_object.rs:344、impl:384、:599、geo:82）为新键插入，Arc 真共享，无此问题。同文件已有正确工具 dict_handle（sorted_set_object.rs:736-741，get_key_value 取字典内共享句柄克隆，set_expiration 在用），更新臂未用。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
其一，记账失真：heap_memory_size 按单份成员字节实计，被更新过 分值的成员实际驻留两份（字典旧 Arc + 树新 Arc），每个此类成员漏记 round_up_ptr(len) 与一份 Arc 分配头；MEMORY USAGE（object_heap_estimate 直读该值）虚低，TIERED_PROMOTE_BYTES/DEMOTE_BYTES 体积维升降阶判定（should_promote/should_demote 以 heap_memory_size 为输入）双向失真——大成员高分值更新频率场景（如计数器语义的 ZINCRBY）误差可累积至半数字节数级。其二，热路径分配与复制浪费：ZINCRBY 存量臂每调用执行 member to_vec 一次复制 + Arc::from(Vec) 再一次分配复制，GEOADD/ZADD 更新臂同形；以 dict_handle 克隆（引用计数自增）即可零字节复制，排行榜类 ZINCRBY 高频键上为纯无效内存带宽。其三，注释与实现矛盾：四处注释与记账口径单点注释声明「消成员字节双份驻留」，后续维护者会据假声明做优化与排障判断（wcol/tests/zset_heap_accounting.rs 仅锁新增臂 per_entry 构成，未锁更新臂，缺口未被测试暴露）。无 panic 面、无 RESP 行为分叉（成员等值按字节比较，双份分配不影响语义）。

涉及代码：
rust 文件与函数：
wedb/wcol/src/zset/sorted_set_object.rs:SortedSetObject::add（更新臂 :330-341，:334 新建 Arc）
wedb/wcol/src/zset/sorted_set_object_impl.rs:SortedSetObject::sorted_set_add（更新臂 :423-431，:424 新建 Arc）
wedb/wcol/src/zset/sorted_set_object_impl.rs:SortedSetObject::sorted_set_increment（存量臂 :585-596，:586 新建 Arc）
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_add（更新臂 :93-108，:96 新建 Arc）
wedb/wcol/src/zset/sorted_set_object.rs:SortedSetObject::dict_handle（:736-741，在位未用的共享句柄工具）
wedb/wcol/src/zset/sorted_set_object.rs:account_entry（:774-782，单份口径记账依据）

对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd（更新臂，sortedSetDict 索引器保留旧键 + sortedSet.Add 新数组）
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetIncrement
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoAdd（更新臂）
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Add（存储 API 新增臂，无更新形态）与 UpdateSize（:812，更新臂不调用）

精炼执行方案：
1. 四个更新臂统一改用 dict_handle 取字典内共享句柄：let Some(m) = self.dict_handle(&member) 判定序重排（先取句柄后判选项门，句柄 None 即回到新增臂），字典分值经 get_mut 原地改写（SortedSetObject::add 更新臂现形 *old_score = score 即正确骨架，仅 :334 的 Arc::from(member.to_vec()) 换 dict_handle 克隆），BTreeSet remove 旧条目 + insert 新条目共用同一 m，双索引回归真共享
2. 同步消除更新臂的中间分配：sorted_set_add/sorted_set_increment/geo_add 更新臂不再执行 member to_vec 后 Arc::from 的两段分配（判定与守卫用入参切片，句柄经 dict_handle 取得）
3. 订正 account_entry 与四处「一处分配多处持有」注释为实现事实陈述（修复后为真）
4. 测试验证点：zset_heap_accounting.rs 补更新臂断言——同成员反复 ZINCRBY/GEOADD 更新分值 N 次后 heap_memory_size 恒不变、Arc::ptr_eq 断言字典键与 BTreeSet 成员句柄同一（经 get_key_value 与 sorted_set 迭代取句柄）；既有 wnode/tests/tiered_cmds_align.rs 与 zset 命令族测试回归确认应答零变化

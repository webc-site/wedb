甄别结论：通过（甄别席 J5，2026-09-27，定级 P3——ACL LIST 扩容窗瞬态漏报，迁移完成自愈，鉴权/点读不受影响，修复单点小）。亲验：acl_store.rs for_each_user 链首校验确为裸 store.index.load().find_tag(key)；windex/src/find.rs:21 find_tag 纯 hash+链探针无协同；wkv/src/store/resize.rs 相位状态机 Rest→PrepareGrow→InProgressGrow（:510 发布）在位，未迁分块新表桶恒空成立；wkv/src/session/mod.rs:722 find_tag_cooperative 单点在位；写路径「旁路探针前先 ensure_split」纪律于 wkv/src/session/raw/write/mod.rs 亲验自证；头注「与 scan_cursor 同口径」确失真。C# 锚按审核订正亲验：garnet 无 ACLStorage.cs，实为 garnet/libs/server/ACL/AccessControlList.cs:24 _userHandles。方案改协同单点消费、错误沿既有 Result 轨上抛，零第二机制。查重：r135c-rehash 案一仅收 scan_cursor 面，for_each_user 为另一消费点，不重复。派沙箱席 c01f。

审核通过（2026-09-27）：缺陷与危害逐锚点亲验属实——acl_store.rs:172 确为裸 find_tag（windex/src/find.rs:21 纯 hash+桶探针无协同）；resize.rs:477-510 先建空新表、切表后发 InProgressGrow 相位，未迁分块桶在新表恒空，链首校验采 None 即漏报，全量迁移完成后自愈；头注失真属实（array_key_iteration_functions.rs:125 scan_cursor 活键判定已走 find_tag_cooperative）；写路径先协同后探针纪律属实（raw/write/mod.rs:183-187、211）。方案增补：闭包内 wkv 迁移内核错误沿 live_probe_err 同口径（array_key_iteration_functions.rs:51，wkv Error 折 HLogError 经 whlog Err 通道、外层 scan_err）上抛，勿新建第二套错误轨；接线点唯一（for_each_user 唯一裸探针，调用方仅 network_acl_snapshot，错误承接 RESP_ERR_ACL_STORE_SCAN_FAILED 已在位）。C# 锚点勘误：garnet 无 ACLStorage.cs，实为 garnet/libs/server/ACL/AccessControlList.cs:24 _userHandles（ConcurrentDictionary，ACLCommands.cs:70 弱一致枚举），契约事实不变，执行时按此路径对齐。

ACL 用户枚举链首校验用裸 find_tag 无扩容协同，IN_PROGRESS_GROW 窗漏报用户且头注同口径声明失真

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ACL 用户表驻 ConcurrentDictionary（garnet/libs/server/Acl/ACLStorage.cs），ACL LIST 枚举走弱一致枚举，无 rehash 探针窗，任何时刻可枚举全部在场用户，不存在扩容期漏项窗口。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
Rust 将 ACL 用户物化到 hlog+哈希索引后，for_each_user（wedb/wnode/src/resp/acl_store.rs:159-186）链首校验在 :172 用 store.index.load().find_tag(key) 裸探针——无 ensure_split 协同。索引 IN_PROGRESS_GROW 进行期未迁分块的桶在新表恒空，find_tag 采得 None != Some(addr)，该用户当前最新版本被按「链首不符」剔除，本轮 ACL LIST 漏报该用户。头注 :150-153 声称「与 array_key_iteration_functions::scan_cursor 同口径」已失真：scan_cursor 活键判定已走 find_tag_cooperative 协同单点（票 zcode-r135c-rehash 案一修复，wkv/src/session/mod.rs:708-726），本处漏接；写路径同类探针均已先 ensure_split（wkv/src/session/raw/write/mod.rs:187-190 自证纪律）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
扩容迁移窗内 ACL LIST 瞬态漏项（迁移完成后自愈）；ACL 鉴权/点读路径已协同不受影响；失真头注会误导后续复审按 SCAN 纪径误信本处已协同。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/acl_store.rs:for_each_user
wedb/wkv/src/session/mod.rs:find_tag_cooperative

对应 c# 文件与函数：
garnet/libs/server/Acl/ACLStorage.cs:ACLUsers（ConcurrentDictionary 弱一致枚举）

精炼执行方案：
1 for_each_user :172 链首校验改经 find_tag_cooperative（迁移内核错误沿 Result 上抛，严禁折成跳过/剔除）
2 头注「同口径」声明随修复恢复真实，或改述为协同单点消费面
3 测试验证点：并发扩容进行期 ACL LIST 全量用户可见（可用大用户数触发 IN_PROGRESS_GROW 后并发枚举断言全集）

收口记录（收票席 R4 批次，2026-09-28）：合入 e76df226（验货 71506c7d+dev 前进复查）。收口形态=for_each_user 链首校验改经 wkv find_tag_cooperative 协同单点（先分裂协同后探针，与点读/扫描族一套机制），迁移内核错误沿既有 live_probe_err whlog Err 通道上抛、外层 scan_err 收口，严禁折成剔除（扩容窗裸 find_tag 误剔活用户致 ACL LIST 瞬态漏报收口）；C# 锚 AccessControlList.cs:24 _userHandles 弱一致枚举双侧亲验。锁测 acl_for_each_user_sees_all_users_during_grow_window（手工装配 InProgressGrow 部分滞留窗）回装裸 find_tag 即红。deviations 无需新增（回归 C# 契约）。

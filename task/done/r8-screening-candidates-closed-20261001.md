r8 波甄别席淘汰档（wcol / wtxn 两只只读席 + 主控复跑，2026-10-01，基线 7397ed1）

本档只登记「疑点经双侧现码亲验不成立」或「已在册不另立案」的候选，防后续席重复开掘。

## 淘汰 1：删空自愈路径键级 TTL 旁路残留（拟 P2 → 不立案）
疑点：集合删空只写墓碑不清 TTL，留「值亡 TTL 存」。双侧不成立：
- C# 侧删除即记录体连同 Expiration 元字段一体消失（DeleteMethods/SaveObject 单漏斗）。
- rust 侧异步收口 rmw_helpers.rs:720-731 空+分层臂走 bftree_drain(keep_ttl=false)、空+信封臂走 delete_string；
  wkv 删单点 collection.rs:104/:112/:179 del_ttl 级联；同步核 wkv/src/session/raw/write/mod.rs:516-555
  亦有 try_strip_ttl_sync_unprotected_with_prefix（:567 起）在墓碑后随剥；object_store_utils.rs:1233 obj_save_or_gc_sync 走同核。
- 崩溃窗「值亡 TTL 存」形态已在册：done/wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal
  （无主 TTL 判死丢弃 + GC 回收，良性自愈）。

## 淘汰 2：zset 成员级 TTL「幽灵账」（拟 P2 → 不立案）
疑点：sorted_set_object_impl.rs:660-662 set_expiration 遇 KeyAlreadyExpired 仅 remove_member 不清 ExpiryLedger。
C# 同形亲验：SortedSetObject.cs:713-725 该臂同样仅 sortedSetDict.Remove + sortedSet.Remove + UpdateSize、
不动 expiration 字典——1:1 忠实移植，quirk 归上游；hash 侧清账差异亦系 C# 原形。非登记分叉，非缺陷。

## 并案 3：read_varsize_iid 读失败与键缺失同形（不另立案）
某席报告以「data_provider.rs 区段 read_varsize_iid 失败语义倒置（false=命中 / true=miss）」转述。主控现码复跑否证其形：
- 定义单点 wvector/src/store.rs:351 read_varsize_iid：经 read_bool_raw 下发读，闭包命中才置 result=Some(..)，
  返回 None 同时覆盖「确无此键」与「IO 读失败」两形——是**折叠同形**，不存在 false/true 取反的倒置。
- 该「回调层 IO 故障与缺失同形」面已在册并明文备案另案：§181（done/wvector-store-read-failure-folded-to-empty-missing），
  收口落点 wnode/src/resp/vector/vector_store_callbacks.rs 系同侪在途禁触域，按 禁跨域代修 不另开票。
- 数据面安全性复验：Q8 装载臂（provider/data_provider.rs:313-321）在量化状态读 None 时先过
  `start_point_cache.contains_key(&0) → Err(InvalidQuantizer)` 硬错门，有数据在场面不会静默新建量化器，
  无「按新 scale 重解旧量化码」的腐败臂；to_external_id（:625-637）把 None 折成 Err(StoreError::Read) 透明上抛。

## 淘汰 4：版本轨/锁轨双轨、finish_abandoned、EXPIRE/PERSIST/DELETE/rename 版本 bump（wtxn 席自排）
双轨系 §115 自陈在册形态；finish_abandoned 已由 done 票收口；EXPIRE/PERSIST/DELETE/rename 各臂版本推进
双侧亲验齐备，无缺臂。txn_keys u32 偏移截断需 4GB 级队列前提，判弱不立案。

优先级：低
来源：next/agy.db.md 条 17 与 next/muse.db.md 条 16 两轮同题合并（agy 原引证
WedbStore::compact 在 store/gc.rs 有漂移，实际定义在 compact.rs）。取证基线：主仓 dev 当下代码。

问题
紧缩驱动主从入口不分：wcompact LogCompactor 的裸 compact/compact_with_filter/
compact_lazy 三入口均为 pub，绕过 wkv 编排即跳过 VDB 换号过滤、TTL 判死与 CPR 纪元
屏障；wkv 侧 WedbStore::compact 门面已注明「生产入口」但 wcompact 侧无对称防线，
新人可直连底层造成物理记录误判活。

取证
- wedb/wcompact/src/compactor/mod.rs:278 pub async fn compact（裸入口）、:169
  compact_with_filter、:260 compact_lazy。
- 生产链单点：wedb/wkv/src/compact.rs:265 pub async fn compact（doc 已注明对标
  TsavoriteKV.Compact、生产入口显式注入 WedbCompactionFunctions）、:258 compactor、
  后台驱动 wedb/wkv/src/gc.rs:657 try_compact -> store.compact。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs
  紧缩内核；garnet/libs/server/Databases/DatabaseManagerBase.cs:449 注入
  GarnetRecordTriggers（业务判死只在 server 层注入，native 层裸内核不对外直发）。

修法建议
wcompact 裸 compact 方法族收紧：compact（无过滤版）删除或改 pub(crate) + doc 注明
仅测试专用；compact_with_filter / compact_lazy 保留 pub 但 doc 标注「须由 wkv
WedbStore 门面注入业务判死后调用，直连仅限 wcompact 测试」；wcompact 内 tests
直调点同步改。收口后生产唯一链 = GcManager::try_compact -> WedbStore::compact ->
LogCompactor。与 next/db-wkv-gc-split.md（gc.rs 拆分引用同链）无文件冲突可并行。

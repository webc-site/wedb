use std::sync::Arc;

use wdev::Device;
use wval::KeyTag;

use super::range_index_blocking;
use crate::{
  error::{Error, Result},
  session::StoreSession,
  store::StoreEvent,
};

impl<D: Device> StoreSession<D> {
  /// 树态键排空回收单点：双物理域原子墓碑（信封域 + 元记录）+ BfTree 树实例注销 +
  /// 换号旁表回收 + RangeIndexDrop AOF 入账；删键臂（keep_ttl=false）级联清理信封、
  /// 元记录与随键 TTL/ETag 旁路记录，旁路清退由 `keep_ttl` 分流（口径详见
  /// `drain_and_delete_collection_meta` 文档：删键臂 false 清 TTL/ETag
  /// 杜绝孤儿，降阶迁移臂 true 只墓碑元记录、不碰 TTL/ETag 旁路，对标 C#
  /// 对象记录重写原样前移 HasExpiration/HasETag、零发 TTL 事件）。
  ///
  /// 失败分级（[`Error::Swapped`] 错误面，供分层臂 WATCH 栅栏判别置脏去留）：
  /// 信封墓碑 / 元记录墓碑失败 = 键未消亡、树内容零变更，原样上抛；元记录
  /// 墓碑已落盘后的失败（del_ttl/del_etag/树注销/AOF 入账）= 键已死、树物理面已变更，
  /// 包装分级上抛保持置脏真实推进。
  ///
  /// 删键臂（keep_ttl=false）连带对信封域写幂等墓碑，两域回收一处收口：
  /// 升阶是非原子三步写（先发流块、再落元记录、后删信封），崩溃、AOF 截断
  /// 部分回放或信封删除 IO 失败都会留下「信封旧快照 + Meta 存根 + 树」双态
  /// 残留；双态期命令路由 Meta 优先读写无误，但排空臂若只清 Meta 域，删空后
  /// 命令回落信封域探测（`contains_key` / `read_tag_with` 双域臂），已删空集合
  /// 以升阶时刻的完整旧数据幽灵复活。C# 集合恒驻对象域单一物理域，删记录即
  /// 连尾随字段同亡、绝无第二域残留（libs/server/Storage/Functions/
  /// ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker 的 HasRemoveKey →
  /// ExpireAndStop 与 DeleteMethods 删除臂）；本仓分层态键消亡必须两域齐清，
  /// 落实 SKILL.md「严格删空生命周期与原子墓碑，杜绝幽灵空元记录」。墓碑先于
  /// 元记录落笔：命令窗口内回落域先死、路由域后死，杜绝中途幽灵读。迁移臂
  /// （keep_ttl=true）绝不触碰信封域：键全程存活，降阶臂随后 obj_save 写回新
  /// 信封，墓碑即自相残杀；分层重灌臂经 promote 直接换树、不再经本函数（先建
  /// 后拆，见 promote_collection_to_bftree）。纯 RangeIndex 键
  /// 无信封记录，delete_raw 哈希探针落空零写零入账（幂等），与 DEL 复合臂
  /// [`crate::session::StoreSession::delete`] 的双域墓碑共用 delete_raw 同一
  /// 原语，不新增第二条信封删除路径。复制面同口径：本函数末位入账的
  /// RangeIndexDrop 镜像为 StoreDelete(Meta) 条目，副本回放臂只以
  /// keep_ttl=true 形排空 Meta 域（见 wnode `AofProcessor::store_delete`），
  /// 信封域与 TTL/ETag 旁路的消亡各由主端逐域镜像条目承接，跨域级联一律禁止
  ///——级联会把懒降阶刚落盘的信封连同随键 TTL 在副本上抹成整键消失。
  pub async fn handle_bftree_drain_and_delete(&self, key: &[u8], keep_ttl: bool) -> Result<()> {
    // 删键臂信封域幂等墓碑（回落域先于路由域消亡）
    if !keep_ttl {
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      self.delete_raw(&env_k).await?;
    }
    self.drain_and_delete_collection_meta(key, keep_ttl).await?;
    // 树身份键 = 物理 Meta 键（与上方元记录墓碑同键），跨库同名键的树注册与
    // 数据文件按物理域隔离。树注销/文件删除失败时元记录墓碑已落盘——键路由域
    // 已死、树物理面已变更，经 [`Error::Swapped`] 分级上抛（分层臂 WATCH 判据
    // 据此保持置脏，推进真实反映变更）；孤儿树文件由 migration-tmp 启动清扫与
    // 惰性恢复收敛
    let mgr = Arc::clone(&self.store.range_index);
    let del_key = self.session_meta_key(key);
    let _ = range_index_blocking(move || {
      mgr
        .delete_index(&del_key)
        .map_err(|e| Error::swapped(e.into()))
    })
    .await??;
    // 删除面唯一收敛点的旁表逆操作：树已销毁，同步注销换号回收登记
    self.unregister_bftree_key(key);
    // 事件域取会话物理域（与本键记录前缀同源），入账键方与记录落域一致。
    // AOF 入账失败按 error.rs AofEnqueue 契约上抛拒绝本命令：墓碑与树注销
    // 均已落盘、键已消亡，经 Swapped 分级保持分层臂 WATCH 判据真实推进
    let (ns, db) = self.virtual_domain();
    self
      .store
      .emit_event(
        self.aof_session_id,
        StoreEvent::RangeIndexDrop { ns, db, key },
      )
      .map_err(Error::swapped)
  }
}

//! 冷上下文挂起待物化面（严格会话 `set_context` 报告映射未装载时的
//! 暂存载荷与落位物化单点）。

use std::sync::Arc;

use wacl::UserHandle;

use super::core::RespServerSession;

/// 冷上下文挂起待物化面（严格会话 `set_context` 报告映射未装载时的暂存载荷：
/// 目标 (ns, db) 与认证 / HELLO 元数据落位载荷。会话标量在装载确认前严禁
/// 提前覆写——对标 C# `TryGetOrSetDatabaseSession` 的 success 门（只有底层
/// 就绪才 `SwitchActiveDatabaseSession`），装载失败即弃本载荷，旧标量原样）
pub(super) struct ColdContextPending {
  /// 目标命名空间
  pub(super) ns: u64,
  /// 目标库（纯切库臂物化为 `active_db_id`；认证臂切租不改库）
  pub(super) db: u64,
  /// 认证落位载荷（None = 纯切库 SELECT 臂）
  pub(super) auth: Option<ColdAuthCommit>,
  /// HELLO 元数据落位载荷（协议版本 + 客户端名；随认证臂挂起一并暂存）
  pub(super) hello: Option<ColdHelloCommit>,
}

/// 认证落位暂存载荷（句柄 + 读前采样的挂载代数 + 来源标记）
pub(super) struct ColdAuthCommit {
  pub(super) user_handle: Arc<UserHandle>,
  pub(super) generation: Option<u64>,
  pub(super) from_store: bool,
}

/// HELLO 元数据暂存载荷（冷租户认证挂起未确认前严禁单向脏写的会话元数据）
pub(super) struct ColdHelloCommit {
  pub(super) resp_protocol_version: Option<u8>,
  pub(super) client_name: Option<String>,
}

impl ColdContextPending {
  /// 挂起载荷物化单点：SlowWait 成功应答回写时执行，会话标量就此与底层
  /// StoreSession 物理域对齐（物理域切换已在装载 future 内经 `set_context`
  /// 重放完成；本函数只落外层镜像，杜绝双写实现）
  pub(super) fn materialize_into(self, s: &mut RespServerSession) {
    match self.auth {
      Some(commit) => s.materialize_authenticated_handle(
        commit.user_handle,
        self.ns,
        commit.generation,
        commit.from_store,
      ),
      // 冷库切库成功提交点：与热库臂同经 watch 作废单点（C# Switch 换任面
      // 只在确认就绪后发生，r133c-selectdb 案二；auth 换租分支不物化库标量，
      // 无切库面不触）
      None => {
        s.invalidate_watch_on_db_switch(self.db);
        s.active_db_id = self.db;
      }
    }
    if let Some(hello) = self.hello {
      if let Some(version) = hello.resp_protocol_version {
        s.update_resp_protocol_version(version);
      }
      if let Some(name) = hello.client_name {
        s.set_client_name(Some(&name));
      }
    }
  }
}

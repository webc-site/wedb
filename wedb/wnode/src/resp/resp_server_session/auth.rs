//! 鉴权臂（对标 libs/server/Resp/BasicCommands.cs:NetworkAUTH 与
//! RespServerSession.cs 的 SetUserHandle / AuthenticateUser / CanRunDebug、
//! HELLO 会话状态转换；含 rust 侧改权代数收敛的 ACL 挂载态机械）。

use std::sync::Arc;

use wacl::{User, UserHandle, acl_password_check};
use wconf::ConnectionProtectionOption;
use wdev::Device;
use wresp::{
  catalog::is_no_auth,
  cmd_strings::{self as cs, write_map_len},
  command::RespCommand,
  ext::RespVecExt,
};

use super::core::{REDIS_PROTOCOL_VERSION, RespServerSession};
use crate::resp::{
  acl_commands::{AclAuthOutcome, RESP_ERR_ACL_FOREIGN_NAMESPACE},
  acl_store::AclStore,
};

/// 会话 ACL 挂载态（连接本地，随句柄生死；对标 C# 会话直持共享 UserHandle
/// 时「句柄即最新」的隐含前提——本仓句柄不共享，改权经引擎代数向在途会话广播）
///
/// 唯一失效判据即 RespServerSession::refresh_acl_mount_if_stale 的代数比较，
/// 无第二处判据。
#[derive(Clone, Copy)]
pub(super) struct AclMount {
  /// 挂载时刻的引擎 ACL 变更代数；None = 挂载早于存储执行域注入（装配期
  /// attach_acl 先于 set_garnet_api），首次鉴权预门补采
  generation: Option<u64>,
  /// 句柄是否取自 ACL 存储真源记录：false = 引导期内存单例（requirepass /
  /// nopass 的 default，存储恒无同名记录），陈旧时点查无记录亦维持挂载
  from_store: bool,
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:SetUserHandle
  ///
  /// 挂载用户句柄的唯一出口：同步刷新会话用户名（CLIENT LIST / SLOWLOG 展示面）
  /// 并快照挂载时刻的引擎 ACL 代数（`from_store` 标记句柄是否取自存储真源记录，
  /// 见 [`AclMount`]）
  pub fn set_user_handle(&mut self, user_handle: Arc<wacl::UserHandle>, from_store: bool) {
    self.user_handle = Some(user_handle.user().name.clone());
    self.acl_user_handle = Some(user_handle);
    // 免认证形态（NoAuth / requirepass 直连）无 ACL 判定面可收敛，不登记挂载态
    self.acl_mount = self.acl_authenticator.as_ref().map(|_| AclMount {
      generation: self
        .garnet_api
        .as_ref()
        .and_then(|api| api.acl_generation()),
      from_store,
    });
  }

  /// 跨连接改权收敛预门（会话侧唯一失效判据：挂载代数与引擎 ACL 代数比较）
  ///
  /// C# 的 ACL SETUSER/DELUSER 对全局共享 UserHandle 就地 CAS 换新（libs/server/
  /// Resp/ACLCommands.cs:NetworkAclSetUser），在途会话下一条命令自然读到新权限；
  /// 本仓句柄连接本地持有（零全局内存口径），故改权经 AclStore 写删出口推进
  /// 引擎代数向在途会话广播：代数落后即按会话已绑 `(ns, 用户名)` 点查存储真源
  /// 重建句柄，相等即句柄最新（零存储读，与 C# 共享句柄的免判等成本同阶）
  ///
  /// 位点唯一为鉴权预门 [`RespServerSession::check_acl_permissions`] 入口——每
  /// 命令一次且在批处理纪元保护区外（ACL 点查的冷记录降级为阻塞回读，持守卫
  /// 等待驱逐会自锁，故不可下沉到 `acl_permits`）；Lua `redis.call` 面经外层
  /// 脚本命令预门刷新后的同一句柄判定，与直令面同口径、无第二套判据
  pub(crate) fn refresh_acl_mount_if_stale(&mut self) {
    let Some(mount) = self.acl_mount else {
      return;
    };
    let Some(api) = &self.garnet_api else {
      return;
    };
    let Some(current) = api.acl_generation() else {
      return;
    };
    if mount.generation == Some(current) {
      return;
    }
    let Some(username) = self
      .acl_user_handle
      .as_ref()
      .map(|handle| handle.user().name.clone())
    else {
      return;
    };
    // 记录已删 / 读失败 / 不可解析三态合一：撤销挂载按未认证处理
    let mut adopted = None;
    let mut revoke = false;
    match api.acl_user_record(self.namespace, username.as_bytes()) {
      Some(Ok(Some(record))) => match User::from_rule_bytes(&username, &record) {
        Ok(new_user) => adopted = Some(new_user),
        Err(err) => {
          log::warn!(
            "ACL 命名空间 {} 用户 {username} 规则解析失败，按未认证处理: {err}",
            self.namespace
          );
          revoke = true;
        }
      },
      // 引导期内存单例（requirepass / nopass 的 default）存储恒无同名记录，
      // 维持挂载；命名用户句柄本就取自记录，无记录即用户已删
      Some(Ok(None)) if mount.from_store => {
        log::warn!(
          "ACL 命名空间 {} 用户 {username} 记录已删，按未认证处理",
          self.namespace
        );
        revoke = true;
      }
      Some(Ok(None)) => {
        self.acl_mount = Some(AclMount {
          generation: Some(current),
          ..mount
        });
        return;
      }
      // 存储访问失败：保持现挂载与陈旧代数，下一命令重判（不误撤健康会话）
      Some(Err(err)) => {
        log::warn!(
          "ACL 命名空间 {} 用户 {username} 规则点查失败，本命令沿用现权限并待重判: {err}",
          self.namespace
        );
        return;
      }
      None => return,
    }
    if revoke {
      self.revoke_acl_mount();
      return;
    }
    if let Some(new_user) = adopted {
      self.adopt_acl_user(new_user, Some(current));
    }
  }

  /// 以新规则整体替换挂载句柄（dev 语义：`UserHandle` 为构造即定格的只读快照、
  /// 无共享 CAS 域，换代即重读存储后整体替换 `Arc<UserHandle>`，见
  /// wacl/src/user_handle.rs 类型文档）。会话句柄与认证器镜像同换新 Arc（两处
  /// 恒同一），挂载代数对齐传入值、from_store 置真（句柄取自存储真源记录）
  ///
  /// `generation` 由调用方在点查记录【之前】采得（Acquire 与 bump 的 Release
  /// 配对，保证随后的点查必见该代数下已落盘的记录），杜绝缓存到更新的代数而漏
  /// 掉并发改权；消费串行下无并发写者，整体替换即原子，无需 CAS
  pub(crate) fn adopt_acl_user(&mut self, new_user: Arc<User>, generation: Option<u64>) {
    let handle = Arc::new(UserHandle::new(new_user));
    if let Some(acl) = &self.acl_authenticator {
      acl.lock().user_handle = Some(Arc::clone(&handle));
    }
    self.user_handle = Some(handle.user().name.clone());
    self.acl_user_handle = Some(handle);
    self.acl_mount = self.acl_authenticator.as_ref().map(|_| AclMount {
      generation,
      from_store: true,
    });
  }

  /// 撤销 ACL 挂载：会话句柄与认证器镜像同撤（两处挂载恒为同一 Arc），
  /// 下一命令按未认证落 NOAUTH
  fn revoke_acl_mount(&mut self) {
    self.acl_user_handle = None;
    self.acl_mount = None;
    self.user_handle = None;
    if let Some(acl) = &self.acl_authenticator {
      acl.lock().user_handle = None;
    }
  }

  /// libs/server/Resp/RespServerSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&mut self, resp_protocol_version: u8) {
    self.resp_protocol_version = resp_protocol_version;
  }

  /// libs/server/Resp/RespServerSession.cs:AuthenticateUser
  ///
  /// ACL 认证器挂载时按其校验（定位用户 → 口令比对 → 记录句柄
  /// `aclAuthenticator.GetUserHandle()`）；免认证形态 CanAuthenticate = false
  /// → 恒返回 false（C# 取 accessControlList.GetDefaultUserHandle 兜底，
  /// rust 无 ACL 实例可取，门控对该形态恒放行承接同一网络效果）——
  /// 该分支同面承接
  /// libs/server/Auth/GarnetNoAuthAuthenticator.cs:Authenticate
  /// （Debug.Fail 死桩；免认证档不设独立类型，见 wacl/src/auth/mod.rs 头注）
  pub fn authenticate_user(&mut self, username: &[u8], password: &[u8]) -> bool {
    if !self.authenticator_can_authenticate {
      // 不支持认证的认证器直接落到默认用户（C# GetDefaultUserHandle 分支）
      if self.user_handle.is_none() {
        self.user_handle = Some("default".to_string());
      }
      return false;
    }
    let Some(acl) = &self.acl_authenticator else {
      return false;
    };
    // 认证器可变态内 &mut（记录用户句柄）；会话消费串行，锁无竞争。句柄取出
    // 即释放 guard——挂载出口 set_user_handle 需整个会话的可变借用
    let (success, user_handle, target_ns) = {
      let mut acl = acl.lock();
      let success = acl.authenticate(username, password, acl_password_check);
      (
        success,
        if success {
          acl.get_user_handle().cloned()
        } else {
          None
        },
        acl.get_namespace(),
      )
    };
    let Some(user_handle) = user_handle else {
      return success;
    };
    // 认证器面句柄取自引导期内存单例（default / requirepass），非存储记录
    self.set_user_handle(user_handle, false);
    self.namespace = target_ns;
    if let Some(api) = &self.garnet_api
      && !api.set_context(target_ns, self.active_db_id)
    {
      // 冷租户/冷库：映射未装载，登记挂起面由应答组装点异步点查装载
      self.cold_ctx = Some((target_ns, self.active_db_id));
    }
    success
  }

  /// libs/server/Resp/RespServerSession.cs:CanRunDebug
  pub fn can_run_debug(&self) -> bool {
    can_run_with_protection(self.enable_debug_command(), self.is_local_connection())
  }

  /// EnableDebugCommand 配置视图（C# storeWrapper.serverOptions 选项承接）
  fn enable_debug_command(&self) -> wconf::ConnectionProtectionOption {
    self.connection_protection_debug
  }

  /// 认证成功落位：刷新认证器句柄 + 会话本地句柄 / 命名空间 / 存储域上下文
  ///
  /// 对标 C# 认证器与 RespServerSession 共享同一 `UserHandle` 的语义；rust 侧
  /// 句柄连接本地持有（无全局用户字典），故认证器与会话两处同步刷新。
  fn apply_authenticated_handle(&mut self, user_handle: Arc<wacl::UserHandle>, target_ns: u64) {
    if let Some(acl) = &self.acl_authenticator {
      let mut acl = acl.lock();
      acl.user_handle = Some(Arc::clone(&user_handle));
      acl.namespace = target_ns;
    }
    self.set_user_handle(user_handle, true);
    self.namespace = target_ns;
    if let Some(api) = &self.garnet_api
      && !api.set_context(target_ns, self.active_db_id)
    {
      // 冷租户/冷库：映射未装载，登记挂起面由应答组装点异步点查装载
      self.cold_ctx = Some((target_ns, self.active_db_id));
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  ///
  /// AUTH [<username>] <password>：免认证形态回认证器拒绝文案；ACL 档命名用户
  /// 经 [`RespServerSession::authenticate_user_via_store`] 点查底层存储（存储
  /// 为唯一真源），成功后连接本地挂 `Arc<UserHandle>`（句柄随连接析构释放，
  /// 无全局用户字典）；`default` / requirepass 回落内存认证器。按用户名有无
  /// 回 WRONGPASS 变体
  pub fn network_auth_session<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &AclStore<'_, D>,
  ) -> wresp::Result<bool> {
    wresp::check_arg_count!(parse_state, 1..=2, &mut self.output, "AUTH");

    if !self.authenticator_can_authenticate {
      // C# 默认 GarnetNoAuthAuthenticator：CanAuthenticate = false
      cs::write_error_raw(
        self.get_string_output(),
        "ERR Client sent AUTH, but configured authenticator does not accept passwords",
      );
      return Ok(true);
    }

    let username = if parse_state.len() == 2 {
      parse_state[0]
    } else {
      &[]
    };
    let password = parse_state[parse_state.len() - 1];
    // 命名用户：存储点查认证（句柄连接本地持有）
    if !username.is_empty() {
      match self.authenticate_user_via_store(store, username, password) {
        AclAuthOutcome::Success(user_handle, target_ns) => {
          self.apply_authenticated_handle(user_handle, target_ns);
          if let Some((ns, db)) = self.take_cold_ctx() {
            self.park_cold_context_load(&store.storage().store, ns, db, cs::RESP_OK.to_vec());
            return Ok(true);
          }
          cs::write_raw(self.get_string_output(), cs::RESP_OK);
          return Ok(true);
        }
        AclAuthOutcome::StorageError => {
          cs::write_error_raw(self.get_string_output(), cs::RESP_ERR_SLOW_PATH_STORAGE);
          return Ok(true);
        }
        // 非 ns0 会话禁显式 `#` 前缀换租（与 ACL 管理臂同口径）；不回落内存认证器
        AclAuthOutcome::ForeignNamespace => {
          cs::write_error_raw(self.get_string_output(), RESP_ERR_ACL_FOREIGN_NAMESPACE);
          return Ok(true);
        }
        // 存储无记录/口令不匹配：回落内存认证器（default / requirepass 形态）
        AclAuthOutcome::Denied => {}
      }
    }
    if self.authenticate_user(username, password) {
      if let Some((ns, db)) = self.take_cold_ctx() {
        self.park_cold_context_load(&store.storage().store, ns, db, cs::RESP_OK.to_vec());
        return Ok(true);
      }
      cs::write_raw(self.get_string_output(), cs::RESP_OK);
    } else if username.is_empty() {
      cs::write_error_raw(
        self.get_string_output(),
        cs::RESP_WRONGPASS_INVALID_PASSWORD,
      );
    } else {
      cs::write_error_raw(
        self.get_string_output(),
        cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD,
      );
    }
    Ok(true)
  }

  /// 写出 ACL 拦截错误（对标 libs/server/Resp/RespServerSession.cs:697-706）
  ///
  /// 已认证用户无命令权限时写出 RESP_ERR_NOPERM（`-NOPERM this user has no permissions to run the command\r\n`）；
  /// 未认证会话写出 RESP_ERR_NOAUTH（`-NOAUTH Authentication required.\r\n`）。
  pub fn write_acl_permission_error(&mut self, is_authenticated: bool) {
    let err = if is_authenticated {
      cs::RESP_ERR_NOPERM
    } else {
      cs::RESP_ERR_NOAUTH
    };
    self.abort_error_message(err);
  }

  /// ACL 门位图段（C# AdminCommands.cs:CheckACLPermissions 主体；&self 纯判定，
  /// 主循环与 Lua redis.call 路径同用——LuaRunner.Functions.cs:2993）
  ///
  /// 恒为挂载句柄的位图判定，不内嵌失效判定（改权收敛见
  /// `Self::refresh_acl_mount_if_stale`，其位点在预门而非此处：点查存储须
  /// 在批处理纪元保护区外，而本判定 Lua 脚本窗口内亦调用）
  ///
  /// 无 ACL 认证器（免认证形态）：C# default 用户 +@all → 恒放行。
  /// ACL 档：(!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝
  ///（per-user CanAccessCommand 位图；IsNoAuth 豁免见
  /// libs/server/Resp/Parser/RespCommand.cs:701-707）。
  pub fn acl_permits(&self, cmd: RespCommand) -> bool {
    if self.acl_authenticator.is_none() {
      return true;
    }
    let permitted = self
      .acl_user_handle
      .as_ref()
      .is_some_and(|handle| handle.load().can_access_command(cmd));
    permitted || is_no_auth(cmd)
  }

  /// 处理 HELLO 命令的会话状态转换：
  ///
  /// 校验 → 认证 → 升级协议 / 落客户端名 → 按会话真实状态组 HELLO 应答 map
  ///（RESP2 退化为双倍数组）。返回 false 表示认证失败（WRONGPASS）。
  ///
  /// 不移植 C# BasicCommands.cs:1784-1789 的 pending 异步守卫臂
  /// （协议版本变化且 asyncCompleted < asyncStarted 时回
  /// 「存在进行中异步操作不允许协议变更」错误）之因：rust 命令在会话
  /// 循环内 await 到完成，无 C# AsyncProcessor 的 asyncStarted /
  /// asyncCompleted 在途计数面（GET_WithPending 族），守卫不可达。若未来
  /// 引入异步自定义命令面，须连同计数面与本守卫臂一并落地。
  pub fn process_hello_command_state<D: Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    store: Option<&AclStore<'_, D>>,
    output: &mut Vec<u8>,
  ) -> bool {
    if !username.is_empty() {
      // 免认证形态拦截：不支持口令认证时统一拒斥（与 AUTH 门禁保持一致）
      if !self.authenticator_can_authenticate {
        cs::write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
        return false;
      }

      // 命名用户：存储点查（句柄连接本地持有）；default / requirepass 回落内存认证器
      let mut authenticated = false;
      if let Some(store) = store {
        match self.authenticate_user_via_store(store, username, password) {
          AclAuthOutcome::Success(user_handle, target_ns) => {
            self.apply_authenticated_handle(user_handle, target_ns);
            authenticated = true;
          }
          AclAuthOutcome::StorageError => {
            cs::write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
            return false;
          }
          // 非 ns0 会话禁显式 `#` 前缀换租（与 ACL 管理臂同口径）；不回落内存认证器
          AclAuthOutcome::ForeignNamespace => {
            cs::write_error_raw(output, RESP_ERR_ACL_FOREIGN_NAMESPACE);
            return false;
          }
          AclAuthOutcome::Denied => {}
        }
      }
      if !authenticated && !self.authenticate_user(username, password) {
        cs::write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
        return false;
      }
    }

    if let Some(version) = resp_protocol_version {
      self.update_resp_protocol_version(version);
    }
    if let Some(name) = client_name {
      self.set_client_name(Some(name));
    }

    // 应答 map 按升级后的协议版本写头（C# BasicCommands.cs:1829 WriteMapLength：
    // RESP3 %8、RESP2 双倍数组）；字段序对齐 C#：server/version/
    // garnet_version/proto/id/mode/role + modules 空数组；proto/id 直读会话状态；
    // mode/role 集群形态（C# EnableCluster && IsReplica 分支）
    let (mode, role) = match &self.cluster_session {
      None => ("standalone", "master"),
      Some(_) => (
        "cluster",
        if self
          .cluster_provider
          .as_ref()
          .is_some_and(|p| p.is_replica())
        {
          "replica"
        } else {
          "master"
        },
      ),
    };
    write_map_len(output, 8, self.resp_protocol_version);
    output.write_resp_bulk_string(b"server");
    output.write_resp_bulk_string(b"redis");
    output.write_resp_bulk_string(b"version");
    output.write_resp_bulk_string(REDIS_PROTOCOL_VERSION.as_bytes());
    output.write_resp_bulk_string(b"garnet_version");
    output.write_resp_bulk_string(env!("CARGO_PKG_VERSION").as_bytes());
    output.write_resp_bulk_string(b"proto");
    output.write_resp_int(i64::from(self.resp_protocol_version));
    output.write_resp_bulk_string(b"id");
    output.write_resp_int(self.id);
    output.write_resp_bulk_string(b"mode");
    output.write_resp_bulk_string(mode.as_bytes());
    output.write_resp_bulk_string(b"role");
    output.write_resp_bulk_string(role.as_bytes());
    output.write_resp_bulk_string(b"modules");
    output.write_resp_array_len(0);
    true
  }
}

/// 连接保护共同判定（调试命令与模块加载使用）
fn can_run_with_protection(option: wconf::ConnectionProtectionOption, is_local: bool) -> bool {
  match option {
    ConnectionProtectionOption::Yes => true,
    ConnectionProtectionOption::No => false,
    ConnectionProtectionOption::Local => is_local,
  }
}

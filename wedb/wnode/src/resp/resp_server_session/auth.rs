//! 鉴权臂（对标 libs/server/Resp/BasicCommands.cs:NetworkAUTH 与
//! RespServerSession.cs 的 SetUserHandle / AuthenticateUser / CanRunDebug、
//! HELLO 会话状态转换；含 rust 侧改权代数收敛的 ACL 挂载态机械）。

use std::{mem::take, sync::Arc};

use wacl::{
  AccessControlList, User, UserHandle, access_control_list::DEFAULT_USER_NAME, acl_password_check,
};
use wconf::ConnectionProtectionOption;
use wdev::Device;
use wresp::{
  catalog::is_no_auth,
  cmd_strings::{self as cs, write_map_len},
  command::RespCommand,
  ext::RespVecExt,
};
use wtxn::{TxnSession, TxnState};

use super::core::{
  ColdAuthCommit, ColdContextPending, ColdHelloCommit, REDIS_PROTOCOL_VERSION, RespServerSession,
};
use crate::resp::{
  acl_commands::{AclAuthOutcome, RESP_ERR_ACL_FOREIGN_NAMESPACE},
  acl_store::AclStore,
};

/// 订阅态禁跨命名空间切换文案（wedb 自有面：C# 无 namespace 概念，AUTH
/// 无此门；多租户订阅隔离的守门文案，AUTH 与 HELLO 认证臂共用）
const RESP_ERR_NS_SWITCH_WHILE_SUBSCRIBED: &str = "ERR Can't change namespace while subscribed";

/// 事务进行中禁 AUTH 文案（wedb 自有面：AUTH 元数据补 NoMulti 后排队臂
/// 由 NetworkSKIP 报错中止；本文案承接事务执行态直通臂的入口拦截，格式
/// 对标 RESP_ERR_GENERIC_WATCH_IN_MULTI）
pub(super) const RESP_ERR_AUTH_IN_MULTI: &str = "ERR AUTH inside MULTI is not allowed";

/// 会话 ACL 挂载态（连接本地，随句柄生死；对标 C# 会话直持共享 UserHandle
/// 时「句柄即最新」的隐含前提——本仓句柄不共享，改权经引擎代数向在途会话广播）
///
/// 唯一失效判据即 RespServerSession::refresh_acl_mount_if_stale 的代数比较，
/// 无第二处判据。
#[derive(Clone, Copy)]
pub(super) struct AclMount {
  /// 与挂载句柄配对的引擎 ACL 变更代数（读前采样口径：点查记录【之前】或进入
  /// 校验前提前采得，见 [`RespServerSession::set_user_handle`]）；None = 挂载
  /// 早于存储执行域注入（装配期 attach_acl 先于 set_garnet_api），首次鉴权预门补采
  generation: Option<u64>,
  /// 句柄是否取自 ACL 存储真源记录：false = 引导期内存单例（requirepass /
  /// nopass 的 default，装配时存储尚无同名记录），陈旧时点查无记录亦维持
  /// 挂载；SETUSER default 落盘后记录在场，刷新臂经 adopt 收敛至存储句柄，
  /// 本标记不构成分支判据的第二例（唯一失效判据恒为代数比较）
  from_store: bool,
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:SetUserHandle
  /// libs/server/Cluster/IClusterSession.cs:SetUserHandle
  ///（接口声明折叠：C# 集群会话 `ClusterSession.SetUserHandle` 只写自身
  /// userHandle 字段供集群命令权限判定；rust 集群切面与 RESP 会话同体，
  /// 句柄单点挂载即本函数）
  ///
  /// 挂载用户句柄的唯一出口：挂载即定格用户名（CLIENT LIST / SLOWLOG 展示面
  /// 经 [`RespServerSession::user_name`] 直读同一句柄，无字符串镜像）并登记
  /// 挂载代数（`from_store` 标记句柄是否取自存储真源记录，见 [`AclMount`]）
  ///
  /// `generation` 由调用方显式传入，全臂统一读前采样口径：须在点查用户记录
  /// 【之前】（内存臂为进入校验前提前）采得引擎 ACL 代数，与 bump 的
  /// Release / 读的 Acquire 配对，杜绝挂载现场读后采样吞掉点查至挂载窗口内
  /// 并发 SETUSER/DELUSER 的推进、旧权限滞留整个会话生命周期
  pub fn set_user_handle(
    &mut self,
    user_handle: Arc<wacl::UserHandle>,
    generation: Option<u64>,
    from_store: bool,
  ) {
    self.acl_user_handle = Some(user_handle);
    // 免认证形态（NoAuth / requirepass 直连）无 ACL 判定面可收敛，不登记挂载态
    self.acl_mount = self.acl_authenticator.as_ref().map(|_| AclMount {
      generation,
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
  /// 预门停车判据（[`Self::refresh_acl_mount_if_stale`] 的无副作用前视）：
  /// 挂载存在、执行域可达且引擎代数已知、代数落后、且已绑用户名——同时
  /// 成立时刷新才会真正点查存储，同步门链段据此停车交泵异步刷新；其余
  /// 形态刷新为无害空转，门链原地直过
  pub(crate) fn acl_refresh_park_needed(&self) -> bool {
    let Some(mount) = &self.acl_mount else {
      return false;
    };
    let Some(api) = &self.garnet_api else {
      return false;
    };
    let Some(current) = api.acl_generation() else {
      return false;
    };
    mount.generation != Some(current) && self.user_name().is_some()
  }

  /// 位点为鉴权预门 [`RespServerSession::check_acl_permissions`] 的停车臂——
  /// 门链同步段只判「挂载是否陈旧」，陈旧即停车交网络泵在本臂 await 点查
  ///（冷记录降级异步落盘回读，严禁运行时上下文内同步收割），完成后泵回驱
  /// 原命令重评门链，代相等即零存储读直过。口径仅在命令边界闭环：Lua
  /// `redis.call` 面经外层脚本命令预门刷新后的同一句柄判定，与直令面同
  /// 口径；EVAL 入口后窗口内到达的 bump 本臂不可达（同步窗无 await 点），
  /// 该面收口为 dispatch_resp 环尾窗内改权错误帧中断脚本（中止语义，非
  /// C# 逐条 fresh 续跑形，见 deviations §159）
  pub(crate) async fn refresh_acl_mount_if_stale(&mut self) {
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
    let Some(username) = self.user_name().map(str::to_string) else {
      return;
    };
    // 记录已删 / 读失败 / 不可解析三态合一：撤销挂载按未认证处理
    let mut adopted = None;
    let mut revoke = false;
    match api
      .acl_user_record(self.namespace, username.as_bytes())
      .await
    {
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
      // 引导期内存单例（requirepass / nopass 的 default）存储尚无同名记录
      // 时维持挂载；SETUSER default 落盘后记录在场，走上方 adopt 臂收敛。
      // 命名用户句柄本就取自记录，无记录即用户已删
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
  /// wacl/src/user_handle.rs 类型文档）。句柄与挂载态统一经
  /// [`Self::set_user_handle`] 单点出口落位（from_store 置真：句柄取自存储真源记录）
  ///
  /// `generation` 由调用方在点查记录【之前】采得（Acquire 与 bump 的 Release
  /// 配对，保证随后的点查必见该代数下已落盘的记录），杜绝缓存到更新的代数而漏
  /// 掉并发改权；消费串行下无并发写者，整体替换即原子，无需 CAS
  pub(crate) fn adopt_acl_user(&mut self, new_user: Arc<User>, generation: Option<u64>) {
    self.set_user_handle(Arc::new(UserHandle::new(new_user)), generation, true);
  }

  /// 撤销 ACL 挂载：会话句柄经 set_user_handle 单点真源撤除，
  /// 下一命令按未认证落 NOAUTH
  fn revoke_acl_mount(&mut self) {
    self.acl_user_handle = None;
    self.acl_mount = None;
  }

  /// libs/server/Resp/RespServerSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&mut self, resp_protocol_version: u8) {
    self.resp_protocol_version = resp_protocol_version;
  }

  /// libs/server/Resp/RespServerSession.cs:AuthenticateUser
  ///
  /// ACL 认证器挂载时按其校验（定位用户 → 口令比对；C# 认证器记录句柄供
  /// GetUserHandle 直读，rust 侧认证器无状态纯判定返回句柄、由会话
  /// acl_user_handle 单真源挂载）；免认证形态 CanAuthenticate = false
  /// → 返回 false 但同落 default 用户句柄（C# 的
  /// accessControlList.GetDefaultUserHandle 兜底臂，rust 挂
  /// [`AccessControlList::nopass_default_handle`] 进程单例）——
  /// 该分支同面承接
  /// libs/server/Auth/GarnetNoAuthAuthenticator.cs:Authenticate
  /// （Debug.Fail 死桩；免认证档不设独立类型，见 wacl/src/auth/mod.rs 头注）
  ///
  /// 本方法是 ns0 引导期内存认证器的唯一入口：非 0 命名空间调用直接返回
  /// false 且不触达认证器、不改写会话任何状态——引导单例成功即把会话拖回
  /// ns0（租户逃逸），故门禁为多租户隔离底线，逃逸即红
  pub fn authenticate_user(&mut self, username: &[u8], password: &[u8]) -> bool {
    // 安全门禁：非 0 租户会话绝不回落 ns0 引导认证器（严禁覆写 namespace）
    if self.namespace != 0 {
      return false;
    }
    // 读前采样口径：进入校验前提前采得引擎代数（内存臂无存储点查，与预门臂、
    // SETUSER 快路径同一采样面），杜绝挂载现场读后采样吞掉窗口内并发改权
    let generation = self
      .garnet_api
      .as_ref()
      .and_then(|api| api.acl_generation());
    if !self.authenticator_can_authenticate {
      // 不支持认证的认证器落到默认用户句柄（C# GetDefaultUserHandle 兜底臂：
      // success = true 但返回 false；rust 免认证档无 ACL 实例，挂 nopass
      // default 进程单例，CLIENT INFO/LIST 的 user 契约与 C# 恒带 default 一致）
      self.set_user_handle(
        AccessControlList::nopass_default_handle(),
        generation,
        false,
      );
      return false;
    }
    // 认证器为无状态纯判定（只认 ns0 引导 default 形态），成功即返回引导期
    // 内存单例句柄；只读共享免锁，借用随表达式收尾
    let Some(user_handle) = self
      .acl_authenticator
      .as_ref()
      .and_then(|acl| acl.authenticate(username, password, acl_password_check))
    else {
      return false;
    };
    // 门禁保证 ns 恒 0：句柄取自引导期内存单例（default / requirepass），非存储记录
    self.apply_authenticated_handle(user_handle, 0, generation, false);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:CanRunDebug
  pub fn can_run_debug(&self) -> bool {
    can_run_with_protection(self.enable_debug_command(), self.is_local_connection())
  }

  /// EnableDebugCommand 配置视图（C# storeWrapper.serverOptions 选项承接）
  fn enable_debug_command(&self) -> wconf::ConnectionProtectionOption {
    self.connection_protection_debug
  }

  /// 订阅态跨命名空间切换拦截判据（wedb 自有面：C# 无 namespace，AUTH/HELLO
  /// 无此门；多租户订阅隔离不变量的守门）
  ///
  /// 会话持有活跃订阅（通道/模式/分片任一：`is_subscription_session` 为真或
  /// `num_active_channels > 0`）且认证目标命名空间不同于当前绑定
  /// （`target_ns != self.namespace`）时禁切换——broker 订阅键以
  /// `ChannelNsPrefix`（`{ns}:channel`）挂旧 ns：旧租户 PUBLISH 仍投递本会话
  /// 邮箱，无参退订仅遍历新前缀命中不到旧订阅（订阅悬挂、计数悬挂），单连接
  /// 同时持有跨租户订阅状态，违背命名空间物理隔离不变量。同 ns 重新认证或
  /// 变更用户（`target_ns == self.namespace`）不受限，正常通行
  fn blocked_by_subscription(&self, target_ns: u64) -> bool {
    target_ns != self.namespace
      && (self.is_subscription_session || self.pubsub.num_active_channels > 0)
  }

  /// 认证成功落位试探口：先试存储域上下文绑定，冷租户/冷库即整体暂存挂起
  /// 面返回 true；热租户当场物化返回 false
  ///
  /// 对标 C# 认证器与 RespServerSession 共享同一 `UserHandle` 的语义；rust 侧
  /// 句柄连接本地持有（无全局用户字典），会话侧 `acl_user_handle` 即唯一真源。
  /// `generation` 为调用方读前采样（点查/校验之前提前采得）的引擎 ACL 代数，
  /// 转传 set_user_handle 与句柄配对；`from_store` 区分句柄来源（存储真源记录 /
  /// 引导期内存单例，见 [`AclMount`]）
  ///
  /// 冷租户/冷库（`set_context` 报告映射未装载）时严禁提前覆写 `namespace`
  /// 与 `acl_user_handle`——底层物理域未切，标量与句柄先动即撕裂（异步装载
  /// 失败后外层报新租户、读写穿透旧租户存储域，破坏多租户隔离防线）。目标
  /// 与落位载荷整体暂存 [`ColdContextPending`]，由 SlowWait 成功应答回写时
  /// 经 [`Self::materialize_authenticated_handle`] 物化；失败即弃暂存，旧
  /// 认证态原样保持（与 C# 认证失败即时终止、绝不提前变更会话上下文同口径）
  fn apply_authenticated_handle(
    &mut self,
    user_handle: Arc<wacl::UserHandle>,
    target_ns: u64,
    generation: Option<u64>,
    from_store: bool,
  ) -> bool {
    if let Some(api) = &self.garnet_api
      && !api.set_context(target_ns, self.active_db_id)
    {
      self.cold_ctx = Some(ColdContextPending {
        ns: target_ns,
        db: self.active_db_id,
        auth: Some(ColdAuthCommit {
          user_handle,
          generation,
          from_store,
        }),
        hello: None,
      });
      return true;
    }
    self.materialize_authenticated_handle(user_handle, target_ns, generation, from_store);
    false
  }

  /// 认证落位物化体（热租户当场执行；冷挂起经 SlowWait 成功回写后补落）：
  /// 挂载句柄（经 [`Self::set_user_handle`] 单点真源）+ 会话命名空间 + 摘
  /// 订阅与事务清退
  ///
  /// 命名空间变更（`target_ns != self.namespace`）时的兜底闭环（摘订阅机制
  /// 对标 C# RespServerSession.Dispose 尾部
  /// `subscribeBroker?.RemoveSubscription`，即 SubscribeBroker.cs:RemoveSubscription）：
  /// 命令面拦截（[`Self::blocked_by_subscription`]）闭合后正常路径不可达，此处
  /// 为防御层——摘除 broker 中本会话全部旧 ns 订阅（通道/模式/分片）、计数与
  /// 订阅态归零、排空并丢弃邮箱中残留的旧租户待发消息，杜绝旧租户消息渗入
  /// 新命名空间会话（drain 侧剥离失败丢弃见
  /// wpubsub::session_commands::PubSubSessionCommands::drain_pubsub_frames）
  ///
  /// 事务面同位联动清退（wedb 自有面，C# 单租户无 namespace 切换即无此臂；
  /// 机制对标 libs/server/Transaction/TransactionManager.cs 的 Reset 与
  /// TxnWatchedKeysContainer.cs:Reset 的 EXEC/DISCARD 收尾同款调用）：旧租户
  /// 监视键以旧前缀定格 scoped_key_hash，残留即并入新租户事务锁集（跨租户
  /// 假性锁竞争）与版本校验（旧租户写推进误判新租户事务中止）；排队态遗留
  /// 即 EXEC 跨租户回放。会话事务状态镜像与集群槽位校验缓存一并归零
  pub(super) fn materialize_authenticated_handle(
    &mut self,
    user_handle: Arc<wacl::UserHandle>,
    target_ns: u64,
    generation: Option<u64>,
    from_store: bool,
  ) {
    if target_ns != self.namespace {
      if let Some(broker) = self.pubsub.broker() {
        broker.remove_subscription(self.id as u64);
      }
      self.pubsub.num_active_channels = 0;
      self.is_subscription_session = false;
      self.pubsub.drain_mailbox_into();
      if let Some(txn) = self.txn_manager.as_mut() {
        txn.reset();
        txn.watch_container.reset();
      }
      self.txn_state = TxnState::None;
      self.reset_cluster_slot_verification_result();
    }
    self.set_user_handle(user_handle, generation, from_store);
    self.namespace = target_ns;
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  ///
  /// AUTH [<username>] <password>：免认证形态回认证器拒绝文案；命名用户（含
  /// 单参数规范化出的 `default`）经
  /// [`RespServerSession::authenticate_user_via_store`] 点查底层存储（存储为
  /// 唯一真源），成功后连接本地挂 `Arc<UserHandle>`（句柄随连接析构释放，
  /// 无全局用户字典）；仅 ns0 会话在存储【无记录】（NoRecord）时回落引导期
  /// 内存认证器（requirepass / nopass 的 default，装配期存储尚无同名记录）；
  /// 记录在场即存储为唯一真源，Denied（停用/口令不符）一律直接 WRONGPASS，
  /// 引导单例旧口令绝不复活。按用户名有无回 WRONGPASS 变体
  pub async fn network_auth_session<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &AclStore<'_, D>,
  ) -> wresp::Result<bool> {
    wresp::check_arg_count!(parse_state, 1..=2, &mut self.output, "AUTH");

    // 事务进行中禁 AUTH（wedb 自有面：排队态由 AUTH 的 NoMulti 元数据在
    // NetworkSKIP 报错中止；本门承接事务执行态（Running 经
    // process_transactional_command 直通抵达此处）与未来旁路面——未决事务
    // 内换租即排队命令跨租户回放，违反多租户单向强隔离）
    if self.txn_state != TxnState::None {
      cs::write_error_raw(self.get_string_output(), RESP_ERR_AUTH_IN_MULTI);
      return Ok(true);
    }

    if !self.authenticator_can_authenticate {
      // C# 默认 GarnetNoAuthAuthenticator：CanAuthenticate = false
      cs::write_error_raw(
        self.get_string_output(),
        "ERR Client sent AUTH, but configured authenticator does not accept passwords",
      );
      return Ok(true);
    }

    // 单参数 AUTH <password>（及显式空用户名）统一规范化为 `default` 用户：
    // 非 0 租户会话按会话绑定命名空间点查租户内 default 参与门禁（票面 §1），
    // 不再旁路点查直落内存认证器；WRONGPASS 变体按用户名是否为空择定
    let empty_username = parse_state.len() == 1 || parse_state[0].is_empty();
    let username = if empty_username {
      DEFAULT_USER_NAME.as_bytes()
    } else {
      parse_state[0]
    };
    let password = parse_state[parse_state.len() - 1];
    let wrongpass: &str = if empty_username {
      cs::RESP_WRONGPASS_INVALID_PASSWORD
    } else {
      cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD
    };

    // 命名用户：存储点查认证（句柄连接本地持有）
    match self
      .authenticate_user_via_store(store, username, password)
      .await
    {
      AclAuthOutcome::Success(user_handle, target_ns, generation) => {
        // 订阅态禁跨命名空间切换（同 ns 重新认证/变更用户不拦，判据与
        // 文案单点见 blocked_by_subscription / RESP_ERR_NS_SWITCH_WHILE_SUBSCRIBED）
        if self.blocked_by_subscription(target_ns) {
          cs::write_error_raw(
            self.get_string_output(),
            RESP_ERR_NS_SWITCH_WHILE_SUBSCRIBED,
          );
          return Ok(true);
        }
        // 代数随句柄一并挂载（点查前读前采样，见 authenticate_user_via_store）
        self.apply_authenticated_handle(user_handle, target_ns, generation, true);
        if let Some((ns, db)) = self.cold_pending_ctx() {
          let mut out = take(&mut self.output);
          self.park_cold_context_load(
            &store.storage().store,
            ns,
            db,
            cs::RESP_OK.to_vec(),
            &mut out,
            RESP_ERR_AUTH_IN_MULTI,
          );
          self.output = out;
          return Ok(true);
        }
        cs::write_raw(self.get_string_output(), cs::RESP_OK);
        return Ok(true);
      }
      // 非成功四态出帧与收尾两臂同形，文案裁决单点见 store_auth_reject
      reject => {
        if let Some(err) = self.store_auth_reject(&reject, wrongpass) {
          cs::write_error_raw(self.get_string_output(), err);
          return Ok(true);
        }
      }
    }
    // 仅 ns0 会话在存储无记录时回落引导内存认证器（requirepass / nopass 的
    // default 单例，装配期不落盘）
    if self.authenticate_user(username, password) {
      if let Some((ns, db)) = self.cold_pending_ctx() {
        let mut out = take(&mut self.output);
        self.park_cold_context_load(
          &store.storage().store,
          ns,
          db,
          cs::RESP_OK.to_vec(),
          &mut out,
          RESP_ERR_AUTH_IN_MULTI,
        );
        self.output = out;
        return Ok(true);
      }
      cs::write_raw(self.get_string_output(), cs::RESP_OK);
    } else {
      cs::write_error_raw(self.get_string_output(), wrongpass);
    }
    Ok(true)
  }

  /// 存储点查认证非成功态的出帧文案裁决（AUTH 臂与 HELLO 臂同形共用：判定
  /// 条件与回落顺序一致，差异仅出帧目标与收尾返回，留在调用方）。Some = 出
  /// 该错误帧后调用方立即收尾返回；None = NoRecord 且 ns0 会话，坠落调用方
  /// 下方回落引导期内存认证器臂。`wrongpass` 为该臂 WRONGPASS 变体：AUTH 按
  /// 用户名是否为空择定，HELLO 恒用户名+口令变体（异文案不并档）
  fn store_auth_reject<'a>(&self, outcome: &AclAuthOutcome, wrongpass: &'a str) -> Option<&'a str> {
    match outcome {
      AclAuthOutcome::StorageError => Some(cs::RESP_ERR_SLOW_PATH_STORAGE),
      // 非 ns0 会话禁显式 `#` 前缀换租（与 ACL 管理臂同口径）；不回落内存认证器
      AclAuthOutcome::ForeignNamespace => Some(RESP_ERR_ACL_FOREIGN_NAMESPACE),
      // 记录在场即存储为唯一真源：停用/口令不符/规则损坏/用户名非法一律
      // 直接 WRONGPASS，绝不回落引导期内存认证器——回落即 SETUSER default
      // 换密/停用后旧 requirepass 经回落臂复活的认证面失效缝（对标 C#
      // GarnetACLAuthenticator.Authenticate 查得句柄即定、查无才 false，
      // 全链无任何回落臂形态；两臂同一收口，无第二套回落判据）
      AclAuthOutcome::Denied => Some(wrongpass),
      // 存储无该用户记录：多租户隔离底线——非 0 租户会话即 WRONGPASS，绝不
      // 回落 ns0 引导期内存认证器（回落即覆写 namespace 逃逸至超管空间，
      // 逃逸即红）
      AclAuthOutcome::NoRecord if self.namespace != 0 => Some(wrongpass),
      // 仅 ns0 会话坠落调用方下方回落臂（引导认证器单点门只认 default 形态，
      // §90 异名+无记录拒绝锁面在该臂不破）
      AclAuthOutcome::NoRecord => None,
      // Success 由两臂调用方先行接管（订阅门禁与落位应答各异），不经本裁决
      AclAuthOutcome::Success(..) => unreachable!("Success 不经非成功态裁决"),
    }
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
  /// 主循环与 Lua redis.call 路径同用——LuaRunner.Functions.cs:2993）。
  /// 窗口面注记（deviations §159）：脚本窗内挂载一经 bump 落后，本位图即
  /// 陈旧快照、不再作终审——消费面经 `RespScriptingApi::dispatch_resp` 环尾
  /// 停车收口、acl_check_cmd 面经 `ScriptingApi::acl_mount_stale` 预门先行
  /// 改错；本判定仅在代数追平后（含直令面与快路预检放行后的重入消费）有
  /// 终裁权
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
      .is_some_and(|handle| handle.user().can_access_command(cmd));
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
  pub async fn process_hello_command_state<D: Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    store: Option<&AclStore<'_, D>>,
    output: &mut Vec<u8>,
  ) -> bool {
    // 冷租户/冷库挂起哨兵：认证落位未确认前协议版本与客户端名严禁提前
    // 单向脏写（装载失败须与认证失败同口径回滚），暂存挂起面随物化落位
    let mut cold_pending = false;
    if !username.is_empty() {
      let wrongpass = cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD;
      // 免认证形态拦截：不支持口令认证时统一拒斥（与 AUTH 门禁保持一致）
      if !self.authenticator_can_authenticate {
        cs::write_error_raw(output, wrongpass);
        return false;
      }

      // 命名用户：存储点查（句柄连接本地持有）；仅 ns0 会话在存储无记录
      //（NoRecord）后回落引导内存认证器，记录在场的 Denied 直接 WRONGPASS
      let mut authenticated = false;
      if let Some(store) = store {
        match self
          .authenticate_user_via_store(store, username, password)
          .await
        {
          AclAuthOutcome::Success(user_handle, target_ns, generation) => {
            // 订阅态禁跨命名空间切换（同 ns 重新认证/变更用户不拦；认证
            // 中断即 false，协议版本与客户端名均不落）
            if self.blocked_by_subscription(target_ns) {
              cs::write_error_raw(output, RESP_ERR_NS_SWITCH_WHILE_SUBSCRIBED);
              return false;
            }
            // 代数随句柄一并挂载（点查前读前采样，见 authenticate_user_via_store）；
            // true = 冷租户/冷库挂起（标量与句柄暂存挂起面待物化）
            cold_pending =
              self.apply_authenticated_handle(user_handle, target_ns, generation, true);
            authenticated = true;
          }
          // 非成功四态出帧与收尾同 AUTH 臂同形，文案裁决单点见 store_auth_reject
          reject => {
            if let Some(err) = self.store_auth_reject(&reject, wrongpass) {
              cs::write_error_raw(output, err);
              return false;
            }
          }
        }
      }
      if !authenticated {
        if !self.authenticate_user(username, password) {
          cs::write_error_raw(output, wrongpass);
          return false;
        }
        // 回落臂冷挂起判定：authenticate_user 内部落位试探已登记挂起面
        cold_pending = self.cold_ctx.is_some();
      }
    }

    self.commit_hello_state_and_write_reply(
      resp_protocol_version,
      client_name,
      cold_pending,
      output,
    );
    true
  }

  /// HELLO 会话状态落位与应答组 map 单点（process_hello_command_state 尾段
  /// 抽出，一处定义）：cold_pending = 认证挂起未确认，目标元数据并入挂起面，
  /// SlowWait 成功回写时与认证标量一并物化、失败即弃，旧协议版本与客户端名
  /// 原样保持；否则当场落位。应答 map 按目标协议版本组（挂起态版本未物化，
  /// 直读会话标量会以旧版本成帧——C# NetworkHELLO 的协议升级在认证同步段内
  /// 即完成，应答恒带新版本）。
  ///
  /// 无 AUTH 选项组的 HELLO 零存储点查、零停泊（协议回显/客户端名均会话
  /// 元数据），EXEC 重放窗同步快臂（core.rs dispatch_via_garnet_api，
  /// deviations §58d）以 cold_pending = false 直调本口直出应答 map
  pub(crate) fn commit_hello_state_and_write_reply(
    &mut self,
    resp_protocol_version: Option<u8>,
    client_name: Option<&str>,
    cold_pending: bool,
    output: &mut Vec<u8>,
  ) {
    if cold_pending {
      if let Some(pending) = &mut self.cold_ctx {
        pending.hello = Some(ColdHelloCommit {
          resp_protocol_version,
          client_name: client_name.map(str::to_string),
        });
      }
    } else {
      if let Some(version) = resp_protocol_version {
        self.update_resp_protocol_version(version);
      }
      if let Some(name) = client_name {
        self.set_client_name(Some(name));
      }
    }
    let effective_proto = if cold_pending {
      resp_protocol_version.unwrap_or(self.resp_protocol_version)
    } else {
      self.resp_protocol_version
    };

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
    write_map_len(output, 8, effective_proto);
    output.write_resp_bulk_string(b"server");
    output.write_resp_bulk_string(b"redis");
    output.write_resp_bulk_string(b"version");
    output.write_resp_bulk_string(REDIS_PROTOCOL_VERSION.as_bytes());
    output.write_resp_bulk_string(b"garnet_version");
    output.write_resp_bulk_string(env!("CARGO_PKG_VERSION").as_bytes());
    output.write_resp_bulk_string(b"proto");
    output.write_resp_int(i64::from(effective_proto));
    output.write_resp_bulk_string(b"id");
    output.write_resp_int(self.id);
    output.write_resp_bulk_string(b"mode");
    output.write_resp_bulk_string(mode.as_bytes());
    output.write_resp_bulk_string(b"role");
    output.write_resp_bulk_string(role.as_bytes());
    output.write_resp_bulk_string(b"modules");
    output.write_resp_array_len(0);
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

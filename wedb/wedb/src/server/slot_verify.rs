use wresp::cmd_strings::{
  cluster::{ERR_CLUSTERDOWN, ERR_TRYAGAIN, write_redirect_error},
  write_error_raw,
};

use crate::server::{
  cluster_config::{ClusterConfig, ClusterPreferredEndpointType},
  hash_slot::SlotState,
  worker::NodeRole,
};

/// 槽位验证结果状态（对标 libs/cluster/Session/SlotVerifiedState.cs 六值枚举）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlotVerifiedState {
  /// 验证通过
  #[default]
  Ok,
  /// 槽位未提供服务（单节点故障或主节点处于恢复态）
  ClusterDown,
  /// 槽位已转移至远端节点（MOVED 重定向）
  Moved,
  /// 槽位迁移进行中目标节点重定向（ASK 重定向）
  Ask,
  /// 同槽多键在迁移窗口状态不一致（库级定槽下同命令各键恒同槽，
  /// C# 的 CROSSSLOT 裁决随键级哈希一并废除，doc/zh/db.md 4.4）
  TryAgain,
}

/// 槽位验证裁决（对标 C# ClusterSlotVerificationResult：struct{state, slot}，
/// 不携带端点——渲染时按状态查 config 取端点组帧，见
/// [`write_slot_verification_message`]）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClusterSlotVerificationState {
  /// 裁决状态
  pub state: SlotVerifiedState,
  /// 裁决槽位
  pub slot: u16,
}

impl ClusterSlotVerificationState {
  /// 按状态与槽位构造裁决
  pub fn new(state: SlotVerifiedState, slot: u16) -> Self {
    Self { state, slot }
  }

  /// 是否验证通过
  pub fn is_ok(&self) -> bool {
    self.state == SlotVerifiedState::Ok
  }
}

/// 槽位验证会话态（C# ClusterSession 会话标志在验证链的投影）
///
/// - `session_asking`：ASKING 命令剩余计数（导入槽位放行）
/// - `read_only_session`：READONLY 命令会话级只读态（读路径 enableReplicaReads）
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotVerifySessionState {
  /// 会话 ASKING 标记
  pub session_asking: bool,
  /// 会话只读（READONLY）
  pub read_only_session: bool,
}

/// 渲染单点（libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:WriteClusterSlotVerificationMessage）：
/// 按 state 查 config 取端点后经 RespWriteUtils 组帧；C# 侧文案口
/// GetSlotVerificationMessage 为同文件私有构件、已按 ignore 登记，不占符号锚
///
/// MOVED/ASK 内部查 config 取端点，CLUSTERDOWN/TRYAGAIN 直写文案常量；
/// `Ok` 不出帧
pub fn write_slot_verification_message(
  config: &ClusterConfig,
  vres: ClusterSlotVerificationState,
  pref_type: ClusterPreferredEndpointType,
  output: &mut Vec<u8>,
) {
  match vres.state {
    SlotVerifiedState::Moved => {
      let (endpoint, port) = config.get_endpoint_from_slot(vres.slot, pref_type);
      write_redirect_error(output, "MOVED", vres.slot, &endpoint, port);
    }
    SlotVerifiedState::Ask => {
      let (endpoint, port) = config.ask_endpoint_from_slot(vres.slot, pref_type);
      write_redirect_error(output, "ASK", vres.slot, &endpoint, port);
    }
    SlotVerifiedState::ClusterDown => write_error_raw(output, ERR_CLUSTERDOWN),
    SlotVerifiedState::TryAgain => write_error_raw(output, ERR_TRYAGAIN),
    SlotVerifiedState::Ok => {}
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeySlotVerify
///
/// 1:1 对标 C# SingleKeySlotVerify 槽位验证状态机。C# 的 CanOperateOnKey
/// （等待至可访问 + Exists 存在性判定）与 WaitForSlotToStabalize（槽位稳定
/// 等待）为调用方内联自旋，rust 侧等待外提为宿主挂起重评，本状态机接收
/// 等待完成后的裁决布尔（`can_operate`）
pub fn single_key_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  read_only: bool,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
) -> ClusterSlotVerificationState {
  if read_only {
    single_key_read_slot_verify(config, slot, session, is_recovering, can_operate)
  } else {
    single_key_read_write_slot_verify(config, slot, session, is_recovering, can_operate)
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadSlotVerify
///
/// C# 该口是 SingleKeySlotVerify 体内的局部函数，rust 提为同文件私有函数
///
/// `can_operate` 为 C# CanOperateOnKey 裁决（自旋等待至键可访问后按
/// Exists 判定：键仍在本地 → true；已迁走/等待超时 → false）
fn single_key_read_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
) -> ClusterSlotVerificationState {
  let is_local = config.is_local(slot, session.read_only_session);
  let state = config.get_state(slot);

  if is_local {
    if is_recovering {
      return match config.local_node_role() {
        NodeRole::Replica => ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot),
        NodeRole::Primary | NodeRole::Unassigned => {
          ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot)
        }
      };
    }

    match state {
      SlotState::Stable => ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot),
      // MIGRATING：CanOperateOnKey 等待结束后键仍存在才放行，否则 ASK 重定向
      SlotState::Migrating => {
        if can_operate {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot)
        } else {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ask, slot)
        }
      }
      _ => ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot),
    }
  } else {
    match state {
      SlotState::Stable => ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot),
      SlotState::Importing => {
        if session.session_asking {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot)
        } else {
          ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot)
        }
      }
      _ => ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot),
    }
  }
}

/// libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:SingleKeyReadWriteSlotVerify
///
/// C# 该口同上为局部函数，rust 提为同文件私有函数
///
/// `can_operate` 语义同 [`single_key_read_slot_verify`]；C# 的
/// waitForStableSlot 等待循环（IMPORTING/MIGRATING 期间让出重试）由宿主
/// 外提承接，本函数只在槽位已稳定（或命令不要求稳定等待）后进入
fn single_key_read_write_slot_verify(
  config: &ClusterConfig,
  slot: u16,
  session: SlotVerifySessionState,
  is_recovering: bool,
  can_operate: bool,
) -> ClusterSlotVerificationState {
  // 副本重定向读写请求至主节点（C# 于此豁免内部写会话；rust 回放不经
  // RESP 分派、直达存储域，本门仅拦客户端会话，无需豁免臂）
  if config.local_node_role() == NodeRole::Replica {
    return ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot);
  }

  let is_local = config.is_local(slot, false);
  let state = config.get_state(slot);

  if is_local {
    if is_recovering {
      return match config.local_node_role() {
        NodeRole::Replica => ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot),
        NodeRole::Primary | NodeRole::Unassigned => {
          ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot)
        }
      };
    }

    match state {
      SlotState::Stable => ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot),
      // MIGRATING：CanOperateOnKey 等待结束后键仍存在才放行，否则 ASK 重定向
      SlotState::Migrating => {
        if can_operate {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot)
        } else {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ask, slot)
        }
      }
      _ => ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot),
    }
  } else {
    match state {
      SlotState::Stable => ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot),
      SlotState::Importing => {
        if session.session_asking {
          ClusterSlotVerificationState::new(SlotVerifiedState::Ok, slot)
        } else {
          ClusterSlotVerificationState::new(SlotVerifiedState::Moved, slot)
        }
      }
      _ => ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, slot),
    }
  }
}

/// 迭代式槽位校验缓存态（C# ClusterSession 成员 cachedVerificationResult /
/// configSnapshot / initialized 的投影；configSnapshot 不缓存——rust
/// ClusterConfig 整体克隆过重，逐键经 ClusterManager 读锁取当前配置，
/// 毫秒级 Prepare 窗口内由 RwLock 写阻塞保证无撕裂）
#[derive(Debug, Default)]
pub struct IterativeSlotVerifyCache {
  /// 是否已记录首键裁决（C# initialized）
  initialized: bool,
  /// 缓存裁决槽位（C# cachedVerificationResult.slot）
  slot: u16,
  /// 缓存裁决状态（C# cachedVerificationResult.state）
  state: SlotVerifiedState,
}

/// 迭代步进状态机（C# RespClusterIterativeSlotVerify 网络迭代校验的缓存
/// 步进内核：首键初始化缓存并透传裁决；后续键裁决漂移 → TRYAGAIN（缓存记
/// 首个偏差），一致透传本键裁决）
///
/// 库级定槽形态（doc/zh/db.md 4.1/4.4）：整批事务键恒共会话槽位（MULTI/EXEC
/// 与事务过程期内 SELECT/SWAPDB 均被拒），C# 的键间 CROSSSLOT 裁决失去成立
/// 前提，随键级哈希一并废除；槽位仍随裁决透传，供缓存错误按端点组帧
pub fn iterative_slot_verify_step(
  cache: &mut IterativeSlotVerifyCache,
  verdict: ClusterSlotVerificationState,
) -> bool {
  // 首键：初始化结果缓存
  if !cache.initialized {
    let ok = verdict.is_ok();
    *cache = IterativeSlotVerifyCache {
      initialized: true,
      slot: verdict.slot,
      state: verdict.state,
    };
    return ok;
  }

  // 首键已失败：短路捕获首个错误（C# 校验失败提前返回）
  if cache.state != SlotVerifiedState::Ok {
    return false;
  }

  // 裁决漂移（键迁移推进致状态类别变化；理论上库级定槽不会跨槽，槽位若变
  // 即上下文被改写，同按一致性失败降级）→ TRYAGAIN
  if verdict.state != cache.state || verdict.slot != cache.slot {
    cache.state = SlotVerifiedState::TryAgain;
    return false;
  }

  verdict.is_ok()
}

impl IterativeSlotVerifyCache {
  /// 缓存的验证状态（write_cached 错误构造输入；未初始化恒 Ok 不出错误行）
  pub fn state(&self) -> SlotVerifiedState {
    self.state
  }

  /// 缓存裁决槽位
  pub fn slot(&self) -> u16 {
    self.slot
  }

  /// 是否已记录首键裁决
  pub fn initialized(&self) -> bool {
    self.initialized
  }

  /// 重置缓存（新事务批次起点；C# ResetCachedSlotVerificationResult 同名语义）
  pub fn reset(&mut self) {
    *self = Self::default();
  }
}

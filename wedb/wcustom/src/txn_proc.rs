//! 事务过程静态派发（对标 libs/server/Custom/CustomCommandManager.cs:
//! Register(CustomTransactionProcedure) 与 libs/server/Servers/RegisterApi.cs:
//! NewTransactionProc 的静态化承接）。
//!
//! C# 经 ExpandableMap 运行时分配 id 并按 id 回查；转写规范删除动态注册管理
//! 层，rust 以 const fn match 静态派发承接：AOF 回放按日志头 procedure_id、
//! RUNTXP 同径，单次比较直达，零锁零查表零分配零函数指针。
//!
//! ## 号段口径
//!
//! C# `transactionProcMap = new ExpandableMap<CustomTransaction>(MinMapSize, 0,
//! byte.MaxValue)`（CustomCommandManager.cs:67）配 `TryGetNextId`
//! （ExpandableMap.cs:185，`currIndex` 起 -1）→ 过程 id 即注册顺序，首注册为
//! 0，号段随宿主注册序浮动（main/GarnetServer/Program.cs 与
//! test/standalone/Garnet.test.scripting/RespCustomCommandTests.cs 各取一序）。
//! rust 静态表把号段固化为编译期常量：一经 AOF 落盘不可重排，新过程只可追加
//! 号位，不可挪用。
//!
//! | 号位 | 过程 | C# 对位 | 元数 | 状态 |
//! |---|---|---|---|---|
//! | 0 | DEFAULT | 基类最小投影（无同名过程） | 0（不校验） | 已落地 |
//! | 1 | NOOP | NoOpModule/NoOpTxn.cs | 1 | 已落地 |
//! | 2 | READ_WRITE | GarnetServer/Extensions/ReadWriteTxn.cs | 4 | 待存储注入面 |
//! | 3 | MSET_PX | GarnetServer/Extensions/MSetPx.cs:MSetPxTxn | 0 | 待存储注入面 |
//! | 4 | MGET_IFPM | GarnetServer/Extensions/MGetIfPM.cs | 0 | 待存储注入面 |
//! | 5 | GET_TWO_KEYS_NO_TXN | GarnetServer/Extensions/GetTwoKeysNoTxn.cs | 3 | 待存储注入面 |
//! | 6 | SAMPLE_UPDATE | GarnetServer/Extensions/SampleUpdateTxn.cs | 9 | 待存储注入面 |
//! | 7 | SAMPLE_DELETE | GarnetServer/Extensions/SampleDeleteTxn.cs | 6 | 待存储注入面 |
//!
//! 「待存储注入面」= 过程体段需 `api.GET/SET/SETEX/DELETE/SortedSetAdd/
//! SortedSetRemove`，而这些原语只存在于宿主 wnode 存储执行域，事务三段式
//! （[`wtxn::TransactionManager::run_transaction_proc`]）未向过程体传递任何
//! 存储句柄，故本表**不预注册空壳号位**（半注册 = RUNTXP 过了元数校验却在主段
//! 静默无效果，比 C# 未注册路径的 ERR_NO_TRANSACTION_PROCEDURE 更坏）。
//!
//! 表内规划号位与服务端扩展族的对应为 `C# 宿主注册序 + 2`：C# 侧
//! main/GarnetServer/Program.cs 以注册序取号（READWRITETX 首注册即 0，
//! MSETPX/MGETIFPM/GETTWOKEYSNOTXN/SAMPLEUPDATETX/SAMPLEDELETETX 顺次 1..5），
//! rust 侧 0 号位归 default 兜底投影、1 号位归 NoOpModule 过程，故扩展族整体
//! 后移两位。
//!
//! ## 能力缺位清单
//!
//! 全 C# 仓 30 个事务过程子类（服务端扩展族 6 + modules 1 + 测试与基准 23），
//! 体段完全不依赖 `api.*` 存储原语的只有两个：modules/NoOpModule 的 NoOpTxn
//! （输出空 → 宿主回 +OK，已落地 1 号位）与 test/standalone/Garnet.test.acl
//! 的 AclNoOpTxn（主段自写 +OK 一条 simple string）。后者是 ACL 测试宿主内
//! 自造的夹具过程（该宿主首个注册，C# 内 id 0），既被 js/check/ignore/test.yml
//! 以整文件声明不实现，也不该占用一经 AOF 落盘即不可回收的生产号位，故只登记
//! 不移植；其「主段自写输出」的体段形态已由 tests 面的夹具过程在同一三段式上
//! 覆盖。其余 28 个（含 TestProcedureHash/Set/Lists/SortedSets/Bitmap/HLL、
//! DeleteTxn、ObjectExpiryTxn、SortedSetRemoveTxn、WriteWithExpiryTxn、
//! BulkIncrementBy、BulkRead、RateLimiterTxn、SortedSetCountTxn、TxnCustomCmd、
//! AofFinalizeDoubleReplayTxn、ClusterDelRmw、TestCluster{ReadOnly,ReadWrite}
//! CustomTxn、LargeGetTxn、RandomSubstituteOrExpandValForKeyTxn、CustomTxnSet）
//! 每个体段都至少调一次存储原语，注入面缺席期一律不移植。

use crate::custom_transaction_procedure::{CustomTxnProc, DefaultTxnProc, NoOpTxnProc};

/// 槽位 id（编译期固定；AOF 日志头 procedure_id 与 RUNTXP 首参直取）
pub mod txn_proc_slot {
  /// 空事务过程（DefaultTxnProc：三段式直通，libs/server/Custom/
  /// CustomTransactionProcedure.cs 抽象基类最小投影）
  pub const DEFAULT: u8 = 0;
  /// 空操作事务过程（C# modules/NoOpModule/NoOpTxn.cs:NoOpTxn，
  /// 由 NoOpModule.cs 以名 `NoOpModule.NOOPTXN`、元数 1 注册）
  pub const NOOP: u8 = 1;
}

/// 已注册号位全集（新增号位必须同步登记：派发与元数据一致性核对据此遍历，
/// 杜绝「加常量不加过程体」的半注册）
pub const REGISTERED_SLOTS: &[u8] = &[txn_proc_slot::DEFAULT, txn_proc_slot::NOOP];

/// 按槽位 id 静态构造事务过程实例（未匹配 = 未注册；对标
/// CustomCommandManagerSession.cs:GetCustomTransactionProcedure 的未命中路径）
pub const fn txn_proc(id: u8) -> Option<CustomTxnProc> {
  match id {
    txn_proc_slot::DEFAULT => Some(CustomTxnProc::Default(DefaultTxnProc {
      id: txn_proc_slot::DEFAULT,
    })),
    txn_proc_slot::NOOP => Some(CustomTxnProc::NoOp(NoOpTxnProc {
      id: txn_proc_slot::NOOP,
    })),
    _ => None,
  }
}

/// 按槽位 id 静态取过程元数据（过程名 + 元数；RUNTXP 元数校验与
/// C# 注册名承接，编译期常量直取）
///
/// 元数为 C# `RespCommandsInfo.Arity` 原值（含 RUNTXP 首参的过程 id：
/// C# NetworkRUNTXP 以 `count != arity` 判，count 为 `RUNTXP` 之后的
/// 参数个数）；0 = 不校验。
pub const fn txn_proc_meta(id: u8) -> Option<(&'static str, i32)> {
  match id {
    txn_proc_slot::DEFAULT => Some(("default", 0)),
    txn_proc_slot::NOOP => Some(("NoOpModule.NOOPTXN", 1)),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use wtxn::TxnProcedure;

  use super::*;

  #[test]
  fn dispatches_by_slot_id() {
    // 槽位 id 静态派发：已注册号位逐个命中
    for id in REGISTERED_SLOTS {
      assert!(txn_proc(*id).is_some(), "号位 {id} 已登记却派发不出过程");
    }
    // 未注册号位（含号段规划位 2..=7 与越界 id）必不命中
    assert!(txn_proc(2).is_none());
    assert!(txn_proc(7).is_none());
    assert!(txn_proc(9).is_none());
    assert!(txn_proc(u8::MAX).is_none());
  }

  #[test]
  fn builds_proc_with_slot_id() {
    for id in REGISTERED_SLOTS {
      let built = txn_proc(*id).unwrap();
      assert_eq!(built.id(), *id, "过程自报 id 与注册号位不一致");
    }
  }

  #[test]
  fn meta_matches_slot_id() {
    let (name, arity) = txn_proc_meta(txn_proc_slot::DEFAULT).unwrap();
    assert_eq!(name, "default");
    assert_eq!(arity, 0);

    // C# NoOpModule.cs 以 Arity = 1 注册（过程本身零参数）
    let (name, arity) = txn_proc_meta(txn_proc_slot::NOOP).unwrap();
    assert_eq!(name, "NoOpModule.NOOPTXN");
    assert_eq!(arity, 1);

    assert!(txn_proc_meta(9).is_none());
  }

  #[test]
  fn registered_slots_carry_unique_meta() {
    // 注册面自洽：每个已注册号位都要有元数据，且过程名不重复
    let mut names: Vec<&str> = REGISTERED_SLOTS
      .iter()
      .map(|id| {
        let (name, arity) = txn_proc_meta(*id).expect("号位缺元数据");
        assert!(arity >= 0, "元数为负不合法");
        assert!(!name.is_empty(), "过程名为空");
        name
      })
      .collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "过程注册名重复");
  }
}

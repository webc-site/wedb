use std::{io, result};

use thiserror::Error;
use wbase::group_commit::Broken;

use crate::range_index::RangeIndexError;

/// 集合与范围索引树算子统一错误 (原 wcol::Error 下沉；对标 Garnet 存储层算子错误)
#[derive(Error, Debug)]
pub enum CollectionError {
  /// 底层 BfTree 操作失败
  #[error(transparent)]
  Tree(#[from] wbftree::Error),

  /// 数据损坏或格式不合法
  #[error("数据格式损坏: {0}")]
  Corrupted(&'static str),

  /// 参数非法
  #[error("参数非法: {0}")]
  InvalidArgument(&'static str),

  /// 空值非法（底层树约束）
  #[error("空值非法")]
  EmptyValue,

  /// 键或字段超长
  #[error("键或字段超长")]
  KeyTooLong,
}

/// 集合树算子 Result 别名
pub(crate) type CollectionResult<T> = result::Result<T, CollectionError>;

/// wedb_store 统一错误类型
#[derive(Error, Debug)]
pub enum Error {
  #[error(transparent)]
  Device(#[from] wdev::Error),

  #[error(transparent)]
  Epoch(#[from] wepoch::Error),

  #[error(transparent)]
  HLog(#[from] whlog::Error),

  #[error(transparent)]
  Index(#[from] windex::Error),

  #[error(transparent)]
  Record(#[from] wrecord::Error),

  #[error(transparent)]
  Value(#[from] wval::Error),

  #[error("配置错误: {0}")]
  InvalidConfig(String),

  /// 索引容量与配置不一致（恢复组件装配预检）
  #[error(
    "index_size 配置与实际索引容量不一致: 配置 {config} 桶, 实际 {actual} 桶; 恢复组件装配禁止缩表或漂移, 请将 index_size 设为 {actual} 与索引快照一致, 或全新建库"
  )]
  IndexSizeMismatch {
    /// 配置声称的哈希索引桶数
    config: usize,
    /// 实际构建/恢复出的哈希索引桶数
    actual: usize,
  },

  #[error(transparent)]
  Io(#[from] io::Error),

  /// 集合与范围索引树错误单点上浮入口（wbftree::Error 经 CollectionError::Tree
  /// 唯一注入，禁止直挂 BfTree 变体造成同源双入口）
  #[error(transparent)]
  Collection(#[from] CollectionError),

  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),

  #[error(transparent)]
  Compact(#[from] wcompact::Error),

  #[error(transparent)]
  Cpr(#[from] wcpr::Error),

  /// 刷盘/提交流水线中断（类型单点 wbase::group_commit::Broken）
  #[error(transparent)]
  GroupCommit(#[from] Broken),

  /// AOF 日志入队失败（写监听端口转发拒绝：主存写入已生效，AOF 缺条目，
  /// 调用方须以错误拒绝该命令防主从发散）
  #[error("AOF 日志入队失败: {0}")]
  AofEnqueue(String),

  /// 页驱逐内核收到降级哨兵（DEGRADE_ASYNC 误喂 raw 级驱逐循环的显性拒绝：
  /// 哨兵若放行，old_page 溢出回绕会把只读线推过 tail，全表记录被判磁盘区）
  #[error("页驱逐内核拒绝降级哨兵误喂（tag 级 DEGRADE_ASYNC 严禁接入 raw 级驱逐循环）")]
  EvictSentinel,

  /// 阻塞卸载任务异常退出（compio spawn_blocking 句柄 Err：闭包 panic 或
  /// 运行时关闭取消，非业务错误；单点上浮杜绝字符串散落调用点）
  #[error("阻塞卸载任务异常退出: {0}")]
  BlockingJoin(String),

  /// 在线扩容尾段重扫超限（分块迁移重试的确定性上界，防静默哈希区间驱动线程
  /// 永久自旋；常态不可达：回滚分块每圈重试 CAS 必决出，超限即并发异常，扩容
  /// 中止走检查点索引重建自愈）
  #[error("在线扩容分块迁移重扫超限: 仍有 {0} 个分块未完成迁移")]
  GrowRescanExhausted(usize),

  /// 一致读等待回放推进超时（对标 C# VirtualSublogReplayState.WaitForSequenceNumber
  /// 超时抛 TimeoutException 中止一致读：libs/server/AOF/ReadConsistency/
  /// VirtualSublogReplayState.cs:224；读中止不静默续读）
  #[error("一致读等待回放推进超时")]
  ConsistentReadTimeout,

  /// 键处于 RENAME 迁移原子窗（非持久内存迁移 claim 在册）：写臂显式拒绝的
  /// 锁忙/重试语义，客户端重试即收敛（窗口毫秒级）。禁「视同不存在」穿透——
  /// 穿透会在 dst 信封域物化重建对象或走 RI 重建路径，换一种已 ACK 写丢失形
  #[error("键迁移进行中，请稍后重试")]
  MigrationBusy,

  /// 升阶树页缓存全局预算耗尽（wbftree CacheBudgetExhausted 的单点上浮形态，
  /// promote.rs 唯一映射口）：调用方（wnode 升阶臂 / 到期重灌臂）据此回落
  /// 信封态或推迟重灌，命令面不失败
  #[error("树页缓存预算耗尽")]
  CacheBudgetExhausted,

  /// 树物理面已实际变更之后的失败（升阶新树已原子换入后 save meta / 信封删除
  /// 失败；排空元记录墓碑已落盘后 del_ttl/del_etag/树注销失败）：内层为真实
  /// 存储错误，Display 与 source 透明转发。换入分级判据供分层臂 WATCH 栅栏
  /// 消费——该形态树内容已实质变更，已置脏保持为真（推进真实反映树变更）；
  /// 与未换入失败（树内容零变更，须复位脏标记杜绝假推进误夭折并发 WATCH
  /// 事务）在错误面清晰可判别
  #[error(transparent)]
  Swapped(Box<Error>),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
  /// 「树物理面已实际变更之后的失败」单点构造（一处定义，禁散落 `Box::new`）：
  /// promote.rs 换入已生效臂与 drain.rs / collection.rs 墓碑已落盘臂的统一包装口，
  /// 消费方经 `matches!(e, Error::Swapped(_))` 判别置脏去留
  pub(crate) fn swapped(e: Error) -> Self {
    Self::Swapped(Box::new(e))
  }
}

/// wbftree 树错误单点注入：统一经 CollectionError::Tree 上浮为 Collection 变体，
/// 杜绝同源双入口（原 BfTree 直挂变体已删）；`?` 传播点零改动
impl From<wbftree::Error> for Error {
  fn from(e: wbftree::Error) -> Self {
    Self::Collection(e.into())
  }
}

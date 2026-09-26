//! 集合项经纪取件源（存储适配域）
//!
//! 对标 C# CollectionItemBroker.TryGetResult 内联的 storageSession 事务
//! 取件路径（GET 判型 → 事务 RMW 弹出/搬移 → 提交）：Rust 侧以经纪专属
//! `wkv::StoreSession`（装配时 `store.new_session()` 派生）承接，每次试取
//! 短暂进入批处理纪元，走与命令层同一信封格式（`[类型标签][bitcode 载荷]`）
//! 的同步快路径。
//!
//! 刻意差异（对照 C#）：
//! - C# 以 Tsavorite 事务保证弹/搬原子性；Rust 同步快路径无事务域，
//!   BLMOVE 按先写目标再写源顺序落库（源写失败时目标可能重复入队，
//!   仅磁盘候选降级这一命令层同步路径本就不可达的场景可触发）；
//!   List 与 ZSet 出件臂均与命令层装载型写臂同规格取 rmw 窗并做落笔前
//!   域归属复验（票 load-type-rmw-window zset 侧先落、list 侧补票
//!   zcode-r165c-listmove；BLMOVE 经 try_sync_rmw_window_pair 桶升序双窗，
//!   同键旋转计划折叠去重为单窗），不可取即整体拒写重试；
//! - 磁盘候选与活跃分层键同步不可出件，一律按不可取处理；阻塞族命令层
//!   park 前预探（obj_load_sync_degrades），此类键整体路由慢路径异步臂出件，
//!   慢路径装载未取到时经经纪等待面闭环（slow.rs BlockWaitFace，C#
//!   BlockingWait 键态解耦语义）。Degrade 臂以 [`TryGetOutcome::degrade`]
//!   独立标定"键已离开经纪同步服务域"，且经 `is_live_tiered_collection`
//!   复判成因——唯 Meta 存活分层键（真升阶，同步装载恒 Degrade）送客：
//!   经纪命中即对该键全队列送空应答清队摘键（客户端空回复重试经预探路由
//!   慢路径，闭环不饥饿）。信封 / String 域磁盘候选为过渡态，保持不可取
//!   挂队——更新事件物化内存信封后经纪即可出件（冷墓碑挂起等待是阻塞族
//!   契约，list_blocking_cold_wait 域），park 后升阶的存量观察者（升阶
//!   写入必经慢路径收尾 notify 试取即命中真升阶）与升阶/降阶过渡窗竞态
//!   均由此收口，"稳定态不再入经纪"的注册面论证配合出件面送客自洽。

use smallvec::SmallVec;
use wcol::{
  ObjLoad,
  itembroker::{
    collection_item_broker::{
      CollectionItemStore, TryGetOutcome, try_get_next_list_item, try_get_next_sorted_set_item,
      try_move_next_list_item,
    },
    collection_item_observer::CollectionItemResult,
  },
  list::list_object::{ListObject, OperationDirection},
  object_payload::GarnetObjectPayload,
  zset::sorted_set_object::SortedSetObject,
};
use wdev::Device;
use wkv::{BatchStoreSession, StoreResult, StoreSession};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::object_store_utils::{
  obj_load_typed_sync, obj_save_or_gc, obj_writeback_recheck_sync, try_sync_rmw_window_pair,
};

/// 同步探测键是否为活跃 BfTree 独立分层集合
#[inline]
fn is_live_tiered_collection<D: Device>(
  batch: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> bool {
  matches!(
    batch.try_read_tag_sync(key, KeyTag::Meta, |raw| MetaValue::from_slice(raw).ok()),
    Ok(StoreResult::Success(Some(meta))) if meta.is_live() && meta.collection_type == tag
  )
}

/// 经纪专属取件源：绑定独立存储会话的同步 List/ZSet 出件执行体
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult
/// （存储会话域适配，装配处注入 CollectionItemBroker::new）
///
/// 域语义（对照 C#）：C# 取件经观察者自身会话的 storageSession，域随观察者；
/// rust 单例会话在每次试取入口按观察者域 (ns, db) `set_context` 等价切换
/// （经纪主循环单消费者串行换域，无竞态）。
pub struct CollectionItemSource<D: Device> {
  /// 经纪专属存储会话（独立纪元参与者，不与任何连接会话共享）
  session: StoreSession<D>,
}

/// 独占单写者安全论证（与 [`StoreGarnetApi`](crate::resp::garnet_api::StoreGarnetApi)
/// 的 `Sync` 收紧同批同口径，wepoch participant.rs 线程亲和契约的宿主侧承接）：
///
/// 本会话的**唯一触达面**是经纪主循环单消费者臂——`CollectionItemStore::
/// try_get_result` 只经 `CollectionItemBroker` 主循环事件段调用
/// （`initialize_observer` / `try_assign_item_from_key` 两个落点同处
/// `start_async` 单任务循环，compio thread-per-core 任务不迁线程；模块头
/// 「经纪主循环单消费者串行换域，无竞态」即本论证本体）。连接线程侧的
/// `handle_collection_update` 只投事件队列（papaya + 队列锁，纯 `Sync`
/// 安全面），从不触达本会话；纪元参与者的进入/退出因此恒在主循环属主线程
/// 的同步段内配对。
///
/// 违反纪律即 UB：任何新增自他线程调用 `try_get_result` 的执行体必须改为
/// 经主循环事件投递。
unsafe impl<D: Device> Sync for CollectionItemSource<D> {}

impl<D: Device> CollectionItemSource<D> {
  /// 构造取件源（`session` 为装配处 `store.new_session()` 派生的独立会话）
  pub fn new(session: StoreSession<D>) -> Self {
    Self { session }
  }

  /// 方向字节解码（cmdArgs 内单字节 OperationDirection；未知值 → Unknown）
  fn decode_direction(byte: Option<&u8>) -> OperationDirection {
    match byte {
      Some(0) => OperationDirection::Left,
      Some(1) => OperationDirection::Right,
      _ => OperationDirection::Unknown,
    }
  }

  /// List 族出件：BLPOP/BRPOP 单弹、BLMPOP 批量弹、BLMOVE 弹+推
  ///（TryGetResult 的 List 存储会话域适配：信封装载判型 → 对象层出件 →
  /// 变更回写；出件语义复用 try_get_next_list_item 族对象层实现）
  fn list_outcome(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    let batch = self.session.enter_batch();
    // 装载型取件臂双保护·同步档（与 zset_outcome 及命令层 list_move_core 同套
    // 单机制，先窗后装，跨「装载 → 出件 → 写回」全程，票 zcode-r165c-listmove
    // 案二）：BLMOVE 取 dstKey（cmd_args[0]）与源键的桶升序排他双窗，同键旋转
    // 两槽计划折叠去重为单窗；BLPOP/BRPOP/BLMPOP 单键窗。失闩非键缺/空集，
    // 报争用位交经纪让核重投（同 zset 臂裁决，非空键不悬挂）
    let move_dst = if command == RespCommand::Blmove {
      cmd_args.first().map(Vec::as_slice)
    } else {
      None
    };
    let windows = match move_dst {
      Some(dst) => try_sync_rmw_window_pair(&batch, key, dst),
      None => batch.try_rmw_window(key).map(|w| SmallVec::from_iter([w])),
    };
    let Some(_windows) = windows else {
      return TryGetOutcome::contended();
    };
    let mut scratch = Vec::new();
    let mut src = match obj_load_typed_sync(
      &batch,
      key,
      GarnetObjectType::List,
      &mut scratch,
      ListObject::from_blob,
    ) {
      ObjLoad::Missing => return TryGetOutcome::none(),
      // 源类型不符：首次试取（failOnSrcTypeMismatch）即回 WRONGTYPE
      ObjLoad::WrongType => {
        return if fail_on_src_type_mismatch {
          TryGetOutcome::found(0, CollectionItemResult::type_mismatch())
        } else {
          TryGetOutcome::none()
        };
      }
      // 活跃分层键同步不可出件，报 degrade 位清退送客；普通磁盘候选/冷墓碑继续保持挂起
      ObjLoad::Degrade => {
        if is_live_tiered_collection(&batch, key, GarnetObjectType::List) {
          return TryGetOutcome::degrade();
        }
        return TryGetOutcome::none();
      }
      ObjLoad::Present(o) => o,
    };

    let curr_count = src.list.len();
    if curr_count == 0 {
      return TryGetOutcome::with_count(0);
    }

    match command {
      RespCommand::Blpop | RespCommand::Brpop => {
        let Some(item) = try_get_next_list_item(&mut src, command) else {
          return TryGetOutcome::with_count(curr_count);
        };
        let remaining = curr_count - 1;
        // 落笔前域归属复验（窗内对面 DEL/SET 交叠即弃写按不可取处理，同 zset 臂）
        if !obj_writeback_recheck_sync(&batch, key, true) {
          return TryGetOutcome::none();
        }
        if !save_list(&batch, key, &src) {
          return TryGetOutcome::with_count(remaining);
        }
        TryGetOutcome::found(remaining, CollectionItemResult::single(key.to_vec(), item))
      }
      RespCommand::Blmpop => {
        // cmd_args: [popDirection(1B), popCount(i32 LE 4B)]
        if cmd_args.len() < 2 || cmd_args[1].len() < 4 {
          return TryGetOutcome::with_count(curr_count);
        }
        let pop_dir = Self::decode_direction(cmd_args[0].first());
        let pop_count = usize::try_from(i32::from_le_bytes(
          cmd_args[1][..4].try_into().unwrap_or([0; 4]),
        ))
        .unwrap_or(0);
        if pop_dir == OperationDirection::Unknown || pop_count == 0 {
          return TryGetOutcome::with_count(curr_count);
        }

        let cmd = if pop_dir == OperationDirection::Left {
          RespCommand::Blpop
        } else {
          RespCommand::Brpop
        };
        let mut items = Vec::with_capacity(pop_count);
        for _ in 0..pop_count {
          match try_get_next_list_item(&mut src, cmd) {
            Some(item) => items.push(item),
            None => break,
          }
        }
        if items.is_empty() {
          return TryGetOutcome::with_count(curr_count);
        }
        // 落笔前域归属复验（同 BLPOP/BRPOP 臂）
        if !obj_writeback_recheck_sync(&batch, key, true) {
          return TryGetOutcome::none();
        }
        if !save_list(&batch, key, &src) {
          return TryGetOutcome::with_count(curr_count);
        }
        TryGetOutcome::found(
          curr_count - items.len(),
          CollectionItemResult::multiple(key.to_vec(), items),
        )
      }
      RespCommand::Blmove => {
        // cmd_args: [dstKey, srcDir(1B), dstDir(1B)]
        if cmd_args.len() < 3 {
          return TryGetOutcome::with_count(curr_count);
        }
        let dst_key = &cmd_args[0];
        let (src_dir, dst_dir) = (
          Self::decode_direction(cmd_args[1].first()),
          Self::decode_direction(cmd_args[2].first()),
        );
        if src_dir == OperationDirection::Unknown || dst_dir == OperationDirection::Unknown {
          return TryGetOutcome::with_count(curr_count);
        }

        // 同键旋转（C# TryGetNextListResult 同键形态）：源与目标同一列表，
        // 严禁二次独立装载 dst 后双写回——后写覆盖前写致元素蒸发（票
        // zcode-r165c-listmove 案一，与命令层 list_move_core 同键臂同款就地旋转）
        if key == dst_key.as_slice() {
          // 同端搬移或单元素轮转：列表不变，原样返回端元素且不落库
          //（C# 同键 no-pop 捷径，兼防清空丢 TTL）
          if src_dir == dst_dir || curr_count == 1 {
            let unchanged = if src_dir == OperationDirection::Right {
              src.list.back().cloned()
            } else {
              src.list.front().cloned()
            };
            return match unchanged {
              Some(item) => {
                TryGetOutcome::found(curr_count, CollectionItemResult::single(key.to_vec(), item))
              }
              None => TryGetOutcome::with_count(curr_count),
            };
          }
          // 异端旋转：已装载的 src 上端点弹出并推入对端，仅一次写回，
          // 元素数不变、不派发目标键冗余唤醒（弹推经同一记录写回即原子）
          let popped = if src_dir == OperationDirection::Right {
            src.list.pop_back()
          } else {
            src.list.pop_front()
          };
          let Some(moved_item) = popped else {
            return TryGetOutcome::with_count(curr_count);
          };
          if dst_dir == OperationDirection::Left {
            src.list.push_front(moved_item.clone());
          } else {
            src.list.push_back(moved_item.clone());
          }
          if !obj_writeback_recheck_sync(&batch, key, true) {
            return TryGetOutcome::none();
          }
          if !save_list(&batch, key, &src) {
            return TryGetOutcome::with_count(curr_count);
          }
          return TryGetOutcome::found(
            curr_count,
            CollectionItemResult::single(key.to_vec(), moved_item),
          );
        }

        // 目标键装载：存在即须为 List（C# 目标类型恒校验，不看 fail 标志）
        let mut dst_existed = false;
        let mut dst = match obj_load_typed_sync(
          &batch,
          dst_key,
          GarnetObjectType::List,
          &mut scratch,
          ListObject::from_blob,
        ) {
          ObjLoad::Missing => ListObject::new(),
          ObjLoad::WrongType => {
            return TryGetOutcome::found(curr_count, CollectionItemResult::type_mismatch());
          }
          // 目标键已分层：经纪域写不进（信封写回降级），搬移无法完成——同报
          // degrade 送客；普通冷目标键保持挂起
          ObjLoad::Degrade => {
            if is_live_tiered_collection(&batch, dst_key, GarnetObjectType::List) {
              return TryGetOutcome::degrade();
            }
            return TryGetOutcome::with_count(curr_count);
          }
          ObjLoad::Present(o) => {
            dst_existed = true;
            o
          }
        };

        let Some(moved_item) = try_move_next_list_item(&mut src, &mut dst, src_dir, dst_dir) else {
          return TryGetOutcome::with_count(curr_count);
        };

        // 先写目标再写源（取舍见模块文档）；两键落笔前各按自身装载态复验
        // 域归属（deviations §100 写回序不豁免复验，同命令层 list_move_core）
        if !obj_writeback_recheck_sync(&batch, dst_key, dst_existed)
          || !save_list(&batch, dst_key, &dst)
          || !obj_writeback_recheck_sync(&batch, key, true)
          || !save_list(&batch, key, &src)
        {
          return TryGetOutcome::with_count(curr_count);
        }
        TryGetOutcome::moved(
          curr_count - 1,
          CollectionItemResult::single(key.to_vec(), moved_item),
          dst_key.clone(),
        )
      }
      _ => TryGetOutcome::with_count(curr_count),
    }
  }

  /// ZSet 族出件：BZPOPMIN/BZPOPMAX 单弹、BZMPOP 批量弹
  ///（TryGetResult 的 ZSet 存储会话域适配：信封装载判型 → 对象层出件 →
  /// 变更回写；出件语义复用 try_get_next_sorted_set_item 对象层实现）
  fn zset_outcome(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    let batch = self.session.enter_batch();
    let mut scratch = Vec::new();
    // 装载型取件臂双保护·同步档（票 load-type-rmw-window zset 留尾）：经纪取件
    // 与连接线程 run_sync_rmw 抢同一用户键桶排他闩（窗位为 store 全域，scoped
    // 口径同键同桶），装载前取窗跨「装载 → 弹出 → 写回」全程；落笔前域归属
    // 复验不过一律按不可取处理（观察者保持挂起，下次更新事件重试），绝不裸写
    // 回顶掉已 ACK 写。失闩非键缺/空集：外层事务（EXEC 重放期与本臂 scoped
    // 同桶互斥，票 wtxn-wkv-keybucket-hash-scope-desync）或他窗正持本键桶，
    // 报争用位交经纪让核重投 CollectionUpdated 事件闭环——只挂队不重投则
    // 外层唯一写点已过、后续无写入时 timeout=0 永久悬挂（C# 无此形态，其
    // TryGetResult 经外层事务直取，CollectionItemBroker.cs:585-598）
    let Some(_window) = batch.try_rmw_window(key) else {
      return TryGetOutcome::contended();
    };
    let mut obj = match obj_load_typed_sync(
      &batch,
      key,
      GarnetObjectType::SortedSet,
      &mut scratch,
      SortedSetObject::from_blob,
    ) {
      ObjLoad::Missing => return TryGetOutcome::none(),
      ObjLoad::WrongType => {
        return if fail_on_src_type_mismatch {
          TryGetOutcome::found(0, CollectionItemResult::type_mismatch())
        } else {
          TryGetOutcome::none()
        };
      }
      // 活跃分层键同步不可出件，报 degrade 位清退送客；普通磁盘候选/冷墓碑继续保持挂起
      ObjLoad::Degrade => {
        if is_live_tiered_collection(&batch, key, GarnetObjectType::SortedSet) {
          return TryGetOutcome::degrade();
        }
        return TryGetOutcome::none();
      }
      ObjLoad::Present(o) => o,
    };

    let curr_count = obj.purge_expired_len();
    if curr_count == 0 {
      // 删空自愈（对标 C# SortedSetObject.Operate :452-453 REMOVE_KEY）：
      // 全成员到期剔空时须写删空墓碑清退幽灵空键，再回计数 0
      if obj.mutated_by_ttl() {
        if !obj_writeback_recheck_sync(&batch, key, true) {
          return TryGetOutcome::none();
        }
        let _ = obj_save_or_gc(&batch, key, GarnetObjectType::SortedSet, &obj, true, |o| {
          o.to_blob()
        });
      }
      return TryGetOutcome::with_count(0);
    }

    let Some(result) = try_get_next_sorted_set_item(key, &mut obj, curr_count, command, cmd_args)
    else {
      return TryGetOutcome::with_count(curr_count);
    };
    // BZPOPMIN/BZPOPMAX 单弹；BZMPOP 按结果元素数
    let popped = result.items.as_ref().map_or(1, Vec::len);

    if !obj_writeback_recheck_sync(&batch, key, true) {
      return TryGetOutcome::none();
    }
    if !obj_save_or_gc(
      &batch,
      key,
      GarnetObjectType::SortedSet,
      &obj,
      obj.sorted_set_dict.is_empty(),
      |o| o.to_blob(),
    )
    .unwrap_or(false)
    {
      return TryGetOutcome::with_count(curr_count);
    }
    TryGetOutcome::found(curr_count - popped, result)
  }
}

/// List 信封写回（空列表整键回收）；false = 磁盘候选降级
fn save_list(
  batch: &wkv::BatchStoreSession<'_, impl Device>,
  key: &[u8],
  obj: &ListObject,
) -> bool {
  obj_save_or_gc(
    batch,
    key,
    GarnetObjectType::List,
    obj,
    obj.list.is_empty(),
    |o| o.to_blob(),
  )
  .unwrap_or(false)
}

impl<D: Device> CollectionItemStore for CollectionItemSource<D> {
  fn try_get_result(
    &self,
    ns: u64,
    db: u64,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    // 取件域切换到观察者所属域（非严格会话对已绑定在线租户为纯内存物化；
    // C# 域随 observer.Session.storageSession 的等价承接）
    self.session.set_context(ns, db);
    match command {
      RespCommand::Blpop | RespCommand::Brpop | RespCommand::Blmove | RespCommand::Blmpop => {
        self.list_outcome(key, command, cmd_args, fail_on_src_type_mismatch)
      }
      RespCommand::Bzpopmin | RespCommand::Bzpopmax | RespCommand::Bzmpop => {
        self.zset_outcome(key, command, cmd_args, fail_on_src_type_mismatch)
      }
      _ => TryGetOutcome::none(),
    }
  }
}

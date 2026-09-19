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
//! - 磁盘候选与活跃分层键同步不可出件，一律按不可取处理；阻塞族命令层
//!   park 前预探（obj_load_sync_degrades），此类键整体路由慢路径异步臂出件，
//!   经纪域余下的 Degrade 仅为升阶/降阶过渡窗口竞态——观察者挂起至下一次
//!   更新事件试取自愈（过渡态终会落回内存信封或分层稳定态，稳定态不再入经纪）。

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
use wkv::StoreSession;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

use super::object_store_utils::{obj_load_typed_sync, obj_save_or_gc};

/// 经纪专属取件源：绑定独立存储会话的同步 List/ZSet 出件执行体
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult
/// （存储会话域适配，装配处注入 CollectionItemBroker::new）
pub struct CollectionItemSource<D: Device> {
  /// 经纪专属存储会话（独立纪元参与者，不与任何连接会话共享）
  session: StoreSession<D>,
}

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
      // 磁盘候选：观察者保持挂起（见模块文档刻意差异）
      ObjLoad::Degrade => return TryGetOutcome::none(),
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
        if items.is_empty() || !save_list(&batch, key, &src) {
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

        // 目标键装载：存在即须为 List（C# 目标类型恒校验，不看 fail 标志）
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
          ObjLoad::Degrade => return TryGetOutcome::with_count(curr_count),
          ObjLoad::Present(o) => o,
        };

        // 同键同端搬移或单元素轮转：列表不变，原样返回端元素且不落库
        //（C# TryGetNextListResult 同键捷径，兼防清空丢 TTL）
        if key == dst_key.as_slice() && (src_dir == dst_dir || curr_count == 1) {
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

        let Some(moved_item) = try_move_next_list_item(&mut src, &mut dst, src_dir, dst_dir) else {
          return TryGetOutcome::with_count(curr_count);
        };

        // 先写目标再写源（取舍见模块文档）
        if !save_list(&batch, dst_key, &dst) || !save_list(&batch, key, &src) {
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
      ObjLoad::Degrade => return TryGetOutcome::none(),
      ObjLoad::Present(o) => o,
    };

    let curr_count = obj.purge_expired_len();
    if curr_count == 0 {
      return TryGetOutcome::with_count(0);
    }

    let Some(result) = try_get_next_sorted_set_item(key, &mut obj, curr_count, command, cmd_args)
    else {
      return TryGetOutcome::with_count(curr_count);
    };
    // BZPOPMIN/BZPOPMAX 单弹；BZMPOP 按结果元素数
    let popped = result.items.as_ref().map_or(1, Vec::len);

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
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
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

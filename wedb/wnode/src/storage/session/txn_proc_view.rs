//! 事务过程存储视图（C# TransactionManager.cs:82-88 garnetTxPrepareApi /
//! garnetTxMainApi / garnetTxFinalizeApi 三 api 字段与 libs/server/API/
//! IGarnetApi.cs 原语面的宿主承接：过程体经 [`wtxn::TxnProcApi`] 访问存储，
//! 视图包 [`StorageSession`] 既有原语，快路径同步直回、慢路径 compio
//! 单线程内联收割（对标 C# CompletePending(wait: true)，收割口为全仓单点
//! `wbase::future::blocking_wait`，与 acl_store / vector_store_callbacks 同源，
//! 非第二套驱动）。
//!
//! main/finalize 两段共用本视图：C# 的 `TransactionalGarnetApi` 事务视图与
//! `BasicGarnetApi` 直连视图在 rust 合一，事务隔离由条带锁层完成；prepare
//! 段的只读界与读即 WATCH 由 wtxn [`wtxn::TxnWatchApi`] 包装承接（C#
//! GarnetWatchApi<BasicGarnetApi> 对位）。
//!
//! 映射口径：本视图各方法只是 C# 过程体 api 面的一跳派发（C# 侧 GarnetApi
//! 多态跳板转 storageSession 同名原语），跳板与接口层按 js/check/ignore/
//! server.yml 的甄别不建映射；主存 GET / SET / SETEX / Increment 的落点锚点
//! 单点挂在 [`StorageSession`] 的对应原语上，有序集两法的落点挂在 RESP 层
//! rmw 骨架冷臂，本文件只述派发链路不复挂锚点（一处定义）。

use itoa::Buffer;
use wbase::{future::blocking_wait, num::strict_i64};
use wcol::zset::sorted_set_object::SortedSetOperation;
use wdev::Device;
use wresp::resp_memory_writer::format_double;
use wtxn::TxnProcApi;
use zmij::Buffer as FloatBuf;

use crate::{
  resp::objects::sorted_set_commands::{Rmw, slow::zset_rmw_cold},
  storage::session::{
    common::{UserRead, read_user_sync},
    storage_session::StorageSession,
  },
};

/// 事务过程存储视图（包一个存储会话；构造点即 C# 三 api 的装配点）
pub struct TxnProcView<'s, 'a, D: Device> {
  /// 底层存储会话
  storage: &'s StorageSession<'a, D>,
}

impl<'s, 'a, D: Device> TxnProcView<'s, 'a, D> {
  /// 包裹存储会话（在线宿主与 AOF 回放宿主共用本类型）
  pub fn new(storage: &'s StorageSession<'a, D>) -> Self {
    Self { storage }
  }

  /// 有序集单成员操作内核（ZADD score member / ZREM member 单对形态，
  /// C# api.SortedSetAdd / api.SortedSetRemove 的投影；直接复用 RESP 层
  /// rmw 骨架冷臂单源，应答字节写弃置缓冲，成功以对象在场判定——
  /// 对标 C# GarnetStatus::Ok）
  fn zset(&mut self, key: &[u8], op: SortedSetOperation, score: f64, member: &[u8]) -> bool {
    let mut score_buf = FloatBuf::new();
    let score_str = format_double(score, &mut score_buf);
    let args: [&[u8]; 2] = [score_str.as_bytes(), member];
    let mut scratch = Vec::new();
    // resp_version 取 RESP2（视图丢弃应答字节，操作结果不受协商版本影响）
    matches!(
      blocking_wait(zset_rmw_cold(
        self.storage,
        key,
        op,
        &args,
        (0, 0),
        2,
        &mut scratch,
      )),
      Ok(Rmw::Present(_))
    )
  }
}

impl<D: Device> TxnProcApi for TxnProcView<'_, '_, D> {
  /// GET 派发（双域读：String 域命中即值，对象键 / 缺失折叠为 None；
  /// 同步域直读，冷键降级 [`StorageSession::read_string`]）
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    match read_user_sync(&self.storage.batch, key, |v| v.to_vec()) {
      Ok(UserRead::Hit(v)) => Some(v),
      Ok(UserRead::WrongType | UserRead::Missing) => None,
      // 冷键磁盘候选：同步域内联收割异步读（对标 CompletePending(wait: true)）
      Ok(UserRead::Deferred) => blocking_wait(self.storage.read_string(key)).ok().flatten(),
      Err(_) => None,
    }
  }

  /// SET 派发（SET 语义清既有 TTL，转 [`StorageSession::upsert_string`]）
  fn set(&mut self, key: &[u8], val: &[u8]) -> bool {
    blocking_wait(self.storage.upsert_string(key, val)).is_ok()
  }

  /// SETEX 派发（ticks 为 .NET TimeSpan 口径，转 [`StorageSession::setex`]）
  fn setex(&mut self, key: &[u8], val: &[u8], expiry_ticks: i64) -> bool {
    blocking_wait(self.storage.setex(key, val, expiry_ticks)).is_ok()
  }

  /// DELETE 派发（返回键是否存在，转 [`StorageSession::delete_string`]；
  /// C# 侧该删除走统一存删除面，即 UnifiedStore/UnifiedStoreOps.cs 的 DELETE）
  fn delete(&mut self, key: &[u8]) -> bool {
    blocking_wait(self.storage.delete_string(key)).unwrap_or(false)
  }

  /// Increment 派发（值域整数字符串，RMW 写回保留既有 TTL；对象键 /
  /// 非整数旧值 / 溢出折叠为 None 不落写。本视图按同步域自读自写组臂，
  /// 主存 Increment 的落点锚点在 StorageSession 的 increment 原语上）
  fn increment(&mut self, key: &[u8], delta: i64) -> Option<i64> {
    let next = match read_user_sync(&self.storage.batch, key, strict_i64) {
      Ok(UserRead::Hit(cur)) => cur?.checked_add(delta)?,
      Ok(UserRead::Missing) => delta,
      Ok(UserRead::WrongType) => return None,
      // 冷键：String 域异步读闭环；None = 对象键 / 真空缺，保守不写
      Ok(UserRead::Deferred) => {
        let cur = blocking_wait(self.storage.read_string(key))
          .ok()
          .flatten()?;
        strict_i64(&cur)?.checked_add(delta)?
      }
      Err(_) => return None,
    };
    let mut buf = Buffer::new();
    blocking_wait(self.storage.rmw_string(key, buf.format(next).as_bytes())).ok()?;
    Some(next)
  }

  /// SortedSetAdd 派发（键不存在自动新建，经本类型的 zset 内核复用
  /// RESP 层 rmw 骨架冷臂 Zadd 臂）
  fn sorted_set_add(&mut self, key: &[u8], score: f64, member: &[u8]) -> bool {
    self.zset(key, SortedSetOperation::Zadd, score, member)
  }

  /// SortedSetRemove 派发（返回是否实际移除，经本类型的 zset 内核复用
  /// RESP 层 rmw 骨架冷臂 Zrem 臂）
  fn sorted_set_remove(&mut self, key: &[u8], member: &[u8]) -> bool {
    // ZREM 无 score 参：占位值不参与解析（run_operate 的 Zrem 臂只读 member）
    self.zset(key, SortedSetOperation::Zrem, 0.0, member)
  }
}

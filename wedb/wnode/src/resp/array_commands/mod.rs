//! 数组命令族快路径（对标 libs/server/Resp/ArrayCommands.cs 扁平命令臂）
//!
//! 同步快臂驻本文件；SCAN 过滤解析见 `scan`，LCS 选项与出帧见 `lcs`，
//! 慢路径执行臂见 `slow` 与 `mset_slow`。外部路径经本文件 re-export 保持。

use std::borrow::Cow;

use itoa::Buffer;
use smallvec::SmallVec;
use wbase::hash_slot::slot_of;
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_db_index_arg, unpack_args, unpack_args_rest},
  cmd_strings as cs,
  cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message, write_error_raw},
  ext::{RespVecExt, is_resp3},
  resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
};
use wval::{GarnetObjectType, KeyTag};

mod lcs;
mod mset_slow;
mod scan;
pub(crate) mod slow;

use lcs::{parse_lcs_options, write_lcs_output};
pub use scan::{ScanFilter, parse_scan_filter};

use crate::{
  resp::{
    basic_commands::{RiWriteGate, ri_write_gate},
    resp_server_session::{MsetnxResume, RespServerSession},
    vector::vector_manager::VectorManager,
  },
  storage::session::common::{
    TagRead, UserRead, read_envelope_sync, read_tag_sync, read_tag_sync_with_prefix,
    read_user_sync, read_user_sync_with_prefix,
    ttl_sync::{meta_collection_type_of, probe_alive_with_registry},
  },
};

/// 信封内层标签 → Redis TYPE 类型串单点
///
/// 内建段走 [`GarnetObjectType`] 小写名；扩展段统一走
/// [`custom_objects::custom_object_type_name`] 编译期静态清单（C# modules
/// 注册名，使 TYPE 与 EXISTS 对扩展对象键的存活口径一致——C# HandleType
/// 对 custom object 的 ValueObject 四类型 switch 无 default 臂输出零字节
/// quirk，rust 刻意差异：回注册名）；未知标签仍 none（畸形信封防御臂）
const fn envelope_object_type_name(tag: u8) -> Option<&'static str> {
  if let Some(obj_type) = GarnetObjectType::from_u8(tag) {
    return Some(obj_type.as_str());
  }
  super::custom_objects::custom_object_type_name(tag)
}

impl RespServerSession {
  /// libs/server/Resp/ArrayCommands.cs:NetworkDEL
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:DELETE_MainStore
  /// libs/server/API/GarnetApiUnifiedCommands.cs:DELETE
  ///（C# NetworkDEL 逐键循环调 `api.DELETE(key)` 单键删除 API，rust 无该
  /// API 包装层，循环内删除原语与单键 DELETE 语义折叠于本函数删除臂）
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。向量集清退下沉至 wkv
  /// 用户键删除单点（双域判未命中后经 [`crate::storage::session::storage_session::vector_registry_delete_hook`]
  /// 摘除登记表项，对标 C# MainStore RemoveKey 回调 → VectorManager.RequestDeletion，
  /// GarnetRecordTriggers.OnDispose 的 Deleted 臂），本层不再另配第二套清退判据。
  pub fn network_del<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.del_deleted_count = 0;
    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 单命令执行窗口内 ns/db 原子变量不可变（RESP 命令原子性，批内 SELECT
    // 不可能插入 DEL 循环），循环零前缀重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut deleted_count = 0i64;
    for key in parse_state {
      // 逐键短窗（票 zcode-r32-rmwmatrix 立项一：对标 C# InternalDelete.cs:60
      // FindTagAndTryEphemeralXLock——C# NetworkDEL 逐键 api.DELETE 每键独立
      // 进记录闩域，rust 逐键取窗即该形态 1:1 对位；盲删落在他者读算写间隙
      // 即「DEL :1 而键以新值复活」非可串行化，RENAME 双窗亦凭同址桶闩互斥）。
      // 失闩沿既有 Ok(false) 降级慢路径：保留快路径已删除键计数（沿 MSETNX
      // resume 尾参先例），慢路径继承计数续传
      let Some(_window) = store.try_rmw_window(key) else {
        self.del_deleted_count = deleted_count;
        return Ok(false);
      };
      // 单点删除：返回值已含登记表缺席收口（向量集键命中即 true，计 1）
      let deleted = match store.try_delete_sync_with_prefix(prefix_slice, key) {
        Ok(Ok(deleted)) => deleted,
        // 环形页翻转 / 复合对象元数据：须降级完整异步路由，本次不产生输出；
        // 保留快路径已删除键计数（沿 MSETNX resume 尾参先例），慢路径继承计数续传
        Ok(Err(_)) => {
          self.del_deleted_count = deleted_count;
          return Ok(false);
        }
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      };
      deleted_count += i64::from(deleted);
    }

    output.write_resp_int(deleted_count);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMGET
  ///
  /// C# 无 arity 校验（0 参即 `*0`），1:1 保留。
  /// 0 堆分配流式写出：直读内存切片写入 RespWriter 缓冲，遇异步降级整体回滚截断。
  pub fn network_mget<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if is_resp3(self.resp_protocol_version) {
      let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
      Self::do_network_mget(
        parse_state,
        store,
        self.session_metrics.as_deref(),
        &mut writer,
      )
    } else {
      let mut writer = RespWriter::<_, Resp2>::new_ref(output);
      Self::do_network_mget(
        parse_state,
        store,
        self.session_metrics.as_deref(),
        &mut writer,
      )
    }
  }

  #[inline]
  fn do_network_mget<P: RespProtocol, D: Device>(
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    metrics: Option<&wmetric::SessionMetricsHandle>,
    writer: &mut RespWriter<&mut Vec<u8>, P>,
  ) -> wresp::Result<bool> {
    let start_len = writer.len();
    writer.write_array_length(parse_state.len());
    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 单命令执行窗口内 ns/db 原子变量不可变（批内 SELECT 不可能插入 MGET
    // 循环），循环零前缀重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    // 逐键命中/未命中本地累加、非 deferred 收尾一次入账（终值与 C# 批量
    // GET 循环逐键累加同口径；deferred 整批转慢路径重执，即时入账会双计）
    let (mut found, mut notfound) = (0u64, 0u64);
    for key in parse_state {
      // 双域读：String 域命中写值；信封域命中（对象键）写 nil（Redis MGET
      // 对非字符串键同答 nil，不报错）
      match read_user_sync_with_prefix(store, prefix_slice, key, None, |v| {
        writer.write_bulk_string(v)
      }) {
        Ok(UserRead::Hit(())) => found += 1,
        // 对齐 C# MGetReadArgBatch.SetStatus（非 Found 即计入 notfound）：
        // 信封域命中（WrongType）与缺键计入 notfound；MGET 对对象键同答
        // nil 不报错
        Ok(UserRead::WrongType) => {
          notfound += 1;
          writer.write_null();
        }
        Ok(UserRead::Missing) => {
          notfound += 1;
          writer.write_null();
        }
        Ok(UserRead::Deferred) => {
          writer.buf_mut().truncate(start_len);
          return Ok(false);
        }
        // 存储错误不得伪装键缺席（C# 磁盘收割异常上抛掐断连接，绝无
        // 「nil 混入数组」形态）：整命令回滚至数组头前换错误帧，逐键
        // found/notfound 一并不入账（与 Deferred 回滚同机制）；错误帧走
        // [`RespVecExt::write_resp_error`] 单点，与 GET 同线面字节
        Err(_) => {
          let buf = writer.buf_mut();
          buf.truncate(start_len);
          buf.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }
    if let Some(metrics) = metrics {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSET
  pub fn network_mset<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      parse_state.len() >= 2 && parse_state.len().is_multiple_of(2),
      output,
      "MSET"
    );

    // 全键读改写窗口（票 zcode-r32-rmwmatrix 立项一：对标 C# MainStoreOps.cs
    // MSET_Conditional 全键排他锁内批量 SET——折叠先例注释自引的「全键锁内」
    // 即本窗，判定与批量写收敛一键组闩域；沿 network_msetnx 同款
    // [`wkv::BatchStoreSession::try_rmw_window_sorted`] 桶序取闩，哈希升序
    // 全序定序与他命令键组闩交叉无循环等待面）。失闩沿 Ok(false) 降级慢路径
    // 同序持窗重放。取窗先于预检（票 zcode-r141c-msetbig 案二）：窗外预检与
    // 本核取窗之间他核会话可持同键闩完成集合写/promote 落下 RangeIndex 元
    // 记录，批内核 Meta 在场探针只降级不裁决，降级前缀键已提交、慢臂窗内门
    // 改答 WRONGTYPE——半提交假拒击穿「零键落库」契约；全仓字符串写臂皆「先
    // 取窗、窗内门、后写」（SET 共同体 apply_set_with_expiry 契约同款），
    // 本快臂序反系孤例，就此收口
    // 键值对视图（取窗/预检/批量写三处共用，免逐处重展 as_chunks）
    let chunks = parse_state.as_chunks::<2>().0;
    let Some(_windows) = store.try_rmw_window_sorted(chunks.iter().map(|c| c[0])) else {
      return Ok(false);
    };

    // RI 键门窗内预检（复用 ri_write_gate 单点，判据不另起第二套）：任一键
    // 为存活 RangeIndex 整命令拒 WRONGTYPE——窗内预检、窗内裁决，预检先于任
    // 何写入，零键落库无半提交；Deferred（元记录有磁盘候选）弃窗沿用既有
    // 出口整体降级慢路径，由慢臂持窗后异步对偶门复裁决闭环
    for chunk in chunks {
      match ri_write_gate(store, chunk[0], output) {
        RiWriteGate::Pass => {}
        RiWriteGate::Blocked => return Ok(true),
        RiWriteGate::Deferred => return Ok(false),
      }
    }

    // 批量接口单次折叠（transpile SKILL 工程准则；rust 工程优化无 c# 对应，
    // 折叠先例对标 C# MainStoreOps 的 MSET_Conditional 件全键锁内批量 SET）：
    // 批外层复用纪元守卫零新增 enter、会话前缀单次外提、借用对排序去重保末值
    //（MSET 重复键后者胜）。对齐 NetworkSET：Ok(Err(page_id)) 为环形页翻转/
    // 异步闭环信号，吞掉即静默丢写，须整体降级（已写键随慢路径整命令重放幂等，
    // 此时尚未写出任何应答，可安全重试）
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    let pairs = chunks.iter().map(|c| (c[0], c[1]));
    match store.try_upsert_batch_sync_with_prefix(prefix_slice, pairs) {
      Ok(Ok(())) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }

    cs::write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSETNX
  ///
  /// 全有或全无条件批量写，存储侧映射
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional
  ///（全键排他锁内
  /// EXISTS 判定 + 锁内批量 SET + Commit）：判定与批量写收敛进全键桶序
  /// 读改写窗口（[`wkv::BatchStoreSession::try_rmw_window_sorted`]），与 C#
  /// 全键排他事务锁同形——thread-per-core 多 worker 跨核并发下同步段无
  /// await 只保证单 worker 内不可分割，跨 worker 插入窗口由键组闩封堵。
  /// 任一环节须异步闭环时整体降级慢路径（[`Self::msetnx_resume`] 携带续跑
  /// 模式），绝不以半提交状态应答。
  pub fn network_msetnx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      !parse_state.is_empty() && parse_state.len().is_multiple_of(2),
      output,
      "MSETNX"
    );

    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 判定与写入两循环共用同一外提前缀，零逐键重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    // 键值对视图（取窗/判定/落笔三循环共用，免逐处重展 as_chunks）
    let chunks = parse_state.as_chunks::<2>().0;

    // 全键读改写窗口（票 zcode-r15-generic 发现一，对标 C# MainStoreOps.cs:349
    // MSET_Conditional 全键排他锁内「EXISTS 判定 + 锁内批量 SET + Commit」：
    // 判定与写入一体）：桶序取闩（哈希升序定序，与他命令键组闩交叉无循环
    // 等待面），闩内完成判定与批量写全序列，杜绝「逐键判定通过后、批量写
    // 落库前」他 worker 会话 SET 插入的全有或全无契约破坏。失闩沿既有
    // Ok(false) 降级慢路径同序持窗重放，绝不自旋等闩
    let Some(_windows) = store.try_rmw_window_sorted(chunks.iter().map(|c| c[0])) else {
      return Ok(false);
    };

    // 检查是否有任何键已存在（闩窗内折叠存活探针单源：三域 + 向量登记表
    // 第四态，对象信封与升阶键 Meta 同计存在，C# NX 语义；对标 C# Reader
    // 主存单记录——向量索引与 String 同槽同探针，MainStoreOps.cs:375 EXISTS
    // 对存活向量记录恒判在）。票 zcode-r161c-msetnx 案一：NX 存在性判定
    // 唯一收口于本窗内折叠，派发层不再另出终态应答。降级（Ok(None)：
    // 磁盘候选 / TTL 待裁决）发生时尚未写入任何键，整体移交慢路径完整
    // 裁决，安全重放
    self.msetnx_resume = MsetnxResume::Replay;
    for chunk in chunks {
      match probe_alive_with_registry(store, prefix_slice, chunk[0], vector) {
        Ok(Some(true)) => {
          output.write_resp_int(0);
          return Ok(true);
        }
        // C# EXISTS 非 NOTFOUND 即判存在；rust 存储错误与 EXISTS 命令同
        // 口径回错（NetworkEXISTS），不得吞作"不存在"继续写入
        Ok(Some(false)) => {}
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    // 判定段放行后逐键 upsert 落笔（SET 语义覆写，对标 C# MSET_Conditional
    // 判后锁内无条件 SET 循环）：NX 判定的唯一仲裁源=闩窗内存活探针判定段，
    // 物理在场不作第二判据（票 zcode-r141c-msetbig 案一，与 SETNX 窗内探针
    // 判死直 upsert 同型；共守「闩窗内存活探针=唯一 NX 判据」纪律，与
    // ing/zcode-r131c-dumprest 案一 RESTORE 位互见勿起第三形）——闩窗全程
    // 持有，探针判死的过期残留记录由 upsert 原位覆写并自带旧 TTL 清退自愈，
    // 绝不再回假 :0。重复键「written 复写」分支与条件回滚面随之仅保留存储
    // 错误 Err 臂；回滚比对基准随末次写入值前移，禁残留中间值（票
    // zcode-r37-lockfix 发现 B：回滚禁裸删吞并发已确认写——已写键逐键重读
    // String 域，内容即本命令所写才删）。返回是否存在删除降级（环形页翻转）
    // 残留，调用方据此置 Rollback 态降级慢路径持窗收尾，杜绝「已写键残留而
    // 应答失败」的原子性破面（C# MSET_Conditional 全键锁内折叠无回滚形态，
    // 本臂为 rust 快慢路径分工的自有收口）
    let mut written: SmallVec<[(&[u8], &[u8]); 8]> = SmallVec::with_capacity(chunks.len());
    // 回滚已写键：入窗复验待删内容确系本次所写再删，避免吞掉并发盲写 SET 值；
    // 妥善处理删除错误与降级（降级置 MsetnxResume::Rollback 转慢路径）
    let rollback = |written: &[(&[u8], &[u8])]| -> bool {
      let mut degraded = false;
      for (k, v) in written {
        let is_our_write = match read_tag_sync_with_prefix(
          store,
          prefix_slice,
          k,
          KeyTag::String,
          |cur| cur == *v,
        ) {
          Ok(TagRead::Hit(matches)) => matches,
          Ok(TagRead::Missing) => false,
          Ok(TagRead::Deferred) => true,
          Err(_) => false,
        };
        if is_our_write {
          match store.try_delete_sync_with_prefix(prefix_slice, k) {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => degraded = true,
            Err(_) => {}
          }
        }
      }
      degraded
    };

    for chunk in chunks {
      let key = chunk[0];
      let val = chunk[1];
      if let Some(pos) = written.iter_mut().find(|(wk, _)| *wk == key) {
        pos.1 = val;
      } else {
        written.push((key, val));
      }
      match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => {}
        // 环形页翻转/异步闭环信号（非错误）：判定已整体通过、已写键
        // 保持，置 Continue 交慢路径跳过判定续写全部键值（upsert 同值幂等）
        Ok(Err(_)) => {
          self.msetnx_resume = MsetnxResume::Continue;
          return Ok(false);
        }
        Err(_) => {
          if rollback(&written) {
            self.msetnx_resume = MsetnxResume::Rollback;
            return Ok(false);
          }
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    output.write_resp_int(1);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSELECT
  ///
  /// C# 校验序：arity → 整数 → MaxDatabases 上界 → 切库（TrySwitchActiveDatabaseSession）。
  /// 切库经 [`wkv::StoreSession::set_active_db`] 原子改写会话前缀：批处理
  /// 物理键编码前缀按命令边界重算（批量命令在单命令窗口内一次外提，
  /// 见 [`Self::network_mget`]/[`Self::network_mset`]/[`Self::network_del`]），
  /// 纪元守卫仅保护内存直读，切库无 NewEpoch 交叉，安全。
  pub fn network_select<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([index_raw]) = unpack_args(parse_state, output, "SELECT") else {
      return Ok(true);
    };

    // 线面按 C# int32 档收口，内部库 ID 仍 u64（[`parse_db_index_arg`]）
    let Some(index) =
      parse_db_index_arg(index_raw, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, output)
    else {
      return Ok(true);
    };
    if !self.try_switch_active_database_session(index) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // 冷库挂起面：切库报告映射未装载时挂起磁盘点查装载，+OK 交由 SlowWait
    // 闭环（装载完成并重放切库后原样应答并物化标量，后续命令消费到的一定是
    // 装载后上下文；装载失败即弃暂存，active_db_id 保持旧库零撕裂）
    if let Some((ns, db)) = self.cold_pending_ctx() {
      // 事务窗围栏文案按 SELECT 域传入（deviations §58b 事务窗零停泊红线，
      // 同库排队准入后重放撞冷库窄窗即回 SELECT_IN_TXN 族帧）
      self.park_cold_context_load(
        store.session.store(),
        ns,
        db,
        Cow::Borrowed(cs::RESP_OK),
        output,
        cs::RESP_ERR_SELECT_IN_TXN_UNSUPPORTED,
      );
      return Ok(true);
    }
    output.extend_from_slice(cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSWAPDB
  ///
  /// C# 校验序：arity → 集群门槛 → 两个整数（invalid first/second DB index）
  /// → 负下标与 MaxDatabases 上界 → storeWrapper.TrySwapDatabases 全局换库。
  /// 集群门禁按 doc/zh/db.md SWAPDB 条款改库槽位归属 + 槽态判定（偏离 C#
  /// 一刀切，槽位真值源 `Slot = Mixer(namespace, db)` 单点
  /// wbase::hash_slot::slot_of）：校验序相应调整为 arity → 两个整数 → 上界 →
  /// 归属门禁 → 同库短路——门禁需库号参与，门槛必然后置于下标解析。两库槽位
  /// 均由本地节点掌管且处于 Stable 态才放行（同库交换亦须本节点持有该槽），
  /// 分属不同节点、本节点不持有或任一槽位处于 MIGRATING/IMPORTING 迁移窗口
  /// 即拦截回 RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE——O(1) 换号只换
  /// logic_db→vdb 指针、物理数据原地，迁移窗口内换库会将在途搬迁数据与另一库
  /// 归属对调，必须与在途搬迁互斥。真实换库为 wkv O(1) 虚库 ID
  /// 原子互换（`StoreSession::swap_databases`，零物理搬移），命令面同步执行段
  /// 按降级约定返回 `Ok(false)` 绝不误答 +OK，异步域由 `swap_command_slow`
  /// 承接闭环。
  pub fn network_swapdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([idx1_raw, idx2_raw]) = unpack_args(parse_state, output, "SWAPDB") else {
      return Ok(true);
    };

    // 逐库短路解析（[`parse_db_index_arg`]）：超档字面量走 NotInteger，
    // 按 C# NetworkSWAPDB 各回 invalid first/second DB index 档
    let Some(idx1) = parse_db_index_arg(idx1_raw, cs::RESP_ERR_INVALID_FIRST_DB_INDEX, output)
    else {
      return Ok(true);
    };
    let Some(idx2) = parse_db_index_arg(idx2_raw, cs::RESP_ERR_INVALID_SECOND_DB_INDEX, output)
    else {
      return Ok(true);
    };
    if idx1 >= self.max_databases || idx2 >= self.max_databases {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // 集群归属门禁（doc/zh/db.md：单机直接执行；集群校验两库槽位是否均由
    // 当前本地节点掌管且处于 Stable 态——分属不同物理节点、或任一库槽位
    // 处于 MIGRATING/IMPORTING 迁移窗口（换号与在途搬迁互斥）均拦截）
    if let Some(provider) = self.cluster_provider.as_ref()
      && provider.is_cluster_enabled()
    {
      let ns = self.namespace;
      if !(provider.is_slot_local_stable(slot_of(ns, idx1))
        && provider.is_slot_local_stable(slot_of(ns, idx2)))
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE);
        return Ok(true);
      }
    }
    // C# TrySwapDatabases：同库交换短路 +OK（无搬移语义）
    if idx1 == idx2 {
      output.extend_from_slice(cs::RESP_OK);
      return Ok(true);
    }
    // 异库交换：虚库 ID 互换由异步慢路径 swap_command_slow 闭环（同步域不得静默伪成功）
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkDBSIZE
  pub fn network_dbsize(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "DBSIZE");
    // 全库扫描无法在同步快路径完成，对标 C# 降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkKEYS
  pub fn network_keys(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([_pattern]) = unpack_args(parse_state, output, "KEYS") else {
      return Ok(true);
    };
    // 键空间扫描无法在同步快路径完成，降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSCAN
  ///
  /// 同步段仅做参数校验（统一解析器 [`parse_scan_filter`]，与慢路径
  /// 异步执行段共用同一解析单源）；扫描本身在慢路径执行器闭环
  /// （[`crate::resp::slow_path::SlowWait`]）
  pub fn network_scan(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "SCAN");
    // 参数校验通过后，游标扫描降级异步路径执行
    match parse_scan_filter(parse_state) {
      Ok(_) => Ok(false),
      Err(err) => {
        write_error_raw(output, err);
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkTYPE
  /// libs/server/API/GarnetApiUnifiedCommands.cs:TYPE
  ///（C# NetworkTYPE 调 `api.TYPE(key)` → storageSession.Read_UnifiedStore
  ///（HandleType 判定内核）；rust 无该 API 包装层，判定与应答折叠于本函数）
  pub fn network_type<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "TYPE") else {
      return Ok(true);
    };
    if vector.is_some_and(|vm| {
      vm.read_stored_index(store.session_prefix().as_slice(), key)
        .is_some()
    }) {
      output.write_resp_simple_string(cs::TYPE_VECTORSET);
      return Ok(true);
    }
    // 三域读（对标 C# ReadMethods.cs:HandleType 的 ValueIsObject 分支）：
    // String 域命中 → string；Meta 域命中（升阶键）按 MetaValue.collection_type
    // 映射；信封域命中按内层标签映射 zset/list/hash/set 与扩展注册名
    //（[`envelope_object_type_name`]）；三域皆缺 → none；任一域存储错误独立
    // 回错误帧（C# IO 异常沿调用栈上抛断连，status 枚举域只有 NOTFOUND/
    // WRONGTYPE 绝无 IO 错误折 none——与 GET/STRLEN 同口径 RESP_ERR_GENERIC，
    // 快慢路径与同域命令三态互斥）
    match read_user_sync(store, key, None, |_| ()) {
      Ok(UserRead::Hit(())) => {
        output.write_resp_simple_string(cs::TYPE_STRING);
      }
      Ok(UserRead::WrongType) => {
        // 升阶键：Meta 元记录存活且带集合类型，直读类型名
        match read_tag_sync(store, key, KeyTag::Meta, meta_collection_type_of) {
          Ok(TagRead::Hit(Some(obj_type))) => {
            output.write_resp_simple_string(obj_type.as_str());
          }
          Ok(TagRead::Deferred) => return Ok(false),
          // Meta 域缺失/死记录：落信封域读（对象信封键口径，含扩展注册名）
          Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {
            match read_envelope_sync(store, key, |raw| {
              raw.first().copied().and_then(envelope_object_type_name)
            }) {
              // 信封域命中已由双探确认；内层标签缺省兜底 none；磁盘候选降级
              Ok(TagRead::Hit(Some(name))) => {
                output.write_resp_simple_string(name);
              }
              Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {
                output.write_resp_simple_string(cs::TYPE_NONE);
              }
              Ok(TagRead::Deferred) => return Ok(false),
              // 信封域存储错误不得伪装 none（对齐 slow::type_cmd Err 上抛形态）
              Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
            }
          }
          // Meta 域存储错误不得伪装 none / 不得借信封域兜底吞错
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::Missing) => {
        output.write_resp_simple_string(cs::TYPE_NONE);
      }
      // String 域存储错误不得伪装「键不存在」（与 GET 同口径）
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      Ok(UserRead::Deferred) => return Ok(false),
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:WriteOutputForScan
  pub fn write_output_for_scan(cursor_value: i64, keys: &[&[u8]], output: &mut Vec<u8>) {
    output.write_resp_array_len(2);
    let mut cur_buf = Buffer::new();
    let cur_str = cur_buf.format(cursor_value);
    output.write_resp_bulk_string(cur_str.as_bytes());
    output.write_resp_array_len(keys.len());
    for key in keys {
      output.write_resp_bulk_string(key);
    }
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkLCS
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCS
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCSInternal
  ///（读两键 + LCS 计算内核：LEN/IDX/默认三形态与缺键空应答皆在本函数闭环）
  pub fn network_lcs<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(([key1, key2], rest)) = unpack_args_rest(parse_state, output, "LCS") else {
      return Ok(true);
    };
    // 选项解析单源（慢路径执行臂共用 [`parse_lcs_options`]）
    let opts = match parse_lcs_options(rest) {
      Ok(opts) => opts,
      Err(err) => {
        write_error_raw(output, err);
        return Ok(true);
      }
    };

    // 双域读五态：String 域命中即值 / 确证缺键 / 信封域命中（list 等对象键）
    // 报 WRONGTYPE / 磁盘候选整体降级慢路径 / 存储 IO 失败。IO 失败与缺键
    // 严禁合流——合流即伪应答空 LCS（C# storageApi.LCS 的 StringGet 异常
    // 上抛会话 catch 报错，不产出伪结果）
    enum StrRead {
      /// String 域命中
      Val(Vec<u8>),
      /// 确证缺键（两域皆缺或已过期）
      Missing,
      /// 对象键 → WRONGTYPE
      WrongType,
      /// 磁盘候选 / TTL 待异步裁决，整体降级慢路径
      Deferred,
      /// 存储 IO 失败（RESP_ERR_SLOW_PATH_STORAGE，与 exec_slow 同一口径）
      IoFail,
    }
    let read_string_val = |key| match read_user_sync(store, key, None, |v| v.to_vec()) {
      Ok(UserRead::Hit(v)) => StrRead::Val(v),
      Ok(UserRead::Missing) => StrRead::Missing,
      Ok(UserRead::WrongType) => StrRead::WrongType,
      Ok(UserRead::Deferred) => StrRead::Deferred,
      Err(_) => StrRead::IoFail,
    };

    // 逐键命中/未命中本地累加、非 deferred 收尾一次入账（同
    // [`Self::do_network_mget`] 快臂机制）：C# LCSInternal（MainStoreOps.cs
    // :619/:625）两次 GET 经 :29/:38 incr_session_found/notfound 每键各按实
    // 计一条，恒计无双计环境；rust 快臂 Deferred 整命令转慢路径重放，慢臂
    // 簿记入口 read_user 逐键恰一条已收口，即时入账必双计，故 deferred 静默
    // 回滚不入账。WrongType / IO 失败提前回帧入已累积前键（C# 首键 GET 计数
    // 先于次键 return/抛出即落账），错误键本身不入账——C# WRONGTYPE 臂静默
    // return 不 incr，严禁照抄 MGET 对象键计 notfound 的批次专属口径
    let metrics = self.session_metrics.as_deref();
    let commit = |found: u64, notfound: u64| {
      if let Some(metrics) = metrics {
        metrics.incr_total_found(found);
        metrics.incr_total_notfound(notfound);
      }
    };

    let mut vals = [None, None];
    let (mut found, mut notfound) = (0u64, 0u64);
    for (slot, key) in vals.iter_mut().zip([key1, key2]) {
      match read_string_val(key) {
        StrRead::Val(v) => {
          *slot = Some(v);
          found += 1;
        }
        StrRead::Missing => notfound += 1,
        StrRead::WrongType => {
          commit(found, notfound);
          write_error_raw(output, RESP_ERR_WRONG_TYPE);
          return Ok(true);
        }
        StrRead::Deferred => return Ok(false),
        StrRead::IoFail => {
          commit(found, notfound);
          write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
          return Ok(true);
        }
      }
    }
    commit(found, notfound);

    write_lcs_output::<D>(
      vals[0].as_deref(),
      vals[1].as_deref(),
      &opts,
      is_resp3(self.resp_protocol_version),
      output,
    );
    Ok(true)
  }
}

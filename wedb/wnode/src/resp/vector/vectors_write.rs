//! 向量写命令族（VADD / VREM / VSETATTR 的命令体与 VADD 参数解析段），
//! 自 resp_server_session_vectors.rs 拆出（对标 C# RespServerSessionVectors.cs
//! 各 NetworkV* 写臂与 VectorStoreOps 锁链）。

use std::borrow::Cow;

use wresp::wrong_num_args;
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks};

use super::{
  resp_server_session_vectors::{
    ERR_EF_RANGE, ERR_EMPTY_VECTOR_SET_KEY, ERR_INVALID_DISTANCE_METRIC,
    ERR_INVALID_OPTION_AFTER_ELEMENT, ERR_M_RANGE, ERR_QUANT_MISMATCH, ERR_QUANT_SPECIFIED_TWICE,
    ERR_REDUCE_EXCEEDS_DIMS, ERR_REDUCE_MUST_BE_POSITIVE, RespServerSessionVectors, VectorReply,
    bool_reply, dup_flag, err_dup, wna_entry,
  },
  vector_manager::{MAX_EXPLORATION_FACTOR, VectorAddArgs, VectorManagerResult},
  vector_manager_locking::CreateIndexParams,
  vectors_parse::{
    Cur, DEFAULT_VADD_BUILD_EF, DEFAULT_VADD_NUM_LINKS, MAX_M, METRIC_OPTS, MIN_M, QUANT_OPTS,
    VADD_OPERAND, WNA_VADD, is_x_quant, lookup,
  },
};

/// VADD 同步解析段的产出（C# NetworkVADD 校验完毕、调 storageApi 前的
/// 参数快照）：键/元素/属性借参数缓冲，VALUES 文本浮点落堆（Cow），标量
/// 集为合成默认值后的终值。执行段 [`RespServerSessionVectors::network_vadd_slow`]
/// 以此取齐 [`CreateIndexParams`] / [`VectorAddArgs`]。
struct VaddPlan<'a> {
  /// 集合键（借参数缓冲）
  key: &'a [u8],
  /// 元素键（借参数缓冲）
  element: &'a [u8],
  /// 向量格式
  value_type: VectorValueType,
  /// 向量字节（FP32/XU8/XI8 借参数零拷贝，VALUES 合成落堆）
  values: Cow<'a, [u8]>,
  /// 属性（缺省空串）
  attributes: &'a [u8],
  /// REDUCE 降维（0 = 无）
  reduce_dims: u32,
  /// 量化器（默认 Q8）
  quant: VectorQuantType,
  /// 建索探索因子（默认 200）
  build_ef: u32,
  /// 每层链数（默认 16）
  num_links: u32,
  /// 距离度量（默认 L2）
  distance_metric: VectorDistanceMetricType,
  /// 会话库级定槽（doc/zh/db.md 4.1）
  slot: u16,
  /// 向量维度（value_type 步长推导，供 manager 校验）
  dims: u32,
}

impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  // ======================== VADD ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// `VADD key [REDUCE dim] (FP32 | XU8 | XI8 | VALUES num) vector element
  ///   \[CAS\] \[NOQUANT | Q8 | BIN | XNOQUANT_U8 | XPREQ8 | XNOQUANT_I8 | XBIN_I8 | XBIN_U8\]
  ///   [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// 插入链为存储异步操作（compio 写盘跨 await），本函数 async 化闭环
  ///（对标 cluster 链 pending_slow 挂起先例与 C# 同步栈 NetworkVADD 的
  /// rust 快慢分臂对偶）：生产 RESP 臂经 [`Self::network_vector_write_slow`]
  /// 挂起驱动，直调方（测试 / 复制回放）自持运行时 await。
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：`slot` 为调用会话库级槽位
  ///（`RespServerSession::active_db_slot`），索引登记的槽位随会话所属库，
  /// 键内容不参与定槽；`resp3` 选择成功/重复应答的布尔或整数形态。
  pub async fn network_vadd(
    &self,
    prefix: &[u8],
    args: &[&[u8]],
    slot: u16,
    resp3: bool,
  ) -> VectorReply {
    match self.parse_vadd(args, slot) {
      Ok(plan) => self.network_vadd_slow(prefix, plan, resp3).await,
      Err(reply) => reply,
    }
  }

  /// VADD 参数解析段（C# NetworkVADD 选项循环的纯解析投影）：零存储触达，
  /// 校验/默认值合成/维度推导产出 [`VaddPlan`]，错误应答就地返回。
  ///
  /// 与执行段 [`Self::network_vadd_slow`] 的拆分线对齐 C# 原序：C# 在调
  /// storageApi 前完成全部参数校验（X 系量化互斥判定注释「before calling
  /// storageApi」），拆分后解析期错误不经挂起面，语义与字节面同源。
  fn parse_vadd<'a>(&self, args: &'a [&'a [u8]], slot: u16) -> Result<VaddPlan<'a>, VectorReply> {
    if let Some(reply) = self.entry(args, 4.., WNA_VADD) {
      return Err(reply);
    }

    let key = args[0];
    let mut cur = Cur::new(args, 1);

    // REDUCE dim（C# TryGetInt 严格 i32，溢出同非法；缺失/非法/非正统一报
    // REDUCE 文案；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32）
    let mut reduce_dims = 0u32;
    if cur.at(b"REDUCE") {
      reduce_dims = cur.i32_value(
        ERR_REDUCE_MUST_BE_POSITIVE,
        |v| v > 0,
        ERR_REDUCE_MUST_BE_POSITIVE,
      )? as u32;
    }

    // 向量格式分派：FP32 / VALUES num / XU8|XB8 / XI8（ELE 归入非法格式文案）
    let vector = self.vector_operand(&mut cur, &VADD_OPERAND)?;
    let value_type = vector.value_type;
    let values = vector.values;
    if usize::try_from(reduce_dims).unwrap_or(vector.dims + 1) > vector.dims {
      return Err(VectorReply::err(ERR_REDUCE_EXCEEDS_DIMS));
    }

    // 元素键
    let element = cur.next(WNA_VADD)?;

    // 选项循环（C#：元素后顺序未指定，逐一识别）
    let mut cas_seen = false;
    let mut quant: Option<VectorQuantType> = None;
    let mut build_ef: Option<i32> = None;
    let mut attributes: Option<&[u8]> = None;
    let mut num_links: Option<i32> = None;
    let mut distance_metric: Option<VectorDistanceMetricType> = None;

    while cur.more() {
      // REDUCE 在元素之后无论何种写法均非法
      if cur.at(b"REDUCE") {
        return Err(VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT));
      }
      // 量化器选项（含 XPREQ8 别名）单表识别，表序即原链式识别序
      if let Some(quant_type) = lookup(QUANT_OPTS, cur.peek()) {
        if quant.is_some() {
          return Err(VectorReply::err(ERR_QUANT_SPECIFIED_TWICE));
        }
        quant = Some(quant_type);
        cur.skip();
      } else if cur.at(b"CAS") {
        // CAS 仅识别不处理
        dup_flag!(cur, &mut cas_seen, "CAS");
      } else if cur.at(b"EF") {
        if build_ef.is_some() {
          return Err(err_dup!("EF"));
        }
        build_ef = Some(cur.i32_value(
          ERR_INVALID_OPTION_AFTER_ELEMENT,
          |v| v > 0 && v <= MAX_EXPLORATION_FACTOR as i32,
          ERR_EF_RANGE,
        )?);
      } else if cur.at(b"SETATTR") {
        if attributes.is_some() {
          return Err(err_dup!("SETATTR"));
        }
        attributes = Some(cur.value(ERR_INVALID_OPTION_AFTER_ELEMENT)?);
      } else if cur.at(b"M") {
        if num_links.is_some() {
          return Err(err_dup!("M"));
        }
        num_links = Some(cur.i32_value(
          ERR_INVALID_OPTION_AFTER_ELEMENT,
          |v| (MIN_M..=MAX_M).contains(&v),
          ERR_M_RANGE,
        )?);
      } else if cur.at(b"XDISTANCE_METRIC") {
        if distance_metric.is_some() {
          return Err(err_dup!("XDISTANCE_METRIC"));
        }
        let metric = cur.value(ERR_INVALID_OPTION_AFTER_ELEMENT)?;
        distance_metric = Some(
          lookup(METRIC_OPTS, metric)
            .ok_or_else(|| VectorReply::err(ERR_INVALID_DISTANCE_METRIC))?,
        );
      } else {
        return Err(VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT));
      }
    }

    if key.is_empty() {
      return Err(VectorReply::err(ERR_EMPTY_VECTOR_SET_KEY));
    }

    // 默认值（对齐 C#：Q8 / 200 / 16 / L2）
    let quant = quant.unwrap_or(VectorQuantType::Q8);
    let build_ef = build_ef.unwrap_or(DEFAULT_VADD_BUILD_EF) as u32;
    let num_links = num_links.unwrap_or(DEFAULT_VADD_NUM_LINKS) as u32;
    let distance_metric = distance_metric.unwrap_or(VectorDistanceMetricType::L2);

    // X 系量化器与 REDUCE 互斥：C# 在调storageApi 前以此判 BadParams（自定义
    // 文案为空 → 回落 quantization mismatch 文案）
    if is_x_quant(quant) && reduce_dims != 0 {
      return Err(VectorReply::err(ERR_QUANT_MISMATCH));
    }

    Ok(VaddPlan {
      key,
      element,
      value_type,
      values,
      attributes: attributes.unwrap_or(b""),
      reduce_dims,
      quant,
      build_ef,
      num_links,
      distance_metric,
      slot,
      // 向量维度（供 manager 校验；上限校验已收口于取参骨架）
      dims: vector.dims as u32,
    })
  }

  /// VADD 执行段（C# ReadOrCreateVectorIndex → TryAdd → ReplicateVectorSetAdd
  /// 锁链的投影）。
  ///
  /// 共享索引锁在 [`Self::parse_vadd`] 之后的 `read_or_create_vector_index`
  /// 取得，覆盖 `try_add` 全程（manager 契约「假定索引已锁定」，防并发
  /// DEL/UNLINK/FLUSHDB 摘除 context）：guard 随本 async 栈帧跨 await 存活。
  /// 与线程槽守卫「绝不跨 await」纪律分属两轴——每键数据锁的跨 await 由
  /// 慢路径 [`crate::resp::slow_path::SlowFuture`] 的 Send 承诺承担（compio
  /// thread-per-core 下 poll 恒在属主任务线程，guard 永不跨线程 move/drop；
  /// 对标 C# ReadOrCreateVectorIndex 返回锁对象持续至 TryAdd 完成的同一
  /// 语义），ActiveVectorSessionGuard 则由慢路径 SlowPollSessionBound 包装
  /// 在每次 poll 边界重绑——同步段收割 inline_wait 移除后，本函数不再内联
  /// 重入 tick。
  ///
  /// 守卫自取得起存活至本函数返回（同 C# VectorStoreOps.cs:192 using 罩
  /// TryAdd 与 OK 后 ReplicateVectorSetAdd 全程）：并发 DEL/UNLINK/FLUSHDB
  /// 的排他删除锁（ReadForDeleteVectorIndex）被排挡至写体与 AOF 注入完成，
  /// 杜绝 service.insert miss 折 Duplicate 伪应答与 Arc 保活孤儿写。
  async fn network_vadd_slow(&self, prefix: &[u8], plan: VaddPlan<'_>, resp3: bool) -> VectorReply {
    let VaddPlan {
      key,
      element,
      value_type,
      values,
      attributes,
      reduce_dims,
      quant,
      build_ef,
      num_links,
      distance_metric,
      slot,
      dims,
    } = plan;

    // 读或创建索引记录（缺失或需重建时按选项建原生索引，对齐 C#
    // ReadOrCreateVectorIndex）；VADD 写臂取独占形态不降级——同键并发插入
    // 的存在性预检与图插入跨 await 非原子，独占条带锁即线性化点（见
    // read_or_create_vector_index_exclusive 文档）
    let params = CreateIndexParams {
      hash_slot: slot,
      dims,
      reduce_dims,
      quant,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (index, _lock) = match self
      .manager
      .read_or_create_vector_index_exclusive(prefix, key, Some(&params))
      .await
    {
      Ok(acquired) => acquired,
      // 按结果码出各自文案（对齐 C#：状态错误帧而非一律分配上限；上下文
      // 耗尽即 MAX 文案，存储失败/参数拒绝即 Invalid 族文案）
      Err(result) => return VectorReply::err(result.error_msg()),
    };

    // 经 manager 执行插入（重复/参数不匹配校验在 try_add 内）
    let stored = index.to_bytes();
    let add_args = VectorAddArgs {
      element,
      value_type,
      values: values.as_ref(),
      attributes,
      reduce_dims,
      quant_type: quant,
      num_links,
      distance_metric,
    };
    match self.manager.try_add(prefix, key, &stored, &add_args).await {
      Ok(VectorManagerResult::OK) => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetAdd 的
        // OK-后 ReplicateVectorSetAdd；重复添加幂等跳过，不入日志）
        if let Some(reply) = self.aof_failed(
          "vadd",
          self
            .manager
            .replicate_vector_set_add(prefix, key, dims, build_ef, &add_args),
        ) {
          return reply;
        }
        // 对齐 C#：成功 → RESP3 布尔真 / RESP2 整数 1，重复 → 布尔假 / 整数 0
        bool_reply(true, resp3)
      }
      Ok(VectorManagerResult::Duplicate) => bool_reply(false, resp3),
      Ok(other) => VectorReply::err(other.error_msg()),
      Err(e) => VectorReply::Error(e.message.into()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVREM
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetRemove 锁点 :225，锁内 :227-234
  /// 原文自陈 "After a successful read we remove the vector while holding a
  /// shared lock / That lock prevents deletion, but everything else can
  /// proceed in parallel"——共享守卫全程罩住 TryRemove 写体与 OK 后合成
  /// 复制注入；rust 同款：守卫随本 async 栈帧跨 `try_remove` / AOF 注入
  /// 存活，DEL 独占臂被排挡至写体完成，杜绝「remove 落已弃上下文 + AOF
  /// 注入穿透删除锁」的主从发散竞态（manager 契约：try_remove 假定调用方
  /// 已持共享读守卫）。
  ///
  /// 应答契约（7398c0625 #2184）：C# 以 WriteBoolean 分派——RESP3 `#t`/`#f`
  /// 布尔、RESP2 `:1`/`:0` 整数（RespServerSessionVectors.cs:192-213 与
  /// :1893-1896），`resp3` 选择形态。
  pub async fn network_vrem(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VREM");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return bool_reply(false, resp3);
    };
    match self
      .manager
      .try_remove(prefix, args[0], &index.to_bytes(), args[1])
      .await
    {
      Ok(VectorManagerResult::OK) => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetRemove 的
        // OK-后 ReplicateVectorSetRemove）
        if let Some(reply) = self.aof_failed(
          "vrem",
          self
            .manager
            .replicate_vector_set_remove(prefix, args[0], args[1]),
        ) {
          return reply;
        }
        bool_reply(true, resp3)
      }
      // 缺元素 → 假/0（对标 C#「非 OK→WriteBoolean(false)」，缺席语义不变）
      Ok(_) => bool_reply(false, resp3),
      // 存储读失败 → ERR 错误帧、不写 AOF（禁故障窗假成功 0/1 应答与存储分叉）
      Err(e) => VectorReply::Error(e.message.into()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSETATTR
  ///
  /// `VSETATTR key element attributes`（RESP3 以布尔应答；缺失元素 → 假 / 0，非错误）。
  ///
  /// 属性写为存储异步操作（compio 写盘跨 await），本函数 async 化闭环
  ///（对标 cluster 链 pending_slow 挂起先例）：生产 RESP 臂经
  /// [`Self::network_vector_write_slow`] 挂起驱动，直调方（测试 / 复制回放）
  /// 自持运行时 await。C# 对位 VectorStoreOps.cs:VectorSetSetAttribute
  /// 锁点 :262——`using (ReadVectorIndex)` 共享锁全程罩住 TrySetAttribute
  /// 写体（该臂无自陈注释，锁域即证据）；rust 同款：守卫随本 async 栈帧
  /// 跨 `try_set_attribute` / AOF 注入存活，与 VREM 臂共享读防删排挡归一
  ///（manager 契约：try_set_attribute 假定调用方已持共享读守卫）。
  pub async fn network_vsetattr(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    wna_entry!(self, args, 3..=3, "VSETATTR");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return bool_reply(false, resp3);
    };
    match self
      .manager
      .try_set_attribute(prefix, args[0], &index.to_bytes(), args[1], args[2])
      .await
    {
      // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetSetAttribute
      // 的成功后 ReplicateVectorSetSetAttribute）
      Ok(true) => {
        if let Some(reply) = self.aof_failed(
          "vsetattr",
          self
            .manager
            .replicate_vector_set_set_attribute(prefix, args[0], args[1], args[2]),
        ) {
          return reply;
        }
        bool_reply(true, resp3)
      }
      // 缺元素/缺席 → 假（0）不变（对标 C#）
      Ok(false) => bool_reply(false, resp3),
      // 存储读写故障 → ERR 错误帧、不写 AOF（禁假阴性应答与存储分叉）
      Err(e) => VectorReply::Error(e.message.into()),
    }
  }
}

//! 向量查询命令族（VSIM / VEMB / VCARD / VDIM / VGETATTR / VINFO /
//! VISMEMBER / VLINKS / VRANDMEMBER 的命令体、VSIM 参数解析段与
//! RESP2/RESP3 检索结果序列化），自 resp_server_session_vectors.rs 拆出。

use std::borrow::Cow;

use wbase::num::strict_i32;
use wresp::{cmd_strings as cs, options::equals_ignore_case, wrong_num_args};
use wvector::{
  VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks,
  unpack_length_prefixed,
};

use super::{
  resp_server_session_vectors::{
    ERR_COUNT_RANGE, ERR_EF_RANGE, ERR_EPSILON_MUST_BE_POSITIVE, ERR_EXPECTED_INTEGER_COUNT,
    ERR_FILTER_EF_RANGE, ERR_KEY_NOT_FOUND, ERR_UNKNOWN_OPTION, ERR_VEMB_UNEXPECTED_OPTION,
    ERR_VLINKS_UNEXPECTED_OPTION, RespServerSessionVectors, VectorReply, bool_reply, dup_flag,
    err_dup, wna_entry,
  },
  vector_manager::{
    MAX_EXPLORATION_FACTOR, MAX_FILTERING_SCALE_FACTOR, MAX_RETRIEVE_COUNT, VectorSearchOptions,
  },
  vectors_parse::{
    Cur, DEFAULT_VSIM_COUNT, DEFAULT_VSIM_EF, DEFAULT_VSIM_EPSILON, DEFAULT_VSIM_FILTER_EF,
    Operand, VSIM_OPERAND, WNA_VSIM,
  },
};

/// VSIM 同步解析段的产出（C# NetworkVSIM 校验完毕、调 storageApi 前的参数
/// 快照）：键/元素/过滤借参数缓冲，VALUES 文本浮点落堆（Cow），检索标量
/// 为合成默认值后的终值。执行段 [`RespServerSessionVectors::network_vsim`]
/// 以此取齐 [`VectorSearchOptions`] 与检索中心。
struct VsimPlan<'a> {
  /// 集合键（借参数缓冲）
  key: &'a [u8],
  /// ELE 形态的查询元素（Some 即以元素为中心，忽略 `values`）
  element: Option<&'a [u8]>,
  /// 查询向量格式
  value_type: VectorValueType,
  /// 查询向量字节
  values: Cow<'a, [u8]>,
  /// 检索参数（默认值合成后的终值；`count` 即应答上限）
  search: VectorSearchOptions<'a>,
  /// WITHSCORES
  with_scores: bool,
}

impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  // ======================== VSIM ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `VSIM key (ELE | FP32 | XU8 | XI8 | VALUES num) (vector | element)
  ///   \[WITHSCORES\] \[WITHATTRIBS\] \[COUNT num\] \[EPSILON delta\] \[EF factor\]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `resp3` 选择应答协议版本。
  pub async fn network_vsim(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    let VsimPlan {
      key,
      element,
      value_type,
      values,
      search,
      with_scores,
    } = match self.parse_vsim(args) {
      Ok(plan) => plan,
      Err(reply) => return reply,
    };

    // 键不存在：对齐 C# NOTFOUND → 空数组（非错误）。命中走读+重建锁协议
    //（C# ReadVectorIndex + RecreateIndex）：登记记录 ptr=0 时在独占锁内
    // 重载原生索引——恢复回建后首次检索即由此装载，非裸读登记表。
    // 重建臂含登记写透 `.await`（真异步，无内联收割），读路径 async 化
    let (stored, _index_guard) = match self.manager.read_vector_index(prefix, key).await {
      (Some(index), guard) => (index.to_bytes(), guard),
      (None, _) => return VectorReply::Array(Vec::new()),
    };

    let result = match element {
      Some(elem) => {
        self
          .manager
          .element_similarity(&stored, elem, &search)
          .await
      }
      None => {
        self
          .manager
          .value_similarity(&stored, value_type, values.as_ref(), &search)
          .await
      }
    };

    let output = match result {
      Ok(out) => out,
      Err(e) => return VectorReply::Error(e.message.into()),
    };

    // 拆包命中
    let ids: Vec<&[u8]> = unpack_length_prefixed(&output.output_ids);
    let attrs: Option<Vec<&[u8]>> = search
      .include_attributes
      .then(|| unpack_length_prefixed(&output.output_attributes));

    if resp3 {
      RespServerSessionVectors::write_resp3_result(
        search.count,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    } else {
      RespServerSessionVectors::write_resp2_result(
        search.count,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    }
  }

  /// VSIM 参数解析段（C# NetworkVSIM 校验完毕、调 storageApi 前的纯解析
  /// 投影）：查询向量取参与选项循环骨架复用 VADD 同款〔
  /// [`Self::vector_operand`]／[`Cur`]〕，各档非法文案逐字保持原文案。
  fn parse_vsim<'a>(&self, args: &'a [&'a [u8]]) -> Result<VsimPlan<'a>, VectorReply> {
    if let Some(reply) = self.entry(args, 3.., WNA_VSIM) {
      return Err(reply);
    }

    let key = args[0];
    let mut cur = Cur::new(args, 1);

    // 查询向量（ELE 形态以既有元素为中心；C# 对缺失的元素参数不做显式
    // 校验，空切片语义）
    let vector = if cur.at(b"ELE") {
      cur.skip();
      Operand {
        value_type: VectorValueType::Invalid,
        values: Cow::Borrowed(&[]),
        dims: 0,
        element: Some(cur.next_or_empty()),
      }
    } else {
      self.vector_operand(&mut cur, &VSIM_OPERAND)?
    };

    // 选项（默认值对齐 C#：count=10 / delta=2 / EF=100 / FILTER-EF=16）
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: Option<i32> = None;
    let mut epsilon: Option<f32> = None;
    let mut ef: Option<i32> = None;
    let mut filter: Option<&[u8]> = None;
    let mut filter_ef: Option<i32> = None;
    // 对标 C# 仅做选项语法识别，当前执行流未启用真值比较与单线程模式
    let mut truth_seen = false;
    let mut no_thread_seen = false;

    while cur.more() {
      if cur.at(cs::WITHSCORES) {
        dup_flag!(cur, &mut with_scores, "WITHSCORES");
      } else if cur.at(b"WITHATTRIBS") {
        dup_flag!(cur, &mut with_attribs, "WITHATTRIBS");
      } else if cur.at(cs::COUNT) {
        if count.is_some() {
          return Err(err_dup!("COUNT"));
        }
        count = Some(cur.i32_value(
          WNA_VSIM,
          |v| v >= 0 && v <= MAX_RETRIEVE_COUNT as i32,
          ERR_COUNT_RANGE,
        )?);
      } else if cur.at(b"EPSILON") {
        if epsilon.is_some() {
          return Err(err_dup!("EPSILON"));
        }
        epsilon = Some(cur.f32_value(WNA_VSIM, |v| v > 0.0, ERR_EPSILON_MUST_BE_POSITIVE)?);
      } else if cur.at(b"EF") {
        if ef.is_some() {
          return Err(err_dup!("EF"));
        }
        ef = Some(cur.i32_value(
          WNA_VSIM,
          |v| v > 0 && v <= MAX_EXPLORATION_FACTOR as i32,
          ERR_EF_RANGE,
        )?);
      } else if cur.at(b"FILTER") {
        if filter.is_some() {
          return Err(err_dup!("FILTER"));
        }
        filter = Some(cur.value(WNA_VSIM)?);
      } else if cur.at(b"FILTER-EF") {
        if filter_ef.is_some() {
          return Err(err_dup!("FILTER-EF"));
        }
        filter_ef = Some(cur.i32_value(
          WNA_VSIM,
          |v| v >= 4 && v <= MAX_FILTERING_SCALE_FACTOR as i32,
          ERR_FILTER_EF_RANGE,
        )?);
      } else if cur.at(b"TRUTH") {
        // TODO 语义与 C# 一致：仅识别
        dup_flag!(cur, &mut truth_seen, "TRUTH");
      } else if cur.at(b"NOTHREAD") {
        // C# 忽略 NOTHREAD
        dup_flag!(cur, &mut no_thread_seen, "NOTHREAD");
      } else {
        return Err(VectorReply::err(ERR_UNKNOWN_OPTION));
      }
    }

    // EPSILON / FILTER-EF 参与检索（对齐 C# 传参语义：maxFilteringEffort ??= 16
    // 放大过滤候选队列；delta 截断最大距离 —— 缺省对齐 Garnet 2.0f32）
    Ok(VsimPlan {
      key,
      element: vector.element,
      value_type: vector.value_type,
      values: vector.values,
      search: VectorSearchOptions {
        // 结果数/EF 均经范围校验（>=0），max(0) 为原写法保留
        count: count.unwrap_or(DEFAULT_VSIM_COUNT).max(0) as usize,
        search_exploration_factor: ef.unwrap_or(DEFAULT_VSIM_EF).max(0) as usize,
        filter: filter.unwrap_or(b""),
        max_filtering_effort: filter_ef.unwrap_or(DEFAULT_VSIM_FILTER_EF).max(0) as usize,
        delta: epsilon.unwrap_or(DEFAULT_VSIM_EPSILON),
        include_attributes: with_attribs,
      },
      with_scores,
    })
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVEMB
  ///
  /// `VEMB key element [RAW]` → 嵌入向量数组；RAW 时输出
  /// [量化器名, 原始量化字节, 范数, (Q8 量化范围)]。
  pub async fn network_vemb(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=3, "VEMB");
    // RAW 形态：第三参须为 RAW 关键字，其后按 raw 分支取原始量化数据
    if args.len() == 3 && !equals_ignore_case(args[2], b"RAW") {
      return VectorReply::err(ERR_VEMB_UNEXPECTED_OPTION);
    }
    let raw = args.len() == 3;

    // C#：键/元素缺失统一写空数组。命中走读+重建锁协议（恢复回建后
    // 首次嵌入读取由此装载原生索引，同 VSIM 口径）
    let (stored, _index_guard) = match self.manager.read_vector_index(prefix, args[0]).await {
      (Some(index), guard) => (index.to_bytes(), guard),
      (None, _) => return VectorReply::Array(Vec::new()),
    };

    if raw {
      return match self.manager.try_get_raw_embedding(&stored, args[1]).await {
        Some((bytes, quant, norm, range)) => {
          // 量化器名映射：BIN/XBIN_* → bin；Q8/XNOQUANT_* → q8；NOQUANT → fp32
          let quant_name: &[u8] = match quant {
            VectorQuantType::Bin | VectorQuantType::XbinI8 | VectorQuantType::XbinU8 => b"bin",
            VectorQuantType::Q8 | VectorQuantType::XnoQuantU8 | VectorQuantType::XnoQuantI8 => {
              b"q8"
            }
            VectorQuantType::NoQuant => b"fp32",
            VectorQuantType::Invalid => b"fp32",
          };
          let mut items = vec![
            VectorReply::Simple(quant_name),
            VectorReply::Bulk(Some(bytes.into())),
            VectorReply::Double(norm),
          ];
          // 仅 Q8 追加量化范围
          if quant == VectorQuantType::Q8 {
            items.push(VectorReply::Double(range.unwrap_or(0.0)));
          }
          VectorReply::Array(items)
        }
        None => VectorReply::Array(Vec::new()),
      };
    }

    match self.manager.try_get_embedding(&stored, args[1]).await {
      Ok(Some(embedding)) => VectorReply::Array(
        embedding
          .into_iter()
          .map(|v| VectorReply::Double(f64::from(v)))
          .collect(),
      ),
      Ok(None) => VectorReply::Array(Vec::new()),
      // 存在性判定链存储读失败 → ERR 错误帧（禁 nil 假阴性）
      Err(e) => VectorReply::Error(e.message.into()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVCARD
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetCardinality 锁点 :459——
  /// `using (ReadVectorIndex)` 共享读锁全程罩住基数读取体；rust 同款：
  /// 守卫随本 async 栈帧跨 card 读存活，ptr=0 冷记录经独占重建后降级共享
  /// 命中（懒回建窗静默零答结构性消失）。
  pub async fn network_vcard(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VCARD");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return VectorReply::Integer(0);
    };
    VectorReply::Integer(self.manager.service.card(index.context) as i64)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVDIM
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetDimensions 锁点 :394（using 锁全程
  /// 持读优化共享锁），rust 同款锁定读面（防删锁与族形欠账一并收口）。
  pub async fn network_vdim(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VDIM");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → "ERR Key not found"
      return VectorReply::err(ERR_KEY_NOT_FOUND);
    };
    VectorReply::Integer(i64::from(index.dimensions))
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVGETATTR
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetGetAttribute 锁点 :565（using 锁
  /// 全程罩住属性读取体），rust 同款：守卫随本 async 栈帧跨属性读 await
  /// 存活。
  pub async fn network_vgetattr(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VGETATTR");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → null
      return VectorReply::Bulk(None);
    };
    match self
      .manager
      .fetch_single_vector_element_attributes(&index.to_bytes(), args[1])
      .await
    {
      Some(attr) => VectorReply::Bulk(Some(attr.into())),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVINFO
  ///
  /// `VINFO key` → 14 项元信息（quant-type/distance-metric/input-vector-dimensions/
  /// reduced-dimensions/build-exploration-factor/num-links/size）。
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetInfo 锁点 :428（using 锁全程罩住
  /// 元信息与 size 读取体）；rust 同款：size 臂走 service.card 需原生索引
  /// 在位，锁定读面使 ptr=0 冷记录先重建后应答（懒回建窗 size=0 静默零答
  /// 结构性消失）。
  pub async fn network_vinfo(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VINFO");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → null 数组
      return VectorReply::NullArray;
    };
    // 对齐 C# 小写枚举名（Invalid 在 C# 侧抛异常；此处防御性报错）
    let quant: &[u8] = match index.quant_type {
      VectorQuantType::NoQuant => b"f32",
      VectorQuantType::Bin => b"bin",
      VectorQuantType::Q8 => b"q8",
      VectorQuantType::XnoQuantU8 => b"xnoquant_u8",
      VectorQuantType::XnoQuantI8 => b"xnoquant_i8",
      VectorQuantType::XbinI8 => b"xbin_i8",
      VectorQuantType::XbinU8 => b"xbin_u8",
      VectorQuantType::Invalid => return VectorReply::err(b"ERR Invalid VectorQuantType"),
    };
    let metric: &[u8] = match index.distance_metric {
      VectorDistanceMetricType::Cosine => b"cosine",
      VectorDistanceMetricType::InnerProduct => b"inner-product",
      VectorDistanceMetricType::L2 => b"l2",
      VectorDistanceMetricType::XCosineNormalized => b"cosine-normalized",
    };
    let bulk_int = |v: u32| VectorReply::BulkInt(i64::from(v));
    VectorReply::Array(vec![
      VectorReply::Simple(b"quant-type"),
      VectorReply::Simple(quant),
      VectorReply::Simple(b"distance-metric"),
      VectorReply::Simple(metric),
      VectorReply::Simple(b"input-vector-dimensions"),
      bulk_int(index.dimensions),
      VectorReply::Simple(b"reduced-dimensions"),
      bulk_int(index.reduce_dims),
      VectorReply::Simple(b"build-exploration-factor"),
      bulk_int(index.build_exploration_factor),
      VectorReply::Simple(b"num-links"),
      bulk_int(index.num_links),
      VectorReply::Simple(b"size"),
      // C# :1616 WriteInt64AsBulkString(size)；基数为 u64 计数，转 i64 与本文件
      // VCARD（:980 `card(..) as i64`）同口径
      VectorReply::BulkInt(self.manager.service.card(index.context) as i64),
    ])
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVISMEMBER
  ///
  /// `VISMEMBER key element`（RESP3 以布尔应答）。C# 对位
  /// VectorStoreOps.cs:VectorSetIsMember 锁点 :484——守卫随本 async 栈帧
  /// 跨成员读 await 存活。
  pub async fn network_vismember(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VISMEMBER");
    let member = match self.manager.read_vector_index(prefix, args[0]).await {
      (Some(index), _guard) => {
        match self.manager.is_member(&index.to_bytes(), args[1]).await {
          Ok(member) => member,
          // 存在性判定链存储读失败 → ERR 错误帧（禁 false 假阴性应答）
          Err(e) => return VectorReply::Error(e.message.into()),
        }
      }
      (None, _) => false,
    };
    bool_reply(member, resp3)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
  ///
  /// `VLINKS key element [WITHSCORES]`。C# 侧输出为 TODO（恒 +OK）；
  /// 此处返回层 0 邻接的实际元素（超集语义），键/元素缺失写 null。
  /// WITHSCORES 透传已算距离（id/score 扁平成对，形同 VSIM RESP2
  /// WITHSCORES 布局，零新机制）；悬垂邻接（VREM 删除窗/半途失败残留的
  /// fsm 空闲 id）已在 neighbors 遍历臂跳过，存活成员恒回 Array 非 null。
  /// C# 对位 VectorStoreOps.cs:VectorSetLinks 锁点 :512——守卫随本 async
  /// 栈帧跨邻接读 await 存活。
  pub async fn network_vlinks(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=3, "VLINKS");
    let with_scores = if args.len() == 3 {
      if !equals_ignore_case(args[2], cs::WITHSCORES) {
        return VectorReply::err(ERR_VLINKS_UNEXPECTED_OPTION);
      }
      true
    } else {
      false
    };
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return VectorReply::Bulk(None);
    };
    match self.manager.service.links_of(index.context, args[1]).await {
      Some(links) => {
        let mut items = Vec::with_capacity(links.len() * (1 + usize::from(with_scores)));
        for (id, score) in links {
          items.push(VectorReply::Bulk(Some(id.into())));
          if with_scores {
            items.push(VectorReply::Double(f64::from(score)));
          }
        }
        VectorReply::Array(items)
      }
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVRANDMEMBER
  ///
  /// C# 侧输出为 TODO（恒 +OK）；此处返回实际取样元素（超集语义）。
  /// C# 对位 VectorStoreOps.cs:VectorSetRandomMembers 锁点 :541——守卫随
  /// 本 async 栈帧跨取样 await 存活。
  ///
  /// count 钳制：正数取 min(count, card)（C# 少取合法契约），负数归零回空
  /// 数组；会话层单点钳制，service.sample 不设第二道门。
  pub async fn network_vrandmember(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=2, "VRANDMEMBER");
    let count = match args.get(1) {
      Some(raw) => {
        let Some(v) = strict_i32(raw) else {
          return VectorReply::err(ERR_EXPECTED_INTEGER_COUNT);
        };
        v
      }
      None => 1,
    };
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND：指定 count → 空数组；未指定 → null
      return if args.len() == 2 {
        VectorReply::Array(Vec::new())
      } else {
        VectorReply::Bulk(None)
      };
    };
    // 钳制：正 count 取 min(count, card)。C# VectorStoreOps.cs:VectorSetRandomMembers
    // 为零分配 TODO 桩，接口注释钉 "It is OK to fetch fewer than the requested
    // number of elements"（IGarnetApi.cs:2154，少取合法），card 即天然上界；
    // 不钳则 sample 按 count 两笔 vec 预分配（12B/ID，i32::MAX ≈ 25.7GB）分配
    // 失败直接 abort 进程（拒绝服务面），同族 VSIM COUNT 有 MAX_RETRIEVE_COUNT
    // 门，本命令以 card 承担对称防御。负 count 归零维持回空数组现状；
    // card 直读标量零成本。
    let count = (count.max(0) as usize).min(self.manager.service.card(index.context) as usize);
    let samples = self.manager.service.sample(index.context, count).await;
    // 未指定 count（默认 1）回 bulk 单元素/缺集 null；显式 count 恒走数组臂。
    // 判定钉在 args.len() 上——count 已被钳制 shadow，card=0 时 min(1,0)=0
    // 不得误改此臂语义（空集仍回 null，与钳制前一致）。
    if args.len() == 1 {
      return match samples.into_iter().next() {
        Some(s) => VectorReply::Bulk(Some(s.into())),
        None => VectorReply::Bulk(None),
      };
    }
    VectorReply::Array(
      samples
        .into_iter()
        .map(|v| VectorReply::Bulk(Some(v.into())))
        .collect(),
    )
  }
}

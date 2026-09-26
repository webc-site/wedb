use core::str;

use wbftree::{BfTreeService, ScanReturnField};
use wcol::{
  list::{list_object::ListOperation, list_object_impl::read_list_position_params},
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::decode_member},
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
};
use wval::GarnetObjectType;

use super::common::{
  OutFace, RespHead, SCAN_FROM_HEAD, TieredCollectionArgs, TieredCtx, emit_tiered_mirror,
  finish_tiered_arm, save_tiered_meta, scan_count, stream_scan_face, tiered_guard, tiered_precheck,
  tiered_write_mirror, tree_put_ok, tree_put_rejected,
};

/// 列表族写面判定（一处定义）：四 push 写臂取独占写锁（LPUSH 序号分配依赖锁内
/// 刷新后的 meta.size，免装载快照错位）；LPOP / RPOP 无树内臂，与纯读、其余
/// 穿透臂一律共享读锁（见本模块头注「树内零墓碑」）
pub(crate) fn list_needs_write(op: ListOperation) -> bool {
  matches!(
    op,
    ListOperation::Rpush | ListOperation::Rpushx | ListOperation::Lpush | ListOperation::Lpushx
  )
}

/// 分层列表头端序号（C# `LinkedList.First` 指针的分层态对位）
///
/// 树最左键一次定位：`scan_cnt=1` 只界定**返回条数**、不界定**遍历条数**——
/// 底层单趟游标对每条记录先做墓碑判定（`leaf_node.rs` 的 `is_absent()` 臂排在
/// `bound_key` 比较之前并直接返回 Deleted），再对存活记录扣减 scan_cnt，故本臂
/// 「一次下降 + 一条记录即返回」的 O(1) 只对**无墓碑树**成立；游标之后若存在连续
/// 墓碑，那一段必须在**单次 `next()` 内**逐条跳完（实测每跳一条压一帧 ≈680B，
/// 早停回调与 count 都来不及生效），页读次数与栈深度皆随连跑长度增长
/// （实测分档与机理：task/reject/tiered-zset-demote-stack.md 二.表 S1..S4 与一.表）。
/// 故本臂的栈安全性完全依赖「树内零墓碑」这一写形不变量，而该不变量由删除不
/// 逐成员落树、统一走整值重灌面保证（见 `rmw_helpers::apply_rmw_post_operate`）。
///
/// 序号编码单点在 wcol：`ListObject::export_entries`
/// （wcol/src/types/garnet_object.rs:243）以 `(LIST_SEQ_BASE + 位次)` 的 u128
/// 16B 大端导出元素，升阶与重灌同源；本函数与之同基准。曾有的第二形态
/// （按 u64 位次 8B 导出、信封枚举内另写一份 16B 映射）已删——位置索引与
/// 序号窗口不可混用，混用即令 push 分配的序号与既有键错开整段。
///
/// 尾端无需第二次定位：分层列表序号占用区恒为连续区间
/// `[头, 头 + meta.size - 1]`，因三个写入面都保持连续排布——
/// - 升阶 bulk_load（`IGarnetObject::export_entries` 逐类型委派，
///   wcol/src/types/garnet_object.rs:453）自 `LIST_SEQ_BASE` 起按元素序连续排布；
/// - RPUSH 自尾 +1 向上、LPUSH 自头 -n 向下（本函数消费方之一，见
///   [`tiered_list_arm`] 的 push 双臂；LRANGE 臂另以头 + 位次折出有界区间起点）；
/// - 两端摘除（LPOP/RPOP）与中段删改（LREM/LTRIM/LINSERT/LSET）均无树内臂，
///   一律经 [`tiered_materialize_blob`] 物化后整树重灌，重灌仍走连续排布。
///
/// 故不把头尾游标另存进元记录：那会把「树 + 已持久化 meta.size」本可推出的
/// 派生量变成第二份真值源（写侧任一维护点漏改即与树漂移、序号错乱），且
/// 两个 u128 字段会把全体集合类型共用的 24B [`MetaValue`] 撑到 64B（u128 强制
/// 16 字节对齐，wval/src/meta.rs:72 的 `repr(C, align(8))` 布局与单缓存行双条
/// 记录口径一并作废）。C# 侧游标是链表节点的内存指针、从不落盘，本函数按同一
/// 语义在树内直取，元记录仍只持久化 `size` 一处计数。
///
/// 空树（`MetaValue::is_live` 已挡掉 size 为 0 的元记录，正常不可达）回落
/// `LIST_SEQ_BASE`，与升阶排布同基准。扫描 Err 经 [`scan_count`] 上抛——
/// 静默回落基准会让 push 把序号覆盖到既有元素上（数据销毁），故必须失败
/// 而非猜默认
#[inline]
fn list_head_seq(tree: &BfTreeService) -> Result<u128, ()> {
  let mut head = LIST_SEQ_BASE;
  scan_count(
    tree.scan_with_count_callback(SCAN_FROM_HEAD, 1, ScanReturnField::Key, |k, _| {
      if let Ok(arr) = <[u8; 16]>::try_from(k) {
        head = u128::from_be_bytes(arr);
      }
      false
    }),
  )?;
  Ok(head)
}

/// 执行分层态列表命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_list<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, ListOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let handled = tiered_list_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态列表命令树内主体（读写臂分派，见 [`exec_tiered_list`]）
async fn tiered_list_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, ListOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  let (arg1, arg2) = args12;
  // 四 push 写臂即全部稳态写面（list_needs_write 无读校正臂），镜像面与锁面同域
  let mirror = tiered_write_mirror(GarnetObjectType::List, op, args12, args, list_needs_write);
  // 本命令负载起点（臂尾镜像入账失败撤帧用，与 zset 臂 frame_base 同形）
  let frame_base = output.len();
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, list_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      // 四 push 臂单核（RPUSH / RPUSHX 自尾 +1 向上、LPUSH / LPUSHX 自头 -n 向下）：
      // 序号 u128 16B 大端（保序），头端一次定位（等价 C# ListPush 的
      // list.AddLast / list.AddFirst，ListObjectImpl.cs:229-244 O(1)，不再扫树求
      // cur_max / cur_min）
      ListOperation::Rpush
      | ListOperation::Rpushx
      | ListOperation::Lpush
      | ListOperation::Lpushx => {
        let push_left = matches!(op, ListOperation::Lpush | ListOperation::Lpushx);
        let head = list_head_seq(tree)?;
        // 尾 = 头 + size - 1（序号占用区恒连续，见 [`list_head_seq`] 头注）：
        // RPUSH 向上分配，LIST_SEQ_BASE 距 u128 上界留有 2^128 量级空间，永不环绕；
        // LPUSH 向下分配，u128 低半区距 LIST_SEQ_BASE 留有 2^64 空间，saturating_sub
        // 兜住下溢、绝不回绕覆盖尾端元素（删空即 drain_and_delete 销毁整树，
        // 下次推入重回基准，故该半区实际永不耗尽）
        let base = if push_left {
          head.saturating_sub(args.len() as u128)
        } else {
          head + ctx.meta.size as u128
        };
        // 第 j 个写入槽位 → (元素, 序号键)：右推按 args 正序消费、左推按 args 逆序
        // 消费（C# ListPush 与对象层 list_push
        // （wcol/src/list/list_object_impl.rs:248-266）同为「一参一次 AddFirst」的
        // 循环 ⇒ LPUSH k a b 落 [b, a, 旧…]，即末参占最小序号 = 新头端），两形态
        // 写入序与序号分配与旧双臂逐点一致
        let slot = |j: usize| -> (&[u8], [u8; 16]) {
          let item = if push_left {
            args[args.len() - 1 - j]
          } else {
            args[j]
          };
          (item, (base + j as u128).to_be_bytes())
        };
        // 预校验先于任何写入（RI 批量口径：任一元素越契约即整体失败、零树内副作用；
        // 序号恒 16B 键，落树记录为元素编码后长）
        for j in 0..args.len() {
          let (item, seq) = slot(j);
          if !tiered_precheck(ctx, &seq, item.len(), None, output) {
            break 'arm Ok(true);
          }
        }
        // 「插成功才计数」：pushed 与树内实存一一对应，size 即应答长度
        let mut pushed = 0u64;
        let mut put_rejected = false;
        for j in 0..args.len() {
          let (item, seq) = slot(j);
          if tree_put_ok(ctx, tree, &seq, item, None) {
            pushed += 1;
          } else {
            put_rejected = true;
          }
        }
        ctx.meta.size += pushed;
        // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
        if save_tiered_meta(session, key, ctx).await.is_err() {
          break 'arm Err(());
        }
        if put_rejected {
          tree_put_rejected(output);
          break 'arm Ok(true);
        }
        output.write_resp_int(ctx.meta.size as i64);
        Ok(true)
      }

      ListOperation::Llen => {
        output.write_resp_int(ctx.meta.size as i64);
        Ok(true)
      }

      ListOperation::Lindex => {
        // len 经 [`tiered_guard`] 读臂锁内刷新与树内容同源：增长窗内已存在元素
        // 不再因锁外陈旧 len 偏小折算越界而回假 null
        let len = ctx.meta.size as i64;
        let idx = if arg1 < 0 {
          len + i64::from(arg1)
        } else {
          i64::from(arg1)
        };
        if idx < 0 || idx >= len {
          output.write_resp_null_ver(resp_protocol_version);
          break 'arm Ok(true);
        }
        // 顺序定位第 idx 个（0-based；负索引已折算）且回调内直写命中元素
        // （对位 C# ListIndex 经 ElementAtOrDefault 取 LinkedListNode 的 byte[]
        // 引用后 WriteBulkString 直写 RespMemoryWriter，ListObjectImpl.cs:113-127，
        // 零中间容器零中间拷贝；先例：本文件 LRANGE 臂与 tiered hash.rs Hget 臂），
        // 消除 found: Option<Vec<u8>> 的 O(元素长) 物化 + 二次写出，命中即
        // return false 截断扫描
        let frame_base = output.len();
        let mut found = false;
        // 位次 idx 的元素键 = (头 + idx) 的 u128 大端 16B（序号占用区恒连续，见
        // [`list_head_seq`] 头注），单元素有界扫一次定位：底层游标按
        // keys[pos] >= start_key 定位起点，代价 O(1) 定位 + O(1) 读，与 LRANGE 臂
        // 同一机制。旧臂 &[0u8] / usize::MAX 自树头扫到 idx，LINDEX k -1 在千万级
        // 列表上付整树页级 IO 的读放大，就此消除
        // 扫描 Err 经 [`scan_count`] 上抛前 `output.truncate(frame_base)` 撤帧
        // （直写后半成品帧残留应答流即协议错位，口径对齐 LRANGE 臂与
        // task/done/tiered-scan-error-silent-empty-probe.md 不变式 2），
        // 严禁折成 null——存储故障不得伪装成键缺失
        let head = list_head_seq(tree)?;
        let start_key = (head + idx as u128).to_be_bytes();
        if scan_count(tree.scan_with_count_callback(
          &start_key,
          1,
          ScanReturnField::Value,
          |_, v| {
            output.write_resp_bulk_string(decode_member(v).1);
            found = true;
            false
          },
        ))
        .is_err()
        {
          output.truncate(frame_base);
          break 'arm Err(());
        }
        // claim 窗回退快照残余失配（size > 树内实存，见 [`tiered_guard`] 头注）：
        // 定位落空按实存回 null，与 Hget 臂 alive 标志兜底同口径
        if !found {
          output.write_resp_null_ver(resp_protocol_version);
        }
        Ok(true)
      }

      ListOperation::Lrange => {
        // 起止折算先于任何树访问，对位 C# ListRange 自 list.Count 折算与钳制
        // （ListObjectImpl.cs:144-157）：len 经 [`tiered_guard`] 读臂锁内刷新，
        // 与树内容同源（计数与元素同一活对象求值的分层等价，读锁与写臂「树写
        // → meta 回写」窗口互斥），不再依赖扫完才得的条数
        let len = ctx.meta.size as i64;
        let mut start = i64::from(arg1);
        let mut stop = i64::from(arg2);
        start = if start < 0 { len + start } else { start };
        if start < 0 {
          start = 0;
        }
        stop = if stop < 0 { len + stop } else { stop };
        if stop >= len {
          stop = len - 1;
        }
        if start > stop {
          output.extend_from_slice(cs::RESP_EMPTYLIST);
          break 'arm Ok(true);
        }
        // 位次 i 的元素键 = (头 + i) 的 u128 大端 16B（序号占用区恒连续，见
        // [`list_head_seq`] 头注），故 [start, stop] 折成「自 start 键起、条数上界
        // = stop - start + 1」的有界区间扫：底层游标按 keys[pos] >= start_key 定位
        // 起点、存活记录扣减 scan_cnt 后即停，代价随窗口而非全树基数增长。旧臂
        // &[0u8] / usize::MAX 无条件全树扫进 Vec<Vec<u8>> 再裁剪，千万级列表上一条
        // LRANGE k 0 10 也付整树反序列化的读放大与逐成员堆分配峰值，就此消除
        let head = list_head_seq(tree)?;
        let start_key = (head + start as u128).to_be_bytes();
        let count = (stop - start + 1) as usize;
        // 出帧走 [`stream_scan_face`]（与 SMEMBERS / HGETALL 同一预留-回填骨架，
        // 不变式见 wresp::ext::RESP_FRAME_HEAD_RESERVED 文注）：`count` 只作预留
        // 位宽估算提示，帧头以实际出帧条数回填——帧头与实体同源，消除对「计数
        // == 实存」的前置依赖：迁移 claim 窗（读臂回退快照，见 [`tiered_guard`]
        // 头注）等残余失配降级为按实存出帧的合法前缀应答，不再撤帧上抛；扫描
        // Err 撤帧后上抛（半成品帧残留应答流即协议错位，严禁折成空列表，口径见
        // task/done/tiered-scan-error-silent-empty-probe.md 不变式 2）
        if stream_scan_face(
          tree,
          output,
          OutFace::window(
            &start_key,
            count,
            ScanReturnField::Value,
            RespHead::array(resp_protocol_version),
          ),
          |out, _, v| out.write_resp_bulk_string(decode_member(v).1),
        )
        .is_err()
        {
          break 'arm Err(());
        }
        Ok(true)
      }

      ListOperation::Lpos => {
        // LPOS 树内单扫臂（collection.md §8.1 要点2 只读非穿透、§8.5 同构口径）：
        // 正向窗口 [0, bound)（bound = maxlen∧len 折出，maxlen==0 则全数）自树头
        // 有界扫，缺省形命中即写整数截断（Lindex 臂同形）；COUNT 形命中位次回调内
        // 直写 + 帧头预留-回填（LRANGE 臂同机制）；负 rank 窗口折 [len−maxlen, len)
        // （maxlen==0 则 [0,len)）起点键一次定位，单趟正向扫仅收命中位次 i64
        // （每条 ≤8B，严禁物化元素字节），尾侧按降序取 |rank| 跳过 + count 截取
        // （与对象层 list_position rank<0 支逐点对表）。时间面 O(N) 一趟树扫系
        // §8.1.3/§8.3 主动接受的折损声明，负 rank×COUNT=0 全命中形的命中位次 Vec
        // 属结果窗口内成本；不建成员值二级索引（第二套机制）。旧形经
        // tiered_materialize_blob 整值三拷贝物化回对象层再扫，缺省形 8 字节应答
        // 亦付全树反序列化，就此消除（本文件 LINDEX 臂/LRANGE 臂头注两度同罪
        // 清零先例）。词元与三门拒绝复用 wcol read_list_position_params 单源，
        // 严禁第二解析器
        let element = args.first().copied().unwrap_or(&[]);
        let params = match read_list_position_params(args) {
          Ok(params) => params,
          Err(error) => {
            cs::write_error_raw(output, str::from_utf8(error).unwrap_or(""));
            break 'arm Ok(true);
          }
        };
        // len 经 [`tiered_guard`] 读臂锁内刷新与树内容同源（Lindex/Lrange 同口径）
        let len = ctx.meta.size as i64;
        // COUNT=0 全命中截断钳 len（对象层同式，C# `count == 0 ? list.Count : count`）
        let count = if params.count == 0 { len } else { params.count };
        let frame_base = output.len();
        let head = list_head_seq(tree)?;
        if params.rank > 0 {
          let bound = if params.maxlen == 0 {
            len
          } else {
            len.min(params.maxlen)
          };
          let mut rank = params.rank;
          let mut pos = 0i64;
          let mut n = 0usize;
          let mut hit = false;
          let write_head = |buf: &mut Vec<u8>, n| buf.write_resp_array_len(n);
          let upper = count.min(bound).max(0) as usize;
          let reserved = resp_frame_head_len(upper, write_head);
          // 缺省形不落预留头（命中即单整数帧）；COUNT 形帧头预留-回填，
          // 扫描 Err 经 [`scan_count`] 上抛前 `output.truncate(frame_base)` 撤帧
          // （口径对齐 LRANGE 臂与 task/done/tiered-scan-error-silent-empty-probe
          // 不变式 2），严禁折成 null/空数组
          if !params.is_default_count {
            reserve_resp_frame_head(output, reserved);
          }
          if bound > 0
            && scan_count(tree.scan_with_count_callback(
              &head.to_be_bytes(),
              bound as usize,
              ScanReturnField::Value,
              |_, v| {
                let idx = pos;
                pos += 1;
                if decode_member(v).1 == element {
                  if rank == 1 {
                    output.write_resp_int(idx);
                    if params.is_default_count {
                      hit = true;
                      return false;
                    }
                    n += 1;
                    // record 满 count 截断（C# noOfFoundItem == count 判据）
                    return (n as i64) != count;
                  }
                  rank -= 1;
                }
                true
              },
            ))
            .is_err()
          {
            output.truncate(frame_base);
            break 'arm Err(());
          }
          if params.is_default_count {
            // claim 窗快照失配兜底与 Lindex 臂 found 标志同口径：扫尽无命中回 null
            if !hit {
              output.write_resp_null_ver(resp_protocol_version);
            }
          } else {
            backfill_resp_frame_head(output, frame_base, reserved, n, write_head);
          }
        } else {
          // 负 rank：窗口折 [len−maxlen, len)（maxlen==0 则 [0,len)），起点键
          // head+(len−maxlen) 一次定位正向扫，尾侧降序选取（对象层 :422-443 同式）
          let window_start = if params.maxlen == 0 {
            0
          } else {
            (len - params.maxlen).max(0)
          };
          let window_len = len - window_start;
          let mut hits: Vec<i64> = Vec::new();
          if window_len > 0 {
            let start_key = (head + window_start as u128).to_be_bytes();
            let mut pos = window_start;
            if scan_count(tree.scan_with_count_callback(
              &start_key,
              window_len as usize,
              ScanReturnField::Value,
              |_, v| {
                let idx = pos;
                pos += 1;
                if decode_member(v).1 == element {
                  hits.push(idx);
                }
                true
              },
            ))
            .is_err()
            {
              output.truncate(frame_base);
              break 'arm Err(());
            }
          }
          // 降序取：跳过 |rank|-1 次出现后按 count 截取（rank=i32::MIN 恒不及 1
          // 形与对象层同款：skip 超界自然空回）
          let skip = params.rank.unsigned_abs() as i64 - 1;
          if params.is_default_count {
            match hits.iter().rev().nth(skip as usize) {
              Some(idx) => output.write_resp_int(*idx),
              None => output.write_resp_null_ver(resp_protocol_version),
            }
          } else {
            let picked: Vec<i64> = hits
              .iter()
              .rev()
              .skip(skip as usize)
              .take(count.max(0) as usize)
              .copied()
              .collect();
            if picked.is_empty() {
              output.extend_from_slice(cs::RESP_EMPTYLIST);
            } else {
              output.write_resp_array_len(picked.len());
              for idx in picked {
                output.write_resp_int(idx);
              }
            }
          }
        }
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答。LPOP / RPOP 同在此穿透（弹出臂已
      // 摘除，树内无逐成员删除，见本模块头注「树内零墓碑」）
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾：树守卫存续窗口内入账，早退臂未置脏自然跳过，落盘失败臂
  // 经 save_tiered_meta 以 break 'arm 落本收尾（判据与不变式见 emit_tiered_mirror）；
  // 入队失败撤本命令已落成功帧后按 AofEnqueue 契约上抛拒绝（禁假成功冒答）
  if emit_tiered_mirror(session, key, ctx, mirror).is_err() {
    output.truncate(frame_base);
    return Err(());
  }
  result
}

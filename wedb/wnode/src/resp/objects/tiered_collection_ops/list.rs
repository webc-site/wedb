use wbftree::{BfTreeService, ScanReturnField};
use wcol::{
  list::list_object::ListOperation,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::decode_member},
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::common::{
  TieredCollectionArgs, TieredCtx, TieredMirror, emit_tiered_mirror, finish_tiered_arm,
  save_tiered_meta, scan_count, tiered_guard, tiered_precheck, tiered_write_mirror, tree_put_ok,
  tree_put_rejected,
};

/// 列表族写面判定（一处定义）：四 push 写臂取独占写锁（LPUSH 序号分配依赖锁内
/// 刷新后的 meta.size，免装载快照错位）；LPOP / RPOP 无树内臂，与纯读、其余
/// 穿透臂一律共享读锁（见本模块头注「树内零墓碑」）
fn list_needs_write(op: ListOperation) -> bool {
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
    tree.scan_with_count_callback(&[0u8], 1, ScanReturnField::Key, |k, _| {
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
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  // 四 push 写臂即全部稳态写面（list_needs_write 无读校正臂），镜像面与锁面同域
  let mirror = tiered_write_mirror(GarnetObjectType::List, op, args12, args, list_needs_write);
  let handled = tiered_list_arm(
    session,
    key,
    ctx,
    TieredCollectionArgs::new(op, args12, args, resp_protocol_version),
    output,
    mirror,
  )
  .await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态列表命令树内主体（读写臂分派，见 [`exec_tiered_list`]）
async fn tiered_list_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, ListOperation>,
  output: &mut Vec<u8>,
  mirror: Option<TieredMirror<'_>>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  let (arg1, arg2) = args12;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, list_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      ListOperation::Rpush | ListOperation::Rpushx => {
        // 序号 u128 16B 大端（保序）：RPUSH 自尾 +1 向上连续分配，尾 = 头 + size - 1
        // （头端由 [`list_head_seq`] 一次定位，等价 C# ListPush 的 list.AddLast
        // ListObjectImpl.cs:229-244 O(1)，不再扫树求 cur_max）；LIST_SEQ_BASE 距
        // u128 上界留有 2^128 量级空间，向上永不环绕
        let base = list_head_seq(tree)? + ctx.meta.size as u128;
        // 预校验先于任何写入（RI 批量口径：任一元素越契约即整体失败、零树内副作用；
        // 序号恒 16B 键，落树记录为元素编码后长）
        for (idx, &item) in args.iter().enumerate() {
          let seq = (base + idx as u128).to_be_bytes();
          if !tiered_precheck(ctx, &seq, item.len(), None, output) {
            break 'arm Ok(true);
          }
        }
        // 「插成功才计数」：pushed 与树内实存一一对应，size 即应答长度
        let mut pushed = 0u64;
        let mut put_rejected = false;
        for (idx, &item) in args.iter().enumerate() {
          let seq = (base + idx as u128).to_be_bytes();
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

      ListOperation::Lpush | ListOperation::Lpushx => {
        // LPUSH 自头 -n 向下连续分配（等价 C# ListPush 的 list.AddFirst
        // ListObjectImpl.cs:229-244 O(1)，不再扫树求 cur_min）：u128 低半区距
        // LIST_SEQ_BASE 留有 2^64 空间，saturating_sub 兜住下溢，绝不回绕覆盖
        // 尾端元素（删空即 drain_and_delete 销毁整树，下次推入重回基准，
        // 故该半区实际永不耗尽）
        let base = list_head_seq(tree)?.saturating_sub(args.len() as u128);
        // 命令内多参序：C# ListPush 与对象层 list_push
        // （wcol/src/list/list_object_impl.rs:248-266）同为「一参一次 AddFirst」的
        // 循环 ⇒ LPUSH k a b 落 [b, a, 旧…]，故序号自低向高按 args 逆序分配
        // （末参占最小序号 = 新头端），与内存态两态一致
        // 预校验先于任何写入（RI 批量口径，同 RPUSH 臂；序号恒 16B 键）
        for (idx, &item) in args.iter().rev().enumerate() {
          let seq = (base + idx as u128).to_be_bytes();
          if !tiered_precheck(ctx, &seq, item.len(), None, output) {
            break 'arm Ok(true);
          }
        }
        // 「插成功才计数」：pushed 与树内实存一一对应，size 即应答长度
        let mut pushed = 0u64;
        let mut put_rejected = false;
        for (idx, &item) in args.iter().rev().enumerate() {
          let seq = (base + idx as u128).to_be_bytes();
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
        // 顺序定位第 idx 个（0-based；负索引已折算）；扫描 Err 经 [`scan_count`]
        // 上抛（应答尚未落帧），严禁折成 null——存储故障不得伪装成键缺失
        let mut skipped = 0i64;
        let mut found: Option<Vec<u8>> = None;
        scan_count(tree.scan_with_count_callback(
          &[0u8],
          usize::MAX,
          ScanReturnField::Value,
          |_, v| {
            if skipped < idx {
              skipped += 1;
              return true;
            }
            found = Some(decode_member(v).1.to_vec());
            false
          },
        ))?;
        match found {
          Some(v) => output.write_resp_bulk_string(&v),
          None => output.write_resp_null_ver(resp_protocol_version),
        }
        Ok(true)
      }

      ListOperation::Lrange => {
        // 起止折算先于任何树访问，对位 C# ListRange 自 list.Count 折算与钳制
        // （ListObjectImpl.cs:144-157）：分层列表无成员级 TTL（写臂恒 expiry=None），
        // meta.size 与树内存活条数恒等，故出帧条数在扫第一条之前即已确定，与本模块
        // LLEN / LINDEX 同一信任面，不再依赖扫完才得的条数
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
        // 帧头先落、成员逐条直出 output（零中间容器，对位 C# 的
        // WriteArrayLength(count) + 边迭代边 WriteBulkString）。扫描 Err 与实扫条数
        // 短于帧头承诺（仅树引擎配置与存根契约偏离态可达，序号连续不变量下恒等）
        // 同口径撤帧上抛：半成品帧残留应答流即协议错位，严禁折成空列表（口径见
        // task/done/tiered-scan-error-silent-empty-probe.md）
        let frame_base = output.len();
        output.write_resp_array_len(count);
        let scanned = scan_count(tree.scan_with_count_callback(
          &start_key,
          count,
          ScanReturnField::Value,
          |_, v| {
            output.write_resp_bulk_string(decode_member(v).1);
            true
          },
        ));
        if scanned != Ok(count) {
          output.truncate(frame_base);
          break 'arm Err(());
        }
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答。LPOP / RPOP 同在此穿透（弹出臂已
      // 摘除，树内无逐成员删除，见本模块头注「树内零墓碑」）
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾：树守卫存续窗口内入账（锁内 emit，AOF 序与树内提交
  // 序严格一致，见 emit_tiered_mirror 文注）；早退臂未走写漏斗不置脏自然跳过，
  // 落盘失败臂经 save_tiered_meta 以 break 'arm 落本收尾——镜像先行再上抛
  //（树已变更 ⟺ 镜像已入账，见 emit_tiered_mirror 头注不变式）
  emit_tiered_mirror(session, key, ctx, mirror);
  result
}

//! HyperLogLog RESP 命令（对标 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs
//! 与 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs 的 RMW 语义）
//!
//! PFADD/PFCOUNT/PFMERGE 直读本模块 [`HyperLogLog`] 的稀疏/稠密编码；
//! 写回一律落完整分配载荷（对标 C# 记录的物理长度恒为分配长度：稀疏含
//! SparseMemorySectorSize 预留增长扇区，稠密恒为 12304 字节），尾部扇区
//! 缓冲切除即下一发 PFADD 无从命中 `can_grow_in_place` 原位臂。限定：本
//! 宣言钉「分配长度全量落盘」口径，批量折叠下该分配长度本身为折叠单发形
//! （经 [`HyperLogLog::sparse_fits`] 峰值复检：建键以零段基座按扇区上探、
//! 扩容/合并出形不足即升稠密），不对齐 C# 逐元素原位/拷贝增长交错轨迹的
//! 终态长度形——折叠形为唯一法定形，见 doc/zh/deviations.md §135）。再限定：
//! 「恒为分配长度」的分配长度数值在 rust 侧恒由 update_grow/merge_grow 分配
//! 公式单源算得，不含 C# InPlace/TryMerge 热臂保留原分配形（对标仅口径非数值，
//! PFMERGE dest 轨裁决见 doc/zh/deviations.md §145）。快路径读
//! 原语遇磁盘候选（冷数据）返回 [`HllLoad::Degrade`]，命令臂 `Ok(false)`
//! 转慢路径异步装载裁决（[`slow_hll_add`] 等）——Tsavorite RMW 挂起
//! pending 磁盘读后重放、NOTFOUND 才允许新建的同栈语义，冷区基数绝不被
//! 盲插覆盖。
//! 写回全链 RMW 语义（快路径 try_rmw_sync / 慢路径 rmw_string，缺失键
//! 新建、既有键恒保留 key 级 TTL——对标 C# CopyUpdater 的 PFADD/PFMERGE
//! 分支 TryCopyOptionals 保留 Expiration，不对齐 InPlace 臂 RemoveExpiration，
//! 见 doc/zh/deviations.md 第 17 条），杜绝快慢两路漂移。PFMERGE 写回
//! 按源并入事实门控（全源缺失零 SET 不建幽灵键，既有 dest 不盲写推进
//! WATCH 版本），对标 C# HyperLogLogMerge 的 SET_Conditional 仅在源 GET
//! 命中后于循环内执行。
//!
//! 写回信号分流契约：快路径 [`store_hll`] 对 `try_rmw_sync` 的降级
//!（环形页翻转 / TTL 磁盘候选）与存储 I/O 错误三态上交，命令臂降级即
//! `Ok(false)` 转慢路径 `rmw_string` 闭环重放、I/O 错误即 RESP_ERR_GENERIC
//! 独占应答——杜绝「:1/+OK 应答已出而载荷未落库、WATCH 未推进、AOF 零
//! 传播」的丢写面（C# 同位失败经 IPUResult.Failed 上抛，无吞信号路径）。
//!
//! PFCOUNT 多键尾键缺失回真实并集基数（Redis 语义），不对齐 C#
//! HyperLogLogLength 尾键 NOTFOUND 恒回 0 的上游缺陷，见
//! doc/zh/deviations.md 第 16 条。多键并集累加器恒稠密化（[`hll_union_seed`]
//! 稀疏首键亦展开为稠密），不对齐 C# 稀疏累加器遇稠密源 TryMerge 恒败臂
//! （HyperLogLog.cs:957）被静默剔除的同函数姊妹缺陷面，见
//! doc/zh/deviations.md 第 108 条。
//!
//! 两条读路径不变量（改动须同守）：
//! - HYLL 校验前置在读闭包内的借用切片上（[`valid_hyll_payload`] 单入口），
//!   非法载荷零值拷贝即落 WRONGTYPE 决策，仅合法分支物化 owned 副本；
//! - 多键 PFCOUNT 的稠密缓冲按命令复用（[`hll_union_absorb`] 单入口），
//!   申请次数为常数，不随键数增长。

use wdev::Device;
use whyperlog::{HyperLogLog, SPARSE_MEMORY_SECTOR_SIZE, SPARSE_SIZE_MAX_CAP};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{
    RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE_HLL, RESP_OK, RESP_RETURN_VAL_0, RESP_RETURN_VAL_1,
    write_error_raw,
  },
  ext::RespVecExt,
};

use crate::{
  resp::resp_server_session::RespServerSession, storage::session::storage_session::StorageSession,
};

/// HLL 键同步装载三态（消解 wkv 读面 `Ok(None)` 的两义：磁盘候选须降级 ≠ 键缺失）
enum HllLoad {
  /// String 域命中且载荷长度合法
  Present(Vec<u8>),
  /// 双域皆缺：键不存在
  Missing,
  /// String 域磁盘候选 / TTL 待裁决：命令臂转 `Ok(false)` 慢路径异步装载
  Degrade,
}

/// 慢路径异步装载三态（磁盘候选经 `read_tag_with` 闭环后消除降级态）
enum HllCold {
  Present(Vec<u8>),
  Missing,
  /// 集合对象键或载荷非法（C# output 0xFF → HyperLogLogWrongType）
  WrongType,
}

/// 钉住记录上的 HYLL 校验 + 命中物化（HLL 读路径唯一校验入口，快/慢两路共用）
///
/// 对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:655-660 PFADD 原位
/// 臂与 :1219-1223 CopyUpdater 臂：`IsValidHYLL(logRecord.PinnedValuePointer,
/// valueLen)` 直接在钉住的只读记录上判，非法即 `*output = 0xFF` 回 WRONGTYPE，
/// 全程不复制载荷。rust 读原语（`read_user_sync` / `read_tag_with`）递给闭包的
/// 正是同一份借用切片，故校验在闭包内就地完成：非法分支返回 `None`，一次字节
/// 都不拷（数百 MB 普通字符串键发 PFADD 不再先复制再拒）；仅合法分支取一份
/// owned 副本脱离读守卫生命周期
#[inline]
fn valid_hyll_payload(hll: &HyperLogLog, ptr: &[u8]) -> Option<Vec<u8>> {
  hll.is_valid_hyll(ptr).then(|| ptr.to_vec())
}

/// 从存储装载 HLL 载荷并校验（同步快路径）
///
/// 双域判定：String 域未命中时反探对象信封域——命中即集合对象键，杜绝
/// HLL 写入覆盖对象键。`Err(())` 为对象键 / 载荷非法 / 存储错误（快路径
/// 原口径统一答 WRONGTYPE_HLL）；[`HllLoad::Degrade`] 绝不视同缺失
fn load_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl Device>,
  key: &[u8],
) -> Result<HllLoad, ()> {
  use crate::storage::session::common::{UserRead, read_user_sync};
  match read_user_sync(store, key, None, |v| valid_hyll_payload(hll, v)) {
    Ok(UserRead::Hit(Some(raw))) => Ok(HllLoad::Present(raw)),
    // 信封域命中：对象键；Hit(None) 载荷非法（钉住记录上判毕，零拷贝）：
    // C# output 0xFF → WRONGTYPE
    Ok(UserRead::Hit(None)) | Ok(UserRead::WrongType) => Err(()),
    Ok(UserRead::Missing) => Ok(HllLoad::Missing),
    // String 域磁盘候选 / TTL 待裁决：转慢路径异步装载裁决（视同缺失会
    // 令 PFADD 盲插覆盖冷区历史基数——HyperLogLogOps 的 HyperLogLogAdd
    // 在磁盘候选时挂起 pending 读，NOTFOUND 才允许新建）
    Ok(UserRead::Deferred) => Ok(HllLoad::Degrade),
    Err(_) => Err(()),
  }
}

/// HLL 同步写回出口三态（消解 [`wkv::RmwWindow::try_rmw_sync`] 返回值的两义：
/// `Ok(Err(_))` 降级 ≠ 存储错误，调用方须分别处置）
enum HllStore {
  /// 已落盘且 WATCH 版本已推进，调用方可应答成功面
  Written,
  /// 环形页翻转 / TTL 记录磁盘候选：零字节已写、零版本推进，调用方在应答
  /// 写出前 `Ok(false)` 整体转慢路径异步闭环（[`slow_hll_add`] /
  /// [`slow_hll_merge`] 的 `rmw_string` 完整重放），杜绝成功应答掩盖丢写
  Degrade,
  /// 存储 I/O 错误：命令以错误帧独占应答中止（对标 set_pop 写失败臂
  /// RESP_ERR_GENERIC 口径），绝不回成功面
  IoError,
}

/// 将 HLL 完整分配载荷回写（同步快路径）
///
/// 载荷长度即分配长度，原样落盘不截断（对标 C# `logRecord.ValueSpan.Length`
/// 恒为 `SparseInitialLength` / `UpdateGrow` / `MergeGrow` 算得的分配长度，含
/// 尾部 `SparseMemorySectorSize` 预留增长扇区）——预留扇区在一读一写间被切除，
/// [`HyperLogLog::can_grow_in_place`] 即恒判不可原位增长，每次 PFADD 都退化为
/// 重分配 + 全量拷贝的新版本记录。
///
/// RMW 语义写回（保留既有 key 级 TTL，PFADD/PFMERGE 共用；对标 C#
/// CopyUpdater 臂 TryCopyOptionals 保留 Expiration，不对齐 InPlace 臂
/// RemoveExpiration——见 doc/zh/deviations.md 第 17 条）；写回一律落在调用方
/// 于装载旧值前取到的 [`RmwWindow`] 内，同键并发读改写由本键桶排他闩串行。
/// 降级 / 存储错误经 [`HllStore`] 三态上交调用方分流，本函数不吞任何信号
///（C# 同位失败经 `IPUResult.Failed` / `TrySetContentLengths` false 上抛，
/// RMWMethods.cs:665，绝无「写未发生却回成功」路径）
fn store_hll<D: Device>(window: &wkv::RmwWindow<'_, '_, D>, blob: &[u8]) -> HllStore {
  match window.try_rmw_sync(blob) {
    Ok(Ok(_)) => HllStore::Written,
    Ok(Err(_)) => HllStore::Degrade,
    Err(err) => {
      log::error!("HLL try_rmw_sync failed: {err:?}");
      HllStore::IoError
    }
  }
}

/// HLL 同步装载四态收尾单源（与本区 `store_hll_or_bail!` 并列同族形）：
/// Present 以 `$raw` 绑定载荷交 `$present` 闭包承接，Missing 各命令应答形态
/// 不同走 `$missing`；Degrade 绝不视同缺失，转 `Ok(false)` 异步重放
///（降级前不残留输出）、Err 落 WRONGTYPE_HLL 帧即 `Ok(true)` 闭环。
/// `load_hll` 调用恒展开在调用点作用域，装载守卫绑定的持有期不因收口缩短
macro_rules! load_hll_or_bail {
  ($hll:expr, $store:expr, $key:expr, $output:expr, $missing:expr, |$raw:pat_param| $present:expr) => {
    match load_hll($hll, $store, $key) {
      Ok(HllLoad::Present($raw)) => $present,
      Ok(HllLoad::Missing) => $missing,
      // 磁盘候选：降级慢路径异步装载裁决，降级前不残留输出
      Ok(HllLoad::Degrade) => return Ok(false),
      Err(()) => {
        write_error_raw($output, RESP_ERR_WRONG_TYPE_HLL);
        return Ok(true);
      }
    }
  };
}

macro_rules! store_hll_or_bail {
  ($window:expr, $blob:expr, $output:expr) => {
    match store_hll($window, $blob) {
      HllStore::Written => {}
      HllStore::Degrade => return Ok(false),
      HllStore::IoError => {
        $output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  };
}

/// 新键初始载荷：按元素数推算初始长度（超上限即稠密），init 内部按长度分派编码
///
/// C# 逐元素 count=1 使稀疏尾移写峰 1B 裕度结构性恒成立，SparseInitialLength
/// 无须设防；rust 批量折叠 init 盲插 N 元且 init_sparse 先落 128B 零段基座，
/// 最坏写峰 = 零段基座 + 2B×N + 1B——SparseInitialLength 的 2N 扇区取整形
/// （roundup128(2N) ∈ [2N, 2N+127]）恒缺此裕度。故以唯一容纳谓词
/// [`HyperLogLog::sparse_fits`] 以零段基座为 current 复检，不足按扇区上探，
/// 越 SPARSE_SIZE_MAX_CAP 即升稠密 12304（task zcode-r151c-pfconv 案一，
/// 折叠形为唯一法定形见 doc/zh/deviations.md §135）
fn hll_init_payload(hll: &HyperLogLog, elements: &[&[u8]]) -> Vec<u8> {
  let n = elements.len();
  let mut len = hll.sparse_initial_length(n);
  if len != hll.dense_bytes() {
    let current = hll.sparse_init_current();
    while !hll.sparse_fits(current, n, len) {
      len += SPARSE_MEMORY_SECTOR_SIZE;
      if len > SPARSE_SIZE_MAX_CAP {
        len = hll.dense_bytes();
        break;
      }
    }
  }
  let mut blob = vec![0_u8; len];
  hll.init(elements, &mut blob);
  blob
}

/// 目标键缺失的稀疏初始化载荷（PFMERGE dest 专用）
///
/// 稀疏初始化只写头部 + 零段（前 146B），直接按初始长度分配，
/// 免 SPARSE_SIZE_MAX_CAP（4KB）全量清零与二次拷贝
fn hll_sparse_seed(hll: &HyperLogLog) -> Vec<u8> {
  let mut blob = vec![0_u8; hll.sparse_bytes()];
  hll.init_sparse(&mut blob);
  blob
}

/// PFADD 元素并入载荷内核（快/慢路径共用）：有变更返回 Some(新载荷)
///
/// 稠密原位更新；稀疏可原位更新则原位，否则经 update_grow 扩容/稠密化。
/// 扩容分支的 updated 语义对标 C# CopyUpdater 的
/// `updated = HyperLogLog.DefaultHLL.CopyUpdate(...)` 回写
///（RMWMethods.cs:1237）→ `*output = updated ? 1 : 0`：迁移无寄存器
/// 变更同判 false，杜绝无变更脏写与虚假 :1
///
/// C# :0 臂非全静默（deviations §132）：Succeeded 派发推版本落 AOF
///（RMWMethods.cs:425-430）、无裕/磁盘驻拷贝臂迁移写回（:1215-1301/
/// :1518-1530）；rust 零写回零推进零镜像系 §123 同轨裁决侧，严禁按
/// C# 补齐空推进/空迁移写回
fn hll_add_payload(hll: &HyperLogLog, mut blob: Vec<u8>, elements: &[&[u8]]) -> Option<Vec<u8>> {
  let mut updated = false;
  if hll.is_dense(&blob) {
    hll.update(elements, &mut blob, &mut updated);
  } else if !hll.update(elements, &mut blob, &mut updated) {
    let n = elements.len();
    let mut new_len = hll.update_grow(n, &blob);
    // C# 逐元素 count=1 使尾移写峰 1B 裕度恒成立，rust 批量折叠须对
    // update_grow 出形以唯一容纳谓词复检（roundup128(2N)==2N 之形恰缺裕度、
    // copy_update 盲插即击穿切片界）——不足按本式既有稠密化口径升稠密，不另
    // 起扇区上探（task zcode-r151c-pfconv 案一，见 doc/zh/deviations.md §135）
    if new_len != hll.dense_bytes()
      && !hll.sparse_fits(hll.sparse_current_size_in_bytes(&blob), n, new_len)
    {
      new_len = hll.dense_bytes();
    }
    let mut grown = vec![0_u8; new_len];
    let updated = hll.copy_update(elements, &blob, &mut grown);
    return updated.then_some(grown);
  }
  updated.then_some(blob)
}

/// 源并入目标载荷内核（快/慢路径共用）：多源择大，容量不足按 MergeGrow 迁移
///
/// C# 逐元素合并裕度恒成立，rust 批量折叠对 merge_grow 出形与等长原位形以
/// 唯一容纳谓词 [`HyperLogLog::sparse_fits`] 复检（源每枚非零寄存器经
/// UpdateSparseReg 最坏 +2B；roundup128(2nz)==2nz 之形与 crafted rle=payload
/// 等号合法满容量目标恰击穿 1B 峰值裕度）——不足即升稠密（与 update_grow
/// 既有稠密化口径同式；与 C# 冷臂形的冲突面归案二登记，见
/// doc/zh/deviations.md §135）
///
/// 分配长度单臂裁决：本内核不设 C# InPlace/TryMerge 热臂「保留原分配」形，
/// dest 新物理长度恒取 merge_grow 出形（C# 热/冷两臂长度互斥系 §17/§18 同族
/// 漂移，rust 单臂自洽，严禁按 C# 热臂改回，见 doc/zh/deviations.md §145）
fn hll_merge_payload(hll: &HyperLogLog, mut dst: Vec<u8>, src: &[u8]) -> Vec<u8> {
  let mut new_len = hll.merge_grow(src, &dst);
  if new_len != hll.dense_bytes()
    && !hll.sparse_fits(
      hll.sparse_current_size_in_bytes(&dst),
      hll.sparse_count_non_zero(src),
      new_len,
    )
  {
    new_len = hll.dense_bytes();
  }
  if new_len != dst.len() {
    let mut grown = vec![0_u8; new_len];
    let old_len = dst.len();
    hll.copy_update_merge(src, &dst, &mut grown, old_len, new_len);
    grown
  } else {
    hll.merge(src, &mut dst);
    hll.set_card(&mut dst, i64::MIN);
    dst
  }
}

/// 并集累加器种子：稠密载荷直接接管（免 C# 的每键 memcpy），稀疏载荷展开一次
/// 稠密缓冲（校验已保证稠密载荷恰为 dense_bytes、稀疏载荷 ≤ SPARSE_SIZE_MAX_CAP）
fn hll_union_seed(hll: &HyperLogLog, raw: Vec<u8>) -> Vec<u8> {
  if hll.is_dense(&raw) {
    return raw;
  }
  let mut seed = vec![0_u8; hll.dense_bytes()];
  hll.init_dense(&mut seed);
  hll.sparse_to_dense(&raw, &mut seed);
  seed
}

/// PFCOUNT 多键虚拟并集并入口（快/慢路径共用，稠密化唯一实现）
///
/// 对标 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:118-172
/// `HyperLogLogLength` 的 `sectorAlignedMemoryHll1/2 ??=` 两块常驻缓冲 +
/// `TryMerge` 累加：稠密缓冲按命令复用而非按键分配。与 C# 的刻意差异是本仓
/// 载荷本已是 owned Vec，故首键稠密时直接接管为累加器；其余键一律交
/// [whyperlog::HyperLogLog::try_merge] 单点分派（内部按两侧编码走
/// dense_to_dense / sparse_to_dense），整条命令的 dense_bytes 级分配 ≤ 1 次，
/// 不随键数增长
fn hll_union_absorb(hll: &HyperLogLog, acc: &mut Option<Vec<u8>>, raw: Vec<u8>) {
  match acc {
    slot @ None => *slot = Some(hll_union_seed(hll, raw)),
    Some(dst) => {
      _ = hll.try_merge(&raw, dst, hll.dense_bytes());
    }
  }
}

/// 慢路径双域异步装载（[`load_hll`] 的磁盘候选闭环对位）
///
/// `read_tag_with` 内存未命中时异步装载磁盘冷区（含惰性过期物理清除）后
/// 裁决，对标 C# RMW 的 CompletePending 重放；String 域未命中探对象信封域
/// （双域次序同 [`load_hll`]，校验同走 [`valid_hyll_payload`] 单入口）。
/// `Err(())` 为存储错误
async fn load_hll_cold(
  hll: &HyperLogLog,
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
) -> Result<HllCold, ()> {
  use wval::KeyTag;
  Ok(
    match storage
      .read_tag_with(key, KeyTag::String, |v| valid_hyll_payload(hll, v))
      .await
    {
      // 外层 Some = String 域命中，内层 Some = 载荷合法（非法零拷贝即拒）
      Ok(Some(Some(raw))) => HllCold::Present(raw),
      // 内层 None：载荷长度非法（C# output 0xFF → WRONGTYPE）
      Ok(Some(None)) => HllCold::WrongType,
      // String 域确认缺失：探对象信封域
      Ok(None) => match storage
        .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
        .await
      {
        Ok(Some(())) => HllCold::WrongType,
        Ok(None) => HllCold::Missing,
        Err(_) => return Err(()),
      },
      Err(_) => return Err(()),
    },
  )
}

/// WRONGTYPE 应答帧写出（慢路径）
fn reply_wrong_type_hll(output: &mut Vec<u8>) {
  write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
}

/// PFADD 慢路径执行臂（exec_slow 分派；`Err(())` 为存储错误，调用方统一应答）
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogAdd
///（RMW 语义承接：异步装载磁盘冷区后裁决，缺失才新建）
pub(crate) async fn slow_hll_add(
  storage: &StorageSession<'_, impl Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();
  let key = parse_state.first().copied().unwrap_or(&[]);
  let elements = parse_state.get(1..).unwrap_or(&[]);

  // 零元素：C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
  // 不建键（含 WRONGTYPE 探测），直答 :0
  if elements.is_empty() {
    output.extend_from_slice(RESP_RETURN_VAL_0);
    return Ok(());
  }

  // 读改写原子窗口（异步域让核等待臂）先于冷区装载取，覆盖「异步装载—并入
  // 元素—写回」全程（对标 C# HyperLogLogAdd 的 RMW 单次桶闩内闭环）
  let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;

  let updated = match load_hll_cold(&hll, storage, key).await? {
    HllCold::Missing => {
      let blob = hll_init_payload(&hll, elements);
      // RMW 新建（InitialUpdater：新记录无 Expiration）；装载已探对象信封域
      // 拦截对象键，无需 SET 语义清退
      storage.rmw_string(&window, &blob).await.map_err(|_| ())?;
      true
    }
    HllCold::Present(raw) => match hll_add_payload(&hll, raw, elements) {
      Some(blob) => {
        // RMW 写回保留既有 key 级 TTL（对标 RMWMethods.cs:CopyUpdater 的
        // PFADD 分支 TryCopyOptionals 保留 Expiration；SET 语义会误清）
        storage.rmw_string(&window, &blob).await.map_err(|_| ())?;
        true
      }
      // 寄存器无变更不写（C# pfaddUpdated == 0 同口径）
      None => false,
    },
    HllCold::WrongType => {
      reply_wrong_type_hll(output);
      return Ok(());
    }
  };

  output.extend_from_slice(if updated {
    RESP_RETURN_VAL_1
  } else {
    RESP_RETURN_VAL_0
  });
  Ok(())
}

/// PFCOUNT 慢路径执行臂：逐键异步装载后虚拟并集计数（不改写存储）
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength
pub(crate) async fn slow_hll_count(
  storage: &StorageSession<'_, impl Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();

  // 单键短路：直接对原载荷估算，免 12KB 临时稠密缓冲与全量展开拷贝
  if parse_state.len() == 1 {
    let key = parse_state[0];
    let card = match load_hll_cold(&hll, storage, key).await? {
      HllCold::Missing => 0,
      HllCold::WrongType => {
        reply_wrong_type_hll(output);
        return Ok(());
      }
      HllCold::Present(mut raw) => hll.count(&mut raw),
    };
    output.write_resp_int(card as i64);
    return Ok(());
  }

  let mut acc: Option<Vec<u8>> = None;
  for key in parse_state {
    match load_hll_cold(&hll, storage, key).await? {
      HllCold::Missing => {}
      HllCold::WrongType => {
        reply_wrong_type_hll(output);
        return Ok(());
      }
      HllCold::Present(raw) => hll_union_absorb(&hll, &mut acc, raw),
    }
  }

  let card = acc.as_mut().map(|a| hll.count(a)).unwrap_or(0);
  output.write_resp_int(card);
  Ok(())
}

/// PFMERGE 慢路径执行臂：逐源异步装载择大并入目标后回写 +OK；源出错（WRONGTYPE
/// 或存储错误）时已并入部分落盘（部分提交），零并入（源出错或全源缺失）不写回
/// dest 不建键
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogMerge
pub(crate) async fn slow_hll_merge(
  storage: &StorageSession<'_, impl Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();
  let dest = parse_state.first().copied().unwrap_or(&[]);
  let sources = parse_state.get(1..).unwrap_or(&[]);

  // 零源：C# HyperLogLogMerge 源循环零次，不 GET 不 SET 不建键（dest 亦不
  // 探测 WRONGTYPE），直答 +OK
  if sources.is_empty() {
    output.extend_from_slice(RESP_OK);
    return Ok(());
  }

  // 读改写原子窗口（异步域让核等待臂）先于冷区装载取，覆盖「异步装载—择大
  // 并入—写回目标」全程；源键只读不取窗，同命令恒只持一把窗
  let window = storage.batch.rmw_window(dest).await.map_err(|_| ())?;

  // dest 前置拒与快臂同款（修复侧回指 doc/zh/deviations.md §144）
  let mut dst = match load_hll_cold(&hll, storage, dest).await? {
    HllCold::Present(raw) => raw,
    HllCold::Missing => hll_sparse_seed(&hll),
    HllCold::WrongType => {
      reply_wrong_type_hll(output);
      return Ok(());
    }
  };

  // 逐源装载并入；错误路径部分提交（WRONGTYPE 与存储错误均部分提交、零并入
  // 不写回）仅在已并入至少一源时写回——C# 每源 GET 合法后立即 SET_Conditional
  // 写 dst，源报 WRONGTYPE/载荷非法即 return/break、存储异常上抛，finally
  // Commit（createTransaction 门内 autocommit 恒提交）：已并入源随错误一并
  // 落盘，零并入（出错或全源缺失）则 dst 从未被 SET（缺失不被凭空建键）
  let mut merged = false;
  for src_key in sources {
    match load_hll_cold(&hll, storage, src_key).await {
      // 存储错误臂：已并入部分先补写落盘再上抛（对齐快臂 Err 臂与 C# finally
      // Commit，dest 终态不随执行臂漂移）；补写自身失败径直上抛——应答面未出
      //（错误帧由调用方统一承接 RESP_ERR_SLOW_PATH_STORAGE），绝不落成功面
      Err(()) => {
        if merged {
          storage.rmw_string(&window, &dst).await.map_err(|_| ())?;
        }
        return Err(());
      }
      Ok(HllCold::Missing) => {}
      Ok(HllCold::WrongType) => {
        if merged {
          storage.rmw_string(&window, &dst).await.map_err(|_| ())?;
        }
        reply_wrong_type_hll(output);
        return Ok(());
      }
      Ok(HllCold::Present(raw)) => {
        dst = hll_merge_payload(&hll, dst, &raw);
        merged = true;
      }
    }
  }

  // RMW 写回保留 dest 既有 key 级 TTL（对标 RMWMethods.cs:CopyUpdater 的
  // PFMERGE 分支 TryCopyOptionals 保留 Expiration；SET 语义会误清）；
  // 写回按源并入事实门控——C# SET_Conditional 仅在源 GET 命中后于循环内
  // 执行，全源缺失零 SET：dest 缺失不被凭空建键，dest 既有不盲写、WATCH
  // 版本不推进
  if merged {
    storage.rmw_string(&window, &dst).await.map_err(|_| ())?;
  }
  output.extend_from_slice(RESP_OK);
  Ok(())
}

impl RespServerSession {
  /// PFADD key [element ...]：登记元素，寄存器有变更回 :1 否则 :0
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogAdd
  pub fn hyper_log_log_add<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "PFADD");

    // 零元素：C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
    // 不建键（含 WRONGTYPE 探测），直答 :0
    if parse_state.len() == 1 {
      output.extend_from_slice(RESP_RETURN_VAL_0);
      return Ok(true);
    }

    let key = parse_state[0];
    let elements = &parse_state[1..];
    let hll = HyperLogLog::new();

    // 读改写原子窗口先于装载取，覆盖「装载旧载荷—并入元素—写回」全程（对标
    // C# HyperLogLogAdd 单次 storageApi.RMW 的桶闩内闭环）；取不到闩即降级，
    // 降级前不残留输出
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    let existing = load_hll_or_bail!(&hll, store, key, output, None, |raw| Some(raw));

    let updated = match existing {
      None => {
        let blob = hll_init_payload(&hll, elements);
        store_hll_or_bail!(&window, &blob, output);
        true
      }
      Some(raw) => match hll_add_payload(&hll, raw, elements) {
        Some(blob) => {
          store_hll_or_bail!(&window, &blob, output);
          true
        }
        // 寄存器无变更不写（C# pfaddUpdated == 0 同口径）
        None => false,
      },
    };

    output.extend_from_slice(if updated {
      RESP_RETURN_VAL_1
    } else {
      RESP_RETURN_VAL_0
    });
    Ok(true)
  }

  /// PFCOUNT key [key ...]：单键直读基数；多键做虚拟并集（不改写存储）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogLength
  pub fn hyper_log_log_length<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "PFCOUNT");

    let hll = HyperLogLog::new();

    // 单键短路：直接对原载荷估算，免 12KB 临时稠密缓冲与全量展开拷贝
    if parse_state.len() == 1 {
      let key = parse_state[0];
      let card = load_hll_or_bail!(&hll, store, key, output, 0, |mut raw| hll.count(&mut raw));
      output.write_resp_int(card as i64);
      return Ok(true);
    }

    // 逐键并入虚拟并集（累加器恒稠密，见 [`hll_union_absorb`]）；应答在裁决后
    // 统一写出，任一键磁盘候选即整体降级，无半成品应答残留
    let mut acc: Option<Vec<u8>> = None;
    for key in parse_state {
      load_hll_or_bail!(&hll, store, key, output, continue, |raw| {
        hll_union_absorb(&hll, &mut acc, raw)
      });
    }

    let card = acc.as_mut().map(|a| hll.count(a)).unwrap_or(0);
    output.write_resp_int(card);
    Ok(true)
  }

  /// PFMERGE dest [src ...]：多源择大并入目标，回 +OK（零源合法，dest 不
  /// 触达；全源缺失零写回不建键不盲写）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogMerge
  pub fn hyper_log_log_merge<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# HyperLogLogMerge 仅要求 Count>=1（存储层对 Count==0 才短路）
    check_arg_count!(parse_state, 1.., output, "PFMERGE");

    // 零源：C# 源循环零次，不 GET 不 SET 不建键（dest 亦不探测 WRONGTYPE），
    // 直答 +OK
    if parse_state.len() == 1 {
      output.extend_from_slice(RESP_OK);
      return Ok(true);
    }

    let dest = parse_state[0];
    let sources = &parse_state[1..];
    let hll = HyperLogLog::new();

    // 读改写原子窗口先于目标装载取，覆盖「择大并入全源—写回目标」全程；源键
    // 只读不取窗，同命令恒只持一把窗，无嵌套取闩面（对标 C# HyperLogLogMerge
    // 逐源 GET 后单次 dest SET 的桶闩内闭环）；取不到闩即整体降级，无半成品写面
    let Some(window) = store.try_rmw_window(dest) else {
      return Ok(false);
    };
    // dest 前置拒为修复侧：C# HyperLogLogMerge 对 dest 免前置校验且
    // HyperLogLogOps.cs:262 弃 SET_Conditional 返回值，坏形/异型 dest + 源
    // 命中恒 +OK 掩盖（Copy 臂垃圾盲并污写系上游缺陷），rust 恒独占
    // WRONGTYPE_HLL 拒裁，见 doc/zh/deviations.md §144（严禁按 C# 免检形回改）
    // 目标载荷（缺失按稀疏初始化）；+OK 在裁决后统一写出，dest 或任一
    // src 磁盘候选即整体降级，无半成品写面
    let mut dst = load_hll_or_bail!(&hll, store, dest, output, hll_sparse_seed(&hll), |raw| raw);

    // 错误路径部分提交（WRONGTYPE 与存储错误均部分提交、零并入不写回）仅在已
    // 并入至少一源时写回——C# 每源 GET 合法后立即 SET_Conditional 写 dst，源报
    // WRONGTYPE/载荷非法即 return/break、存储异常上抛，finally Commit
    //（createTransaction 门内 autocommit 恒提交）：已并入源随之提交，零并入
    // （出错或全源缺失）则 dst 从未被 SET（缺失不被凭空建键）；本臂存储错误以
    // Degrade 现形（快路径无异步读面），内存 dst 弃置转慢路径自存储重载重做，
    // 慢臂错误路径同款补写（见 [`slow_hll_merge`]），dest 终态两臂一致
    let mut merged = false;
    for src_key in sources {
      let src = match load_hll(&hll, store, src_key) {
        Err(()) => {
          if merged {
            // 错误路径部分提交补写（对标 C# finally Commit）：补写降级时
            // 应答面尚空（错误帧未写），整体转慢路径重放——慢路径遇同源
            // WRONGTYPE 时以 rmw_string 补写已并入部分后答错误帧，部分提交
            // 语义保留；补写 I/O 错误以 RESP_ERR_GENERIC 独占应答（真实存储
            // 故障优先于协议 WRONGTYPE 判定，绝不落成功面）
            store_hll_or_bail!(&window, &dst, output);
          }
          write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(HllLoad::Present(raw)) => raw,
        Ok(HllLoad::Missing) => continue,
        Ok(HllLoad::Degrade) => return Ok(false),
      };
      dst = hll_merge_payload(&hll, dst, &src);
      merged = true;
    }

    // 写回按源并入事实门控——C# SET_Conditional 仅在源 GET 命中后于循环内
    // 执行，全源缺失零 SET：dest 缺失不被凭空建键，dest 既有不盲写、WATCH
    // 版本不推进
    if merged {
      // 环形页翻转 / TTL 磁盘候选：零字节已写、+OK 未出，整体转慢路径
      // 异步重放，杜绝 +OK 掩盖丢写
      store_hll_or_bail!(&window, &dst, output);
    }
    output.extend_from_slice(RESP_OK);
    Ok(true)
  }
}

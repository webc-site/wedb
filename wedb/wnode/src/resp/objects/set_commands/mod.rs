//! 集合命令（对标 libs/server/Resp/Objects/SetCommands.cs）
//!
//! 命令层只做参数校验与编解码：单键语义全部下沉到
//! [`wcol::set::set_object::SetObject`] 的 operate 通道
//! 通道（与 C# GarnetObjectBase.Operate 分层一致）；SINTER/SUNION/SDIFF
//! 族为多键聚合，对标 libs/server/Storage/Session/ObjectStore/SetOps.cs
//! 的装载-折叠语义在命令层就地求值。存取经与 storage 会话域共享的
//! `[类型标签][载荷]` 信封（见 [`super::object_store_utils`]）。
//!
//! 目录化拆分：[`read`] 读命令、[`write`] 写命令与集合运算、[`slow`] 慢路径执行臂。

/// 单键 rmw 计数写臂收尾单源（SADD/SREM 共用，须先于 mod 声明定义）：set_rmw 装载
/// → operate → 回写内闭环，降级转 `Ok(false)` 异步重放、WRONGTYPE/缺键零变更即闭环、
/// 命中仅回填 result1 且负载未写时补整数回复
macro_rules! set_rmw_count_or_bail {
  ($self:expr, $store:expr, $op:expr, $parse_state:expr, $output:expr) => {{
    let key = $parse_state[0];
    match $self.set_rmw($store, key, $op, &$parse_state[1..], (0, 0), $output) {
      Rmw::Degrade => return Ok(false),
      Rmw::WrongType | Rmw::Missing => {}
      // C# 仅回填 result1，整数回复由 RESP 层写出（转调单源 write_rmw_reply）
      Rmw::Present(done @ RespRmwDone { .. }) => super::write_rmw_reply(done, $output),
      // AofFail：SADD/SREM 等写臂信封已生效、增量条目入队失败，撤帧落错误
      // 帧拒绝本命令（AofEnqueue 契约，禁 :N 计数帧假成功）
      Rmw::AofFail => {
        $crate::resp::objects::object_store_utils::write_rmw_aof_fail_frame($output);
        return Ok(true);
      }
    }
    Ok(true)
  }};
}

macro_rules! set_load_or_bail {
  ($store:expr, $key:expr, $output:expr, $missing:expr) => {
    $crate::obj_load_or_bail!(set_load_sync, $store, $key, $output, $missing)
  };
}

macro_rules! load_many_or_bail {
  ($store:expr, $keys:expr, $output:expr) => {
    $crate::load_many_or_bail!($store, $keys, $output)
  };
}

macro_rules! set_windowed_load {
  ($store:expr, $key:expr, $output:expr, mut $name:ident, $missing:expr) => {
    $crate::obj_windowed_load!(set_load_sync, $store, $key, $output, mut $name, $missing);
  };
}

mod read;
pub(crate) mod slow;
mod write;

use wbase::{map::HashSet, num::strict_i32};
use wcol::{
  ObjectOutput,
  set::{
    set_object::{SetObject, SetOperation},
    set_object_impl::NO_COUNT,
  },
};
use wdev::Device;
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

/// `write_rmw_reply` 单源重导出：rmw 计数宏 `set_rmw_count_or_bail!` 于 read/write
/// 子模块展开时经 `super::write_rmw_reply` 引用，与 object_store_utils 转发同源
pub(crate) use super::rmw_helpers::write_rmw_reply;
use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, SyncRmwCmd, SyncRmwHandlers, SyncRmwOutcome, obj_load_typed_sync,
    obj_save_or_gc, run_sync_rmw,
  },
  resp_server_session::RespServerSession,
};

pub(crate) type SetLoad = ObjLoad<SetObject>;
type Rmw = SyncRmwOutcome;

/// SPOP key \[count\] 参数推导单源（快慢路径共用；解析失败时已写出错误应答
/// 并返回 None），返回 (key, count；缺省 NO_COUNT)
///
/// 判定序对标 C# SetCommands.cs 的 SetPop：arity 1..=2 → count 非整数
/// （含溢出）或负数同报 NOT_INTEGER
pub(crate) fn parse_set_pop_args<'a>(
  parse_state: &'a [&'a [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'a [u8], i32)> {
  check_arg_count!(parse_state, 1..=2, output, "SPOP", return None);
  let count = match parse_state.get(1) {
    None => NO_COUNT,
    // C#：非整数（含溢出）或负数 → VALUE_IS_NOT_INTEGER
    Some(raw) => match strict_i32(raw) {
      Some(c) if c >= 0 => c,
      _ => {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return None;
      }
    },
  };
  Some((parse_state[0], count))
}

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
/// 同步装载集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn set_load_sync(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> SetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::Set,
    output,
    SetObject::from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与命令域收尾）
#[inline]
pub(crate) fn set_save_or_gc(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  obj: &SetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::Set,
    obj,
    obj.set.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）无状态变更，不落库（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的新增/删除类操作（SADD/SREM）以变更计数为准：SADD 全命中
///   已有成员（added == 0）零变异不落库不广播，杜绝全量重序列化写放大与 AOF
///   增量污染。
fn should_write_back(
  op: SetOperation,
  out: &ObjectOutput<'_>,
  obj: &SetObject,
  existed: bool,
) -> bool {
  if is_read_only(op)
    || out.payload_view().first() == Some(&b'-')
    || (!existed && obj.set.is_empty())
  {
    return false;
  }
  match op {
    SetOperation::Sadd | SetOperation::Srem => out.result1 > 0,
    _ => true,
  }
}

/// 只读操作（rmw 不落库）
fn is_read_only(op: SetOperation) -> bool {
  matches!(
    op,
    SetOperation::Scard
      | SetOperation::Smembers
      | SetOperation::Sismember
      | SetOperation::Smismember
      | SetOperation::Srandmember
      | SetOperation::Sscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  fn set_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl Device>,
    key: &[u8],
    op: SetOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    output: &mut Vec<u8>,
  ) -> Rmw {
    let (arg1, arg2) = args12;
    let resp_version = self.resp_protocol_version;
    run_sync_rmw(
      store,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::Set,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        SetObject::from_blob,
        SetObject::new,
        |o: &SetObject| o.set.is_empty(),
        |o: &SetObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}

/// 结果集合的 RESP 输出（集合头版本分派：RESP2 *N / RESP3 ~N）
///
/// 借用裸集逐成员直写 bulk（对位 C# SetCommands.cs:252-258 foreach 借用枚举
/// TryWriteBulkString 直写），零成员克隆中转数组（票 zcode-r137c-setstore2 案二）
///
/// 写出 set 成员列表（内部调用 cs::write_set_len 单点）
fn write_set_members(result: &HashSet<Vec<u8>>, output: &mut Vec<u8>, resp_version: u8) {
  cs::write_set_len(output, result.len(), resp_version);
  for member in result {
    output.write_resp_bulk_string(member);
  }
}

// operate 通道执行单源已收敛至 rmw_helpers（原本地副本删除）
pub(crate) use crate::resp::objects::object_store_utils::run_operate;

#[cfg(test)]
mod write_set_members_tests {
  use std::str::from_utf8;

  use wbase::map::HashSet;
  use wresp::{cmd_strings as cs, ext::RespVecExt};

  use super::write_set_members;

  /// 分配探针（本测试二进制内的手工计数包装分配器，原样转调 System，
  /// 仅原子自增；先例形制同 wcol list_object_impl probe 与
  /// tests/tiered_output_frame_head.rs backfill_shape_drops_body_scale_scratch——
  /// 计数窗口重复多次比最小值，结论不受同进程并行分配噪声影响）
  mod probe {
    use std::{
      alloc::{GlobalAlloc, Layout, System},
      sync::atomic::{AtomicUsize, Ordering},
    };

    static ALLOCS: AtomicUsize = AtomicUsize::new(0);

    struct Counting;

    // SAFETY: alloc/dealloc 原样转调 System，计数仅为无副作用的原子自增；
    // realloc/alloc_zeroed 走 trait 默认实现（即本 alloc/dealloc 组合）
    unsafe impl GlobalAlloc for Counting {
      unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
      }

      unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
      }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// 至今累计的分配次数
    pub(super) fn allocs() -> usize {
      ALLOCS.load(Ordering::Relaxed)
    }
  }

  /// 应答头行（首个 CRLF 之前，不含 CRLF）
  fn head(out: &[u8]) -> &str {
    let end = out.windows(2).position(|w| w == b"\r\n").unwrap();
    from_utf8(&out[..end]).unwrap()
  }

  /// 抽出帧头行之外的全部 bulk 实体（成员序非契约，按集合等价比对，
  /// 口径见 doc/zh/deviations.md「集合族双态应答成员序非契约」条目）
  fn bulks(out: &[u8]) -> Vec<Vec<u8>> {
    let mut items = Vec::new();
    let mut pos = out.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    while pos < out.len() {
      assert_eq!(out[pos], b'$', "实体不是 bulk: {:?}", &out[pos..]);
      let line_end = pos + out[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
      let len: usize = String::from_utf8_lossy(&out[pos + 1..line_end])
        .parse()
        .unwrap();
      let body = line_end + 2;
      items.push(out[body..body + len].to_vec());
      pos = body + len + 2;
    }
    items
  }

  /// 多成员集：SINTER/SUNION/SDIFF 结果集合头版本分派（RESP2 *N / RESP3 ~N），
  /// 成员按集合等价断言（帧头与成员集合为契约、序非契约）
  #[test]
  fn set_head_resp2_array_resp3_set() {
    let members: HashSet<Vec<u8>> = [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]
      .into_iter()
      .collect();

    let mut out2 = Vec::new();
    write_set_members(&members, &mut out2, 2);
    assert_eq!(head(&out2), "*3");
    let mut got = bulks(&out2);
    got.sort();
    assert_eq!(got, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);

    let mut out3 = Vec::new();
    write_set_members(&members, &mut out3, 3);
    assert_eq!(head(&out3), "~3");
    let mut got = bulks(&out3);
    got.sort();
    assert_eq!(got, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);
  }

  /// 空集位点（SMEMBERS 缺键 / SPOP count==0 / SPOP 缺键带 count）
  /// 对位 C# RespServerSessionOutput.cs:100 WriteEmptySet
  #[test]
  fn empty_set_resp2_star_resp3_tilde() {
    let members = HashSet::default();

    let mut out2 = Vec::new();
    write_set_members(&members, &mut out2, 2);
    assert_eq!(out2, b"*0\r\n");

    let mut out3 = Vec::new();
    write_set_members(&members, &mut out3, 3);
    assert_eq!(out3, b"~0\r\n");
  }

  /// 分配证据（票 zcode-r137c-setstore2 案二·读臂出帧零中转）：改前形态
  /// （to_members 逐成员 clone 成 `Vec<Vec<u8>>` 中转数组后出帧，已随臂清退，
  /// 此处按原实现同形复刻）vs 现生产借用迭代直写形态，中转数组本体
  /// `n` 次成员克隆 + 1 次数组体分配在改后臂恒不发生
  #[test]
  fn borrowed_frame_drops_member_scratch() {
    const N: usize = 2000;
    let members: HashSet<Vec<u8>> = (0..N)
      .map(|i| format!("member-{i:05}").into_bytes())
      .collect();
    // 出帧缓冲预留足量容量，隔离应答缓冲自身扩容，使计数只反映中转形态差异
    let frame_budget = members.iter().map(|m| m.len() + 16).sum::<usize>() + 64;

    let (mut min_old, mut min_new) = (usize::MAX, usize::MAX);
    for _ in 0..20 {
      let mut out = Vec::with_capacity(frame_budget);
      let before = probe::allocs();
      {
        // 改前形态同形复刻：全量 clone 中转数组后逐条出帧
        let scratch: Vec<Vec<u8>> = members.iter().cloned().collect();
        cs::write_set_len(&mut out, scratch.len(), 2);
        for member in &scratch {
          out.write_resp_bulk_string(member);
        }
      }
      min_old = min_old.min(probe::allocs() - before);

      let mut out = Vec::with_capacity(frame_budget);
      let before = probe::allocs();
      write_set_members(&members, &mut out, 2);
      min_new = min_new.min(probe::allocs() - before);
    }

    // 改前窗口每窗至少实付 N 次成员克隆 + 1 次数组体（自形保证，噪声只增不减）
    assert!(min_old > N, "改前形态窗口未实付中转克隆: {min_old}");
    assert!(
      min_old > min_new,
      "借出帧形态分配未降: 改前 {min_old} 次 vs 改后 {min_new} 次"
    );
  }
}

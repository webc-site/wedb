//! 向量存储回调同键读改写并发回归（对标 Tsavorite InternalRMW 桶独占锁窗口）
//!
//! 缺陷背景（票 wnode-vector-store-callbacks-rmw-non-atomic-lost-update）：C#
//! ReadModifyWriteCallbackUnmanaged 把 RMW 委托 Tsavorite ctx.RMW，在记录所在
//! 哈希桶的独占瞬时锁内完成「回溯读旧值 → 算新值 → 写回」全程
//!（libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:
//! FindOrCreateTagAndTryEphemeralXLock），而 VectorSetAdd 外层仅持共享锁放行
//! 多线程并发 VADD。收口前 rust `WedbVectorStoreCallbacks::rmw` 为无锁两步
//!（read_outcome → f → write），同键并发按构造即丢更新：
//!   * 图邻接边丢失——`wvector/provider/cache.rs:set_neighbors` / `append_vector`
//!     经 `rmw_iid` 改写邻接表，后写者以旧基值整写覆盖先写者的边；
//!   * FSM 位图撕裂——`wvector/fsm.rs:mark_id_unchecked` 经 `rmw_wid` 改写
//!     空闲位图，同一内部 ID 被重复分配，全精度向量互相覆盖。
//!
//! 三条用例均直调生产 `WedbVectorStoreCallbacks`（真实 wkv 存储，无 mock），
//! 收口前按构造即失败，收口后全绿：
//!   * 前两条打回调层：同键并发 rmw 追加（邻接表同型布局）、rmw 与 direct
//!     write 交叠（写回不得以旧基值抹掉已落盘写入）；
//!   * 第三条打全链：多线程并发 insert 同一 Vector Set，断言元素计数、外部
//!     id 映射完整（FSM 无重复分配）、逐向量自召回全命中，并经 drop/recreate
//!     恢复路径复核存储侧 FSM 与映射一致性。
//!
//! 与「向量会话执行域私有化」（票 wepoch-participant-sync-bound-and-vector-
//! session-domain-privatization）的交叉：回调本体不持会话，各执行域（本测试即
//! 各 spawn 线程）经 [`OwnedActiveVectorSession`] 各自绑定专用会话；条带锁挂在
//! 跨执行域共享的同一份回调句柄上，故本测试同时自证「会话私有化不削弱同键
//! 原子窗口」——丢条带锁则写丢失、丢私有化则编译不过或回调臂缺绑报错。

use std::{sync::Arc, thread};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::vector::vector_store_callbacks::{
  OwnedActiveVectorSession, WedbVectorStoreCallbacks,
};
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams,
  VectorDistanceMetricType, VectorQuantType,
  store::{LengthPrefixedIter, StoreCallbacks},
};

/// 邻接表同型值的槽位数（末槽为长度，同 `cache.rs:append_vector` 布局）
const SLOTS: usize = 1025;
/// 值字节数
const VALUE_LEN: usize = SLOTS * 4;
/// 邻居容量（去掉末槽长度位）
const CAP: usize = SLOTS - 1;
/// 测试上下文号（物理键域，任意定值）
const CONTEXT: u64 = 8;
/// 测试键（同键并发争用点）
const KEY: &[u8] = b"adj:shared";
/// 全链并发线程数
const INSERT_THREADS: usize = 4;
/// 全链并发每线程插入数
const PER_THREAD: usize = 25;

/// 生产回调装配下的向量服务类型
type VecService = DiskANNService<WedbVectorStoreCallbacks<SegmentedDevice>>;

/// 生产存储 + 无会话的共享回调句柄（会话由各执行域自行绑定）
fn open_callbacks(
  name: &str,
) -> (
  Arc<WedbVectorStoreCallbacks<SegmentedDevice>>,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name)).unwrap());
  let store =
    Arc::new(WedbStore::open(StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap(), device).unwrap());
  let callbacks = Arc::new(WedbVectorStoreCallbacks::new());
  (callbacks, store, dir)
}

/// 末槽长度位读取
fn stored_len(value: &[u8]) -> usize {
  u32::from_le_bytes(value[CAP * 4..CAP * 4 + 4].try_into().unwrap()) as usize
}

/// 第 j 槽的 u32 读取
fn word(value: &[u8], j: usize) -> u32 {
  u32::from_le_bytes(value[j * 4..j * 4 + 4].try_into().unwrap())
}

/// 邻接表同型追加内核（同 `cache.rs:append_vector`：去重、满员即弃、末槽记账）
fn append_word(data: &mut [u8], mark: u32) {
  let mark_bytes = mark.to_le_bytes();
  let mut len = stored_len(data);
  if (0..len).any(|j| data[j * 4..j * 4 + 4] == mark_bytes[..]) {
    return;
  }
  if len < CAP {
    data[len * 4..len * 4 + 4].copy_from_slice(&mark_bytes);
    len += 1;
    data[CAP * 4..CAP * 4 + 4].copy_from_slice(&(len as u32).to_le_bytes());
  }
}

/// 末态读值（单线程断言面；冷读收割需当前线程 compio runtime，读臂需本域绑会话）
fn final_value(
  callbacks: &Arc<WedbVectorStoreCallbacks<SegmentedDevice>>,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> Vec<u8> {
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  Runtime::new().unwrap().block_on(async {
    let mut out = Vec::new();
    assert!(
      callbacks.read(CONTEXT, KEY, |v| out = v.to_vec()).await,
      "终态读须命中"
    );
    out
  })
}

/// 同键并发 rmw 追加（图邻接边写入同型）：终态必须等于全部已回执标记的并集
///
/// 收口前两线程同读同一旧基值各自整写写回，后写者抹掉先写者的边（Lost
/// Update）；收口后读—算—写在条带锁窗口内串行，一条不丢。
#[test]
fn concurrent_rmw_appends_lose_no_neighbor() {
  const THREADS: usize = 4;
  const ITERS: usize = 200;
  let (callbacks, store, _dir) = open_callbacks("vec-rmw-append.db");

  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let callbacks = Arc::clone(&callbacks);
      let store = Arc::clone(&store);
      thread::spawn(move || {
        // 每线程独立标记值域（t*100000 起步，互不重叠，去重后各留一条）
        let mark = (t as u32) * 100_000 + 1;
        // 每执行域自持专用会话（会话私有化纪律），条带锁在共享回调句柄上互斥
        let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
        Runtime::new().unwrap().block_on(async {
          let mut ok = 0;
          for _ in 0..ITERS {
            if callbacks
              .rmw(CONTEXT, KEY, VALUE_LEN, |data| append_word(data, mark))
              .await
            {
              ok += 1;
            }
          }
          ok
        })
      })
    })
    .collect();
  let applied: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
  assert_eq!(applied, THREADS * ITERS, "全部读改写须回执成功");

  let final_value = final_value(&callbacks, &store);
  assert_eq!(
    stored_len(&final_value),
    THREADS,
    "去重后终态边数必须等于线程数（每线程一个标记）：收口前后写者整写会抹掉先写者的边"
  );
  // 并发落盘顺序不定，按无序集合比对：任一线程的边缺失即丢更新
  let mut marks: Vec<u32> = (0..THREADS).map(|j| word(&final_value, j)).collect();
  marks.sort_unstable();
  let mut expected: Vec<u32> = (0..THREADS).map(|t| (t as u32) * 100_000 + 1).collect();
  expected.sort_unstable();
  assert_eq!(
    marks, expected,
    "并发 rmw 追加丢边：终态标记集 {marks:?} ≠ 全量标记集 {expected:?}"
  );
}

/// 同键并发 rmw 与 direct write 交叠：direct write 不得被读改写写回以旧基值抹掉
///
/// 对标 C# Upsert 与 RMW 同处 Tsavorite 桶独占锁的互斥语义：收口前 rmw 的
/// 「读旧 → 写回」窗口横跨 write 的落盘点，写回以读时旧基值整写覆盖 direct
/// write 的新值；收口后二者在同一把同键条带写锁下串行，write 结果必存活。
#[test]
fn concurrent_rmw_and_write_never_overwrite_each_other() {
  const WRITER_THREADS: usize = 2;
  const WRITE_THREADS: usize = 2;
  const ITERS: usize = 400;
  let (callbacks, store, _dir) = open_callbacks("vec-rmw-vs-write.db");
  // 主线程断言臂（基值写入）亦按纪律先绑定本执行域会话
  let _main_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());

  // 基值：邻接表 {0}（len=1）
  {
    let callbacks = Arc::clone(&callbacks);
    Runtime::new().unwrap().block_on(async {
      let mut base = vec![0u8; VALUE_LEN];
      append_word(&mut base, 0);
      assert!(callbacks.write(CONTEXT, KEY, &base).await, "基值写入须成功");
    });
  }

  let mark_a = 0xAAAA_0001_u32;
  let mark_b = 0xBBBB_0002_u32;

  let handles: Vec<_> = (0..WRITER_THREADS + WRITE_THREADS)
    .map(|t| {
      let callbacks = Arc::clone(&callbacks);
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
        Runtime::new().unwrap().block_on(async {
          if t < WRITER_THREADS {
            // 读改写臂：追加标记 A（去重后至多一条）
            for _ in 0..ITERS {
              assert!(
                callbacks
                  .rmw(CONTEXT, KEY, VALUE_LEN, |data| append_word(data, mark_a))
                  .await
              );
            }
          } else {
            // direct write 臂：整值写 {0, B}（槽 0 保基值 0、槽 1 落 B，
            // B 的落盘不得被读改写写回抹回）
            let mut value = vec![0u8; VALUE_LEN];
            value[4..8].copy_from_slice(&mark_b.to_le_bytes());
            value[CAP * 4..CAP * 4 + 4].copy_from_slice(&2u32.to_le_bytes());
            for _ in 0..ITERS {
              assert!(callbacks.write(CONTEXT, KEY, &value).await);
            }
          }
        })
      })
    })
    .collect();
  handles.into_iter().for_each(|h| h.join().unwrap());

  let final_value = final_value(&callbacks, &store);
  let len = stored_len(&final_value);
  assert!(
    len == 2 || len == 3,
    "终态长度须为合法串行序产物（{{0,A}} 或 {{0,B}} 或 {{0,A,B}}），实际 len={len}"
  );
  let words: Vec<u32> = (0..len).map(|j| word(&final_value, j)).collect();
  assert_eq!(words[0], 0, "基值槽 0 不得被改写");
  assert!(
    words.contains(&mark_b),
    "direct write 的标记 B 在终态中丢失：读改写写回以旧基值覆盖了已落盘写入（words={words:?}）"
  );
  assert!(
    words.iter().filter(|&&w| w == mark_a).count() <= 1,
    "标记 A 去重语义被破坏（words={words:?}）"
  );
}

/// 逐向量自召回全链复核：图遍历（邻接表读链）+ 全精度读 + 映射回溯
fn recall_all(service: &VecService) {
  // search 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  //（thread-per-core 每次阻塞调用解析本线程 runtime），故整体置于上下文内
  Runtime::new().unwrap().block_on(async {
    for t in 0..INSERT_THREADS {
      for i in 0..PER_THREAD {
        let eid = format!("t{t}i{i:03}");
        let query: Vec<u8> = [t as f32 + 1.0, i as f32 + 1.0]
          .iter()
          .flat_map(|v| v.to_le_bytes())
          .collect();
        let out = service
          .search_vector(
            CONTEXT,
            &query,
            SearchParams {
              count: 1,
              search_exploration_factor: 64,
              filter_len: 0,
              max_filtering_effort: 0,
            },
          )
          .await
          .unwrap();
        assert!(out.found >= 1, "自召回 {eid} 不得为空");
        let first = LengthPrefixedIter::new(&out.ids).next().unwrap();
        assert_eq!(
          first,
          eid.as_bytes(),
          "自召回 {eid} 首位须为自身：ID 重复分配或全精度数据被覆盖即错位"
        );
      }
    }
  })
}

/// 全链并发 insert 同一 Vector Set：计数、映射、自召回与恢复一致性
///
/// 对标 C# VectorSetAdd 外层共享锁放行的并发插入实况：FSM 位图经 `rmw_wid`
/// 标记、邻接表经 `rmw_iid` 追加，同键 RMW 非原子时内部 ID 重复分配、全精度
/// 向量互相覆盖、图遍历断链。断言全部元素 `card` 一致、外部 id 映射完整、
/// 逐向量以自身查询零距离召回自身，且 drop/recreate 恢复后同判据成立。
#[test]
fn concurrent_insert_keeps_count_mapping_and_recall() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vec-insert.db")).unwrap());
  let store =
    Arc::new(WedbStore::open(StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap(), device).unwrap());
  let session = store.new_session().unwrap();
  // 主线程装配/断言臂（create_index、recall、恢复复核）绑定专用会话直至测试结束
  let _main_domain = OwnedActiveVectorSession::new(session);
  let service = Arc::new(VecService::default());
  let config = IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  };
  let total = (INSERT_THREADS * PER_THREAD) as u64;

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    service
      .create_index(
        CONTEXT,
        config,
        Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
      )
      .await
      .unwrap();
  });

  // 线程 t 元素 i：外部 id "t{i}"、向量 (t+1, i+1)（两两互异，L2 自身距离恒 0）
  let handles: Vec<_> = (0..INSERT_THREADS)
    .map(|t| {
      let service = Arc::clone(&service);
      let store = Arc::clone(&store);
      thread::spawn(move || {
        // 每插入线程 = 一个执行域，自持绑定专用会话
        let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
        Runtime::new().unwrap().block_on(async {
          for i in 0..PER_THREAD {
            let eid = format!("t{t}i{i:03}");
            let vec: Vec<u8> = [t as f32 + 1.0, i as f32 + 1.0]
              .iter()
              .flat_map(|v| v.to_le_bytes())
              .collect();
            assert_eq!(
              service.insert(CONTEXT, eid.as_bytes(), &vec, b"").await,
              DiskAnnInsertResult::True,
              "并发插入 {eid} 须成功（False = FSM/映射态已损坏）"
            );
          }
        })
      })
    })
    .collect();
  handles.into_iter().for_each(|h| h.join().unwrap());

  assert_eq!(
    service.card(CONTEXT),
    total,
    "元素计数须等于插入总数：FSM 位图撕裂即重复分配、计数漂移"
  );
  recall_all(&service);

  // 恢复一致性：丢弃内存索引后同 context 重建（provider 自存储恢复 FSM 与
  // 起点映射），存储侧位图/映射若被并发写坏，此处计数与召回即崩
  service.drop_index(CONTEXT);
  rt.block_on(async {
    service
      .create_index(
        CONTEXT,
        config,
        Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
      )
      .await
      .unwrap();
    assert_eq!(
      service.card(CONTEXT),
      total,
      "恢复路径元素计数须与插入总数一致"
    );
    for t in 0..INSERT_THREADS {
      for i in 0..PER_THREAD {
        assert!(
          service
            .check_external_id_valid(CONTEXT, format!("t{t}i{i:03}").as_bytes())
            .await,
          "恢复路径外部 id 映射须完整"
        );
      }
    }
  });
  recall_all(&service);
}

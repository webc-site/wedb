//! 逐条提交的增量前缀刷盘回归（对标 C# AllocatorBase.cs:WriteInlinePageAsync
//! :629-636「Write only required bytes within the page」+ :2263-2276 partial 页
//! fromAddress/untilAddress 收口）
//!
//! 锁定三件事：
//! 1. 每笔提交的设备写入量 = 本次新增已封印字节（扇区圆整），**不随页容量放大**——
//!    16MB 生产页下逐条小记录提交曾退化为每笔整页重写（1000 笔单写 = 16GiB 写放大，
//!    吞吐被设备写带宽独占，实测 16.7 txn/s）；
//! 2. 前缀形态下设备文件在页中结束，恢复端仍逐字节读回全部记录（页尾未落盘字节按零
//!    承接，与内存页尾恒零的形态等价）；
//! 3. 驱逐后的冷读与全量扫描不因「整页请求越过文件末端」短读报错。

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  AddressSnapshot, DEFAULT_INITIAL_ADDRESS, DEFAULT_SERVER_PAGE_SIZE, HybridLog, HybridLogConfig,
};

/// 16MB 生产页 + 4 页环形缓冲（与 large_page 同档：页幅远大于单条记录，写放大差异可读）
fn big_page_config() -> HybridLogConfig {
  HybridLogConfig::new(DEFAULT_SERVER_PAGE_SIZE, 4, 0.5).expect("大页配置合法")
}

/// 单条记录的键与值（尺寸取 bench 标准档量级：24B 键 + 150B 值）
fn record_bytes(i: u64) -> (Vec<u8>, Vec<u8>) {
  (
    format!("key:{i:016}").into_bytes(),
    vec![(i % 251) as u8; 150],
  )
}

/// 段 0 当前物理长度（设备落盘足迹的直接读数）
fn file_len(device: &SegmentedDevice) -> u64 {
  device.get_file_size(0).expect("段 0 尺寸可读")
}

/// 逐条提交的设备写入量只覆盖新增封印区间，绝不随页容量放大
#[compio::test]
async fn per_commit_flush_writes_only_new_sealed_bytes() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("per_commit.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(big_page_config(), Arc::clone(&device), Arc::clone(&epoch))?;

  let page_size = DEFAULT_SERVER_PAGE_SIZE as u64;
  let sector = device.sector_size() as u64;

  // 第一笔：24B 键 + 150B 值，封印后落盘
  let (key, value) = record_bytes(0);
  let (addr0, _) = hlog.append(&key, &value, 0, false)?;
  hlog.flush_all().await?;
  hlog.sync().await?;
  let sealed_1 = hlog.flushed_until_address();
  assert!(sealed_1 > addr0, "封印上界必须越过已落盘记录");
  let len_1 = file_len(&device);
  // 旧形态此处为整页 16MiB；前缀形态只允许「本条记录字节 + 扇区交界」
  assert!(
    len_1 * 8 < page_size,
    "单笔提交的设备写入量不得接近页幅: 已写 {len_1} 字节 / 页容量 {page_size}"
  );

  // 第二笔同页续写：增量必须仍是新增封印字节的量级（旧形态会把整页再重写一遍）
  let (key2, value2) = record_bytes(1);
  let (addr1, _) = hlog.append(&key2, &value2, 0, false)?;
  hlog.flush_all().await?;
  hlog.sync().await?;
  let sealed_2 = hlog.flushed_until_address();
  let len_2 = file_len(&device);
  let grew = sealed_2 - sealed_1;
  let written = len_2.saturating_sub(len_1);
  assert!(
    written <= grew + 2 * sector,
    "第二次提交只应写新增封印区间（含扇区交界重叠）: 实写 {written} 字节 / 新增 {grew} 字节"
  );
  assert!(
    len_2 * 8 < page_size,
    "逐条提交的设备足迹不得随提交次数放大到页幅: {len_2} 字节"
  );

  // 前缀记账与设备足迹同序：设备上必有 [起点, flushed) 的全部字节
  assert!(
    len_2 >= sealed_2 - DEFAULT_INITIAL_ADDRESS,
    "持久化前缀必须全部在设备上: 文件 {len_2} < 前缀 {}",
    sealed_2 - DEFAULT_INITIAL_ADDRESS
  );
  // 提交后内存直读不受影响（两笔均仍驻留页内）
  let out = hlog.read_record(addr1).await?;
  assert_eq!(out.value()?, value2);
  assert_eq!(out.key()?, key2);
  let out0 = hlog.read_record(addr0).await?;
  assert_eq!(out0.value()?, value);

  OK
}

/// 逐条提交后恢复：页尾从未落盘，恢复装载与驱逐冷读、全量扫描全部逐字节一致
#[compio::test]
async fn per_commit_flush_survives_recover_and_cold_read() -> Void {
  const N: u64 = 200;
  let dir = tempdir()?;
  let db_path = dir.path().join("per_commit_recover.db");
  let config = big_page_config();

  let mut addrs = Vec::new();
  let mut keys = Vec::new();
  let mut values = Vec::new();
  let tail_final;
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

  // 阶段 1：bench individual writes 的提交形状——每条记录一次 flush_all
  {
    let epoch = Arc::new(LightEpoch::new(16));
    let hlog = HybridLog::new(config.clone(), Arc::clone(&device), epoch)?;
    for i in 0..N {
      let (key, value) = record_bytes(i);
      let (addr, _) = hlog.append(&key, &value, 0, false)?;
      addrs.push(addr);
      keys.push(key);
      values.push(value);
      hlog.flush_all().await?;
    }
    hlog.sync().await?;
    tail_final = hlog.tail_address();
    assert_eq!(
      hlog.flushed_until_address(),
      tail_final,
      "全部提交完成后前缀必须追平 tail"
    );
    // 200 笔逐条提交的设备足迹仍远小于单页（旧形态第一笔即写满 16MiB）
    let len = file_len(&device);
    assert!(
      len * 8 < DEFAULT_SERVER_PAGE_SIZE as u64,
      "200 笔提交的设备足迹应等于记录总量: {len} 字节"
    );
  }

  // 阶段 2：从快照恢复，前缀以下每条记录逐字节读回
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let epoch = Arc::new(LightEpoch::new(16));
  let snapshot = AddressSnapshot::from_bounds(
    DEFAULT_INITIAL_ADDRESS,
    DEFAULT_INITIAL_ADDRESS,
    tail_final,
    tail_final,
    tail_final,
  );
  let hlog = HybridLog::recover(config, Arc::clone(&device), epoch, snapshot).await?;
  assert_eq!(hlog.tail_address(), tail_final);

  // 阶段 3：驱逐全部记录（head 越过 tail），迫使每条走设备冷读——
  // 页尾在设备上不存在，按整页请求必然短读（读侧钳制到前缀的回归点）
  hlog.shift_read_only_address(tail_final);
  hlog.shift_head_address(tail_final);
  for i in 0..N {
    let out = hlog.read_record(addrs[i as usize]).await?;
    assert_eq!(out.key()?, keys[i as usize], "第 {i} 条键值必须逐字节读回");
    assert_eq!(
      out.value()?,
      values[i as usize],
      "第 {i} 条值必须逐字节读回"
    );
  }

  // 阶段 4：全量扫描同样走冷读页缓存，条数与内容不得被页尾短读截断
  let mut seen = 0u64;
  hlog
    .scan(DEFAULT_INITIAL_ADDRESS, tail_final, |addr, rec| {
      assert_eq!(addr, addrs[seen as usize], "扫描顺序必须与追加顺序同址");
      assert_eq!(rec.value(), values[seen as usize]);
      seen += 1;
      Ok(true)
    })
    .await?;
  assert_eq!(seen, N, "扫描必须产出全部已落盘记录");

  OK
}

/// 跨页边界的逐条提交：Pad 尾页与次页前缀同轮落盘，恢复与冷读一致
///
/// 页容量取 64KB（少量记录即可越页），锁定「写区间落在两个页上」时填充闭包的
/// 逐页字节交叠拷贝正确（首尾两页都是部分页，中段无整页）。
#[compio::test]
async fn per_commit_flush_across_page_boundary() -> Void {
  const N: u64 = 600;
  let dir = tempdir()?;
  let db_path = dir.path().join("per_commit_cross_page.db");
  let config = HybridLogConfig::new(64 * 1024, 8, 0.5).expect("64KB 页配置合法");

  let mut addrs = Vec::new();
  let mut values = Vec::new();
  let tail_final;
  {
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let hlog = HybridLog::new(config.clone(), Arc::clone(&device), epoch)?;
    for i in 0..N {
      let (key, value) = record_bytes(i);
      let (addr, _) = hlog.append(&key, &value, 0, false)?;
      addrs.push(addr);
      values.push(value);
      hlog.flush_all().await?;
    }
    hlog.sync().await?;
    tail_final = hlog.tail_address();
    assert_eq!(hlog.flushed_until_address(), tail_final);
    assert!(
      hlog.config.page_id(tail_final - 1) >= 1,
      "负载必须跨入次页，否则本用例未覆盖部分页交界"
    );
    // 设备足迹 = 记录总量（+扇区交界），绝不含跨页之间的重复整页重写
    // （设备偏移即逻辑地址，故上界取 tail 加交界余量）
    let len = file_len(&device);
    assert!(
      len <= tail_final + 4096,
      "设备足迹不应超过持久化前缀加扇区交界: 文件 {len} / tail {tail_final}"
    );
  }

  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let epoch = Arc::new(LightEpoch::new(16));
  let snapshot = AddressSnapshot::from_bounds(
    DEFAULT_INITIAL_ADDRESS,
    DEFAULT_INITIAL_ADDRESS,
    tail_final,
    tail_final,
    tail_final,
  );
  let hlog = HybridLog::recover(config, device, epoch, snapshot).await?;
  hlog.shift_read_only_address(tail_final);
  hlog.shift_head_address(tail_final);

  // 跨页后首页首条、次页邻界条、末页尾条都要能冷读（Pad 尾页与部分页前缀均在设备上）
  for [i, j] in [[0usize, 1usize], [300, 301], [598, 599]] {
    let a = hlog.read_record(addrs[i]).await?;
    assert_eq!(a.value()?, values[i], "第 {i} 条跨页负载值必须一致");
    let b = hlog.read_record(addrs[j]).await?;
    assert_eq!(b.value()?, values[j], "第 {j} 条跨页负载值必须一致");
  }

  let mut seen = 0u64;
  hlog
    .scan(DEFAULT_INITIAL_ADDRESS, tail_final, |_addr, rec| {
      assert_eq!(
        rec.value(),
        values[seen as usize],
        "第 {seen} 条扫描值不一致"
      );
      seen += 1;
      Ok(true)
    })
    .await?;
  assert_eq!(seen, N, "跨页负载的全量扫描必须一条不缺");

  OK
}

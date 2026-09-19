//! 索引快照落盘与还原（对标 C# Tsavorite 索引检查点）
//!
//! 本件只持全量快照写入入口 [`write_index_checkpoint`] 及其主流程，对外成员集合经
//! `pub use` 保持不变；其余按域分件：[`codec`] 64 字节头部编解码与槽位净化、
//! [`batch`] 对齐批缓冲 DirectIO 写出/读入状态机、[`read`] 截断恢复读入口。

mod batch;
mod codec;
mod read;

use std::{fs::remove_file, path::Path};

use compio::{
  fs::{File, rename},
  io::AsyncWriteAtExt,
};
use log::debug;
use wbase::crc::Crc32Hasher;
use windex::HashIndex;

use super::{
  error::Result,
  meta::{IndexMeta, index_filename, index_tmp_filename},
};
pub use crate::index_ckpt::read::read_index_checkpoint_truncated;
use crate::index_ckpt::{
  batch::BatchWriter,
  codec::{HEADER_CRC_OFFSET, HEADER_SIZE, INDEX_VERSION, IndexCkptHeader},
};

/// 异步将 HashIndex 的全部 Buckets 与 OverflowPool 状态原子写入持久化快照文件
///
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:TakeIndexCheckpointAsync
///
/// 纯 compio 异步定位 I/O（`write_all_at` + `sync_all`），无线程创建：io_uring 下真正的
/// 磁盘 I/O 由内核完成，reactor 线程仅提交与收割完成事件，序列化与 CRC32 累积这类
/// 微秒级纯计算留在 reactor 线程正是 thread-per-core 模型的预期行为。
///
/// 写入流程：
/// 1. 写入带有 Magic、Version、Token 及元数据的 64 字节文件头。
/// 2. 逐一写入主哈希表中的所有 64 字节桶数据（清除槽位 7 中的并发自旋锁，剥离未提交
///    试探性标记；指向易失 ReadCache 的条目先经 `rc_skip` 顺链解析为主日志真实地址）。
/// 3. 逐一写入溢出内存池中已分配的所有 64 字节桶数据（空洞以全零桶补齐）。
/// 4. 计算全量桶数据 CRC32 校验和并回写至头部，保证数据完整性校验。
/// 5. 刷盘并执行文件重命名，保证快照写入的完全原子性（避免中途断电留下半截文件）。
///
/// `rc_skip` 对标 C# Tsavorite SkipReadCacheBucket 委托：无 ReadCache 时传恒等闭包
/// `&|addr| addr` 即可（对无 ReadCache 位槽位不产生任何调用开销）。
///
/// 失败清场：任一 I/O 阶段失败（如磁盘满 ENOSPC）时就地回收半截 `.tmp` 残留，
/// 不遗留垃圾文件（正常路径的原子性由「tmp 写 + fsync + rename」保证）。
pub async fn write_index_checkpoint<F: Fn(u64) -> u64>(
  index: &HashIndex,
  entry_count: usize,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  rc_skip: &F,
) -> Result<IndexMeta> {
  let dir = checkpoint_dir.as_ref();
  let res = write_index_checkpoint_inner(index, entry_count, dir, token, rc_skip).await;
  if res.is_err() {
    let _ = remove_file(dir.join(index_tmp_filename(token)));
  }
  res
}

/// 快照写入主流程（见 [`write_index_checkpoint`]）
async fn write_index_checkpoint_inner<F: Fn(u64) -> u64>(
  index: &HashIndex,
  entry_count: usize,
  dir: &Path,
  token: u128,
  rc_skip: &F,
) -> Result<IndexMeta> {
  let final_path = dir.join(index_filename(token));
  let tmp_path = dir.join(index_tmp_filename(token));

  let num_buckets = index.size as u64;
  let overflow_count = index.overflow_pool.allocated_count();

  let mut file = File::create(&tmp_path).await?;

  // 1. 构建并写入 64 字节头部（CRC 字段先占位，写入数据后回写）
  let ckpt_hdr = IndexCkptHeader {
    version: INDEX_VERSION,
    crc: 0,
    token,
    num_buckets,
    overflow_count,
    entry_count: entry_count as u64,
  };
  file.write_all_at(ckpt_hdr.encode(), 0).await.0?;

  let mut hasher = Crc32Hasher::new();
  {
    let mut batch_writer = BatchWriter::new(&mut file, &mut hasher, HEADER_SIZE as u64, rc_skip);

    // 2. 写入主哈希表桶数据
    for bucket in index.buckets.iter() {
      batch_writer.write_bucket(bucket).await?;
    }

    // 3. 写入溢出内存池桶数据（缺失的分配空洞以全零桶补齐，保证与头部声明的数量严格对齐）
    for id in 1..=overflow_count {
      if let Some(bucket) = index.overflow_pool.get(id) {
        batch_writer.write_bucket(bucket).await?;
      } else {
        batch_writer.write_zero_bucket().await?;
      }
    }

    batch_writer.finish().await?;
  }

  // 4. 计算 CRC32 校验和并回写至头部第 12..16 字节
  let crc = hasher.finalize();
  file
    .write_all_at(crc.to_le_bytes(), HEADER_CRC_OFFSET)
    .await
    .0?;
  file.sync_all().await?;

  rename(&tmp_path, &final_path).await?;
  // fsync 父目录：保证 rename 目录项掉电持久，避免恢复时出现 meta 在而 index 丢失的残缺视图
  super::manager::sync_checkpoint_dir(dir)?;

  debug!(
    "成功刷写 Index Checkpoint: token={token:#x}, num_buckets={num_buckets}, overflow_count={overflow_count}, entry_count={entry_count}, crc={crc:#x}"
  );

  Ok(IndexMeta {
    size: index.size,
    overflow_count,
    entry_count,
  })
}

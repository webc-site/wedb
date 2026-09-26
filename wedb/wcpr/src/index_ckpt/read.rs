//! 截断恢复读入口域：从持久化快照文件流式重建 HashIndex，一步到位熔合 tail 截断
//! 与条目净化（对标 C# Tsavorite FinalizeMainIndexRecovery 的后置清位）。
//!
//! 头部校验与长度口径走格式域 [`super::codec`]，块级读取与流式 CRC32 走批缓冲域
//! [`super::batch`]；全量写入口见 [`super`]。

use std::path::Path;

use compio::{
  buf::BufResult,
  fs::{File, metadata},
  io::AsyncReadAtExt,
};
use itoa::Buffer;
use log::debug;
use wbase::crc::Crc32Hasher;
use windex::HashIndex;

use crate::{
  error::{Error, Result},
  index_ckpt::{
    batch::BatchReader,
    codec::{BUCKET_BYTES, HEADER_SIZE, INDEX_VERSION, IndexCkptHeader},
  },
  meta::IndexMeta,
};

/// 异步从持久化快照文件中反序列化并重建 HashIndex（支持一步到位熔合 tail 截断与条目净化）
///
/// 索引恢复收尾两原语对标（C# 恢复后置的二次遍历在此熔合为读取路径零二次遍历）：
/// - libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexRecovery.cs:FinalizeMainIndexRecovery
/// - libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexRecovery.cs:DeleteTentativeEntries
///
/// 单次流式还原同时完成四件净化（对标 C# Tsavorite FinalizeMainIndexRecovery 后置的
/// DeleteTentativeEntries 与溢出槽位锁复位 `bucket_entries[7] &= kAddressBitMask`，
/// 但熔合为读取路径上的零二次遍历）：试探态清零、ReadCache 易失指针清零、Latch 剥离、
/// 溢出 ID 按头部声明的 overflow_count 上界校验（超界野指针截断归零，恢复端仅按该数
/// 分配溢出桶，杜绝 `OverflowPool::get_unchecked` 解引用空 chunk）。
/// 纯 compio 异步定位读取（`read_exact_at`），反序列化与 CRC 校验为纯计算，留在
/// reactor 线程执行；大索引恢复期间的 I/O 等待由内核异步完成，不阻塞事件循环。
/// C# ClearBitsForDiskImages 还会在恢复时清除记录 SEALED 位，wedb 中 SEALED 仅存活于
/// 内存复活池语义、从不写入日志/索引的持久化镜像，故无需对应清除步骤。
pub async fn read_index_checkpoint_truncated(
  index_path: impl AsRef<Path>,
  expected_token: u128,
  tail: Option<u64>,
) -> Result<(HashIndex, IndexMeta)> {
  let path = index_path.as_ref();
  // 与 std Path::exists 等价的异步存在性探测（stat 元数据 syscall）
  if metadata(path).await.is_err() {
    return Err(Error::IndexCkptNotFound(path.to_path_buf()));
  }

  let mut file = File::open(path).await?;
  let file_len = file.metadata().await?.len();
  if file_len < HEADER_SIZE as u64 {
    return Err(Error::InvalidIndexCkpt("文件长度不足头部大小".into()));
  }

  // 1. 读取并校验头部（完成式 I/O 按值移交缓冲，完成后取回）
  let BufResult(res, header) = file.read_exact_at([0u8; HEADER_SIZE], 0).await;
  res?;

  let Some(ckpt_hdr) = IndexCkptHeader::decode_opt(&header) else {
    return Err(Error::InvalidIndexCkpt(
      "文件魔数不匹配或头部长度不足".into(),
    ));
  };

  if ckpt_hdr.version != INDEX_VERSION {
    let mut s = String::from("不支持的版本号: ");
    let mut buf = Buffer::new();
    s.push_str(buf.format(ckpt_hdr.version));
    return Err(Error::InvalidIndexCkpt(s));
  }

  let expected_crc = ckpt_hdr.crc;
  let token = ckpt_hdr.token;

  if token != expected_token {
    return Err(Error::TokenMismatch {
      expected: expected_token,
      actual: token,
    });
  }

  let num_buckets = ckpt_hdr.num_buckets;
  let overflow_count = ckpt_hdr.overflow_count;
  let entry_count = ckpt_hdr.entry_count as usize;

  let total_buckets = num_buckets
    .checked_add(overflow_count)
    .ok_or_else(|| Error::InvalidIndexCkpt("桶总数算术溢出".into()))?;
  let data_len = total_buckets
    .checked_mul(BUCKET_BYTES as u64)
    .ok_or_else(|| Error::InvalidIndexCkpt("数据长度算术溢出".into()))?;
  let expected_len = (HEADER_SIZE as u64)
    .checked_add(data_len)
    .ok_or_else(|| Error::InvalidIndexCkpt("文件总长度算术溢出".into()))?;
  if file_len != expected_len {
    let mut s = String::from("文件长度异常: 期望 ");
    let mut buf = Buffer::new();
    s.push_str(buf.format(expected_len));
    s.push_str(" 字节，实际 ");
    s.push_str(buf.format(file_len));
    s.push_str(" 字节");
    return Err(Error::InvalidIndexCkpt(s));
  }

  // 2. 初始化 HashIndex 实例与批量读取器
  let new_index = HashIndex::new(num_buckets as usize)?;
  let mut hasher = Crc32Hasher::new();
  {
    let mut batch_reader = BatchReader::new(
      &mut file,
      &mut hasher,
      HEADER_SIZE as u64,
      data_len,
      overflow_count,
    );

    // 3. 读取并恢复主哈希表的所有桶（单次流式完成反序列化、试探态净化与截断）
    for bucket in new_index.buckets.iter() {
      batch_reader.read_bucket_into(bucket, tail).await?;
    }

    // 4. 读取并恢复溢出池中的所有桶
    for j in 1..=overflow_count {
      let id = new_index.overflow_pool.allocate()?;
      if id != j {
        let mut s = String::from("溢出桶分配序号错乱: 期望 ");
        let mut buf = Buffer::new();
        s.push_str(buf.format(j));
        s.push_str("，实际 ");
        s.push_str(buf.format(id));
        return Err(Error::InvalidIndexCkpt(s));
      }
      let overflow_bucket = new_index.overflow_pool.get(id).ok_or_else(|| {
        let mut s = String::from("无法获取分配的溢出桶 ");
        let mut buf = Buffer::new();
        s.push_str(buf.format(id));
        Error::InvalidIndexCkpt(s)
      })?;

      batch_reader.read_bucket_into(overflow_bucket, tail).await?;
    }
  }

  // 5. 校验 CRC32 校验和
  let actual_crc = hasher.finalize();
  if actual_crc != expected_crc {
    return Err(Error::ChecksumMismatch {
      expected: expected_crc,
      actual: actual_crc,
    });
  }

  debug!(
    "成功恢复 Index Checkpoint: token={token:#x}, num_buckets={num_buckets}, overflow_count={overflow_count}, entry_count={entry_count}, crc={actual_crc:#x}"
  );

  Ok((
    new_index,
    IndexMeta {
      size: num_buckets as usize,
      overflow_count,
      entry_count,
    },
  ))
}

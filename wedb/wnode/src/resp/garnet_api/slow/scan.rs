//! 全库扫描族慢路径承接（DBSIZE / KEYS / SCAN / EXPDELSCAN）
//!
//! 同步段仅校验，异步段承载实际扫描；向量登记表域内用户键投影合并
//!（KEYS 全量 / SCAN 首页并页）单源在本文件的 [`StoreGarnetApi::merge_vector_keys`]

use std::io;

use itoa::Buffer;
use wbase::{glob::glob_match_nocase, num::parse_db_index};
use wdev::Device;
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_SLOW_PATH_STORAGE, write_error_raw,
  },
  ext::RespVecExt,
};

use super::StoreGarnetApi;
use crate::{
  resp::RespServerSession,
  storage::session::{
    common::array_key_iteration_functions::try_push_key, storage_session::StorageSession,
  },
};

impl<D: Device> StoreGarnetApi<D> {
  /// 登记表域内用户键投影合并（本库向量键；剥域后与存储键同域直推，天然互斥免判重）
  ///
  /// 页上界单源（票 zcode-r147c-hscanmt 案二收敛口径）：`remaining` 为本页
  /// 剩余额度（SCAN 首页为 `limit` 页上界与存储域已收数之差、KEYS 全量臂为
  /// `usize::MAX`），只补投至满额即止不加码——向量登记表域不经游标递进，
  /// 首页满额后的余项按截断收场，不为续投另立第二套游标位段（跨页续投无
  /// 损形不可达成，禁加码为既定裁决，doc/zh/db.md 剥前缀段在册）。
  ///
  /// 分配收口单源：逐键经 [`try_push_key`] 平滑追加（`try_reserve` 前置，
  /// 失败沿 Err 走两臂既有 `RESP_ERR_SLOW_PATH_STORAGE` 单会话错误帧漏斗，
  /// 与 scan_cursor/db_keys 同纪律，杜绝裸 push 扩容触顶
  /// `handle_alloc_error` abort 全进程，票 zcode-r55-alloc）。
  #[inline]
  fn merge_vector_keys(
    &self,
    pattern: &[u8],
    keys: &mut Vec<Vec<u8>>,
    remaining: usize,
  ) -> io::Result<()> {
    let Some(vectors) = &self.vector_session else {
      return Ok(());
    };
    let prefix = self.session.session_prefix();
    let all_keys = pattern == b"*";
    let mut budget = remaining;
    let mut push_err: Option<io::Error> = None;
    vectors
      .manager
      .for_each_domain_user_key(prefix.as_slice(), |k| {
        // 额度耗尽或已折错即止投（登记域照常遍历，零分配零出帧）
        if budget == 0 {
          return;
        }
        if all_keys || glob_match_nocase(pattern, k) {
          match try_push_key(keys, k) {
            Ok(()) => budget -= 1,
            Err(e) => {
              push_err = Some(e);
              budget = 0;
            }
          }
        }
      });
    push_err.map_or(Ok(()), Err)
  }

  /// DBSIZE 慢路径执行段：存储域键计数 + 向量登记表域内计数合并
  pub(super) async fn dbsize_command_slow(&self, storage: &StorageSession<'_, D>) -> Vec<u8> {
    let mut output = Vec::new();
    self
      .session
      .set_context(self.session.namespace(), self.session.active_db());
    // 登记表域内计数（换号换库互不串扰；registry 域内单点承接）
    let prefix = self.session.session_prefix();
    match storage.db_size().await {
      Ok(n) => {
        let vec_count = self
          .vector_session
          .as_ref()
          .map_or(0, |v| v.manager.registry_domain_count(prefix.as_slice()));
        output.write_resp_int((n + vec_count) as i64);
      }
      Err(_) => err_frame!(output),
    }
    output
  }

  /// KEYS 慢路径执行段：全库模式匹配投影（无分页，全量收齐）
  pub(super) async fn keys_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    self
      .session
      .set_context(self.session.namespace(), self.session.active_db());
    let pattern = refs.first().copied().unwrap_or(b"*");
    match storage.db_keys(pattern).await {
      Ok(mut keys) => {
        // KEYS 无分页：全量投影（remaining 无界），仅分配纪律归一
        if self
          .merge_vector_keys(pattern, &mut keys, usize::MAX)
          .is_err()
        {
          bail_frame!(output);
        }
        output.write_resp_array_len(keys.len());
        for key in &keys {
          output.write_resp_bulk_string(key);
        }
      }
      Err(_) => err_frame!(output),
    }
    output
  }

  /// SCAN 慢路径执行段：游标分页扫描 + 向量键首页并页
  pub(super) async fn scan_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    use crate::resp::array_commands::parse_scan_filter;

    let mut output = Vec::new();
    let filter = match parse_scan_filter(refs) {
      Ok(f) => f,
      Err(err) => bail_frame!(output, err),
    };
    // 未知 TYPE 值：C# DbScan 对非空未知 typeObject 直接回空列表 +
    // 游标 0（ArrayKeyIterationFunctions.cs:82-84），不触达扫描
    if filter.type_unknown {
      RespServerSession::write_output_for_scan(0, &[], &mut output);
      return output;
    }
    // TYPE 参数出现时单页无上限（C# long.MaxValue 同口径）
    let count = if filter.type_given {
      usize::MAX
    } else {
      filter.count
    };
    match storage
      .scan_cursor(
        &filter.pattern,
        filter.all_keys,
        filter.cursor as u64,
        count,
        filter.type_filter,
      )
      .await
    {
      Ok((cursor, mut keys)) => {
        if filter.cursor == 0 && !filter.type_given {
          // 向量并页臂归 limit 页上界单源：只补投剩余额度，满额截断不加码
          let remaining = count.saturating_sub(keys.len());
          if self
            .merge_vector_keys(&filter.pattern, &mut keys, remaining)
            .is_err()
          {
            bail_frame!(output);
          }
        }
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        RespServerSession::write_output_for_scan(cursor as i64, &key_refs, &mut output)
      }
      Err(_) => err_frame!(output),
    }
    output
  }

  /// 过期键删除扫描执行段（C# AdminCommands.NetworkEXPDELSCAN：C# 阻塞
  /// 等待 storeWrapper.ExpiredKeyDeletionScan；应答 *2 计数对
  /// `*2\r\n$N\r\n<expired>\r\n$N\r\n<scanned>\r\n`，DBID 已在快
  /// 路径 try_parse_database_id 校验，此处防御性重解析）
  pub(super) async fn expdelscan_command_slow(&self, refs: &[&[u8]]) -> Vec<u8> {
    let mut output = Vec::new();
    let db_id = match refs.first() {
      None => None,
      Some(arg) => match parse_db_index(arg) {
        Ok(idx) => Some(idx as u64),
        Err(_) => bail_frame!(output, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
      },
    };
    match self.session.store.expired_key_deletion_scan(db_id).await {
      Ok((expired, scanned)) => {
        let mut buf = Buffer::new();
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(buf.format(expired).as_bytes());
        output.write_resp_bulk_string(buf.format(scanned).as_bytes());
      }
      Err(_) => err_frame!(output),
    }
    output
  }
}

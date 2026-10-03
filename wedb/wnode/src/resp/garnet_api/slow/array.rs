//! 数组命令族慢路径承接（TYPE / LCS / DEL / UNLINK / MGET / MSET）
//!
//! C# ArrayCommands.NetworkTYPE / NetworkLCS / NetworkDEL / NetworkMGET /
//! NetworkMSET 的 Tsavorite pending 读 / 环形页翻转 CompletePending 重放承接

use wdev::Device;
use wresp::cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, write_error_raw};

use super::StoreGarnetApi;
use crate::{
  resp::array_commands as array_cmds, storage::session::storage_session::StorageSession,
};

impl<D: Device> StoreGarnetApi<D> {
  /// TYPE 慢路径承接执行段（C# NetworkTYPE 的 Read_UnifiedStore pending 就地
  /// 闭环 rust 对偶：快路径三域判型降级至此，应答形态与快路径逐字节一致）
  pub(super) async fn type_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let vector = self.vector_mgr();
    slow_arm!(output, array_cmds::slow::type_cmd, storage, vector, refs);
    output
  }

  /// LCS 慢路径承接执行段（C# NetworkLCS 的 Read_UnifiedStore pending 就地
  /// 闭环 rust 对偶：快路径双键读磁盘候选降级至此，应答形态与快路径逐字节一致）
  pub(super) async fn lcs_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
    resp_version: u8,
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(output, array_cmds::slow::lcs, storage, refs, resp_version);
    output
  }

  /// DEL / UNLINK 慢路径承接执行段（C# NetworkDEL 的 Tsavorite pending 读
  /// CompletePending 重放承接；快照尾参为 8 字节 LE 初始计数）
  pub(super) async fn del_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let (keys, initial_count) = match refs.split_last() {
      Some((cnt_bytes, keys)) if cnt_bytes.len() == 8 => {
        let Ok(arr) = <[u8; 8]>::try_from(*cnt_bytes) else {
          bail_frame!(output);
        };
        (keys, i64::from_le_bytes(arr))
      }
      _ => (refs, 0i64),
    };
    slow_arm!(output, array_cmds::slow::del, storage, keys, initial_count);
    output
  }

  /// MGET 慢路径承接执行段（C# NetworkMGET 的 Tsavorite pending 读重放承接）
  pub(super) async fn mget_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let proto = storage.resp_version;
    slow_arm!(output, array_cmds::slow::mget, storage, refs, proto);
    output
  }

  /// MSET 慢路径承接执行段（C# NetworkMSET 的环形页翻转 CompletePending
  /// 重放承接）
  pub(super) async fn mset_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    // 登记表句柄：窗内清退用（string_slow 臂同形装配，票 zcode-r163c-setguard 案一）
    let vector = self.vector_mgr();
    slow_arm!(output, array_cmds::slow::mset, storage, vector, refs);
    output
  }
}

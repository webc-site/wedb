//! 向量族慢路径承接（只读族冷态真读裁决 / 写族挂起闭环）

use wdev::Device;
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, write_error_raw},
  command::RespCommand,
};

use super::StoreGarnetApi;
use crate::storage::session::storage_session::StorageSession;

impl<D: Device> StoreGarnetApi<D> {
  /// 向量只读族冷态真读裁决执行段（C# 各 NetworkV* 的 res 三态分派：
  /// VectorManager.Locking.cs:ReadVectorIndexCore 的 Read_MainStore 真读
  /// 落盘裁决后 WRONGTYPE / NOTFOUND 族就地应答。快路径守卫遇磁盘候选 /
  /// 存储错误降级至此；写命令保守拒不走本臂，见 doc/zh/deviations.md §22）
  pub(super) async fn vector_read_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
    resp_version: u8,
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let Some(vectors) = &self.vector_session else {
      bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
    };
    vectors
      .network_vector_read_slow(storage, cmd, refs, resp_version, &mut output)
      .await;
    output
  }

  /// 向量写族挂起闭环执行段（VADD / VSETATTR：插入/属性写链为 compio 存储
  /// 异步操作，同步段 inline_wait 内联收割移除后 Allow 态参数快照挂起
  /// SlowWait 转投本臂——对标 cluster 链 pending_slow 转挂先例；慢臂
  /// network_vector_write_slow 真读复判键域后闭环，poll 边界的执行域
  /// 绑定由入口 SlowPollSessionBound 包装承接）
  pub(super) async fn vector_write_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
    resp_version: u8,
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let Some(vectors) = &self.vector_session else {
      bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
    };
    vectors
      .network_vector_write_slow(storage, cmd, refs, resp_version, &mut output)
      .await;
    output
  }
}

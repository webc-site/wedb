//! GETDEL 取删一体（DEL 族同步臂；对标 libs/server/Resp/KeyAdminCommands.cs 的 NetworkGETDEL）

use wdev::Device;
use wresp::{check_args::unpack_args, ext::RespVecExt};

use super::super::super::resp_server_session::RespServerSession;
use crate::{
  read_user_or_bail, resp::truncate_with_generic_error, storage::session::common::read_user_sync,
};

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkGETDEL
  pub fn network_getdel<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "GETDEL") else {
      return Ok(true);
    };

    // 读改写窗口 + 域判探针（零拷贝：只裁 WRONGTYPE/缺席/降级，应答值由取删
    // 一体内核捕获）同窗收口（对标 C# NetworkGETDEL 单次 RMW，RMWMethods.cs:763-768）
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    let start_len = output.len();
    read_user_or_bail!(read_user_sync(store, key, None, |_| ()), output, {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    });
    // 取删一体收口（C# 锁内 CopyRespTo + ExpireAndStop 的同原子性：
    // RMWMethods.cs:764-768 + InternalRMW.cs:70——应答值即本次实际摘除记录
    // 的值，杜绝探针读 old、并发盲写 SET 落 new、删除摘走 new 的答旧删新
    // 撕裂）；闭包直写 output 消除中间 Vec 暂存；摘除空手（并发盲 DEL 已先行）沿串行序 DEL→GETDEL 答 nil
    match store.try_take_sync_with(key, |val| {
      output.write_resp_bulk_string(val);
    }) {
      Ok(Ok(Some(()))) => {}
      Ok(Ok(None)) => {
        output.truncate(start_len);
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      // 先删后答的降级口径不变：摘除遇异步闭环（环形页翻转/冷数据）时
      // 整体降级，避免已答出值而键未删成
      Ok(Err(_)) => {
        output.truncate(start_len);
        return Ok(false);
      }
      Err(_) => truncate_with_generic_error(output, start_len),
    }
    Ok(true)
  }
}

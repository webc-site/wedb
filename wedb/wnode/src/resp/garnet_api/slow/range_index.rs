//! RangeIndex 族慢路径承接（解析校验与执行一体在异步段闭环）

use wdev::Device;
use wresp::command::RespCommand;

use super::StoreGarnetApi;
use crate::resp::range_index::resp_server_session_range_index as ri_cmds;

impl<D: Device> StoreGarnetApi<D> {
  /// RangeIndex 族执行段（resp_server_session_range_index.rs：解析校验与
  /// 执行一体在异步段闭环；ri 门取存储域共享范围索引管理器）
  pub(super) async fn range_index_command_slow(
    &self,
    cmd: RespCommand,
    refs: &[&[u8]],
    resp_version: u8,
  ) -> Vec<u8> {
    use RespCommand as C;

    let mut output = Vec::new();
    // 解析校验 + 执行一体闭环；应答直写 output（错误映射在处理器内）。
    // C# EnableRangeIndexPreview 预览门在 rust 恒开（deviations 登记）
    let _ = match cmd {
      C::Ricreate => ri_cmds::network_ricreate(refs, &self.session, &mut output).await,
      C::Riset => ri_cmds::network_riset(refs, &self.session, &mut output).await,
      C::Riget => ri_cmds::network_riget(refs, &self.session, resp_version, &mut output).await,
      C::Ridel => ri_cmds::network_ridel(refs, &self.session, &mut output).await,
      C::Riscan => ri_cmds::network_riscan(refs, &self.session, &mut output).await,
      C::Rirange => ri_cmds::network_rirange(refs, &self.session, &mut output).await,
      C::Riexists => ri_cmds::network_riexists(refs, &self.session, &mut output).await,
      C::Riconfig => ri_cmds::network_riconfig(refs, &self.session, &mut output).await,
      C::Ricount => ri_cmds::network_ricount(refs, &self.session, &mut output).await,
      _ => ri_cmds::network_rimetrics(refs, &self.session, &mut output).await,
    };
    output
  }
}

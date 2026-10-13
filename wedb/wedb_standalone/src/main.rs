#![recursion_limit = "512"]
// open_recovered_with_config_and_aof 泛型单态化状态机嵌套深（R27 CI 实证 x86 stable 溢出）
//! WeDB 单机服务端主程序入口
//!
//! 存储执行域装配下沉 wnode 基座：StorageSessionProvider 固化
//! new_session → StoreGarnetApi → 会话消费者 → item_broker → runtime_config
//! 公共流程（单机/集群功能一致，存储 + 经纪 + 向量三件套同源），
//! 单机差异仅为 RespSessionConsumer::new 构造（无集群切面）。

use std::{env::args_os, sync::Arc};

use wconf::ServerArgs;
use wedb_standalone::StandaloneArgs;
use wnode::{
  RespSessionConsumer, ServerBootstrap, logging::bootstrap_args_with_logging,
  resp::resp_server_session::RespServerSessionOptions, service::StorageSessionProvider,
};

fn main() -> wnode::Result<()> {
  // 前段三步（先行缓冲安装 → 三层配置解析 → 日志装配回灌）收口 wnode 单点
  //（与集群宿主同一份，对标 C# GarnetServer 构造器 initLogger 段）
  let args = bootstrap_args_with_logging::<StandaloneArgs, _, _>(args_os())?;

  // 采样节拍 / 延迟监视 / 逐命令统计 / 连接上限 / TLS 由
  // ServerBootstrap::run_async 一处从 NodeArgs 投影（对标 C#
  // StoreWrapper.cs:226-227 消费侧直读 options），与集群宿主同一份装配事实
  ServerBootstrap::new(args)
    .banner("WeDB Standalone 单机节点")
    .run_async(|args, _noop_cluster| async move {
      let node = args.node_args();
      // 会话参数基线（NodeArgs → 会话选项映射收口 wnode 单点
      // `RespServerSessionOptions::from`，与集群同一份）
      let session_options = RespServerSessionOptions::from(node);
      let session_factory = move |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          session_options.clone(),
          Arc::new(api),
        ))
      };
      let data_path = node.data_path();
      // --recover 与 AOF 分派及运行时选项装配收口 wnode 基座（对标 C# Options.cs:139
      // Recover → StoreWrapper.RecoverAsync 单机分支；恢复在端点 accept 之前完成）
      let provider =
        StorageSessionProvider::open_from_args(node, data_path, session_factory).await?;
      Ok(Arc::new(provider))
    })
}

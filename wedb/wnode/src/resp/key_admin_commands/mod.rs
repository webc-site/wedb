//! 键管理与生命周期管理命令模块（对标 libs/server/Resp/KeyAdminCommands.cs）
//!
//! 目录化拆分：
//! - [`keys`]：KEYS, RENAME, RENAMENX, EXPIRE, TTL 键名操作与生存期管理
//! - [`types`]：EXISTS, TYPE, DUMP, RESTORE, OBJECT 类型判定与对象原语
//!
//! SCAN / DBSIZE 在 resp/array_commands.rs（C# ArrayCommands.cs 同域）；
//! RANDOMKEY 两侧一致无（C# 全仓无此命令），不实现。

// ============ 族内错误收尾宏单源（keys / types 子域共用，须先于 mod 声明定义；
// 宏体里的名字按展开点解析，各子模块自备 RESP_ERR_GENERIC / RespVecExt 等导入） ============

/// 通用错误帧统一收尾（对标本域十余处同形臂：写 `RESP_ERR_GENERIC` → 本命令闭环）
macro_rules! bail_err_frame {
  ($output:expr) => {{
    $output.write_resp_error(RESP_ERR_GENERIC);
    return Ok(true);
  }};
}

/// 存储步骤三态统一收尾：放行 / 整体降级异步（`Ok(false)` 交调用方整条重放）
/// / 通用错误帧。`$pass`、`$defer` 按原语返回形态给出，判序与降级信号不改
macro_rules! bail_store_step {
  ($output:expr, $res:expr, $pass:pat, $defer:pat) => {
    match $res {
      $pass => {}
      $defer => return Ok(false),
      Err(_) => bail_err_frame!($output),
    }
  };
}

/// 存活探针三态收尾单源（EXISTS 逐键计数、RESTORE NX 判定与 RENAMENX 新键
/// 存活判定共用同一判序）：命中折叠为布尔、磁盘候选/待裁决整体降级
/// `Ok(false)`、硬故障通用错误帧
macro_rules! probe_alive_or_bail {
  ($store:expr, $prefix:expr, $key:expr, $vector:expr, $output:expr) => {
    match probe_alive_with_registry($store, $prefix, $key, $vector) {
      Ok(Some(alive)) => alive,
      Ok(None) => return Ok(false),
      Err(_) => bail_err_frame!($output),
    }
  };
}

mod keys;
pub mod slow;
mod types;

pub use self::keys::{ExpireCmd, ExpireTimeCmd, TtlCmd};

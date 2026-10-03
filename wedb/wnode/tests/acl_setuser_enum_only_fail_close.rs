//! ACL SETUSER 枚举在场、目录缺席名（DELIFEXPIM/RIPROMOTE/RIRESTORE）活链
//! 失败关闭锁测（票 wacl-acl-setuser-enum-only-name-custom-fallback）
//!
//! 在 garnet 中的相对路径: libs/server/Resp/ACLCommands.cs:NetworkAclSetUser
//! （catch ACLException → `ERR {exception.Message}`，:228-233）
//! + libs/server/ACL/User.cs:AddCommand(:187-190)/RemoveCommand(:316-318)
//!   （查 RespCommandsInfo 目录失配抛 "Unable to obtain ACL information,
//!   this shouldn't be possible"）。
//!
//! 锁面：
//! 1. 三名 × 加减臂 × 大小写形 × 去点形，活链回 C# 同文案 -ERR；
//! 2. 失败后用户权限零变化（ACL GETUSER 基线逐帧相等）、零持久残留
//!    （ACL LIST 不含幻影名条目）；
//! 3. 两档一致性条件编译形断言：默认档（roaring/json 在场）正对照未知名
//!    走自定义轨并撞扩展注册门回 "Unknown custom command"；--no-default-features
//!    档注册门按 C# ccm==null 形缺席、未知名 +OK 入自定义轨，而三名在两档
//!    均在解析/授权期即失败关闭——判定先于档位分叉的注册门（wacl 无 feature 门）。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wacl::AccessControlList;
use wnode::{
  SessionProviderFace, WireFormat,
  resp::{
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wnode_test::send_consumer_args as send_cmd;
use wtest_base::test_store_config;

/// C# User.AddCommand/RemoveCommand 抛形经 ACLCommands catch 的应答帧
const ERR_FRAME: &[u8] = b"-ERR Unable to obtain ACL information, this shouldn't be possible\r\n";

/// 枚举在场目录缺席名活链失败关闭：-ERR 同文案、权限零变化、零持久残留；
/// 未知名正对照按档走自定义轨（条件编译形断言，两档共用本活链闭环）
#[test]
fn acl_setuser_enum_only_name_fails_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let acl = Arc::new(AccessControlList::new("").unwrap());
    let provider = StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("enum_only.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions {
            default_user: "default".into(),
            max_databases: 16,
            ..RespServerSessionOptions::default()
          },
          Arc::new(api),
        ))
      },
    )
    .unwrap()
    .with_acl(Arc::clone(&acl));

    let mut consumer = provider.get_session(WireFormat::Ascii, 1).unwrap();

    // 建基线用户：on + 口令 + 全类别
    let out = send_cmd(
      &mut consumer,
      &[b"ACL", b"SETUSER", b"u1", b"on", b">pw1", b"+@all"],
    )
    .await;
    assert_eq!(out, b"+OK\r\n");
    let baseline_getuser = send_cmd(&mut consumer, &[b"ACL", b"GETUSER", b"u1"]).await;
    assert!(!baseline_getuser.starts_with(b"-"), "基线 GETUSER 须成功");
    let baseline_list = send_cmd(&mut consumer, &[b"ACL", b"LIST"]).await;

    // 三名 × 加减臂 × 大小写形 × 去点形：全部回 C# 同文案失败帧
    for op in [
      "+DELIFEXPIM",
      "-DELIFEXPIM",
      "+RIPROMOTE",
      "-ripromote",
      "+RIRESTORE",
      "-RiReStOrE",
      "+RI.PROMOTE",
      "-RI.RESTORE",
    ] {
      let out = send_cmd(&mut consumer, &[b"ACL", b"SETUSER", b"u1", op.as_bytes()]).await;
      assert_eq!(out, ERR_FRAME, "SETUSER u1 {op} 应答偏离 C# 失败关闭帧");
    }

    // 权限零变化 + 零持久残留：GETUSER/LIST 与基线逐帧相等，LIST 不含幻影名
    assert_eq!(
      send_cmd(&mut consumer, &[b"ACL", b"GETUSER", b"u1"]).await,
      baseline_getuser,
      "失败关闭后用户权限面须零变化"
    );
    let list = send_cmd(&mut consumer, &[b"ACL", b"LIST"]).await;
    assert_eq!(list, baseline_list, "失败关闭后 ACL LIST 须零持久残留");
    let list_str = String::from_utf8_lossy(&list).to_ascii_uppercase();
    for name in ["DELIFEXPIM", "RIPROMOTE", "RIRESTORE"] {
      assert!(!list_str.contains(name), "ACL LIST 混入幻影条目 {name}");
    }

    // 第二连接直读存储复核持久面零残留（点查存储真源，非会话缓存）
    let mut verifier = provider.get_session(WireFormat::Ascii, 2).unwrap();
    assert_eq!(
      send_cmd(&mut verifier, &[b"ACL", b"GETUSER", b"u1"]).await,
      baseline_getuser,
      "存储真源复核：失败关闭零持久残留"
    );

    // 正对照：枚举亦无的纯字母未知名仍走自定义轨（与失败关闭轨互不沾染）。
    // 默认档（roaring/json 在场）：新增名撞扩展注册门，回 Unknown custom
    // command 文案（C# ccm!=null 臂），u1 零沾染；--no-default-features 档：
    // 注册门按 C# ccm==null 形缺席，未知名 +OK 入自定义轨（既有档位分歧，
    // 非本票面），故改在独立用户 u2 上放行以证轨形、不动 u1 基线。
    // 两档下三名均先行失败关闭：判定落在 wacl 解析/授权期，feature 无关。
    #[cfg(any(feature = "roaring", feature = "json"))]
    {
      let out = send_cmd(&mut consumer, &[b"ACL", b"SETUSER", b"u1", b"+FOOBAR"]).await;
      assert_eq!(
        out, b"-ERR Unknown custom command 'FOOBAR' (not registered with any loaded module)\r\n",
        "未知名应走自定义轨并撞注册门，而非枚举失败关闭文案"
      );
    }
    #[cfg(not(any(feature = "roaring", feature = "json")))]
    {
      let out = send_cmd(
        &mut consumer,
        &[b"ACL", b"SETUSER", b"u2", b"on", b"+FOOBAR"],
      )
      .await;
      assert_eq!(
        out, b"+OK\r\n",
        "无扩展档未知名照旧入自定义轨（C# ccm==null 形）"
      );
      let got = send_cmd(&mut consumer, &[b"ACL", b"GETUSER", b"u2"]).await;
      assert!(
        String::from_utf8_lossy(&got).contains("FOOBAR"),
        "无扩展档正对照须见自定义轨收录"
      );
    }
  });
}

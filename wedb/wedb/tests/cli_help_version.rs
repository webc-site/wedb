//! --help/--version 用户交互路径回归（task/ing/zcode-r21-standalone）
//!
//! 缺陷：两宿主二进制的命令行解析错误统一走 Error::InvalidArgument → main
//! Termination 以 derive(Debug) 单行打 stderr、退出码 1——`--help` /
//! `--version` 的帮助全文被转义成不可读单行，且退出码非 0。C# 对位
//! （libs/host/ServerSettingsManager.cs:237-258 Console.WriteLine(helpText)，
//! 以及 GarnetServer.cs:92-94 exitGracefully → Environment.Exit(0)）：stdout
//! 干净全文、退出码 0。
//!
//! 修复：解析层收口 ConfigFileArgs::from_args_iter_or_exit——识别 clap
//! ErrorKind::DisplayHelp / DisplayVersion 即 stdout 打干净全文后 exit(0)，
//! 其余解析错误维持原错误路径。
//!
//! 测试对标：真实二进制子进程端到端断言退出码与输出流。

use std::process::{Command, Output};

/// 拉起集群服务端子进程
fn run(args: &[&str]) -> Output {
  Command::new(env!("CARGO_BIN_EXE_wedb"))
    .args(args)
    .output()
    .expect("拉起 wedb 失败")
}

/// --help / --version：退出码 0 + stdout 干净全文（修复前走启动失败路径：
/// 退出码 1、stderr、帮助全文被 derive(Debug) 单行转义）
#[test]
fn help_and_version_print_clean_stdout_and_exit_zero() {
  for flag in ["--help", "--version"] {
    let out = run(&[flag]);
    assert_eq!(out.status.code(), Some(0), "{flag} 退出码非 0");
    assert!(
      out.stderr.is_empty(),
      "{flag} 不应有 stderr 输出: {}",
      String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.trim().is_empty(), "{flag} stdout 为空");
    if flag == "--help" {
      // clap 帮助全文：用法段在（非 Debug 转义单行）
      assert!(stdout.contains("Usage"), "--help 缺用法段: {stdout}");
    }
  }
}

/// 真实解析错误维持原错误路径：退出码 1 + stderr 报错（不得被误判为
/// help/version 走 exit(0)）
#[test]
fn real_parse_error_keeps_error_path() {
  let out = run(&["--port", "not-a-number"]);
  assert_eq!(out.status.code(), Some(1), "解析错误应退出码 1");
  assert!(
    !out.stderr.is_empty(),
    "解析错误应有 stderr 报错（InvalidArgument 路径）"
  );
}

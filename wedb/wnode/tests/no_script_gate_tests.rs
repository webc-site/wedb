//! Lua 脚本窗 no-script 门编译期表 vs JSON 目录真值全量对拍（防双真源漂移）
//!
//! 真值面：wresp/RespCommandsInfo.json（经 `wresp::catalog` 运行时索引，
//! externalOnly 口径）。编译期面：`resp_server_session/lua.rs` 的
//! `NO_SCRIPT_COMMANDS` 清单 / `NO_SCRIPT_BITMAP` 位图与
//! `parser/command_table.rs` 的 `NO_SCRIPT_PARENT` 归一表（经会话 pub 面
//! `attach_no_script_bitmap` + `check_script_permissions` 观测）。
//!
//! 对拍三面：
//! 1. 位图字面等价：目录重建的位图（复刻 C# InitializeNoScriptDetails 旧
//!    运行时算法）与会话挂载位图逐字相等；
//! 2. NoScript 位集双向：目录带 NoScript 标志的判别值集 == 位图置位集，
//!    无多余位、无缺失位；
//! 3. 门决策逐位等价：全判别值空间 0..=65535，旧链（目录归一表 + 目录位图）
//!    与新链（编译期归一表 + 编译期位图）的放行/拦截决策一致——覆盖
//!    BITOP 子命令族（解析表独有、目录无条目）等一切归一差异面。
//!
//! 改 JSON 目录 NoScript 标志或改 rust 清单，任一侧漂移即本文件红。

use std::iter::once;

use wnode::resp::resp_server_session::RespServerSession;
use wresp::{
  catalog::{RespCommandFlags, try_get_resp_command_info_by_cmd, try_get_resp_commands_info},
  command::RespCommand,
};

/// C# 位粒度怪癖：置位除数为 `sizeof(ulong)` 字节数而非 64 位
const BITS_PER_WORD: usize = 8;

/// 目录期望面重建（复刻 lua.rs 位图装配算法，数据全取 JSON 目录）：
/// 返回 (start, 位图)
fn catalog_expectation() -> (i32, Vec<u64>) {
  let all_commands = try_get_resp_commands_info(true).expect("命令目录初始化失败");

  let mut no_script: Vec<u16> = all_commands
    .values()
    .flat_map(|info| once(info).chain(info.sub_commands.iter()))
    .filter(|info| info.flags.intersects(RespCommandFlags::NO_SCRIPT))
    .filter(|info| info.command != RespCommand::None)
    .map(|info| info.command as u16)
    .collect();
  no_script.sort_unstable();
  no_script.dedup();

  let start = *no_script.first().expect("目录含 NoScript 条目");
  let end = *no_script.last().expect("非空");
  let size = (end - start) as usize + 1;
  let mut num_words = size / BITS_PER_WORD;
  // C# 上游怪癖：余数对 numULongs 而非字宽取模，1:1 保留
  if !size.is_multiple_of(num_words) {
    num_words += 1;
  }
  let mut bitmap = vec![0_u64; num_words];
  for discriminant in &no_script {
    let stepped = (discriminant - start) as usize;
    bitmap[stepped / BITS_PER_WORD] |= 1_u64 << (stepped % BITS_PER_WORD);
  }

  (start as i32, bitmap)
}

/// 旧门链位检查（check_script_permissions 删除 OnceLock 前的同款算法）
fn legacy_gate(bitmap: &[u64], start: i32, disc: i32) -> bool {
  let ix = disc - start;
  if ix >= 0
    && let Some(&word) = bitmap.get(ix as usize / BITS_PER_WORD)
  {
    return word & (1_u64 << (ix as usize % BITS_PER_WORD)) == 0;
  }
  true
}

/// 编译期表与目录真值全量对拍（位图字面 / 位集双向 / 门决策三面）
#[test]
fn compiletime_no_script_tables_match_json_catalog() {
  let (start, bitmap) = catalog_expectation();
  let mut session = RespServerSession::default();
  session.attach_no_script_bitmap();
  let mounted = session.no_script_bitmap.expect("位图已挂载");

  // 面 1：位图字面等价（字数与逐位同源）
  assert_eq!(mounted, bitmap.as_slice(), "编译期位图与目录真值不等");

  // 面 2：NoScript 位集双向（目录 NoScript 判别值 <=> 位图置位）
  let mut catalog_no_script = 0_usize;
  for value in 0_u16..=u16::MAX {
    let Some(cmd) = RespCommand::from_repr(value) else {
      continue;
    };
    let Some(info) = try_get_resp_command_info_by_cmd(cmd, false) else {
      continue;
    };
    let set = if value as i32 >= start {
      let stepped = value as usize - start as usize;
      mounted
        .get(stepped / BITS_PER_WORD)
        .is_some_and(|&word| word & (1_u64 << (stepped % BITS_PER_WORD)) != 0)
    } else {
      // 位图射程下界之外无从置位（gate 侧 ix < 0 恒放行的同构面）
      false
    };
    if info.flags.intersects(RespCommandFlags::NO_SCRIPT) {
      assert!(set, "目录 NoScript 判别值 {value} 在位图缺位");
      catalog_no_script += 1;
    } else {
      assert!(!set, "位图位 {value} 在目录无 NoScript 条目（多余位）");
    }
  }
  assert!(catalog_no_script > 0, "目录真值面不得为空");

  // 面 3：门决策全值域对拍（目录位图 + 裸判别值直查 vs 编译期表——C#
  // AdminCommands.cs:99 取解析产出子命令判别值原值，无归一步）
  for value in 0_u16..=u16::MAX {
    let Some(cmd) = RespCommand::from_repr(value) else {
      continue;
    };
    let legacy = legacy_gate(&bitmap, start, value as i32);
    assert_eq!(
      session.check_script_permissions(cmd),
      legacy,
      "判别值 {value}（{cmd:?}）门决策漂移"
    );
  }
}

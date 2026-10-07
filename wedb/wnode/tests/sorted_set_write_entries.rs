#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wcol::zset::sorted_set_object::SortedSetObject;
use wnode::resp::objects::sorted_set_commands::write::write_zset_entries;

fn obj() -> SortedSetObject {
  SortedSetObject::from_entries(vec![(b"a".to_vec(), 1.5), (b"b".to_vec(), 2.5)])
}

/// WITHSCORES：RESP2 扁平 *2n + bulk 分值；RESP3 头 *n + 逐条 *2 + `,num`
///（C# SortedSetCommands.cs:966-988 / 1135-1165 / 1446-1462）
#[test]
fn with_scores_dual_protocol() {
  let mut out2 = Vec::new();
  write_zset_entries(Some(&obj()), true, &mut out2, 2);
  assert_eq!(
    out2,
    b"*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$3\r\n2.5\r\n"
  );

  let mut out3 = Vec::new();
  write_zset_entries(Some(&obj()), true, &mut out3, 3);
  assert_eq!(
    out3,
    b"*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$1\r\nb\r\n,2.5\r\n"
  );
}

/// 不带分值：双版本同形
#[test]
fn without_scores_dual_protocol() {
  let mut out2 = Vec::new();
  write_zset_entries(Some(&obj()), false, &mut out2, 2);
  assert_eq!(out2, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

  let mut out3 = Vec::new();
  write_zset_entries(Some(&obj()), false, &mut out3, 3);
  assert_eq!(out3, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

/// 空结果恒 *0（C# TryWriteEmptyArray，不分版本）
#[test]
fn empty_result_star0() {
  for ver in [2_u8, 3] {
    let mut out = Vec::new();
    write_zset_entries(None, true, &mut out, ver);
    assert_eq!(out, b"*0\r\n");
  }
}

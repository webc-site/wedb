//! COSCAN 成员扫描执行体契约回归（票 wcol-coscan-object-domain-inversion 测试点 a）
//!
//! 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Scan
//!
//! C# 上游实现刻意留空：恒空收集 + 游标 0，忽略 MATCH/COUNT/NOVALUES（稠密
//! 位图全枚举可达数十亿条目，官方建议 BITPOS 循环承担迭代）；其 doc 注释自称
//! 「输出置位十进制键」与实现矛盾，以实现为准。本测试钉住全参数臂恒 `[0, []]`
//! 与清单项执行体指针一致性，杜绝将来误「补真」成成员枚举造成 C# 语义反转。

use wext_roaring::RoaringCommand;

/// 全参数组合臂恒空收集 + 游标 0（含越界游标、非正 COUNT、损坏载荷）
#[test]
fn scan_members_is_empty_scan_ignoring_all_options() {
  let scan = RoaringCommand::OBJECT_ENTRY.scan_members;

  let payloads: &[&[u8]] = &[&[], &[0xff, 0xff], &[0x00]];
  let options: &[(i64, i64, &[u8], bool)] = &[
    (0, 10, b"", false),
    (0, 10, b"bit*", false),
    (0, 1, b"bit*", true),
    (99, -5, b"p*", true),
  ];

  for payload in payloads {
    for &(start, count, pattern, is_no_value) in options {
      let (items, cursor) = scan(payload, start, count, pattern, is_no_value).unwrap();
      assert!(
        items.is_empty(),
        "参数臂 (start={start}, count={count}) 不应有成员产出"
      );
      assert_eq!(
        cursor, 0,
        "参数臂 (start={start}, count={count}) 游标应恒 0"
      );
    }
  }
}

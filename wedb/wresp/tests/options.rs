#[cfg(test)]
mod tests {
  use wresp::options::*;

  #[test]
  fn test_equals_ignore_case() {
    assert!(equals_ignore_case(b"hello", b"HELLO"));
    assert!(equals_ignore_case(b"ZADD", b"zadd"));
    assert!(!equals_ignore_case(b"ZADD", b"zadd1"));
    assert!(!equals_ignore_case(b"", b"a"));
  }

  #[test]
  fn test_sorted_set_add_options_parse() {
    for (raw, expected) in [
      (b"xx" as &[u8], SortedSetAddOption::XX),
      (b"XX", SortedSetAddOption::XX),
      (b"nx", SortedSetAddOption::NX),
      (b"NX", SortedSetAddOption::NX),
      (b"lt", SortedSetAddOption::LT),
      (b"LT", SortedSetAddOption::LT),
      (b"gt", SortedSetAddOption::GT),
      (b"GT", SortedSetAddOption::GT),
      (b"ch", SortedSetAddOption::CH),
      (b"CH", SortedSetAddOption::CH),
      (b"incr", SortedSetAddOption::INCR),
      (b"INCR", SortedSetAddOption::INCR),
    ] {
      assert_eq!(try_get_sorted_set_add_option(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"x", b"xxx", b"inc", b"incr1", b"zz"] {
      assert_eq!(try_get_sorted_set_add_option(bad), None);
    }
  }

  #[test]
  fn test_expire_options_parse() {
    for (raw, expected) in [
      (b"nx" as &[u8], ExpireOption::NX),
      (b"NX", ExpireOption::NX),
      (b"xx", ExpireOption::XX),
      (b"XX", ExpireOption::XX),
      (b"gt", ExpireOption::GT),
      (b"GT", ExpireOption::GT),
      (b"lt", ExpireOption::LT),
      (b"LT", ExpireOption::LT),
    ] {
      assert_eq!(try_get_expire_option(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"n", b"nxx", b"zz", b"none"] {
      assert_eq!(try_get_expire_option(bad), None);
    }
  }

  #[test]
  fn test_expiration_options_parse() {
    for (raw, expected) in [
      (b"ex" as &[u8], ExpirationOption::Ex),
      (b"EX", ExpirationOption::Ex),
      (b"px", ExpirationOption::Px),
      (b"PX", ExpirationOption::Px),
      (b"exat", ExpirationOption::Exat),
      (b"EXAT", ExpirationOption::Exat),
      (b"pxat", ExpirationOption::Pxat),
      (b"PXAT", ExpirationOption::Pxat),
      (b"keepttl", ExpirationOption::Keepttl),
      (b"KEEPTTL", ExpirationOption::Keepttl),
    ] {
      assert_eq!(try_get_expiration_option(raw), Some(expected));
    }
    for bad in [b"" as &[u8], b"e", b"exx", b"pxa", b"keeptt", b"keepttll"] {
      assert_eq!(try_get_expiration_option(bad), None);
    }
  }

  #[test]
  fn test_exist_options_parse() {
    assert_eq!(try_get_exist_options(b"nx"), Some(ExistOptions::Nx));
    assert_eq!(try_get_exist_options(b"NX"), Some(ExistOptions::Nx));
    assert_eq!(try_get_exist_options(b"xx"), Some(ExistOptions::Xx));
    assert_eq!(try_get_exist_options(b"XX"), Some(ExistOptions::Xx));
    assert_eq!(try_get_exist_options(b"none"), None);
    assert_eq!(try_get_exist_options(b""), None);
  }

  #[test]
  fn test_sorted_set_aggregate_type_parse() {
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"sum"),
      Some(SortedSetAggregateType::Sum)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"SUM"),
      Some(SortedSetAggregateType::Sum)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"min"),
      Some(SortedSetAggregateType::Min)
    );
    assert_eq!(
      try_get_sorted_set_aggregate_type(b"MAX"),
      Some(SortedSetAggregateType::Max)
    );
    assert_eq!(try_get_sorted_set_aggregate_type(b"avg"), None);
    assert_eq!(try_get_sorted_set_aggregate_type(b"summ"), None);
    assert_eq!(try_get_sorted_set_aggregate_type(b""), None);
  }

  #[test]
  fn test_sorted_set_aggregate_apply() {
    assert_eq!(SortedSetAggregateType::Sum.apply(2.5, 3.5), 6.0);
    assert_eq!(SortedSetAggregateType::Min.apply(2.5, 3.5), 2.5);
    assert_eq!(SortedSetAggregateType::Max.apply(2.5, 3.5), 3.5);
  }

  #[test]
  fn test_expiration_with_option() {
    let ticks = 1_000_000_000i64;
    let opt = ExpireOption::GT;
    let e = ExpirationWithOption::new(ticks, opt);
    assert_eq!(e.expire_option(), ExpireOption::GT);
    assert_eq!(e.expiration_time_in_ticks(), (ticks >> 4) << 4);

    let reconstructed = ExpirationWithOption::from_word_head_tail(e.word_head(), e.word_tail());
    assert_eq!(e, reconstructed);
    // word 头尾两半逐位一致（word 整体 getter 已删，经 head/tail 投影断言）
    assert_eq!(
      (e.word_head(), e.word_tail()),
      (reconstructed.word_head(), reconstructed.word_tail())
    );
  }
}

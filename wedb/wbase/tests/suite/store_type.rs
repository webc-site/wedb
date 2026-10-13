use wbase::store_type::StoreType;

#[test]
fn test_store_type_roundtrip() {
  assert_eq!(StoreType::from_member_name("Main"), Some(StoreType::Main));
  assert_eq!(StoreType::from_member_name("main"), Some(StoreType::Main));
  assert_eq!(StoreType::from_member_name("MAIN"), Some(StoreType::Main));
  assert_eq!(
    StoreType::from_member_name("Object"),
    Some(StoreType::Object)
  );
  assert_eq!(
    StoreType::from_member_name("object"),
    Some(StoreType::Object)
  );
  assert_eq!(StoreType::from_member_name("All"), Some(StoreType::All));
  assert_eq!(StoreType::from_member_name("all"), Some(StoreType::All));
  assert_eq!(StoreType::from_member_name("None"), Some(StoreType::None));
  assert_eq!(StoreType::from_member_name("none"), Some(StoreType::None));
  assert_eq!(StoreType::from_member_name("unknown"), None);
  assert_eq!(StoreType::from_member_name(""), None);

  for s in [
    StoreType::None,
    StoreType::Main,
    StoreType::Object,
    StoreType::All,
  ] {
    assert_eq!(StoreType::from_member_name(s.as_str()), Some(s));
    assert_eq!(s.as_str(), s.to_string());
    assert_eq!(s.as_str(), s.as_ref());
    assert_eq!(s.as_str().parse::<StoreType>(), Ok(s));
    assert_eq!(StoreType::from_u8(s.as_u8()), Some(s));
    assert_eq!(StoreType::try_from(s.as_u8()), Ok(s));
  }

  assert_eq!(StoreType::Main.as_str(), "Main");
  assert_eq!(StoreType::Object.as_str(), "Object");
  assert_eq!(StoreType::None.as_str(), "None");
  assert_eq!(StoreType::All.as_str(), "All");
  assert_eq!(StoreType::default(), StoreType::Main);

  assert_eq!(StoreType::None.as_u8(), 0);
  assert_eq!(StoreType::Main.as_u8(), 1);
  assert_eq!(StoreType::Object.as_u8(), 2);
  assert_eq!(StoreType::All.as_u8(), 3);
  assert_eq!(StoreType::from_u8(99), None);
  assert!(StoreType::try_from(99).is_err());
}

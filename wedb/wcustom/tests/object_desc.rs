#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wcustom::{CustomArgs, KeyScope};

/// 单键：首参为键，其余为入参；空参数域拆不出键
#[test]
fn single_scope_splits_first_arg_as_key() {
  let args = [b"k".as_slice(), b"$.a".as_slice()];
  let parsed = KeyScope::Single.split(&args).unwrap();
  assert_eq!(
    parsed,
    CustomArgs::Single {
      key: b"k",
      args: &[b"$.a".as_slice()]
    }
  );
  assert!(KeyScope::Single.split(&[]).is_none());
}

/// 多键读：末 tail 个为入参、其余全为键；键数归零即拆分失败
#[test]
fn multi_read_scope_splits_trailing_args() {
  let args = [b"k1".as_slice(), b"k2".as_slice(), b"$".as_slice()];
  let parsed = KeyScope::MultiRead { tail: 1 }.split(&args).unwrap();
  assert_eq!(
    parsed,
    CustomArgs::Multi {
      keys: &[b"k1".as_slice(), b"k2".as_slice()],
      args: &[b"$".as_slice()]
    }
  );
  // 参数数不大于尾部入参数：无键可读，拒绝而非把入参当键
  let no_key = [b"$".as_slice()];
  assert!(KeyScope::MultiRead { tail: 1 }.split(&no_key).is_none());
  assert!(KeyScope::MultiRead { tail: 3 }.split(&args).is_none());
}

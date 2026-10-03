use wcol::{ObjectOutput, SetObject, SetOperation, set::set_object_impl::NO_COUNT};

#[test]
fn test_set_pop_no_count() {
  let mut set = SetObject::default();
  let mut payload = Vec::new();

  // 空集单枚弹出：写 null，result1 为 1
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(payload, b"$-1\r\n");

  // 填充元素并同步更新内存记账
  set.set.insert(b"elem1".to_vec());
  set.update_size(b"elem1", true);
  set.set.insert(b"elem2".to_vec());
  set.update_size(b"elem2", true);

  // 弹出单枚
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(set.set.len(), 1);

  // 再次弹出单枚
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(set.set.len(), 0);

  // 弹空后再弹出：写 null，result1 为 1
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(payload, b"$-1\r\n");
}

#[test]
fn test_set_pop_with_count() {
  let mut set = SetObject::default();
  set.set.insert(b"a".to_vec());
  set.update_size(b"a", true);
  set.set.insert(b"b".to_vec());
  set.update_size(b"b", true);
  set.set.insert(b"c".to_vec());
  set.update_size(b"c", true);

  let mut payload = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], 2, 0, &mut output, 2);
    assert_eq!(output.result1, 2);
  }
  assert_eq!(set.set.len(), 1);

  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    // count 超过当前剩余量
    set.operate(SetOperation::Spop as u8, &[], 5, 0, &mut output, 2);
    assert_eq!(output.result1, 5);
  }
  assert_eq!(set.set.len(), 0);
}

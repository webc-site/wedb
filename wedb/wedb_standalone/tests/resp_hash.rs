mod support;

use core::str;

use support::with_batch;

fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() {
    if frame[pos] != b'$' {
      break;
    }
    let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
    let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
      .unwrap()
      .parse()
      .unwrap();
    let start = len_end + 1;
    items.push(frame[start..start + len].to_vec());
    pos = start + len + 2;
  }
  items
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanSetAndGetOnePair
#[test]
fn can_set_and_get_one_pair() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanSetAndGetMultiPair
#[test]
fn can_set_and_get_multi_pair() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nWorld\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDelSingleField
#[test]
fn can_del_single_field() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_delete(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_delete(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDeleteMultipleFields
#[test]
fn can_delete_multiple_fields() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_delete(&[b"myhash", b"field1", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckEmptyHashKeyRemoved
#[test]
fn check_empty_hash_key_removed() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"h", b"f1", b"v1"], batch, &mut out).unwrap();

    out.clear();
    s.hash_delete(&[b"h", b"f1"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_exists(&[b"h"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckHashOperationsOnWrongTypeObjectSE
#[test]
fn check_hash_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"str", b"plain"], batch, &mut out).unwrap();

    out.clear();
    s.hash_set(&[b"str", b"f", b"v"], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHLen
#[test]
fn can_do_hlen() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_length(&[b"myhash"], batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.hash_length(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoGetAll
#[test]
fn can_do_get_all() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_get_all(&[b"myhash"], batch, &mut out).unwrap();
    let mut items = parse_bulk_array(&out);
    items.sort();
    assert_eq!(
      items,
      vec![
        b"Hello".to_vec(),
        b"World".to_vec(),
        b"field1".to_vec(),
        b"field2".to_vec()
      ]
    );

    out.clear();
    s.hash_get_all(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHExists
#[test]
fn can_do_hexists() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_exists(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_exists(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.hash_exists(&[b"nokey", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHStrLen
#[test]
fn can_do_hstrlen() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_str_length(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":5\r\n");

    out.clear();
    s.hash_str_length(&[b"myhash", b"field2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHKeys
#[test]
fn can_do_hkeys() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_keys(&[b"myhash"], batch, &mut out, true).unwrap();
    let mut keys = parse_bulk_array(&out);
    keys.sort();
    assert_eq!(keys, vec![b"field1".to_vec(), b"field2".to_vec()]);

    out.clear();
    s.hash_keys(&[b"nokey"], batch, &mut out, true).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHVals
#[test]
fn can_do_hvals() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_vals(&[b"myhash"], batch, &mut out).unwrap();
    let mut vals = parse_bulk_array(&out);
    vals.sort();
    assert_eq!(vals, vec![b"Hello".to_vec(), b"World".to_vec()]);
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHMGET
#[test]
fn can_do_hmget() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"myhash", b"field1", b"Hello", b"field2", b"World"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.hash_get_multiple(&[b"myhash", b"field1", b"nofield"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nHello\r\n$-1\r\n");

    out.clear();
    s.hash_get_multiple(&[b"nokey", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHSETNXCommand
#[test]
fn can_do_hsetnx_command() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set_nx(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.hash_set_nx(&[b"myhash", b"field1", b"World"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.hash_get(&[b"myhash", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$5\r\nHello\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHIncrBy
#[test]
fn can_do_hincr_by() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field", b"10"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":11\r\n");

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"-1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.hash_increment(&[b"myhash", b"field", b"-10"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CheckHashIncrementDoublePrecision
#[test]
fn check_hash_increment_double_precision() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_increment(&[b"mykey", b"field", b"10.5"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.5\r\n");

    out.clear();
    s.hash_increment(&[b"mykey", b"field", b"0.1"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$4\r\n10.6\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHashExpire
#[test]
fn can_do_hash_expire() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_expire(
      &[b"myhash", b"3600", b"FIELDS", b"1", b"field1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanFieldPersistAndGetTimeToLive
#[test]
fn can_field_persist_and_get_time_to_live() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"field1", b"Hello"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_expire(
      &[b"myhash", b"3600", b"FIELDS", b"1", b"field1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");

    out.clear();
    s.hash_time_to_live(
      &[b"myhash", b"FIELDS", b"1", b"field1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    let payload = String::from_utf8_lossy(&out);
    let ttl: i64 = payload
      .lines()
      .nth(1)
      .unwrap()
      .trim_start_matches(':')
      .parse()
      .unwrap();
    assert!((3590..=3600).contains(&ttl));

    out.clear();
    s.hash_persist(&[b"myhash", b"FIELDS", b"1", b"field1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHRANDFIELDCommandLC
#[test]
fn can_do_hrandfield_command_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"coin", b"heads", b"obverse"], batch, &mut out)
      .unwrap();

    out.clear();
    s.hash_random_field(&[b"coin"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nheads\r\n");

    out.clear();
    s.hash_random_field(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

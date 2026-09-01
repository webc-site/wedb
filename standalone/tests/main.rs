use aok::{OK, Void};
use std::sync::Arc;
use webc_cmd::{Cmd, ConnectionContext};
use wedb_embed::WeDb;
use wedb_resp::RespValue;
use wedb_standalone::{handle_cmd, handle_cmd_with_ctx};

#[ctor::ctor(unsafe)]
fn log_init() {
  log_init::init();
}

fn create_test_db() -> (tempfile::TempDir, Arc<WeDb>) {
  let dir = tempfile::tempdir().expect("create tempdir");
  let db = Arc::new(WeDb::open(dir.path().to_str().unwrap()).expect("open db"));
  (dir, db)
}

async fn exec(db: &Arc<WeDb>, raw_args: &[&str]) -> RespValue {
  let elements: Vec<RespValue> = raw_args
    .iter()
    .map(|s| RespValue::Blob(s.as_bytes().to_vec()))
    .collect();
  let resp = RespValue::Arr(elements);
  let cmd = Cmd::from_resp(&resp).expect("parse cmd");
  handle_cmd(db, cmd).await
}

async fn exec_ctx(db: &Arc<WeDb>, ctx: &mut ConnectionContext, raw_args: &[&str]) -> RespValue {
  let elements: Vec<RespValue> = raw_args
    .iter()
    .map(|s| RespValue::Blob(s.as_bytes().to_vec()))
    .collect();
  let resp = RespValue::Arr(elements);
  let cmd = Cmd::from_resp(&resp).expect("parse cmd");
  handle_cmd_with_ctx(db, ctx, cmd).await
}

#[compio::test]
async fn test_standalone_strings_and_increx() -> Void {
  let (_dir, db) = create_test_db();

  // SET & GET
  assert_eq!(exec(&db, &["SET", "k1", "v1"]).await, RespValue::ok());
  assert_eq!(
    exec(&db, &["GET", "k1"]).await,
    RespValue::Blob(b"v1".to_vec())
  );

  // APPEND & STRLEN
  assert_eq!(
    exec(&db, &["APPEND", "k1", "_ext"]).await,
    RespValue::Int(6)
  );
  assert_eq!(exec(&db, &["STRLEN", "k1"]).await, RespValue::Int(6));
  assert_eq!(
    exec(&db, &["GET", "k1"]).await,
    RespValue::Blob(b"v1_ext".to_vec())
  );

  // MSET & MGET
  assert_eq!(
    exec(&db, &["MSET", "a", "1", "b", "2", "c", "3"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["MGET", "a", "b", "c", "nonexist"]).await,
    RespValue::Arr(vec![
      RespValue::Blob(b"1".to_vec()),
      RespValue::Blob(b"2".to_vec()),
      RespValue::Blob(b"3".to_vec()),
      RespValue::Null,
    ])
  );

  // INCR, DECR, INCRBY, INCRBYFLOAT
  assert_eq!(exec(&db, &["SET", "num", "10"]).await, RespValue::ok());
  assert_eq!(exec(&db, &["INCR", "num"]).await, RespValue::Int(11));
  assert_eq!(exec(&db, &["DECR", "num"]).await, RespValue::Int(10));
  assert_eq!(exec(&db, &["INCRBY", "num", "5"]).await, RespValue::Int(15));
  assert_eq!(
    exec(&db, &["INCRBYFLOAT", "num", "2.5"]).await,
    RespValue::Blob(b"17.5".to_vec())
  );

  // INCREX with UBOUND & SATURATE
  assert_eq!(exec(&db, &["SET", "cnt", "10"]).await, RespValue::ok());
  assert_eq!(
    exec(&db, &["INCREX", "cnt", "BYINT", "5"]).await,
    RespValue::Arr(vec![RespValue::Int(15), RespValue::Int(5)])
  );
  assert_eq!(
    exec(
      &db,
      &["INCREX", "cnt", "BYINT", "100", "UBOUND", "20", "SATURATE"]
    )
    .await,
    RespValue::Arr(vec![RespValue::Int(20), RespValue::Int(5)])
  );

  // INCREX BYFLOAT
  assert_eq!(
    exec(&db, &["INCREX", "flt", "BYFLOAT", "3.14"]).await,
    RespValue::Arr(vec![
      RespValue::Blob(b"3.14".to_vec()),
      RespValue::Blob(b"3.14".to_vec())
    ])
  );

  // GETRANGE & SETRANGE
  assert_eq!(
    exec(&db, &["SET", "msg", "Hello World"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["GETRANGE", "msg", "0", "4"]).await,
    RespValue::Blob(b"Hello".to_vec())
  );
  assert_eq!(
    exec(&db, &["SETRANGE", "msg", "6", "Redis"]).await,
    RespValue::Int(11)
  );
  assert_eq!(
    exec(&db, &["GET", "msg"]).await,
    RespValue::Blob(b"Hello Redis".to_vec())
  );

  // SETEX & SETNX
  assert_eq!(
    exec(&db, &["SETEX", "ex_k", "60", "ex_v"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["GET", "ex_k"]).await,
    RespValue::Blob(b"ex_v".to_vec())
  );
  assert_eq!(
    exec(&db, &["SETNX", "ex_k", "new_val"]).await,
    RespValue::Int(0)
  );
  assert_eq!(
    exec(&db, &["SETNX", "nx_brand_new", "val"]).await,
    RespValue::Int(1)
  );

  // GETSET & GETDEL
  assert_eq!(
    exec(&db, &["GETSET", "nx_brand_new", "val2"]).await,
    RespValue::Blob(b"val".to_vec())
  );
  assert_eq!(
    exec(&db, &["GETDEL", "nx_brand_new"]).await,
    RespValue::Blob(b"val2".to_vec())
  );
  assert_eq!(exec(&db, &["GET", "nx_brand_new"]).await, RespValue::Null);

  // MSETNX
  assert_eq!(
    exec(&db, &["MSETNX", "ex_k", "v1", "m_new", "v2"]).await,
    RespValue::Int(0)
  );

  // CAS & CAD
  assert_eq!(
    exec(&db, &["CAS", "cas_k", "old", "new"]).await,
    RespValue::Int(-1)
  );
  assert_eq!(exec(&db, &["SET", "cas_k", "old"]).await, RespValue::ok());
  assert_eq!(
    exec(&db, &["CAS", "cas_k", "wrong", "new"]).await,
    RespValue::Int(0)
  );
  assert_eq!(
    exec(&db, &["CAS", "cas_k", "old", "new"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["GET", "cas_k"]).await,
    RespValue::Blob(b"new".to_vec())
  );
  assert_eq!(exec(&db, &["CAD", "cas_k", "new"]).await, RespValue::Int(1));
  assert_eq!(exec(&db, &["GET", "cas_k"]).await, RespValue::Null);

  OK
}

#[compio::test]
async fn test_standalone_bitmaps_and_bitfield() -> Void {
  let (_dir, db) = create_test_db();

  // SETBIT & GETBIT
  assert_eq!(
    exec(&db, &["SETBIT", "bm", "7", "1"]).await,
    RespValue::Int(0)
  );
  assert_eq!(exec(&db, &["GETBIT", "bm", "7"]).await, RespValue::Int(1));
  assert_eq!(exec(&db, &["GETBIT", "bm", "6"]).await, RespValue::Int(0));
  assert_eq!(exec(&db, &["BITCOUNT", "bm"]).await, RespValue::Int(1));

  // BITOP
  assert_eq!(exec(&db, &["SET", "b1", "foobar"]).await, RespValue::ok());
  assert_eq!(exec(&db, &["SET", "b2", "abcdef"]).await, RespValue::ok());
  let bitop_res = exec(&db, &["BITOP", "AND", "dest", "b1", "b2"]).await;
  assert_eq!(bitop_res, RespValue::Int(6));

  // BITFIELD (SET, GET, INCRBY)
  let bf_res = exec(
    &db,
    &[
      "BITFIELD", "bf", "SET", "u8", "0", "200", "GET", "u8", "0", "OVERFLOW", "SAT", "INCRBY",
      "u8", "0", "100",
    ],
  )
  .await;
  assert_eq!(
    bf_res,
    RespValue::Arr(vec![
      RespValue::Int(0),
      RespValue::Int(200),
      RespValue::Int(255)
    ])
  );

  // BITFIELD_RO
  let bf_ro = exec(&db, &["BITFIELD_RO", "bf", "GET", "u8", "0"]).await;
  assert_eq!(bf_ro, RespValue::Arr(vec![RespValue::Int(255)]));

  OK
}

#[compio::test]
async fn test_standalone_hashes_and_hgetdel() -> Void {
  let (_dir, db) = create_test_db();

  // HSET & HGET & HEXISTS
  assert_eq!(
    exec(&db, &["HSET", "h1", "f1", "v1", "f2", "v2", "f3", "v3"]).await,
    RespValue::Int(3)
  );
  assert_eq!(
    exec(&db, &["HGET", "h1", "f1"]).await,
    RespValue::Blob(b"v1".to_vec())
  );
  assert_eq!(exec(&db, &["HEXISTS", "h1", "f1"]).await, RespValue::Int(1));
  assert_eq!(exec(&db, &["HLEN", "h1"]).await, RespValue::Int(3));

  // HSETNX & HSTRLEN
  assert_eq!(
    exec(&db, &["HSETNX", "h1", "f1", "new_v"]).await,
    RespValue::Int(0)
  );
  assert_eq!(
    exec(&db, &["HSETNX", "h1", "f_new", "v_new"]).await,
    RespValue::Int(1)
  );
  assert_eq!(exec(&db, &["HSTRLEN", "h1", "f1"]).await, RespValue::Int(2));

  // HMGET & HGETALL
  let hmget_res = exec(&db, &["HMGET", "h1", "f1", "f2", "nonexist"]).await;
  assert_eq!(
    hmget_res,
    RespValue::Arr(vec![
      RespValue::Blob(b"v1".to_vec()),
      RespValue::Blob(b"v2".to_vec()),
      RespValue::Null
    ])
  );

  // HINCRBY & HINCRBYFLOAT
  assert_eq!(
    exec(&db, &["HSET", "h1", "counter", "10"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["HINCRBY", "h1", "counter", "5"]).await,
    RespValue::Int(15)
  );
  assert_eq!(
    exec(&db, &["HINCRBYFLOAT", "h1", "counter", "2.5"]).await,
    RespValue::Blob(b"17.5".to_vec())
  );

  // HGETDEL single & multiple fields
  assert_eq!(
    exec(&db, &["HGETDEL", "h1", "f1"]).await,
    RespValue::Blob(b"v1".to_vec())
  );
  assert_eq!(exec(&db, &["HEXISTS", "h1", "f1"]).await, RespValue::Int(0));

  let hgd_multi = exec(&db, &["HGETDEL", "h1", "FIELDS", "2", "f2", "f3"]).await;
  assert_eq!(
    hgd_multi,
    RespValue::Arr(vec![
      RespValue::Blob(b"v2".to_vec()),
      RespValue::Blob(b"v3".to_vec())
    ])
  );

  // HRANDFIELD
  let rand_f = exec(&db, &["HRANDFIELD", "h1"]).await;
  match rand_f {
    RespValue::Blob(_) => {}
    _ => panic!("Expected blob from HRANDFIELD"),
  }

  OK
}

#[compio::test]
async fn test_standalone_lists_and_sort() -> Void {
  let (_dir, db) = create_test_db();

  // LPUSH & RPUSH & LRANGE
  assert_eq!(
    exec(&db, &["RPUSH", "l1", "10", "5", "30", "20"]).await,
    RespValue::Int(4)
  );
  assert_eq!(exec(&db, &["LLEN", "l1"]).await, RespValue::Int(4));
  assert_eq!(
    exec(&db, &["LINDEX", "l1", "0"]).await,
    RespValue::Blob(b"10".to_vec())
  );

  // LPUSHX & RPUSHX
  assert_eq!(exec(&db, &["LPUSHX", "l1", "1"]).await, RespValue::Int(5));
  assert_eq!(
    exec(&db, &["LPUSHX", "nonexist_list", "1"]).await,
    RespValue::Int(0)
  );

  // LSET & LTRIM
  assert_eq!(
    exec(&db, &["LSET", "l1", "0", "100"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["LINDEX", "l1", "0"]).await,
    RespValue::Blob(b"100".to_vec())
  );

  // LINSERT & LREM
  assert_eq!(
    exec(&db, &["LINSERT", "l1", "BEFORE", "5", "7"]).await,
    RespValue::Int(6)
  );
  assert_eq!(
    exec(&db, &["LREM", "l1", "1", "7"]).await,
    RespValue::Int(1)
  );

  // SORT numeric ascending & descending with limit
  assert_eq!(
    exec(&db, &["RPUSH", "sort_list", "10", "5", "30", "20"]).await,
    RespValue::Int(4)
  );
  let sort_asc = exec(&db, &["SORT", "sort_list"]).await;
  assert_eq!(
    sort_asc,
    RespValue::Arr(vec![
      RespValue::Blob(b"5".to_vec()),
      RespValue::Blob(b"10".to_vec()),
      RespValue::Blob(b"20".to_vec()),
      RespValue::Blob(b"30".to_vec()),
    ])
  );

  let sort_desc_lim = exec(&db, &["SORT", "sort_list", "DESC", "LIMIT", "0", "2"]).await;
  assert_eq!(
    sort_desc_lim,
    RespValue::Arr(vec![
      RespValue::Blob(b"30".to_vec()),
      RespValue::Blob(b"20".to_vec()),
    ])
  );

  // SORT_RO
  let sort_ro = exec(&db, &["SORT_RO", "sort_list"]).await;
  assert_eq!(sort_ro, sort_asc);

  // LMOVE
  assert_eq!(
    exec(&db, &["LMOVE", "sort_list", "target_list", "LEFT", "RIGHT"]).await,
    RespValue::Blob(b"10".to_vec())
  );
  assert_eq!(
    exec(&db, &["LINDEX", "target_list", "0"]).await,
    RespValue::Blob(b"10".to_vec())
  );

  OK
}

#[compio::test]
async fn test_standalone_sets_and_algebra() -> Void {
  let (_dir, db) = create_test_db();

  assert_eq!(
    exec(&db, &["SADD", "sa", "1", "2", "3", "4"]).await,
    RespValue::Int(4)
  );
  assert_eq!(
    exec(&db, &["SADD", "sb", "3", "4", "5", "6"]).await,
    RespValue::Int(4)
  );

  assert_eq!(exec(&db, &["SCARD", "sa"]).await, RespValue::Int(4));
  assert_eq!(
    exec(&db, &["SISMEMBER", "sa", "1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["SISMEMBER", "sa", "9"]).await,
    RespValue::Int(0)
  );

  // SDIFFCARD & SUNIONCARD & SINTERCARD
  assert_eq!(
    exec(&db, &["SDIFFCARD", "2", "sa", "sb"]).await,
    RespValue::Int(2)
  );
  assert_eq!(
    exec(&db, &["SDIFFCARD", "2", "sa", "sb", "LIMIT", "1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["SUNIONCARD", "2", "sa", "sb"]).await,
    RespValue::Int(6)
  );
  assert_eq!(
    exec(&db, &["SUNIONCARD", "2", "sa", "sb", "LIMIT", "3"]).await,
    RespValue::Int(3)
  );
  assert_eq!(
    exec(&db, &["SINTERCARD", "2", "sa", "sb"]).await,
    RespValue::Int(2)
  );

  // SSCAN
  let scan_res = exec(&db, &["SSCAN", "sa", "0"]).await;
  match scan_res {
    RespValue::Arr(arr) => {
      assert_eq!(arr.len(), 2);
    }
    _ => panic!("Expected array from SSCAN"),
  }

  OK
}

#[compio::test]
async fn test_standalone_zset_and_options() -> Void {
  let (_dir, db) = create_test_db();

  // ZADD & ZSCORE
  assert_eq!(
    exec(&db, &["ZADD", "zs", "100.5", "m1", "200.0", "m2"]).await,
    RespValue::Int(2)
  );
  assert_eq!(
    exec(&db, &["ZSCORE", "zs", "m1"]).await,
    RespValue::Blob(b"100.5".to_vec())
  );
  assert_eq!(exec(&db, &["ZCARD", "zs"]).await, RespValue::Int(2));

  // ZMSCORE
  let zmscore_res = exec(&db, &["ZMSCORE", "zs", "m1", "m2", "nonexist"]).await;
  assert_eq!(
    zmscore_res,
    RespValue::Arr(vec![
      RespValue::Blob(b"100.5".to_vec()),
      RespValue::Blob(b"200".to_vec()),
      RespValue::Null,
    ])
  );

  // ZINCRBY
  assert_eq!(
    exec(&db, &["ZINCRBY", "zs", "50.5", "m1"]).await,
    RespValue::Blob(b"151".to_vec())
  );

  // ZCOUNT
  assert_eq!(
    exec(&db, &["ZCOUNT", "zs", "100", "200"]).await,
    RespValue::Int(2)
  );

  // ZRANK & ZREVRANK
  assert_eq!(exec(&db, &["ZRANK", "zs", "m1"]).await, RespValue::Int(0));
  assert_eq!(
    exec(&db, &["ZREVRANK", "zs", "m1"]).await,
    RespValue::Int(1)
  );

  // ZPOPMIN & ZPOPMAX
  let popmin = exec(&db, &["ZPOPMIN", "zs", "1"]).await;
  assert_eq!(
    popmin,
    RespValue::Arr(vec![
      RespValue::Blob(b"m1".to_vec()),
      RespValue::Blob(b"151".to_vec()),
    ])
  );

  // ZREM
  assert_eq!(exec(&db, &["ZREM", "zs", "m2"]).await, RespValue::Int(1));
  assert_eq!(exec(&db, &["ZCARD", "zs"]).await, RespValue::Int(0));

  OK
}

#[compio::test]
async fn test_standalone_keys_and_lifecycle() -> Void {
  let (_dir, db) = create_test_db();

  assert_eq!(
    exec(&db, &["SET", "my_key", "my_val"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["TYPE", "my_key"]).await,
    RespValue::Simple("string".to_string())
  );
  assert_eq!(exec(&db, &["EXISTS", "my_key"]).await, RespValue::Int(1));

  // TTL & EXPIRE & PERSIST
  assert_eq!(exec(&db, &["TTL", "my_key"]).await, RespValue::Int(-1));
  assert_eq!(
    exec(&db, &["EXPIRE", "my_key", "100"]).await,
    RespValue::Int(1)
  );
  let ttl_val = exec(&db, &["TTL", "my_key"]).await;
  match ttl_val {
    RespValue::Int(n) => assert!(n > 0 && n <= 100),
    _ => panic!("Expected positive int for TTL"),
  }
  assert_eq!(exec(&db, &["PERSIST", "my_key"]).await, RespValue::Int(1));
  assert_eq!(exec(&db, &["TTL", "my_key"]).await, RespValue::Int(-1));

  // KEYS & DBSIZE
  assert_eq!(exec(&db, &["SET", "my_key2", "v2"]).await, RespValue::ok());
  assert_eq!(exec(&db, &["DBSIZE"]).await, RespValue::Int(2));
  let keys_res = exec(&db, &["KEYS", "my_*"]).await;
  match keys_res {
    RespValue::Arr(arr) => assert_eq!(arr.len(), 2),
    _ => panic!("Expected array from KEYS"),
  }

  // RENAME & COPY
  assert_eq!(
    exec(&db, &["RENAME", "my_key", "renamed_key"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["GET", "renamed_key"]).await,
    RespValue::Blob(b"my_val".to_vec())
  );
  assert_eq!(
    exec(&db, &["COPY", "renamed_key", "copied_key"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["GET", "copied_key"]).await,
    RespValue::Blob(b"my_val".to_vec())
  );

  // DEL & UNLINK
  assert_eq!(
    exec(&db, &["DEL", "renamed_key", "copied_key"]).await,
    RespValue::Int(2)
  );
  assert_eq!(
    exec(&db, &["EXISTS", "renamed_key"]).await,
    RespValue::Int(0)
  );

  OK
}

#[compio::test]
async fn test_standalone_geo() -> Void {
  let (_dir, db) = create_test_db();

  // GEOADD
  assert_eq!(
    exec(
      &db,
      &[
        "GEOADD",
        "Sicily",
        "13.361389",
        "38.115556",
        "Palermo",
        "15.087269",
        "37.502669",
        "Catania"
      ]
    )
    .await,
    RespValue::Int(2)
  );

  // GEODIST
  let dist = exec(&db, &["GEODIST", "Sicily", "Palermo", "Catania", "km"]).await;
  match dist {
    RespValue::Blob(b) => {
      let s = String::from_utf8(b).unwrap();
      let d = s.parse::<f64>().unwrap();
      assert!(d > 160.0 && d < 170.0);
    }
    _ => panic!("Expected blob from GEODIST"),
  }

  // GEOHASH & GEOPOS
  let geohash = exec(&db, &["GEOHASH", "Sicily", "Palermo"]).await;
  match geohash {
    RespValue::Arr(arr) => assert_eq!(arr.len(), 1),
    _ => panic!("Expected array from GEOHASH"),
  }

  let geopos = exec(&db, &["GEOPOS", "Sicily", "Palermo"]).await;
  match geopos {
    RespValue::Arr(arr) => assert_eq!(arr.len(), 1),
    _ => panic!("Expected array from GEOPOS"),
  }

  OK
}

#[compio::test]
async fn test_standalone_json_and_probabilistic() -> Void {
  let (_dir, db) = create_test_db();

  // JSON.SET & JSON.GET
  assert_eq!(
    exec(
      &db,
      &["JSON.SET", "doc", "$", r#"{"name":"Alice","age":30}"#]
    )
    .await,
    RespValue::ok()
  );
  let json_get = exec(&db, &["JSON.GET", "doc", "$.name"]).await;
  assert_eq!(json_get, RespValue::Blob(b"[\"Alice\"]".to_vec()));

  // JSON.NUMINCRBY
  assert_eq!(
    exec(&db, &["JSON.NUMINCRBY", "doc", "$.age", "5"]).await,
    RespValue::Blob(b"[35]".to_vec())
  );

  // BF.ADD & BF.EXISTS
  assert_eq!(
    exec(&db, &["BF.ADD", "bf", "item1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["BF.EXISTS", "bf", "item1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["BF.EXISTS", "bf", "nonexist"]).await,
    RespValue::Int(0)
  );

  // CF.ADD & CF.EXISTS & CF.DEL
  assert_eq!(
    exec(&db, &["CF.ADD", "cf", "c_item1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["CF.EXISTS", "cf", "c_item1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["CF.DEL", "cf", "c_item1"]).await,
    RespValue::Int(1)
  );
  assert_eq!(
    exec(&db, &["CF.EXISTS", "cf", "c_item1"]).await,
    RespValue::Int(0)
  );

  // PFADD & PFCOUNT
  assert_eq!(
    exec(&db, &["PFADD", "hll", "apple", "banana", "cherry"]).await,
    RespValue::Int(1)
  );
  assert_eq!(exec(&db, &["PFCOUNT", "hll"]).await, RespValue::Int(3));

  // TDIGEST.CREATE & TDIGEST.ADD & TDIGEST.QUANTILE
  assert_eq!(exec(&db, &["TDIGEST.CREATE", "td"]).await, RespValue::ok());
  assert_eq!(
    exec(&db, &["TDIGEST.ADD", "td", "10", "20", "30", "40", "50"]).await,
    RespValue::ok()
  );
  let q_res = exec(&db, &["TDIGEST.QUANTILE", "td", "0.5"]).await;
  match q_res {
    RespValue::Arr(arr) => {
      assert_eq!(arr.len(), 1);
    }
    _ => panic!("Expected array from TDIGEST.QUANTILE"),
  }

  OK
}

#[compio::test]
async fn test_standalone_stream_and_timeseries() -> Void {
  let (_dir, db) = create_test_db();

  // XADD & XLEN & XRANGE
  let xadd_res = exec(
    &db,
    &["XADD", "mystream", "*", "sensor-id", "1234", "temp", "19.8"],
  )
  .await;
  match xadd_res {
    RespValue::Blob(_) => {}
    _ => panic!("Expected blob ID from XADD"),
  }
  assert_eq!(exec(&db, &["XLEN", "mystream"]).await, RespValue::Int(1));

  let xrange_res = exec(&db, &["XRANGE", "mystream", "-", "+"]).await;
  match xrange_res {
    RespValue::Arr(arr) => assert_eq!(arr.len(), 1),
    _ => panic!("Expected array from XRANGE"),
  }

  // TS.CREATE & TS.ADD & TS.GET
  assert_eq!(
    exec(&db, &["TS.CREATE", "temperature"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec(&db, &["TS.ADD", "temperature", "1000", "25.5"]).await,
    RespValue::Int(1000)
  );
  let ts_get = exec(&db, &["TS.GET", "temperature"]).await;
  assert_eq!(
    ts_get,
    RespValue::Arr(vec![
      RespValue::Int(1000),
      RespValue::Blob(b"25.5".to_vec())
    ])
  );

  // SI.ADD & SI.CARD & SI.EXISTS
  assert_eq!(
    exec(&db, &["SI.ADD", "si_key", "10", "20", "30"]).await,
    RespValue::Int(3)
  );
  assert_eq!(exec(&db, &["SI.CARD", "si_key"]).await, RespValue::Int(3));
  assert_eq!(
    exec(&db, &["SI.EXISTS", "si_key", "20"]).await,
    RespValue::Arr(vec![RespValue::Int(1)])
  );

  OK
}

#[compio::test]
async fn test_standalone_transactions_and_multi() -> Void {
  let (_dir, db) = create_test_db();
  let mut ctx = ConnectionContext::default();

  // MULTI queueing
  assert_eq!(exec_ctx(&db, &mut ctx, &["MULTI"]).await, RespValue::ok());
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["SET", "tx_k", "tx_v"]).await,
    RespValue::queued()
  );
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["GET", "tx_k"]).await,
    RespValue::queued()
  );

  // EXEC
  let exec_res = exec_ctx(&db, &mut ctx, &["EXEC"]).await;
  assert_eq!(
    exec_res,
    RespValue::Arr(vec![RespValue::ok(), RespValue::Blob(b"tx_v".to_vec()),])
  );

  // DISCARD
  assert_eq!(exec_ctx(&db, &mut ctx, &["MULTI"]).await, RespValue::ok());
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["SET", "tx_k2", "val"]).await,
    RespValue::queued()
  );
  assert_eq!(exec_ctx(&db, &mut ctx, &["DISCARD"]).await, RespValue::ok());
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["GET", "tx_k2"]).await,
    RespValue::Null
  );

  OK
}

#[compio::test]
async fn test_standalone_conn_and_namespace() -> Void {
  let (_dir, db) = create_test_db();
  let mut ctx = ConnectionContext::default();

  // PING & ECHO & HELLO & TIME
  assert_eq!(exec_ctx(&db, &mut ctx, &["PING"]).await, RespValue::pong());
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["ECHO", "hello"]).await,
    RespValue::Blob(b"hello".to_vec())
  );
  let hello_res = exec_ctx(&db, &mut ctx, &["HELLO"]).await;
  match hello_res {
    RespValue::Map(_) => {}
    _ => panic!("Expected map from HELLO"),
  }
  let time_res = exec_ctx(&db, &mut ctx, &["TIME"]).await;
  match time_res {
    RespValue::Arr(arr) => assert_eq!(arr.len(), 2),
    _ => panic!("Expected array of 2 from TIME"),
  }

  // NAMESPACE ADD & GET & DEL
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["NAMESPACE", "ADD", "tenant1", "token123"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["NAMESPACE", "GET", "tenant1"]).await,
    RespValue::Blob(b"token123".to_vec())
  );
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["AUTH", "token123"]).await,
    RespValue::ok()
  );
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["NAMESPACE", "CURRENT"]).await,
    RespValue::Blob(b"tenant1".to_vec())
  );
  assert_eq!(
    exec_ctx(&db, &mut ctx, &["NAMESPACE", "DEL", "tenant1"]).await,
    RespValue::ok()
  );

  OK
}

pub mod bloom;
pub mod cluster;
pub mod conn;
pub mod geo;
pub mod hash;
pub mod hll;
pub mod json;
pub mod key;
pub mod list;
pub mod pubsub;
pub mod script;
pub mod search;
pub mod set;
pub mod sortedint;
pub mod stream;
pub mod string;
pub mod tdigest;
pub mod timeseries;
pub mod txn;
pub mod util;
pub mod zset;

pub use util::*;

use std::str::from_utf8;
use wedb_embed::{Error, Result};
use wedb_resp::{RespBorrow, RespValue};

use crate::types::Cmd;

/// 辅助宏：将点号标识符（如 `bf.add`）或普通标识符在编译期转换为字符串常量
macro_rules! cmd_str {
  ($a:ident . $b:ident) => {
    concat!(stringify!($a), ".", stringify!($b))
  };
  ($a:ident) => {
    stringify!($a)
  };
}

/// 声明式命令分发宏：支持自然标识符语法（如 `string: get | set | ...`）
macro_rules! dispatch_commands {
    ($(
        $module:ident: $($($part:ident).+)|*
    ),* $(,)?) => {
        #[inline]
        pub fn parse_command(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
            match cmd_name {
                $(
                    $( cmd_str!($($part).+) )|* => $module::parse(cmd_name, args),
                )*
                _ => Ok(None),
            }
        }
    };
}

// 统一声明式命令注册表：无引号无数组，自然清晰，编译期生成 O(1) 跳转表
dispatch_commands! {
    string:
        append | bitcount | bitfield | bitfield_ro | bitop | bitpos |
        cad | cas | decr | decrby | delex | digest | get | getbit |
        getdel | getex | getrange | getset | incr | incrby |
        incrbyfloat | increx | lcs | mget | mset | msetex | msetnx |
        psetex | set | setbit | setex | setnx | setrange | strlen |
        substr,

    hash:
        hdel | hexists | hexpire | hexpireat | hexpiretime | hget |
        hgetall | hgetdel | hgetex | hincrby | hincrbyfloat | hkeys |
        hlen | hmget | hmset | hpersist | hpexpire | hpexpireat |
        hpexpiretime | hpttl | hrandfield | hrangebylex | hscan | hset |
        hsetex | hsetexpire | hsetnx | hstrlen | httl | hvals,

    list:
        blmove | blmovem | blmpop | blpop | brpop | brpoplpush |
        lindex | linsert | llen | lmove | lmovem | lmpop | lpop |
        lpos | lpush | lpushx | lrange | lrem | lset | ltrim |
        rpop | rpoplpush | rpush | rpushx,

    set:
        sadd | scard | sdiff | sdiffcard | sdiffstore | sinter |
        sintercard | sinterstore | sismember | smembers | smismember |
        smove | spop | srandmember | srem | sscan | sunion |
        sunioncard | sunionstore,

    zset:
        bzmpop | bzpopmax | bzpopmin | zadd | zcard | zcount | zdiff |
        zdiffstore | zincrby | zinter | zintercard | zinterstore |
        zlexcount | zmpop | zmscore | zpopmax | zpopmin | zrandmember |
        zrange | zrangebylex | zrangebyscore | zrangestore | zrank |
        zrem | zremrangebylex | zremrangebyrank | zremrangebyscore |
        zrevrange | zrevrangebylex | zrevrangebyscore | zrevrank | zscan |
        zscore | zunion | zunionstore,

    geo:
        geoadd | geodist | geohash | geopos | georadius | georadius_ro |
        georadiusbymember | georadiusbymember_ro | geosearch | geosearchstore,

    hll:
        pfadd | pfcount | pfmerge | pfselftest,

    stream:
        xack | xackdel | xadd | xautoclaim | xclaim | xdel | xdelex |
        xgroup | xinfo | xlen | xnack | xpending | xrange | xread |
        xreadgroup | xrevrange | xsetid | xtrim,

    key:
        copy | dbsize | del | exists | expire | expireat | expiretime |
        flushall | flushdb | keys | kmetadata | object | persist |
        pexpire | pexpireat | pexpiretime | prefix | pttl | randomkey |
        rename | renamenx | scan | scanprefix | sort | sort_ro |
        touch | ttl | type | unlink,

    conn:
        applybatch | auth | bgsave | client | cmd | command | compact |
        config | debug | disk | dump | echo | flushbackup |
        flushblockcache | flushmemtable | hello | info | kprofile |
        lastsave | latency | memory | monitor | move | movex |
        namespace | perflog | ping | pollupdates | quit | rdb |
        replicaof | reset | restore | role | select | shutdown |
        slaveof | slowlog | sst | stats | swapdb | time,

    pubsub:
        mpublish | psubscribe | publish | pubsub | punsubscribe |
        spublish | ssubscribe | subscribe | sunsubscribe | unsubscribe,

    txn:
        discard | exec | multi | unwatch | watch,

    cluster:
        asking | batch | cluster | clusterx | psync | raft.batch | raft.health |
        raft.join | raft.leave | raft.members | raft.metrics | raft.purge |
        raft.snapshot | raft.snapshot_status | raft.status | raft.txn |
        readonly | readwrite | replconf | txn | wait,

    bloom:
        bf.add | bf.card | bf.exists | bf.info | bf.insert | bf.madd |
        bf.mexists | bf.reserve | cf.add | cf.addnx | cf.count | cf.del |
        cf.exists | cf.info | cf.insert | cf.insertnx | cf.mexists |
        cf.reserve,

    json:
        json.arrappend | json.arrindex | json.arrinsert | json.arrlen |
        json.arrpop | json.arrtrim | json.clear | json.debug | json.del |
        json.forget | json.get | json.info | json.merge | json.mget |
        json.mset | json.numincrby | json.nummultby | json.objkeys |
        json.objlen | json.resp | json.set | json.strappend | json.strlen |
        json.toggle | json.type,

    search:
        ft._list | ft.aliasadd | ft.aliasdel | ft.create | ft.dropindex |
        ft.explain | ft.explainsql | ft.info | ft.list | ft.search |
        ft.searchsql | ft.tagvals,

    timeseries:
        ts.add | ts.alter | ts.create | ts.createrule | ts.decrby | ts.del |
        ts.get | ts.incrby | ts.info | ts.madd | ts.mget | ts.mrange |
        ts.mrevrange | ts.queryindex | ts.range | ts.revrange,

    tdigest:
        tdigest.add | tdigest.byrank | tdigest.byrevrank | tdigest.cdf |
        tdigest.create | tdigest.info | tdigest.max | tdigest.merge |
        tdigest.min | tdigest.quantile | tdigest.rank | tdigest.reset |
        tdigest.revrank | tdigest.trimmed_mean,

    sortedint:
        siadd | sicard | siexists | sirange | sirangebyvalue | sirem |
        sirevrange | sirevrangebyvalue |
        si.add | si.card | si.exists | si.range | si.rangebyvalue | si.rem |
        si.revrange | si.revrangebyvalue,

    script:
        eval | eval_ro | evalsha | evalsha_ro | function | script,
}

impl Cmd {
  /// 从 RESP 数据解析出强类型的 Cmd（零堆分配 slice 借用分发）
  pub fn from_resp(val: &RespValue) -> Result<Self> {
    let mut args: Vec<&[u8]> = Vec::new();
    match val {
      RespValue::Arr(elements) => {
        args.reserve(elements.len());
        for e in elements {
          match e {
            RespValue::Blob(b) => args.push(b.as_slice()),
            RespValue::Simple(s) => args.push(s.as_bytes()),
            _ => {}
          }
        }
      }
      RespValue::Blob(b) => args.push(b.as_slice()),
      RespValue::Simple(s) => args.push(s.as_bytes()),
      _ => return Err(Error::invalid_data("ERR protocol error")),
    }

    Self::from_args(&args)
  }

  /// 从借用型 RESP 视图解析 Cmd（零堆分配）
  pub fn from_borrow(val: &RespBorrow<'_>) -> Result<Self> {
    let mut args: Vec<&[u8]> = Vec::new();
    match val {
      RespBorrow::Arr(elements) => {
        args.reserve(elements.len());
        for e in elements {
          match e {
            RespBorrow::Blob(b) => args.push(b),
            RespBorrow::Simple(s) | RespBorrow::Error(s) => args.push(s.as_bytes()),
            _ => {}
          }
        }
      }
      RespBorrow::Blob(b) => args.push(b),
      RespBorrow::Simple(s) | RespBorrow::Error(s) => args.push(s.as_bytes()),
      _ => return Err(Error::invalid_data("ERR protocol error")),
    }

    Self::from_args(&args)
  }

  /// 从字节切片数组解析 Cmd
  pub fn from_args(args: &[&[u8]]) -> Result<Self> {
    if args.is_empty() {
      return Err(Error::invalid_data("ERR empty cmd"));
    }

    let cmd_name = from_utf8(args[0])
      .map_err(|_| Error::invalid_data("ERR cmd name must be valid utf-8"))?
      .to_ascii_lowercase();

    if let Some(cmd) = parse_command(&cmd_name, args)? {
      Ok(cmd)
    } else {
      Err(Error::invalid_data(format!(
        "ERR unknown command '{cmd_name}'"
      )))
    }
  }
}

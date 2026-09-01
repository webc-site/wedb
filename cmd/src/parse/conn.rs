use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: PING [message]
    "ping" => {
      if args.len() > 1 {
        Ok(Cmd::Ping(Some(arg_string(args[1]))))
      } else {
        Ok(Cmd::Ping(None))
      }
    }
    // @cmd: ECHO message
    "echo" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Echo(arg_string(args[1])))
    }
    // @cmd: INFO [section]
    "info" => {
      let section = if args.len() > 1 {
        Some(arg_string(args[1]))
      } else {
        None
      };
      Ok(Cmd::Info(section))
    }
    // @cmd: ROLE
    "role" => Ok(Cmd::Role),
    // @cmd: SELECT index
    "select" => {
      check_min_args(cmd_name, args, 2)?;
      let db = parse_u64_strict(args[1])?;
      Ok(Cmd::Select(db))
    }
    // @cmd: COMMAND / CMD
    "command" | "cmd" => Ok(Cmd::Command),
    // @cmd: CONFIG GET / SET
    "config" => {
      check_min_args(cmd_name, args, 2)?;
      let sub = args[1];
      if sub.eq_ignore_ascii_case(b"get") {
        let param = if args.len() > 2 {
          arg_string(args[2])
        } else {
          "*".to_string()
        };
        Ok(Cmd::ConfigGet(param))
      } else if sub.eq_ignore_ascii_case(b"set") && args.len() >= 4 {
        Ok(Cmd::ConfigSet(arg_string(args[2]), arg_string(args[3])))
      } else {
        Ok(Cmd::ConfigGet("*".to_string()))
      }
    }
    // @cmd: TIME
    "time" => Ok(Cmd::Time),
    // @cmd: CLIENT subcmd
    "client" => {
      if args.len() < 2 {
        return Ok(Some(Cmd::ClientList));
      }
      let sub = args[1];
      if sub.eq_ignore_ascii_case(b"id") {
        Ok(Cmd::ClientId)
      } else if sub.eq_ignore_ascii_case(b"getname") {
        Ok(Cmd::ClientGetName)
      } else if sub.eq_ignore_ascii_case(b"setname") && args.len() >= 3 {
        Ok(Cmd::ClientSetName(arg_string(args[2])))
      } else if sub.eq_ignore_ascii_case(b"list") {
        Ok(Cmd::ClientList)
      } else if sub.eq_ignore_ascii_case(b"info") {
        Ok(Cmd::ClientInfo)
      } else if sub.eq_ignore_ascii_case(b"kill") {
        let filter = args[2..].iter().map(|a| arg_string(a)).collect();
        Ok(Cmd::ClientKill(filter))
      } else if sub.eq_ignore_ascii_case(b"pause") {
        let timeout = if args.len() >= 3 {
          arg_u64(args[2]).unwrap_or(0)
        } else {
          0
        };
        Ok(Cmd::ClientPause(timeout))
      } else if sub.eq_ignore_ascii_case(b"unpause") {
        Ok(Cmd::ClientUnpause)
      } else if sub.eq_ignore_ascii_case(b"unblock") {
        let id = if args.len() >= 3 {
          arg_i64(args[2]).unwrap_or(0)
        } else {
          0
        };
        let unblock_type = if args.len() >= 4 {
          Some(arg_string(args[3]))
        } else {
          None
        };
        Ok(Cmd::ClientUnblock { id, unblock_type })
      } else if sub.eq_ignore_ascii_case(b"tracking") {
        let enable = args.len() >= 3 && args[2].eq_ignore_ascii_case(b"on");
        let mut client_id = None;
        let mut prefixes = Vec::new();
        let mut bcast = false;
        let mut optin = false;
        let mut optout = false;
        let mut noloop = false;
        let mut i = 3;
        while i < args.len() {
          let opt = args[i];
          if opt.eq_ignore_ascii_case(b"redirect") && i + 1 < args.len() {
            client_id = arg_i64(args[i + 1]);
            i += 2;
          } else if opt.eq_ignore_ascii_case(b"prefix") && i + 1 < args.len() {
            prefixes.push(arg_string(args[i + 1]));
            i += 2;
          } else if opt.eq_ignore_ascii_case(b"bcast") {
            bcast = true;
            i += 1;
          } else if opt.eq_ignore_ascii_case(b"optin") {
            optin = true;
            i += 1;
          } else if opt.eq_ignore_ascii_case(b"optout") {
            optout = true;
            i += 1;
          } else if opt.eq_ignore_ascii_case(b"noloop") {
            noloop = true;
            i += 1;
          } else {
            i += 1;
          }
        }
        Ok(Cmd::ClientTracking {
          enable,
          client_id,
          prefixes,
          bcast,
          optin,
          optout,
          noloop,
        })
      } else if sub.eq_ignore_ascii_case(b"trackinginfo") {
        Ok(Cmd::ClientTrackingInfo)
      } else if sub.eq_ignore_ascii_case(b"getredir") {
        Ok(Cmd::ClientGetRedir)
      } else if sub.eq_ignore_ascii_case(b"setinfo") && args.len() >= 4 {
        Ok(Cmd::ClientSetInfo(arg_string(args[2]), arg_string(args[3])))
      } else if sub.eq_ignore_ascii_case(b"no-touch") || sub.eq_ignore_ascii_case(b"notouch") {
        let on = args.len() >= 3 && args[2].eq_ignore_ascii_case(b"on");
        Ok(Cmd::ClientNoTouch(on))
      } else if sub.eq_ignore_ascii_case(b"no-evict") || sub.eq_ignore_ascii_case(b"noevict") {
        let on = args.len() >= 3 && args[2].eq_ignore_ascii_case(b"on");
        Ok(Cmd::ClientNoEvict(on))
      } else if sub.eq_ignore_ascii_case(b"reply") && args.len() >= 3 {
        Ok(Cmd::ClientReply(arg_string(args[2])))
      } else if sub.eq_ignore_ascii_case(b"help") {
        Ok(Cmd::ClientHelp)
      } else {
        Ok(Cmd::ClientList)
      }
    }
    // @cmd: HELLO [protover]
    "hello" => {
      let protover = if args.len() >= 2 {
        arg_u8(args[1])
      } else {
        None
      };
      Ok(Cmd::Hello(protover))
    }
    // @cmd: QUIT
    "quit" => Ok(Cmd::Quit),
    // @cmd: AUTH [username] password
    "auth" => {
      check_min_args(cmd_name, args, 2)?;
      if args.len() >= 3 {
        Ok(Cmd::Auth {
          username: Some(arg_string(args[1])),
          password: arg_string(args[2]),
        })
      } else {
        Ok(Cmd::Auth {
          username: None,
          password: arg_string(args[1]),
        })
      }
    }
    // @cmd: NAMESPACE <ADD | SET | DEL | GET | CURRENT>
    "namespace" => {
      check_min_args(cmd_name, args, 2)?;
      let sub = args[1];
      if sub.eq_ignore_ascii_case(b"add") && args.len() >= 4 {
        Ok(Cmd::NamespaceAdd(arg_string(args[2]), arg_string(args[3])))
      } else if sub.eq_ignore_ascii_case(b"set") && args.len() >= 4 {
        Ok(Cmd::NamespaceSet(arg_string(args[2]), arg_string(args[3])))
      } else if sub.eq_ignore_ascii_case(b"del") && args.len() >= 3 {
        Ok(Cmd::NamespaceDel(arg_string(args[2])))
      } else if sub.eq_ignore_ascii_case(b"get") && args.len() >= 3 {
        Ok(Cmd::NamespaceGet(arg_string(args[2])))
      } else if sub.eq_ignore_ascii_case(b"id") {
        let ns = if args.len() >= 3 {
          Some(arg_string(args[2]))
        } else {
          None
        };
        Ok(Cmd::NamespaceId(ns))
      } else if sub.eq_ignore_ascii_case(b"rename") && args.len() >= 4 {
        Ok(Cmd::NamespaceRename(
          arg_string(args[2]),
          arg_string(args[3]),
        ))
      } else {
        Ok(Cmd::NamespaceCurrent)
      }
    }
    // @cmd: SWAPDB index1 index2
    "swapdb" => {
      check_min_args(cmd_name, args, 3)?;
      let db1 = parse_u64_strict(args[1])?;
      let db2 = parse_u64_strict(args[2])?;
      Ok(Cmd::SwapDb(db1, db2))
    }
    // @cmd: MOVE key db
    "move" => {
      check_min_args(cmd_name, args, 3)?;
      let db = parse_u64_strict(args[2])?;
      Ok(Cmd::Move(arg_string(args[1]), db))
    }
    // @cmd: MOVEX key target [REPLACE]
    "movex" => {
      check_min_args(cmd_name, args, 3)?;
      let replace = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"replace");
      Ok(Cmd::MoveX {
        key: arg_string(args[1]),
        target: arg_string(args[2]),
        replace,
      })
    }
    // @cmd: SLOWLOG
    "slowlog" => Ok(Cmd::Slowlog),
    // @cmd: MEMORY USAGE key
    "memory" => {
      if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"usage") {
        Ok(Cmd::MemoryUsage(arg_string(args[2])))
      } else {
        Ok(Cmd::MemoryUsage("*".to_string()))
      }
    }
    // @cmd: KPROFILE
    "kprofile" => Ok(Cmd::KProfile),
    // @cmd: PERFLOG
    "perflog" => Ok(Cmd::PerfLog),
    // @cmd: MONITOR
    "monitor" => Ok(Cmd::Monitor),
    // @cmd: SHUTDOWN
    "shutdown" => Ok(Cmd::Shutdown),
    // @cmd: DEBUG
    "debug" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Debug(list))
    }
    // @cmd: DISK
    "disk" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Disk(list))
    }
    // @cmd: COMPACT
    "compact" => Ok(Cmd::Compact),
    // @cmd: BGSAVE
    "bgsave" => Ok(Cmd::Bgsave),
    // @cmd: LASTSAVE
    "lastsave" => Ok(Cmd::Lastsave),
    // @cmd: FLUSHBACKUP
    "flushbackup" => Ok(Cmd::FlushBackup),
    // @cmd: SLAVEOF host port / REPLICAOF host port
    "slaveof" | "replicaof" => {
      check_min_args(cmd_name, args, 3)?;
      let host = arg_string(args[1]);
      let port = arg_u16(args[2]).unwrap_or(0);
      if cmd_name == "slaveof" {
        Ok(Cmd::SlaveOf(host, port))
      } else {
        Ok(Cmd::ReplicaOf(host, port))
      }
    }
    // @cmd: STATS
    "stats" => Ok(Cmd::Stats),
    // @cmd: RDB
    "rdb" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Rdb(list))
    }
    // @cmd: RESET
    "reset" => Ok(Cmd::Reset),
    // @cmd: APPLYBATCH payload
    "applybatch" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::ApplyBatch(args[1].to_vec()))
    }
    // @cmd: DUMP key
    "dump" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Dump(arg_string(args[1])))
    }
    // @cmd: RESTORE key ttl serialized-value [REPLACE]
    "restore" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let ttl = parse_u64_strict(args[2])?;
      let serialized = args[3].to_vec();
      let replace = args.len() >= 5 && args[4].eq_ignore_ascii_case(b"replace");
      Ok(Cmd::Restore {
        key,
        ttl,
        serialized,
        replace,
      })
    }
    // @cmd: POLLUPDATES
    "pollupdates" => Ok(Cmd::PollUpdates),
    // @cmd: SST
    "sst" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Sst(list))
    }
    // @cmd: FLUSHMEMTABLE
    "flushmemtable" => Ok(Cmd::FlushMemTable),
    // @cmd: FLUSHBLOCKCACHE
    "flushblockcache" => Ok(Cmd::FlushBlockCache),
    // @cmd: LATENCY
    "latency" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Latency(list))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

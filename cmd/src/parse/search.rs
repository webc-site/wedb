use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: FT.CREATE index [ON HASH|JSON] [PREFIX cnt pfx ...] SCHEMA fld <TEXT|NUMERIC|TAG|VECTOR HNSW ...> ...
    "ft.create" => {
      check_min_args(cmd_name, args, 2)?;
      let index = arg_string(args[1]);
      let mut on_data_type = "HASH".to_string();
      let mut prefixes = Vec::new();
      let mut fields = Vec::new();
      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"on") && i + 1 < args.len() {
          on_data_type = arg_str(args[i + 1]).to_ascii_uppercase();
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"prefix") && i + 1 < args.len() {
          let count = arg_usize(args[i + 1]).unwrap_or(1);
          i += 2;
          for _ in 0..count {
            if i < args.len() {
              prefixes.push(arg_string(args[i]));
              i += 1;
            }
          }
        } else if opt.eq_ignore_ascii_case(b"schema") {
          i += 1;
          while i < args.len() {
            fields.push(arg_string(args[i]));
            i += 1;
          }
        } else {
          fields.push(arg_string(opt));
          i += 1;
        }
      }
      Ok(Cmd::FtCreate {
        index,
        on_data_type,
        prefixes,
        fields,
      })
    }
    // @cmd: FT.SEARCH index query [NOCONTENT] [LIMIT off cnt] [SORTBY fld [ASC|DESC]] [PARAMS nargs k v ...]
    "ft.search" => {
      check_min_args(cmd_name, args, 3)?;
      let index = arg_string(args[1]);
      let query = arg_string(args[2]);
      let mut nocontent = false;
      let mut return_fields = None;
      let mut offset = 0;
      let mut limit = 10;
      let mut sortby = None;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"nocontent") {
          nocontent = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
          offset = arg_usize(args[i + 1]).unwrap_or(0);
          limit = arg_usize(args[i + 2]).unwrap_or(10);
          i += 3;
        } else if opt.eq_ignore_ascii_case(b"return") && i + 1 < args.len() {
          let count = arg_usize(args[i + 1]).unwrap_or(0);
          i += 2;
          let mut ret_list = Vec::new();
          for _ in 0..count {
            if i < args.len() {
              ret_list.push(arg_string(args[i]));
              i += 1;
            }
          }
          return_fields = Some(ret_list);
        } else if opt.eq_ignore_ascii_case(b"sortby") && i + 1 < args.len() {
          let field = arg_string(args[i + 1]);
          let is_asc = if i + 2 < args.len() && args[i + 2].eq_ignore_ascii_case(b"desc") {
            i += 3;
            false
          } else {
            if i + 2 < args.len() && args[i + 2].eq_ignore_ascii_case(b"asc") {
              i += 3;
            } else {
              i += 2;
            }
            true
          };
          sortby = Some((field, is_asc));
        } else {
          i += 1;
        }
      }

      Ok(Cmd::FtSearch {
        index,
        query,
        nocontent,
        return_fields,
        offset,
        limit,
        sortby,
      })
    }
    // @cmd: FT.SEARCHSQL index query
    "ft.searchsql" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::FtSearchSql {
        index: arg_string(args[1]),
        query: arg_string(args[2]),
      })
    }
    // @cmd: FT.EXPLAIN index query
    "ft.explain" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::FtExplain {
        index: arg_string(args[1]),
        query: arg_string(args[2]),
      })
    }
    // @cmd: FT.EXPLAINSQL index query
    "ft.explainsql" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::FtExplainSql {
        index: arg_string(args[1]),
        query: arg_string(args[2]),
      })
    }
    // @cmd: FT.INFO index
    "ft.info" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::FtInfo(arg_string(args[1])))
    }
    // @cmd: FT._LIST
    "ft.list" | "ft._list" => Ok(Cmd::FtList),
    // @cmd: FT.DROPINDEX index [DD]
    "ft.dropindex" => {
      check_min_args(cmd_name, args, 2)?;
      let index = arg_string(args[1]);
      let drop_docs = args.len() > 2 && args[2].eq_ignore_ascii_case(b"dd");
      Ok(Cmd::FtDropIndex { index, drop_docs })
    }
    // @cmd: FT.ALIASADD alias index
    "ft.aliasadd" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::FtAliasAdd {
        alias: arg_string(args[1]),
        index: arg_string(args[2]),
      })
    }
    // @cmd: FT.ALIASDEL alias
    "ft.aliasdel" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::FtAliasDel {
        alias: arg_string(args[1]),
      })
    }
    // @cmd: FT.TAGVALS index field_name
    "ft.tagvals" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::FtTagVals(arg_string(args[1]), arg_string(args[2])))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::{Error, Result};

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: BF.RESERVE key error_rate capacity [EXPANSION exp] [NONSCALING]
    "bf.reserve" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let error_rate =
        parse_float_strict(args[2]).map_err(|_| Error::invalid_data("Bad error rate"))?;
      if error_rate <= 0.0 || error_rate >= 1.0 {
        return Err(Error::invalid_data("error rate should be between 0 and 1"));
      }
      let capacity = arg_u32(args[3]).ok_or_else(|| Error::invalid_data("Bad capacity"))?;
      if capacity == 0 {
        return Err(Error::invalid_data("capacity should be larger than 0"));
      }

      let mut expansion = 2u16;
      let mut is_nonscaling = false;
      let mut has_expansion = false;
      let mut i = 4;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"nonscaling") {
          is_nonscaling = true;
          expansion = 0;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"expansion") {
          has_expansion = true;
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("Bad expansion"));
          }
          let exp = arg_u16(args[i]).ok_or_else(|| Error::invalid_data("Bad expansion"))?;
          if exp < 1 {
            return Err(Error::invalid_data(
              "expansion should be greater or equal to 1",
            ));
          }
          expansion = exp;
          i += 1;
        } else {
          return Err(err_syntax());
        }
      }

      if is_nonscaling && has_expansion {
        return Err(Error::invalid_data("nonscaling filters cannot expand"));
      }

      Ok(Cmd::BfReserve {
        key,
        error_rate,
        capacity,
        expansion,
        nonscaling: is_nonscaling,
      })
    }
    // @cmd: BF.ADD key item
    "bf.add" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::BfAdd(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: BF.MADD key item [item ...]
    "bf.madd" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let items = args[2..].iter().map(|item| item.to_vec()).collect();
      Ok(Cmd::BfMAdd(key, items))
    }
    // @cmd: BF.INSERT key [CAPACITY cap] [ERROR err] [EXPANSION exp] [NOCREATE] [NONSCALING] ITEMS item [item ...]
    "bf.insert" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut capacity = None;
      let mut error_rate = None;
      let mut expansion = None;
      let mut nocreate = false;
      let mut nonscaling = false;
      let mut has_expansion = false;
      let mut items = Vec::new();
      let mut has_items = false;

      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"capacity") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("Bad capacity"));
          }
          let cap = arg_u32(args[i]).ok_or_else(|| Error::invalid_data("Bad capacity"))?;
          if cap == 0 {
            return Err(Error::invalid_data("capacity should be larger than 0"));
          }
          capacity = Some(cap);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"error") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("Bad error rate"));
          }
          let err =
            parse_float_strict(args[i]).map_err(|_| Error::invalid_data("Bad error rate"))?;
          if err <= 0.0 || err >= 1.0 {
            return Err(Error::invalid_data("error rate should be between 0 and 1"));
          }
          error_rate = Some(err);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"nocreate") {
          nocreate = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"nonscaling") {
          nonscaling = true;
          expansion = Some(0);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"expansion") {
          has_expansion = true;
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("Bad expansion"));
          }
          let exp = arg_u16(args[i]).ok_or_else(|| Error::invalid_data("Bad expansion"))?;
          if exp < 1 {
            return Err(Error::invalid_data(
              "expansion should be greater or equal to 1",
            ));
          }
          expansion = Some(exp);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"items") {
          has_items = true;
          i += 1;
          while i < args.len() {
            items.push(args[i].to_vec());
            i += 1;
          }
          break;
        } else {
          return Err(err_syntax());
        }
      }

      if nonscaling && has_expansion {
        return Err(Error::invalid_data("nonscaling filters cannot expand"));
      }
      if !has_items || items.is_empty() {
        return Err(Error::invalid_data("num of items should be greater than 0"));
      }

      Ok(Cmd::BfInsert {
        key,
        capacity,
        error_rate,
        expansion,
        nocreate,
        nonscaling,
        items,
      })
    }
    // @cmd: BF.EXISTS key item
    "bf.exists" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::BfExists(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: BF.MEXISTS key item [item ...]
    "bf.mexists" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let items = args[2..].iter().map(|item| item.to_vec()).collect();
      Ok(Cmd::BfMExists(key, items))
    }
    // @cmd: BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
    "bf.info" => {
      if args.len() < 2 || args.len() > 3 {
        return Err(err_wrong_args(cmd_name));
      }
      let key = arg_string(args[1]);
      let sub_cmd = if args.len() == 3 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::BfInfo { key, sub_cmd })
    }
    // @cmd: BF.CARD key
    "bf.card" => {
      check_exact_args(cmd_name, args, 2)?;
      Ok(Cmd::BfCard(arg_string(args[1])))
    }
    // @cmd: CF.RESERVE key capacity [BUCKETSIZE size] [MAXITERATIONS max] [EXPANSION exp]
    "cf.reserve" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let capacity = arg_u64(args[2]).ok_or_else(|| Error::invalid_data("invalid capacity"))?;
      if capacity < 2 {
        return Err(Error::invalid_data("capacity must be at least 2"));
      }

      let mut bucket_size = None;
      let mut max_iterations = None;
      let mut expansion = None;
      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"bucketsize") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("invalid bucket size"));
          }
          let bs = arg_u8(args[i]).ok_or_else(|| Error::invalid_data("invalid bucket size"))?;
          if bs == 0 {
            return Err(Error::invalid_data("bucket size must be between 1 and 255"));
          }
          bucket_size = Some(bs);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"maxiterations") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("invalid max iterations"));
          }
          let mi = arg_u16(args[i]).ok_or_else(|| Error::invalid_data("invalid max iterations"))?;
          if mi == 0 {
            return Err(Error::invalid_data("max iterations must be larger than 0"));
          }
          max_iterations = Some(mi);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"expansion") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("invalid expansion factor"));
          }
          let exp =
            arg_u16(args[i]).ok_or_else(|| Error::invalid_data("invalid expansion factor"))?;
          if exp > 32768 {
            return Err(Error::invalid_data("expansion must be between 0 and 32768"));
          }
          expansion = Some(exp);
          i += 1;
        } else {
          return Err(err_syntax());
        }
      }

      Ok(Cmd::CfReserve {
        key,
        capacity,
        bucket_size,
        max_iterations,
        expansion,
      })
    }
    // @cmd: CF.ADD key item
    "cf.add" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::CfAdd(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: CF.ADDNX key item
    "cf.addnx" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::CfAddNx(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: CF.INSERT key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
    "cf.insert" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut capacity = None;
      let mut nocreate = false;
      let mut items = Vec::new();
      let mut has_items = false;

      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"capacity") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("invalid capacity"));
          }
          let cap = arg_u64(args[i]).ok_or_else(|| Error::invalid_data("invalid capacity"))?;
          if cap < 2 {
            return Err(Error::invalid_data("capacity must be at least 2"));
          }
          capacity = Some(cap);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"nocreate") {
          nocreate = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"items") {
          has_items = true;
          i += 1;
          while i < args.len() {
            items.push(args[i].to_vec());
            i += 1;
          }
          break;
        } else {
          return Err(err_syntax());
        }
      }

      if !has_items || items.is_empty() {
        return Err(Error::invalid_data("num of items should be greater than 0"));
      }

      Ok(Cmd::CfInsert {
        key,
        capacity,
        nocreate,
        items,
      })
    }
    // @cmd: CF.INSERTNX key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
    "cf.insertnx" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut capacity = None;
      let mut nocreate = false;
      let mut items = Vec::new();
      let mut has_items = false;

      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"capacity") {
          i += 1;
          if i >= args.len() {
            return Err(Error::invalid_data("invalid capacity"));
          }
          let cap = arg_u64(args[i]).ok_or_else(|| Error::invalid_data("invalid capacity"))?;
          if cap < 2 {
            return Err(Error::invalid_data("capacity must be at least 2"));
          }
          capacity = Some(cap);
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"nocreate") {
          nocreate = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"items") {
          has_items = true;
          i += 1;
          while i < args.len() {
            items.push(args[i].to_vec());
            i += 1;
          }
          break;
        } else {
          return Err(err_syntax());
        }
      }

      if !has_items || items.is_empty() {
        return Err(Error::invalid_data("num of items should be greater than 0"));
      }

      Ok(Cmd::CfInsertNx {
        key,
        capacity,
        nocreate,
        items,
      })
    }
    // @cmd: CF.EXISTS key item
    "cf.exists" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::CfExists(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: CF.MEXISTS key item [item ...]
    "cf.mexists" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let items = args[2..].iter().map(|item| item.to_vec()).collect();
      Ok(Cmd::CfMExists(key, items))
    }
    // @cmd: CF.DEL key item
    "cf.del" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::CfDel(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: CF.COUNT key item
    "cf.count" => {
      check_exact_args(cmd_name, args, 3)?;
      Ok(Cmd::CfCount(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: CF.INFO key
    "cf.info" => {
      check_exact_args(cmd_name, args, 2)?;
      Ok(Cmd::CfInfo(arg_string(args[1])))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

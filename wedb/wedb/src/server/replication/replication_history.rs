use std::{fs, io, mem::take, path::Path, str::from_utf8};

use toml_spanner::{Arena, Context, Error, Failed, Item, ToTomlError, Toml, Value};
use waof::AofAddress;

use crate::server::cluster_manager::{create_hex_id, write_into};

/// 复制历史状态持久化文件名
pub const REPLICATION_STATE_FILE: &str = "replication.toml";

/// 集群检查点持久化子目录名
pub const CLUSTER_SUBDIR: &str = "cluster";

/// 复制历史格式版本
pub const REPLICATION_HISTORY_VERSION: u32 = 1;

/// AofAddress 与 toml-spanner 桥接模块
mod aof_address_toml {
  use super::*;

  pub fn to_toml<'a>(addr: &'a AofAddress, arena: &'a Arena) -> Result<Item<'a>, ToTomlError> {
    let len = addr.length() as usize;
    if len <= 1 {
      Ok(Item::from(addr.get(0).unwrap_or(0)))
    } else {
      let mut array = toml_spanner::Array::new();
      for i in 0..len {
        array.push(Item::from(addr.get(i).unwrap_or(0)), arena);
      }
      Ok(array.into_item())
    }
  }

  pub fn from_toml<'de>(ctx: &mut Context<'de>, item: &Item<'de>) -> Result<AofAddress, Failed> {
    if let Some(n) = item.as_i64() {
      return Ok(AofAddress::create(1, n));
    }
    match item.value() {
      Value::Array(arr) => {
        let mut addr = AofAddress::new(arr.len() as i32);
        for (i, elem) in arr.iter().enumerate() {
          if let Some(n) = elem.as_i64() {
            addr.set(i, n);
          } else {
            return Err(ctx.report_expected_but_found(&"an integer", elem));
          }
        }
        Ok(addr)
      }
      Value::String(s) => AofAddress::from_string(s)
        .ok_or_else(|| ctx.push_error(Error::custom("invalid AofAddress string", item.span()))),
      _ => Err(ctx.report_expected_but_found(&"an integer, array of integers, or string", item)),
    }
  }
}

/// 记录主备复制纪元 ID 与对应截断位点历史（TOML 人类可读格式）
#[derive(Debug, Clone, PartialEq, Eq, Toml)]
#[toml(FromToml, ToToml, ignore_unknown_fields)]
pub struct ReplicationHistory {
  #[toml(default = 1)]
  pub version: u32,
  pub primary_repl_id: String,
  #[toml(default)]
  pub primary_repl_id2: String,
  #[toml(with = aof_address_toml)]
  pub replication_offset: AofAddress,
  #[toml(with = aof_address_toml)]
  pub replication_offset2: AofAddress,
}

impl ReplicationHistory {
  /// 初始化全新的复制历史实例
  pub fn new(aof_physical_sublog_count: usize) -> Self {
    Self {
      version: REPLICATION_HISTORY_VERSION,
      primary_repl_id: create_hex_id(),
      primary_repl_id2: String::new(),
      replication_offset: AofAddress::create(aof_physical_sublog_count as i32, 0),
      replication_offset2: AofAddress::create(aof_physical_sublog_count as i32, i64::MAX),
    }
  }

  /// 序列化为 TOML 文本字节
  pub fn to_byte_array(&self) -> Vec<u8> {
    match toml_spanner::to_string(self) {
      Ok(s) => {
        let mut out =
          String::from("# WeDB Replication State - Auto-generated, do not edit manually\n");
        out.push_str(&s);
        out.into_bytes()
      }
      Err(err) => {
        log::error!("序列化 ReplicationHistory 失败: {err}");
        Vec::new()
      }
    }
  }

  /// 从 TOML 字节切片反序列化 ReplicationHistory
  pub fn from_byte_array(data: &[u8]) -> io::Result<Self> {
    let s = from_utf8(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(s, &arena)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    doc
      .to::<Self>()
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
  }

  /// 更新当前主节点复制 ID（原位修改，零多余分配）
  pub fn update_replication_id(&mut self, primary_repl_id: &str) {
    self.primary_repl_id.clear();
    self.primary_repl_id.push_str(primary_repl_id);
  }

  /// 故障转移时更新位点并轮转主节点 ID（原位移动，零克隆）
  pub fn failover_update(&mut self, replication_offset2: AofAddress) {
    self.primary_repl_id2 = take(&mut self.primary_repl_id);
    self.primary_repl_id = create_hex_id();
    self.replication_offset2 = replication_offset2;
  }

  /// 持久化到 replication.toml 文件（原子写：临时文件 → sync_all → rename → sync_dir 双屏障）
  pub fn flush_to_file(&self, path: &Path) -> io::Result<()> {
    write_into(path, &self.to_byte_array())
  }

  /// 从 replication.toml 恢复配置，若损坏或不存在则初始化新配置
  pub fn recover_or_init(path: &Path, aof_physical_sublog_count: usize) -> Self {
    if let Ok(data) = fs::read(path)
      && let Ok(history) = Self::from_byte_array(&data)
    {
      return history;
    }
    let history = Self::new(aof_physical_sublog_count);
    if let Err(err) = history.flush_to_file(path) {
      log::warn!("初始化持久化 {} 失败: {err}", path.display());
    }
    history
  }
}

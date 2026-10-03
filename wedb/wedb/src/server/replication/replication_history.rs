use std::{fs, io, mem::take, path::Path, str::from_utf8};

use toml_spanner::{Arena, Context, Error, Failed, Item, ToTomlError, Toml, Value};
use waof::{AofAddress, MAX_SUBLOG_COUNT};

use crate::server::cluster_manager::{create_hex_id, write_into};

/// 复制历史状态持久化文件名
pub const REPLICATION_STATE_FILE: &str = "replication.toml";

/// 集群检查点持久化子目录名
pub const CLUSTER_SUBDIR: &str = "cluster";

/// 复制历史格式版本
pub(crate) const REPLICATION_HISTORY_VERSION: u32 = 1;

/// AofAddress 与 toml-spanner 桥接模块
mod aof_address_toml {
  use toml_spanner::Array;

  use super::*;

  pub fn to_toml<'a>(addr: &'a AofAddress, arena: &'a Arena) -> Result<Item<'a>, ToTomlError> {
    let len = addr.length() as usize;
    if len <= 1 {
      Ok(Item::from(addr.get(0).unwrap_or(0)))
    } else {
      let mut array = Array::new();
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
        // 越界硬拒口径向另两解码臂（from_string/from_aof_binary）收口：
        // 空数组与元数超 MAX_SUBLOG_COUNT 一律报 Failed，弃 AofAddress::new
        // clamp 静默截为 4 元与 length=0 位点的失真档形态（C# 构造仅
        // Debug.Assert 门禁，段数越界属上游缺陷不复刻，见 deviations §26）
        if arr.is_empty() || arr.len() > MAX_SUBLOG_COUNT {
          return Err(ctx.push_error(Error::custom_at(
            format!(
              "期望 1..={MAX_SUBLOG_COUNT} 元整数数组，实为 {} 元",
              arr.len()
            ),
            item,
          )));
        }
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
///
/// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:Copy
///（C# ICopyable 深复制臂由派生 `Clone` 承接）
#[derive(Debug, PartialEq, Eq, Toml, Clone)]
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
  ///
  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ToByteArray
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
  ///
  /// 对标 libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FromByteArray：
  /// 读侧唯一入口设代次闸，
  /// version != REPLICATION_HISTORY_VERSION 即异代非法档，与解析失败共用
  /// 同一 Err 通道回 InvalidData（C# :73-74 抛 InvalidDataException）
  pub fn from_byte_array(data: &[u8]) -> io::Result<Self> {
    let s = from_utf8(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(s, &arena)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let history = doc
      .to::<Self>()
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    if history.version != REPLICATION_HISTORY_VERSION {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
          "Incompatible ReplicationHistory version: expected {REPLICATION_HISTORY_VERSION}, got {}",
          history.version
        ),
      ));
    }
    Ok(history)
  }

  /// 更新当前主节点复制 ID（原位修改，零多余分配）
  ///
  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:UpdateReplicationId
  pub fn update_replication_id(&mut self, primary_repl_id: &str) {
    self.primary_repl_id.clear();
    self.primary_repl_id.push_str(primary_repl_id);
  }

  /// 故障转移时更新位点并轮转主节点 ID（原位移动，零克隆）
  ///
  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FailoverUpdate
  pub fn failover_update(&mut self, replication_offset2: AofAddress) {
    self.primary_repl_id2 = take(&mut self.primary_repl_id);
    self.primary_repl_id = create_hex_id();
    self.replication_offset2 = replication_offset2;
  }

  /// 持久化到 replication.toml 文件（原子写：临时文件 → sync_all → rename → sync_dir 双屏障）
  ///
  /// 写侧毁档防护：to_byte_array 序列化失败产物为空字节，绝不以空载荷经
  /// write_into 原子改名把在册档案清成 0 字节（下次启动 can_recover 判
  /// len>0 落空即静默换 repl_id），见空即跳过落盘并告警
  pub fn flush_to_file(&self, path: &Path) -> io::Result<()> {
    let bytes = self.to_byte_array();
    if bytes.is_empty() {
      log::warn!("序列化失败，跳过落盘 {}", path.display());
      return Ok(());
    }
    write_into(path, &bytes)
  }

  /// 从 replication.toml 恢复配置，若损坏或不存在则初始化新配置
  ///
  /// 读侧合法性门，对标 C# RecoverReplicationHistory(:119-131) catch →
  /// InitializeReplicationHistory 唯一出口：解析失败／版本不符（已在
  /// from_byte_array 代次闸拒回）／位点向量长度与装配子日志数不符，
  /// 任一视同损坏或异代档，落既有重建臂按装配 count 重建并覆盖落盘；
  /// 档位点向量长度恒等于装配 count 由本门保证
  pub fn recover_or_init(path: &Path, aof_physical_sublog_count: usize) -> Self {
    if let Ok(data) = fs::read(path) {
      match Self::from_byte_array(&data) {
        Ok(history)
          if history.replication_offset.length() as usize == aof_physical_sublog_count
            && history.replication_offset2.length() as usize == aof_physical_sublog_count =>
        {
          return history;
        }
        _ => log::warn!(
          "磁盘复制历史损坏或异代，按装配子日志数重建: {}",
          path.display()
        ),
      }
    }
    let history = Self::new(aof_physical_sublog_count);
    if let Err(err) = history.flush_to_file(path) {
      log::warn!("初始化持久化 {} 失败: {err}", path.display());
    }
    history
  }
}

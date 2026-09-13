use crate::error::Result;

/// 评测数据库引擎统一定义
pub trait BenchDatabase: Send + Sync {
  type Connection<'a>: BenchDatabaseConnection
  where
    Self: 'a;

  /// 引擎唯一标识名称
  fn name() -> &'static str;

  /// 创建一个新的会话连接
  fn connect(&self) -> Self::Connection<'_>;

  /// 触发物理数据整理/碎片整理 (Compaction / Vacuum)
  fn compact(&mut self) -> bool {
    false
  }

  /// 强制持久化刷盘 (确保所有内存脏数据全部写入磁盘介质)
  fn flush(&mut self) {}
}

/// 数据库会话连接 trait
pub trait BenchDatabaseConnection: Send {
  type WriteTxn<'txn>: BenchWriteTransaction
  where
    Self: 'txn;
  type ReadTxn<'txn>: BenchReadTransaction
  where
    Self: 'txn;

  /// 配置当前连接写入落盘同步行为 (true: fsync, false: nosync)
  fn set_sync(&mut self, _sync: bool) -> bool {
    false
  }

  /// 开启写事务
  fn write_transaction(&self) -> Self::WriteTxn<'_>;

  /// 开启只读事务/快照
  fn read_transaction(&self) -> Self::ReadTxn<'_>;
}

/// 写事务 trait
pub trait BenchWriteTransaction {
  /// 插入单个键值对
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

  /// 批量插入键值对 (默认逐条插入)
  fn insert_batch<'a>(&mut self, pairs: &[(&'a [u8], &'a [u8])]) -> Result<()> {
    for (k, v) in pairs {
      self.insert(k, v)?;
    }
    Ok(())
  }

  /// 删除指定键
  fn remove(&mut self, key: &[u8]) -> Result<()>;

  /// 提交事务
  fn commit(self) -> Result<()>;
}

/// 读事务/快照 trait
pub trait BenchReadTransaction {
  /// 单点读取
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>>;

  /// 范围前向扫描指定的条目数量 (返回扫描到的条目总数与第一字节校验和，供防优化校验)
  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64);

  /// 获取当前表有效记录总条数
  fn len(&mut self) -> u64;

  /// 当前表是否为空
  fn is_empty(&mut self) -> bool {
    self.len() == 0
  }
}

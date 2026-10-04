# 内嵌键值存储性能评测

下面的表格由 [`bench/`](https://github.com/webc-site/wedb/tree/main/bench) 中与 [`redb-bench`](https://github.com/cberner/redb/tree/master/rocksdb-bench) 同构的评测框架产出，主分支每次推送后由 CI 刷新。请勿手工编辑：表格内容从机读结果逐字节重新生成（`node js/readme.js`）。

参评引擎：

| 列名 | 引擎 | 接入方式 |
|:---|:---|:---|
| `hash` | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | wedb 自研追加型混合日志 KV |
| `bftree` | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | wedb 自研有序 bf-tree 索引 |
| `fjall` | [fjall](https://github.com/fjall-rs/fjall) | `fjall` crate，默认持久化档位 |
| `rocksdb` | [RocksDB](https://github.com/facebook/rocksdb) | `rocksdb` crate，`OptimisticTransactionDB` |
| `sqlite` | [SQLite](https://www.sqlite.org) | `rusqlite`，BLOB 主键表 + WAL 模式 |

## 评测参数

| 参数 | 设定 |
|:---|:---|
| **键大小** | 24 B |
| **值大小** | 150 B |
| **缓存预算** | 4 GiB（按物理内存自动收敛） |
| **批量装载 / 有序插入规模** | 5,000,000 / 1,000,000 条 |
| **单条 / nosync / 批量写** | 50,000 / 2,500,000 / 100 批 × 1,000 条 |
| **随机点读 / 区间读** | 100,000 次点读 / 50,000 × 10 元素区间读，取 3 轮中位数 |
| **多线程读** | 4 / 8 / 16 / 32 线程 |
| **随机种子** | 3（各引擎拿到完全相同的键值分布） |

### 口径与方法说明

- **负载一致性**：每个引擎都用同一种子生成的同一份键值分布，因此同一行的各列可直接横向比较。
- **持久化档位**：除 `nosync writes` 外，所有写入段都按事务做持久化提交。`nosync` 段关闭每次提交的 fsync（SQLite 映射为 `synchronous = OFF`，RocksDB 为 `set_sync(false)`），该段结束后恢复持久化写入。
- **进程隔离**：每一列在独立子进程中运行，并受墙钟预算约束。失败、panic 或超时的列整列记为 `N/A`，状态与退出详情写在备注里，不会从表里悄悄消失。
- **行内最优值加粗**（并列全标）。吞吐类行越大越好，`len()` 与两个体积行越小越好。
- **`len()`** 混合了各引擎的计数语义（元数据计数器 vs 全键空间扫描），反映的是计数成本，不能当作性能结论。
- **`pop`** 通过抽样删除外推到完整负载，因为完整弹出会压倒整轮时长；抽样条数记录在机读结果里。
- **体积**为引擎数据目录的递归字节总和（含 WAL、段文件、清单文件），取各自压缩/整理动作前后的值。

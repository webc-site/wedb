优先级：低
来源：next/agy.design.md 条 24 与 next/muse.design.md 条 16（两轮同题合并）。
取证基线：主仓 dev 当下代码。

问题
wval MetaValue 元布局常量半公开：TYPE_OFFSET/NEXT_EXPIRY_OFFSET/U64_LEN 私有，
SIZE_OFFSET/META_VALUE_SIZE 公开；其中 SIZE_OFFSET 全仓无外部消费者，属多余公开面；
半公开不一致使外部无法直读 type 偏移却能直读 size 偏移，布局纪律不闭合。

取证
- wedb/wval/src/meta.rs:34 const TYPE_OFFSET（私有）、:36 pub const SIZE_OFFSET、
  :38 const NEXT_EXPIRY_OFFSET（私有）、:40 const U64_LEN（私有）、:43 pub const META_VALUE_SIZE
- 外部消费实测：META_VALUE_SIZE 有真实跨 crate 消费（wedb/wkv/src/range_index/mod.rs:10、
  :266-:269 与 wedb/wkv/src/range_index/stub.rs:14、:100-:115 的
  [META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE] 信封拼接）；SIZE_OFFSET 外部零消费
  （全仓 grep 仅 meta.rs 内部 :186/:229 使用）
- C# 对标：garnet/libs/server/Objects/Types/GarnetObjectType.cs 相关元数据段，布局
  细节不外露，字段经类型方法读取

修法建议
SIZE_OFFSET 收回私有（无外部消费者，直接收口零影响）；META_VALUE_SIZE 保持 pub（有真实
跨 crate 信封拼接消费，且为 wval 对外布局契约的一部分）；type/expiry 偏移维持私有、经
MetaValue::read_collection_type 等关联方法读取。原则：布局常量默认私有，仅尺寸契约性
常量对外。禁为对称把 TYPE_OFFSET 也公开。

---

判词（落地于 dev 分支 meta-const-vis，基线 03c21af / 合入点 34b2a22）

判成立并已落地。票面四条事实逐条复核为真，唯取证行锚有漂移（不改变结论）：
SIZE_OFFSET 内部消费实测在现刻 HEAD 的 :197/:198/:240（票面 :186/:229 系旧行号）；
其余常量定义行 :34/:36/:38/:40/:43 与 META_VALUE_SIZE 跨 crate 锚点逐字命中。

分常量消费者清单与处置（Grep 全仓含 tests/、js/check 语料，非 zsh glob）
- `TYPE_OFFSET`（私有，维持）：内部 :206/:209/:211/:223/:225，经
  `MetaValue::read_collection_type` / `from_slice` 读取；外部零消费。处置=不动。
- `NEXT_EXPIRY_OFFSET`（私有，维持）：内部 :241；外部零消费。处置=不动。
- `U64_LEN`（私有，维持）：内部 :197；外部零消费。处置=不动。
- `SIZE_OFFSET`（原 pub，本棒收口）：定义 :36；消费者仅同文件 :197/:198/:240
  （`read_size` / `from_slice`），外部零消费（wval/src/lib.rs:19 的 meta 重导出只含
  `META_VALUE_SIZE, MetaValue, StorageEncoding`，不含 SIZE_OFFSET，故无 re-export 壳残留）。
  内部仍在读 → 非死常量，按票面判词**收回私有**（`pub const` → `const`），不删。
- `META_VALUE_SIZE`（pub，维持）：真实跨 crate 契约消费 8 源文件 + 2 测试：
  wval/src/meta.rs:152/:218/:247、wval/src/lib.rs:19（re-export）、wval/tests/meta_value.rs:5/:13/:156、
  wkv/src/session/collection.rs:16/:142、wkv/src/range_index/{mod.rs:10,266-269;
  migration.rs:14,97-98,136-141; stub.rs:14,100-115,255-271,403-406,625-636,681,766-799,823}、
  wnode/src/storage/session/common/ttl_sync.rs:27,167、wnode/src/resp/objects/tiered_demote.rs:45,157、
  wnode/src/resp/objects/object_store_utils.rs:263,340,400,464、wkv/tests/compact/spanbyte_compaction.rs:637-639、
  README/readme（zh/en）:338/:365/:772 已文档化为对外尺寸契约。处置=保持 pub。
- 头注：偏移常量组补一行纪律说明（布局常量默认私有、字段经关联方法读取、仅尺寸契约对外），
  零线格式改动、零 ignore 语料改动（`grep -rn SIZE_OFFSET js/check` 零命中，无登记须清净）。

门禁实测（worktree /tmp/fork/meta-const-vis，CARGO_TARGET_DIR=/tmp/ct-mcv）
- `cargo check --workspace --all-targets`：exit 0，全量输出 `grep -ci '^warning|^error'` = 0，
  合入前二次增量复跑仍 0 error / 0 新警告。
- `cargo nextest run -p wval --no-fail-fast`：17 passed / 0 failed。
- `cargo nextest run -p wkv -p wnode --no-fail-fast`（META_VALUE_SIZE 消费包）：
  1314 run → wkv 全绿；wnode 6 红（aof_flush_replay×3、aof_replay_domain、aof_stored_proc_replay、
  service::ttl_purge_single_deterministic_entry，断言「FlushNs 条目须把旧空间判死」）。
  **基线复测归因**：把 `wedb/wval/src/meta.rs` 临时取回 dev 原状后同 6 条用例逐条同红 →
  与本棒零因果，属 dev 在飞的 AOF/reviv 域的存量红，不在本票处置面。
- `cargo fmt --all -- --check`：exit 0。
- `bun js/check.js` 前后对跑（改动前 / 改动后各一次）：均 exit 0，输出 `diff` 逐字节相同，
  语料 ignore 无变动（worktree `git status` 除本体改动外全净）。

落位
- 提交 `3d6b88a`（fix，仅 wedb/wval/src/meta.rs：+4/-1）、`9e2339e`（docs 归档本票）→
  `git merge dev` 三回合（`c15bce0`、`34b2a22`、`01ec187`，dev 侧增量皆 docs/异域，
  meta.rs 在窗内零改动、无双花）→ 主仓 dev 纯 FF 至 `01ec187`。
- 合入后复验：`git grep` 取 dev 现 tip 的 meta.rs，`const SIZE_OFFSET`（私有）+
  `pub const META_VALUE_SIZE` 在册；合并态再跑 `cargo check --workspace --all-targets`
  exit 0 / 零警告、`cargo nextest run -p wval` 17/17、`cargo fmt --check` exit 0。

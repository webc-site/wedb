papaya 并发字典种子随机化：deterministic 特性全局合并下 SKILL 防碰撞 DoS 承诺在 papaya 域零落地

来源：glm.my 第 7 条（分拣判定成立；与既有 reject 判定正交，非重开旧账）。
取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状
- 承诺：/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:21-22「hash 一律用 gxhash；种子策略：
  gxhash 默认（不加 deterministic）随机种子防碰撞 DoS」，SKILL.md:17 规定并发字典/set 用 papaya + gxhash
  并钉在 /Users/z/git/db/wedb/wedb/wbase/src/map.rs。
- 事实：/Users/z/git/db/wedb/wedb/Cargo.toml:61 `gxhash = { version = "3.5.0", features = ["deterministic"] }`
  是 crate 级特性，resolver = "3"（同文件 :2）下同一构建单元内特性取并集，对全仓生效；
  gxhash 3.5.0 的 `impl Default for GxBuildHasher` 在 deterministic 下恒返回种子 42
  （源码 /Users/z/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/gxhash-3.5.0/src/hasher.rs:157-163），
  显式种子口为同文件 :146-151 `GxBuildHasher::with_seed(i64)`。
- 于是 /Users/z/git/db/wedb/wedb/wbase/src/map.rs:14-19 new_concurrent_map 与 :26-31 new_concurrent_set
  （均为 `.hasher(GxBuildHasher::default())`）与 wcol 的 SCAN 续扫容器拿到完全相同的固定种子：
  以往判定所依据的「papaya 域与 SCAN 域是两个不同消费面」在种子层并不存在，papaya 域零随机性。
- 用户可控键直接入 papaya 表的暴露面（节选，构造均经 new_concurrent_*）：
  /Users/z/git/db/wedb/wedb/wpubsub/src/subscribe_broker.rs:36 ChannelSubscriptions 与
  :41 PatternSubscriptions（SUBSCRIBE/PSUBSCRIBE 任意字节频道名直接入表）、
  /Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_manager.rs:178 key_index_registry
  （VADD 任意键）、/Users/z/git/db/wedb/wedb/wlua/src/commands.rs:33 StoreScriptCache.map。
  gxhash 非抗碰撞哈希，算法公开 + 种子固定即可离线构造同桶键，使 papaya 单桶链化，
  点查与广播分发退化（全仓 47 处 new_concurrent_* 生产构造点共享该退化面）。

与既有判定的关系（务必先读，避免被当成重复提案）
- /Users/z/git/db/wedb/task/reject/qcode-my-r4.md 条 #4 拒绝的是「去掉 workspace 的 deterministic 特性」，
  理由为 SCAN 族位置游标依赖进程内稳定迭代序（正确性依赖），并在结论里点名
  「真正需要防碰撞随机的是 papaya 并发字典……与 SCAN 续扫容器是两个不同消费面」。
- 本票不改 /Users/z/git/db/wedb/wedb/Cargo.toml（禁改依赖声明，且去特性会打断 SCAN），
  只把该判定假设存在的「两个消费面」在种子层真正分开：papaya 构造口用随机种子，
  普通 gxhash 容器的 default() 一字不动。两票结论并存且互不冲突。

修法
1. wbase/src/map.rs 增一个进程级种子单点：`static SEED: OnceLock<i64>`，首次以 workspace 既有
   fastrand（/Users/z/git/db/wedb/wedb/Cargo.toml:54 已在依赖表）取 i64；禁新增依赖、禁自造 RNG。
2. new_concurrent_map / new_concurrent_set 由 `.hasher(GxBuildHasher::default())` 改为
   `.hasher(GxBuildHasher::with_seed(*SEED.get_or_init(...)))`，两函数共用同一进程种子
   （同进程内所有 papaya 表种子一致，保证同键同桶、不引入逐表差异）。
3. 文档注释同步：map.rs 模块头点名「本构造口承载 SKILL 随机种子防 DoS 承诺；
   SCAN 续扫容器的 deterministic 例外不经此处，见 Cargo.toml:55-60 注释与
   reject/qcode-my-r4 条 #4」，防后续棒误删。
4. 复核迭代序依赖：papaya 迭代序本无保证（与 C# ConcurrentDictionary 同口径），但须
   grep 现有测试对 new_concurrent_* 产物断言裸迭代序的用法（若有，改为显式排序后断言），
   确保种子变动不引入新的不稳定读数；wcol/HashMap(gxhash) 侧不在本票射程。

优先级
功能缺口（SKILL 明文安全承诺未落地 + 用户可控键的哈希链化退化面）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/PubSub/SubscribeBroker.cs:23 与 :174
  `ConcurrentDictionary<ByteArrayWrapper, ...>`：C# 侧 HashBytes 同为固定算法，
  随机种子是 SKILL 对 rust 侧的单向增强承诺（非 C# 对等功能），本票落地的是本仓规范而非转写对位。

验收
- 单测：new_concurrent_map 两次进程构造种子非 42（或断言 with_seed 路径生效）；同进程内两表种子相同。
- SCAN/HSCAN/SSCAN/ZSCAN 续扫用例与 PUBSUB/向量/脚本缓存回归逐字节不变。
- grep 全仓 `GxBuildHasher::default()` 的 papaya 构造命中归零（wcol 普通容器命中保留）。
- SKILL.md:21-22 该条在 papaya 域由「承诺」变为「事实」，Cargo.toml:55-60 例外注释仍为真。

## 细化方案（f08-papaya-seed）

甄别补充（全部核实为真）
- gxhash-3.5.0 src/hasher.rs:157-163：deterministic 下 Default 恒种子 42；
  with_seed(i64)（:146-151）为显式种子口，哈希算法相同仅种子不同，功能面等价。
- resolver = "3"（wedb/Cargo.toml:2）+ crate 级 feature：deterministic 对全构建单元生效，
  「papaya 域与 SCAN 域两个消费面」在种子层确不存在，票据主张成立。
- 全仓 grep GxBuildHasher::default()：papaya 构造命中仅 map.rs:16/:29 两处，
  其余（wcol/wkv/wresp/wnode）均为普通 gxhash 容器（SCAN 域，保留不动）；
  无 papaya::builder 旁路构造口，单点改造成立。
- reject/ 已无 qcode-my-r4.md 文件（判定存活于 Cargo.toml:55-60 注释引用），无同题在办。

改动清单
1. wedb/wbase：cargo add fastrand（workspace 既有依赖，非 optional，
   避免手改 [features]；fastrand 零传递依赖）。
2. wedb/wbase/src/map.rs：
   - static SEED: OnceLock<i64>（cfg(any(map, set))），get_or_init(fastrand::i64)；
   - 私有 fn seed() -> i64 单点取值；两构造口改 .hasher(GxBuildHasher::with_seed(seed()))；
   - 模块头文档：点名 SKILL 随机种子防 DoS 承诺由本构造口承接，
     SCAN 续扫容器 deterministic 例外不经此处（见 workspace Cargo.toml 注释）。
3. 测试（map.rs 内）：种子非 42（撞中概率 2^-64 可忽略）；map/set 构造后 SEED 单值不变。

迭代序复核（修法 4 结论）
- 生产迭代 6 处（wlua/timeout.rs:180,283、wlua/commands.rs:59、
  wnode/servers/consumer_registry.rs:389 注释明言任意顺序、
  wcol/itembroker/collection_item_broker.rs:613、wacl/user.rs:618）均顺序无关。
- wpubsub/tests/namespace_isolation.rs 多通道断言用 contains，无裸迭代序断言。

不做
- 不改 workspace Cargo.toml（依赖声明与注释一字不动）。
- 不动 wcol/wkv/wresp 的 GxBuildHasher::default()（SCAN 确定性域）。
- 不跑 test.sh / clippy.sh（流程限定 cargo check）。

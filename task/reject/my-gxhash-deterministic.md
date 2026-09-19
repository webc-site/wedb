拒件：workspace gxhash 开 deterministic 特性与「默认随机种子防 DoS」策略冲突

来源：next/agy.my.md 条 20、next/muse.my.md 条 16（同题合并）。判定：不成立（既有裁决保留 + 例外已注释 + 防护已显式承接）。

拒绝原因
wedb/Cargo.toml:57-61 workspace gxhash 依赖带完整注释：deterministic 固定种子保证 SCAN 族位置游标跨调用续扫依赖容器迭代序进程内稳定（对标 C# Dictionary 进程内确定性迭代序），并明文「此为对 transpile SKILL『gxhash 默认不加 deterministic 随机种子』策略的刻意局部例外…勿据 SKILL 默认再行去除本 feature（qcode.my #4 已据此判定保留）」——即本议题已经 qcode.my 第 4 轮裁决保留，勿翻案。防 DoS 承诺由另一层承接：wedb/wbase/src/map.rs:17-40 papaya 并发字典构造统一 GxBuildHasher::with_seed(进程级 OnceLock 随机 SEED)，头注明言「papaya 域在此以进程级随机种子显式承接…两消费面在种子层就此分离」，并有 test_seed_randomized 断言种子非 42。两消费面（SCAN 续扫确定性 / 并发表随机种子）分离清晰，muse 条 16 主张的「SCAN 容器改显式种子去全局依赖」即 qcode.my #4 已否的方向（会连带非 SCAN 的默认构造 gxhash 表失去确定性并引入两套 hasher 构造口径）。

引证
wedb/Cargo.toml:57-61；wedb/wbase/src/map.rs:9-26/:30-40；garnet C# ConcurrentDictionary 无种子对位。

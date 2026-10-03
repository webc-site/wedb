审核结论：通过（2026-09-28 r437 阶段三独立审核席 + 主控复核；定级 P3，仅 codec 缺陷/磁盘损坏可达，缺省配置不可达）

裁定：走 a 路（扫描臂对齐 fail-fast，援物化臂 corrupt 标志形制 + 既有 truncate+Err 出口，零新机制）。
一、关键补强证据（超出原票面）
1. 本域对「损坏载荷」的在册统一姿态是 fail-fast，且写入册文档：
   wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:39-41 对 score_of_payload 的调用点姿态总注明文
   「各调用点按自身失败面处置：扫描臂置损坏标志、点读臂答 null」——exec_tiered_scan 是全仓唯一未照办的扫描类调用点
   （其余姿态：物化臂 scan.rs corrupt 标志、树内全扫选择臂 zset.rs:zset_scan_select 置标志、
   read_alive_score 答 None）。故本票不是「新立口径」，而是「补漏一项未照办的在册口径」。
2. 同域不变量原文：scan.rs:408-409「存储故障不得伪装成扫描完毕」、:209「一份数据一副面孔」
   ⇒ 把解不出的载荷伪装成合法分值 0.0 与这两条在册不变量同型相悖，b 路须同时改两处单源文档口径，排除。
3. 可行性核验（原票先决点）：scan_all_from_head 回调签名为 visit: FnMut(&[u8], &[u8]) -> bool（common.rs:50-57），
   回调无 Err 通道（false 仅停扫），Err 只携底层迭代/IO 失败 ⇒ 援物化臂同形「false 停扫 + 扫后判 corrupt 标志」，
   与既有 Err 出口合流即可，无需改帧装配。Err(()) 经 shared_object_commands.rs:501-515 的 await? 上抛，
   沿 wkv 慢路径存储错误单源漏斗成 RESP 错误帧（common.rs:20-22），上游零改动。
4. 可达性独立核：写侧分值落树唯一经 to_be_bytes 包装 encode_member_into 入树（zset.rs:226/:257/:274/:386）；
   引擎侧 validate_bftree_record（wkv/range_index/ops.rs:648-662）只校验键/记录总长契约，不校验 8B 分值语义
   ⇒ 非 8B 载荷不在写入面被拦，也不在恢复面被前置拦，故「仅 codec 缺陷/磁盘损坏可达」成立。
二、票面锚订正（施工以符号名定位为准，勿钉行号）
1. 撤帧漏斗段：实际在 :441 起调、:493 .is_err()、:494-:497 出块（truncate :495、Err :496）；
   原票「:486-:495」把 :486-:490 的 COUNT 回绕注释误入漏斗段。
2. 预留-回填帧头：预留 :417-:429、回填 :505-:512；原票「:503-:511」微偏。
3. 物化臂段 :101-:140（原票 :102-:138 微偏），宣称性注释 :103-:107（原票 :103-:106 少一行）。
4. C# Scan 段：Utf8Formatter.TryFormat 在 SortedSetObject.cs:513、else items.Add(null) 在 :516（原票并写 :511-:516 成立）。
5. 查重：deviations grep score_of_payload 零命中、corrupt 仅 whlog 域无关一处；§80 正文在册（deviations.md:187-190）
   只裁非有限分值文本形态（inf/-inf），确不裁损坏载荷。
三、收窄后执行方案（供施工席直接消费）
1. 只改 exec_tiered_scan 的 SortedSet 出分值臂：加 corrupt 局部标志；出帧臂改为与物化臂 :114-:122 同形的 let-else
   （log::error 携键与成员，口径援 :115-:119），置 corrupt 后 return false 停扫；
   把「短路条件式」收为扫后统一判定（scan 返 Err 或 corrupt 任一命中即走 :494-:497 同一 truncate(base)+Err(()) 出口，
   仅改汇合写法，严禁第二套撤帧机制）；同步 :313 头注 Err 契约补「树记录分值载荷损坏」一句、出分值臂注释自陈同步。
   不改帧装配、不改 DbSnapshot、不新增计数器/字段、不改 materialize 注释（(a) 落地后其宣称自动为真，零注释改写）。
2. 测试验证点（可援用例锚）：promote_collection_to_bftree 裸条目夹具
   （wedb/wnode/tests/scan_family_dualstate_frames.rs:promote :53-:63、tiered_scan_frames_byte_exact :410-:449 含 inf/-inf 项；
   另 zset.rs 内联测试 :1073-:1092）。损坏记录构造：载荷长度非 8B 的 encode_member 条目
   （若撞 validate_bftree_record 的 min_record_size 契约，加长成员名或载荷凑满，判据恒为 score_of_payload 返 None，勿写死字节数）。
   断言：同键分层 ZSCAN 得错误帧且 output 回到臂进入点（无前导半帧、流水线邻命令应答无损）；
   同类全扫臂（ZRANGE 走 zset_scan_select）对同一损坏事实同归 Err。
   严禁断言 ZSCORE/ZMSCORE 回错误帧——其在册姿态答 null，写了即造假断言。
   既有回归（正常 8B、inf/-inf 文本、zset 忽略 NOVALUES、start 越界守卫臂）不得弱化。
3. 禁触线：物化臂与 zset_scan_select 现状即正确答案，不动；wresp 预留-回填、scan_converge_cursor、
   read_scan_input、member_ttl codec 一律不动。
四、未尽面（本票不裁不修，确证后另立案）
1. zset.rs:361-:368 附近 ZINCRBY 存在分支对损坏分值静默沿用 cur_score——第三种姿态。
2. decode_member 未知旗标宽松放行（member_ttl.rs:50-51/:60 明文系刻意宽松口径）不动。
3. §80 文本形态裁决与损坏载荷面互不相涉，施工不得顺改 format_double 面。

---

分层态 ZSCAN 扫描臂把损坏分值载荷静默折成 0.0 照常出帧，与同文件物化臂的 fail-fast 撤帧口径分叉，且物化臂注释已把「扫描臂的显式失败口径」宣称为既有事实

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 内存态 ZSCAN 的分值文本化只有一条通路，其失败臂上游不可达：
   - garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:511-516 Scan 内
     `Utf8Formatter.TryFormat(item.Value, ...)` 成功则 items.Add(切片)，else items.Add(null)
   - 该 else 臂是死臂：分值恒为内存态 double，Utf8Formatter 对有限/非有限值皆恒成功；
     本仓已在码内订正旧注（见下 rust 现状第 3 点），故此面不构成「C# 回错误帧」的直接依据。
   - C# 分层（磁盘/树）态无本仓对应扫描臂，故本项判据不来自 C# 行为，而来自本仓自定不变量的一致性：
     同一份树内 8B f64 分值载荷，在两条读路上必须同一失败口径（要么都 fail-fast，要么都静默兜底），
     否则「损坏载荷」这一事实会在物化面报错、在扫描面被伪装成合法分值 0.0 交付客户端。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   - 扫描臂（缺陷面）：wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:466
     `output.write_resp_double_bulk_string(score_of_payload(payload).unwrap_or(0.0));`
     即 score_of_payload 返 None（载荷非 8B/编码损坏）时静默以 0.0 出帧，既无 log::error 亦无撤帧，
     客户端收到一条成员 + 一个假分值，且该成员照常计入 n 与游标收敛。
   - 同臂 :461-463 注释自陈「分值统一走 write_resp_double_bulk_string 文本化（非有限值输出 inf/-inf，
     与内存态 ZSCAN 及 ZRANGE 单源收口，见 doc/zh/deviations.md §80；订正旧注：C# Utf8Formatter 恒成功，
     null 项系上游不可达死臂）」——该注释处理的是「非有限分值」（inf/nan）文本化，与「载荷损坏解不出分值」
     是两个不同事件，注释未覆盖后者，unwrap_or(0.0) 属未在该自陈范围内的兜底。
   - 物化臂（对照面）：同文件 :102-:138 tiered_materialize_blob 的 SortedSet 分支
     `let Some(score) = score_of_payload(payload) else { log::error!(...); corrupt = true; return false; }`，
     扫毕 `if corrupt { return Err(()); }`，注释原文（:103-:106）：
     「非 8B 即编码损坏或 codec 缺陷——fail-fast 中止物化（Err 上抛，调用方不写回），与
     exec_tiered_scan 扫描臂的显式失败口径共用错误面，严禁静默剔除成员后照常回写固化丢失」
     ⇒ 该注释宣称的「exec_tiered_scan 扫描臂的显式失败口径」按现码并不存在，注释与实现互相矛盾（单源真值失真）。
   - 撤帧漏斗在扫描臂已具备：同函数 :441 起调、:493 `.is_err()`、:494-:497 `output.truncate(base); return Err(())`，
     且帧头走「预留-回填」（:417-:429 预留、:505-:512 backfill_resp_frame_head），故中止出帧不留半帧撕裂应答，
     修好此项无需触碰帧装配机制；注意回调 visit 返回 bool（无 Err 通道），损坏臂须援物化臂
     「false 停扫 + 扫后判 corrupt 标志」同形与既有 Err 出口合流。
   - 可达性：外部输入不可构造（分值只由编码侧单源落 8B f64；成员分值文本入参走解析臂），
     仅 codec 缺陷/编码损坏可达 ⇒ 观察级危害。
   - 不在册：doc/zh/deviations.md grep「score_of_payload」「corrupt」与本轮票池 grep 该符号零命中；
     §80 只裁非有限分值文本形态，不裁损坏载荷。

3. 逻辑危害确证
   - 数据面伪装合法：损坏载荷下 ZSCAN 回一个不存在的分值 0.0，客户端无从分辨，
     与物化面同一键同一成员报 Err 形成双态分叉（同一损坏事实两种应答），排障面被误导。
   - 不变量失真面：物化臂注释把扫描臂的「显式失败口径」当作既成事实引用，后续席据此注释施工即踩空，
     属概念真值源被反向污染。
   - 缺省配置不可达（须先有损坏载荷），故定级不升：观察级/一致性面。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan（:459-:466 SortedSet 出分值臂；:493-:497 既有 is_err → truncate(base)+Err 撤帧出口；:417-:429 帧头预留、:505-:512 回填）
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:tiered_materialize_blob（:102-:138 对照臂与其宣称性注释）
score_of_payload / decode_member 定义位（施工席以现码符号定位，勿按本票行号硬钉）

对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Scan（:511-:516 分值文本化与上游不可达 null 死臂）

精炼执行方案：
1. 口径裁定（审核席先判，二选一，严禁第三套）：
   a. 扫描臂对齐物化臂：损坏分值置 corrupt 并经该臂既有错误漏斗上抛（撤帧、零出站帧、log::error 记键与成员），
      须先一手核该回调签名是否已具 Err 通道（若非 Err 通道而是 bool 停扫语义，须走 :486-:495 同一 is_err 漏斗，
      不得为观测面新增第二套撤帧机制；核不到即回报，不硬改）。
   b. 反向裁：认定扫描面静默兜底为刻意口径，则必须改物化臂注释（删除「与 exec_tiered_scan 扫描臂的显式失败口径
      共用错误面」的失实宣称）并在 deviations 册尾顺编登记两臂分叉理由；此时需同时裁「扫描面为何允许把不可解载荷
      伪装成合法分值交付客户端」。
2. 主方案（a）落地面：单点改 :466 出帧臂 + 该臂注释自陈（禁钉行号）；不改帧装配、不改 DbSnapshot、不新增计数器。
3. 测试验证点：
   - 构造损坏分值树载荷（沿用本域既有注入形态：树内直写非 8B 载荷或 codec 侧注错，施工席以现码夹具为准），
     断言分层 ZSCAN 与同键物化路径（如 ZRANGE/ZSCORE 走物化者）对同一损坏事实给同一应答（皆错误帧，非 0.0 假分值），
     且断言零出站半帧（帧头预留被撤）。
   - 既有 tiered ZSCAN 双臂回归（正常 8B 载荷、非有限分值 inf/-inf 文本、NOVALUES、start 越界守卫臂）不得弱化。

---

## 终态注记（施工席 r437，分支 fix-wnode-tiered-zscan-corrupt-failfast）

一、改动面与每处理由（符号名定位，未钉行号）
1. `wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:exec_tiered_scan`
   a. 扫描循环前加 `let mut corrupt = false;` 局部标志（与 `tiered_materialize_blob`、
      `zset_scan_select` 同一 fail-fast 姿态，援 `common.rs:score_of_payload` 在册口径注释
      「扫描臂置损坏标志」）。
   b. 出分值臂（原 `score_of_payload(payload).unwrap_or(0.0)` 单行）改为与物化臂同形的
      `let Some(score) = score_of_payload(payload) else { log::error!(携键与成员); corrupt = true; return false; };`
      再 `output.write_resp_double_bulk_string(score)`；log 文案口径援物化臂。
   c. 「短路条件式」收为扫后统一判定：原 `if !(key_vanished || start_beyond) && scan_all_from_head(...).is_err() { truncate; Err }`
      改为 `if !(key_vanished || start_beyond) { let scan = scan_all_from_head(...); if scan.is_err() || corrupt { truncate(base); return Err(()); } }`，
      与既有 `truncate(base)+Err(())` 出口合流，零新机制、零第二套撤帧。
   d. 头注 `Err(())` 契约补「或树记录分值载荷损坏（分层 zset 树内分值载荷非 8B 大端 f64…）」一句。
   e. 出分值臂注释自陈同步（fail-fast 停扫撤帧、严禁伪装 0.0）；扫后汇合处注释补「扫描 Err 与 corrupt 任一命中同走该出口」。
   净：scan.rs +33 / -10。物化臂 `tiered_materialize_blob` 一字未动（其宣称的「exec_tiered_scan 扫描臂显式失败口径」随本改自动为真）。
2. 新增测试 `wedb/wnode/tests/tiered_zscan_corrupt_score_failfast.rs`（194 行，两 test fn）。

二、与审头方案的偏差及一手依据
1. 撤帧说明落点：审头方案 1 令「同步 :313 头注、出分值臂注释」。:313 与出分值臂注释均已改；审头提到的
   405-409 段注释（纯读面/扫描 Err truncate 前言）未改，改为在汇合处（if 块内）新增扫后统一判定注释承载
   corrupt 合流说明。依据：汇合逻辑已自条件式搬入 if 块，撤帧口径随行就市落在汇合点，语义等价、无禁触。
2. 追加 ZSCORE 答 null 与 start 越界/NOVALUES 健康键两条回归断言，超出审头「两断言」最低集。依据：审头三.2 明文
   「严禁断言 ZSCORE/ZMSCORE 回错误帧——其在册姿态答 null」，故本席断言 ZSCORE `cz z` == `$-1\r\n`（点读臂
   `tree_member_score` 对损坏载荷 `score_of_payload` 返 None 分态，见 zset.rs:tree_member_score），
   非错误帧断言，合规；原票正文（本节上方第 114 行）「ZSCORE…皆错误帧」为审头已推翻的旧口径，未采。
3. 测试局部复制 `bulk`/`scan_frame`/`promote` 三夹具：`scan_family_dualstate_frames.rs` 内为私义 fn 未导出，
   跨文件不可援，故在本测试文件内按同形重建（非新机制，仅测试装配）。

三、测试自证
1. 构造方式：`promote_collection_to_bftree` 注 `("z", encode_member(b"not-an-8b-f64-score!", None))`，
   member_ttl 编码落 `[FLAG_PLAIN, 载荷]`，decode_member 剥 1B 旗标后载荷 21B ≠ 8B ⇒ `score_of_payload` 返 None。
   判据恒为「载荷非 8B」（未写死字节数）；min_record_size 硬下限 2（promote.rs:113 注释 + ops.rs:648-662
   `key.len()+record_len < min_record_size` 拒），成员名 1B + 记录 22B 稳过闸，未撞长度契约。
   损坏成员 `z` 置字典序在合法成员 `a` 之后，故扫描先出 `a` 的合法帧字节再触损坏 ⇒
   旧缺陷形会回 `[a,1.5,z,0.0]` 全帧；修复后 truncate(base) 连同 `a` 的出帧与预留头一并撤回。
2. 断言清单：
   - `tiered_zscan_corrupt_score_failfast`：
     ZSCAN `cz 0` == `-ERR slow path storage error\r\n`（恰等于错误帧本体，无前导半帧，无假分值 0）；
     ZRANGE `cz 0 -1`（走 zset_scan_select）== 同错误帧（全扫臂同归 Err）；
     ZSCORE `cz z` == `$-1\r\n`（点读臂答 null 分态）；
     ZSCAN `hz 0`（健康对照键，含 inf/-inf）== 逐字节全量帧（邻命令应答无损）。
   - `tiered_zscan_corrupt_key_start_beyond_guard`：ZSCAN `cz 99` == `[0,空]`（start 越界守卫早退不触树、不进 corrupt 臂）；
     ZSCAN `hz 0 MATCH a NOVALUES` == `[a,1.5]`（zset 忽略 NOVALUES 分值照常出）。
3. 自测结果：`cargo test --offline -p wnode --test tiered_zscan_corrupt_score_failfast -j 3` → 2 passed。
   未弱化既有回归：`cargo test --offline -p wnode --test scan_family_dualstate_frames -j 3` → 8 passed
   （正常 8B / inf / -inf / NOVALUES / start 越界守卫 / 双态逐字节全等臂俱绿）。
   `cargo check --offline -p wnode --tests -j 3` 通过。

四、未尽面（审头第四节划界，本票不顺手改）
1. zset.rs ZINCRBY 存在分支对损坏分值静默沿用 cur_score（第三种姿态）——未动。
2. `decode_member` 未知旗标宽松放行（member_ttl.rs）——未动。
3. §80 非有限分值文本形态裁决与 format_double 面——未动。
4. 物化臂 `tiered_materialize_blob`、`zset_scan_select`、wresp 预留-回填、`scan_converge_cursor`、
   `read_scan_input`、member_ttl codec、DbSnapshot、wmetric/garnet_api——一律未触。

五、自查风险
1. 缺省配置不可达（须先有损坏载荷），本票为一致性/观察级面；测试经 promote 直注裸条目构造，非生产写路径，
   与既有分层夹具注入形态同例。
2. truncate 撤帧依赖 wresp 预留-回填不变量 2（窄向合法）；若未来帧装配改动需复核 truncate(base) 与预留头撤回仍等价。
3. 交付未跑 workspace 门禁（按硬契约交主控单跑）；本席仅逐测试目标自证。

## 主控验票注记（2026-09-28 r437 收口席）

一、收口经过与归属
- 合入 4fb694f，归档 3ca3e59（纯改名，零内容改动）。合入由并发席在其门禁窗落笔，
  我方在其主树 cargo fix 持锁期全程只读等待；本节为收口复核，终态注记系施工席自写。

二、主控独立双验（不采信席报）
1. 净面等值：对席枝尖 1260218 与 dev 现树双参比对，scan.rs 与新测试文件零差；
   merge stat 3 文件 +291/-10 与审结方案全等。
2. 单机制复核：撤帧出口仍是既有同一处 output.truncate(base) + return Err(())，
   本次只把短路条件式收为「扫后统一判定」（scan.is_err() || corrupt），未新增第二套
   撤帧/回滚机制；corrupt 标志 + return false 停扫形与同文件物化臂、
   zset_scan_select 全扫臂同构，援 common.rs 对 score_of_payload 的在册姿态总注。
3. 宣称自洽复验：物化臂段首「与 exec_tiered_scan 扫描臂的显式失败口径共用错误面」原系失实
   宣称，本改落地后自动转真，未改一字注释——审头「(a) 路落地即宣称转真、零注释改写」预判成立，
   台账未因此多出一条「双态分叉在册」负资产。
4. 测试面合规性逐断言亲读：损坏键 ZSCAN 恰等慢路径存储错误帧（含合法成员 a 的出帧连同预留头
   一并撤回，无前导半帧）；ZRANGE 全扫臂对同一损坏事实同归 Err；ZSCORE 按在册姿态答 null
   （未误断错误帧——原票正文「皆错误帧」旧口径已被审头推翻，席未采，合规）；健康邻键含
   inf/-inf 逐字节全量帧、start 越界守卫臂恒出 [0,空] 不进 corrupt 臂。构造判据恒为
   「载荷非 8B」未写死字节数，且明记未撞 min_record_size 契约闸。
5. 禁触线复核：物化臂、zset_scan_select、wresp 预留-回填、scan_converge_cursor、
   read_scan_input、member_ttl codec、DbSnapshot/garnet_api、wmetric 全未动；无 #[allow]。

三、未尽面（按审头第四节划界在册，勿借本票顺手改）
1. zset.rs ZINCRBY 存在分支对损坏分值静默沿用 cur_score——第三种姿态（既非物化/扫描臂
   的 fail-fast，也非点读臂的答 null），本票不裁不修，确证危害后另立票。
2. decode_member 未知旗标宽松放行（member_ttl.rs 码内注释明文系刻意宽松口径）不动。
3. §80 非有限分值文本形态（inf/-inf）裁决与本面互不相涉，施工未顺改 format_double。
4. C# 分层态无本仓对应扫描臂（其内存态 Utf8Formatter 恒成功、else 臂上游不可达），
   本案判据来自本仓自定不变量一致性，非 C# 行为对齐。

四、门禁
- 本票与姊妹票 heap-row 同批合入，单轮主控门禁已跑毕（dev 尖 eeda83e）：
  ./test.sh --no-fail-fast 5198 tests run / 5198 passed / 1 skipped / EXIT=0，
  ./sh/clippy.sh 三组 EXIT=0，bun js/check.js EXIT=0；新测
  tiered_zscan_corrupt_score_failfast.rs（2 臂）与既有 scan_family_dualstate_frames.rs
  （8 臂）在 --all-features 形态下实证落绿。全量数字与残余非阻断警告归属见
  task/done/r437-r438-gate-record-20260928.md。

<tool_call>
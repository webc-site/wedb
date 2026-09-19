拒件（合并档）：基础选型四条「保持」确认项——sonic_rs pretty 分配 / coarsetime 分工 / fearless_simd 位图 / nested_text 单格式

来源：next/muse.my.md 条 19、20、21、22（四条票面自评均为「正确/保持」）。判定：不成立（无缺口，确认项记档备查，不立项）。

逐条理由
1 条 19 sonic_rs：wext_json 全员 from_slice/to_vec、wlua cjson 走 sonic_rs Value，无 serde_json 残留，与 SKILL「json 用 sonic_rs」一致；「pretty 面每应答分配」票面自评「保持，pretty 加缓冲复用可选」——可选打磨非缺口，JSON pretty 属低频管理面。
2 条 20 coarsetime：实时域 Clock 高精度、单调域 Instant 锚点、TTL 用 now_ticks、慢日志用 now_stopwatch_ticks、粗粒度禁入 TTL——分工正确，票面自评「保持」，零动作。
3 条 21 fearless_simd：fast_key_eq 经 Level 令牌多版本正确；bit_count 走 u64 count_ones 自动向量化（LLVM 向量化已达标），头注已声明与 simd 单点的差异——票面自评「位图保持自动向量化，补互指注释」即现状口径，注释已在。
4 条 22 nested_text：wconf 只读 .nt，旧双格式与 Azure 导入已删（SKILL「配置文件用 nested_text 格式，删除对其他配置文件格式的支持」的完整落地）——票面自评「保持」，零动作。

引证
wedb/wext_json/src/json_object.rs、wedb/wlua/src/functions/cjson.rs；wedb/wbase/src/time.rs now_ticks/now_stopwatch_ticks；wedb/wbase/src/simd.rs fast_key_eq、wedb/wbitmap/src/bit_count.rs；wedb/wconf/src/node_options.rs、runtime_server_options.rs；garnet 对位 modules/GarnetJSON/JsonCommands.cs、libs/common/ConvertUtils.cs、libs/server/Resp/Bitmap/BitmapManagerBitCount.cs、libs/host/GarnetServerOptions.cs。

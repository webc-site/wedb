拒绝结论：判净（核对 C# GeoCommands.cs 与 GeoHash.cs，经纬度范围拦截、strict_f64 拦截 NaN、Infinity 格式化报错逐字节全等，分层态物化后与内存态点查及搜索全等，既定偏离已在册，无缺陷无分叉）

Geo 族地理位置与搜索边界及多态对标审查报告

一、审查视角与背景说明
审查视角：Geo 族地理位置与搜索边界 (GEOADD/GEODIST/GEORADIUS/GEOSEARCH 的经纬度越界防御、NaN/无穷大过滤、分层态与内存态多态应答全等)
核查目标与范围：
1. 核查 wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs 与 wedb/wcol/src/geo/ 及 zset/ 模块中 Geo 族实现。
2. 核查经度（-180 到 180）、纬度（-90 到 90 / EPSG:3857 -85.05112878 到 85.05112878）边界检查与越界错误帧，对非数字（NaN）、无穷大（Infinity）的拦截。
3. 核查底层依托的 SortedSet 在内存态与分层态（TieredSortedSet/BFTree）下的多态表现，点查与范围搜索（GEORADIUS/GEOSEARCH）返回结果是否逐字节全等。
4. 对标 garnet/libs/server/Resp/Objects/SortedSetGeoCommands.cs、SortedSetGeoObjectImpl.cs、GeoHash.cs 与 SortedSetGeoOps.cs。
5. 核验 doc/zh/deviations.md 既有在册条款，严禁将既定架构改良报为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）
1. C# 官方契约与源码现状
对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoHash.GeoToLongValue
garnet/libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoHash.GetCoordinatesFromLong
garnet/libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoHash.Distance
garnet/libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoHash.IsPointWithinRadius
garnet/libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoHash.GetDistanceWhenInRectangle
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:SortedSetObject.GeoAdd
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:SortedSetObject.GeoHash
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:SortedSetObject.GeoDistance
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:SortedSetObject.GeoPosition
garnet/libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:SortedSetObject.GeoSearch
garnet/libs/server/Resp/Objects/SortedSetGeoCommands.cs:RespServerSession.GeoAdd
garnet/libs/server/Resp/Objects/SortedSetGeoCommands.cs:RespServerSession.GeoCommands
garnet/libs/server/Resp/Objects/SortedSetGeoCommands.cs:RespServerSession.GeoSearchCommands
garnet/libs/server/Resp/SessionParseStateExtensions.cs:SessionParseStateExtensions.TryGetGeoLonLat
garnet/libs/server/Resp/SessionParseStateExtensions.cs:SessionParseStateExtensions.TryGetGeoSearchOptions
garnet/libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:StorageSession.GeoSearchReadOnly
garnet/libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:StorageSession.GeoSearchStore
garnet/test/standalone/Garnet.test.collections/GeoHashTests.cs:GeoHashTests.CanEncodeAndDecodeCoordinates

核查确证事实：
1) 经纬度合法域与 Morton 编码边界：
在 GeoHash.cs:20-35 中，LongitudeMin 为 -180.0，LongitudeMax 为 180.0，LatitudeMin 为 -90.0，LatitudeMax 为 90.0。
虽然源码第 26 行注明注释：TODO: These are "wrong" in a sense that according to EPSG:3857 latitude should be from -85.05112878 to 85.05112878，但 Garnet 实际的 Morton 编码与量化算法（Quantize / Dequantize）数学定义严格依赖 180.0 与 360.0 的倒数，测试用例 GeoHashTests.cs 亦显式将 (-90.0, -180.0) 断言编码为整数 0，将 (90.0, 180.0) 断言编码为 0xFFFFFFFFFFFFF。
SessionParseStateExtensions.cs:TryGetGeoLonLat 严格以 GeoHash.LongitudeMin/Max 及 LatitudeMin/Max 为门槛进行判定，越界即输出 GenericErrLonLat 错误帧。
2) 非数字（NaN）与无穷大（Infinity）处理：
TryGetGeoLonLat 调用 TryGetDouble(canBeInfinite: true)。
若输入非数字（如 abc 或 nan），C# 抛出 RESP_ERR_NOT_VALID_FLOAT（ERR value is not a valid float）。
若输入 Infinity / -Infinity，TryGetDouble 解析为 double.PositiveInfinity / NegativeInfinity，后续越界检查命中，通过 string.Format(CmdStrings.GenericErrLonLat, lon, lat) 回显 ERR invalid longitude,latitude pair Infinity,lat，属于标准越界防御。
3) 搜索条件（BYRADIUS/BYBOX）与选项校验：
半径与宽高若解析失败回 NOT_VALID_RADIUS / NOT_VALID_WIDTH / NOT_VALID_HEIGHT；负值回 RADIUS_IS_NEGATIVE / HEIGHT_OR_WIDTH_NEGATIVE。
4) STORE 与 WITH* 互斥：
STORE 或 STOREDIST 与 WITHCOORD / WITHDIST / WITHHASH 互斥，命中回 GenericErrStoreCommand。

三、工程现状确证（Rust wedb 实现核查）
1. 模块结构与实现路径
rust 文件与函数：
wedb/wcol/src/geo/geo_hash.rs:GeoHash::geo_to_long_value
wedb/wcol/src/geo/geo_hash.rs:GeoHash::get_coordinates_from_long
wedb/wcol/src/geo/geo_hash.rs:GeoHash::distance
wedb/wcol/src/geo/geo_hash.rs:GeoHash::is_point_within_radius
wedb/wcol/src/geo/geo_hash.rs:GeoHash::get_distance_when_in_rectangle
wedb/wcol/src/parse_utils.rs:try_get_geo_lon_lat
wedb/wcol/src/parse_utils.rs:geo_longitude_in_range
wedb/wcol/src/parse_utils.rs:geo_latitude_in_range
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_add
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_hash
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_distance
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_position
wedb/wcol/src/zset/geo_impl.rs:SortedSetObject::geo_search
wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:RespServerSession::geo_add
wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:RespServerSession::geo_commands
wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:RespServerSession::geo_search_commands
wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:format_f6
wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:slow::geo
wedb/wnode/src/resp/garnet_api/raw.rs:dispatch_fast
wedb/wnode/src/resp/garnet_api/slow.rs:dispatch_slow

核查确证事实：
1) 坐标范围与定点回显 1:1 对齐：
wedb/wcol/src/geo/geo_hash.rs 中定义 LONGITUDE_MIN/MAX = ±180.0，LATITUDE_MIN/MAX = ±90.0，与 C# Garnet 数学域完全一致。
parse_utils.rs:try_get_geo_lon_lat 经 strict_f64(lon, true) 严格解析，NaN 恒返回 GeoLonLatError::NotFloat，越界返回 GeoLonLatError::OutOfRange。
在 sorted_set_geo_commands.rs:geo_lon_lat_checked 中，NotFloat 组装为 RESP_ERR_NOT_VALID_FLOAT；OutOfRange 经 format_f6 精确以 F6 规则（±Infinity 词形及十进制第 7 位半点远离零舍入）组装 GENERIC_ERR_LON_LAT，错误帧与 C# 逐字节全等。
2) 内存态与分层态（BFTree）多态应答逐字节全等：
同步路径（fast）：zset_load_sync 命中内存态直接调用 SortedSetObject::operate 或 SortedSetObject::geo_search。
异步路径（slow）：分层态或冷键经 load_typed -> load_typed_sealed -> tiered_materialize_blob_sealed 全扫物化为标准内存态 SortedSetObject，而后调用同一套 SortedSetObject::operate 或 SortedSetObject::geo_search。
由于内存态与分层态底层共用同一套对象层运算与成帧逻辑，点查（GEODIST/GEOHASH/GEOPOS）与范围搜索（GEOSEARCH/GEORADIUS）返回的 RESP 结果逐字节全等。
3) 分层态写回与 STORE 清退闭环：
GEOADD 慢路径经 should_write_back 校验后由 geo_save_back 统一执行删空回收、信封写回或分层升阶重灌。
GEOSEARCHSTORE / GEORADIUS STORE 族写变体预先获取 dest 的 RMW 窗，在 slow::geo 中由 store_dest_cold 单点统一清退目的键的树态残留（retire_tiered_dest）并安全清退 TTL，彻底杜绝孤儿树或双域并存。

四、核查视角规约确证与偏离对齐
1. doc/zh/deviations.md 既有在册条款核验：
第 19 条已明确登记：GEOADD XX 缺失键与 GEO STORE 族空结果不创建空 zset 键（不对齐 C# InitialUpdater 空对象残留缺陷），Rust 侧取 Redis 标准行为，EXISTS 恒回 0。测试用例 geo_store_tiered_retire.rs 建立了双向锁定。
第 1 条与第 80 条：浮点数最短往返格式化（format_double）在 ±inf 词形上的有意偏离，GEO 搜索结果中输出的浮点数遵从该全局标准。
第 1354 行：跨运行时 libm 末位 ULP 超越函数计算差异属于硬件与运行时标准限制，在册注明不入偏差账，GEO 域免复勘。
2. 边界安全性确认：
经纬度输入严格过滤 NaN，无穷大由范围门拦截，所有搜索半径与几何尺寸均具备非负约束与溢出防护，不存在任何 panic 面或除零风险。

五、结论总结
本席对 C# Garnet 原型与 Rust wedb 仓内 Geo 族全链路（GEOADD/GEODIST/GEOHASH/GEOPOS/GEOSEARCH/GEOSEARCHSTORE/GEORADIUS 族）进行了深度逐项审查。确证：
1. 经纬度数值范围与编码完全对标 Garnet GeoHash 原型，NaN 与 Infinity 具备多层严格防御与精确错误回显；
2. 内存态与分层态（TieredSortedSet/BFTree）通过统一样本物化与公共对象层操作，查询应答逐字节全等；
3. STORE 变体与 GEOADD 选项在分层态下的写回、删空自愈与树态残留清退机制完善；
4. 既有偏离严格在册，仓内无契约分叉、漏项、竞态或状态脱节问题。

视角结论:已穷尽

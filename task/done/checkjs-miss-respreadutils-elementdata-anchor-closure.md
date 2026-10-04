甄别结论：通过（登记级，零行为改动，主控亲办收口）

收口（2026-09-28 13:0x）：本票为 check.js `# 实现缺失` 残项两族的甄别与收口，两笔改动同笔提交。
- `libs/common/RespReadUtils.cs:TryReadSpanWithLengthHeader / TryReadByteArrayWithLengthHeader`
  → 经双面实测均属 C# 自有重复/死码，转 `js/check/ignore/common.yml` 登记（附逐消费点对位），不实现。
- `libs/server/Resp/Vector/VectorManager.ElementData.cs:ConvertU8ToF32`
  → rust 单实现 `convert_int_to_f32<T>` 已承接（i8/u8 归并），双名合写锚 `:ConvertI8ToF32 / ConvertU8ToF32`
  不被门禁识别为第二锚，形制修正为逐名两行。
- 复验：`bun js/check.js` 后 `# 实现缺失` 段整段消失，`js/check/miss/` 目录清空，EXIT=0。

## 背景

连续两波 `bun js/check.js` 输出恒剩两族 `# 实现缺失`：`js/check/miss/libs/common/RespReadUtils.yml`
（`TryReadByteArrayWithLengthHeader`、`TryReadSpanWithLengthHeader`）与
`js/check/miss/libs/server/Resp/Vector/VectorManager.ElementData.yml`（`ConvertU8ToF32`，
同时挂 `# 仅词元提及`）。本票逐条双面实测后分流：一族属 C# 重复/死码（走 ignore 登记口径），
一族属 rust 已实现而锚形制不合（走改写注释口径）。

## 一、RespReadUtils 两条：C# 自有重复定义 + 全仓死码

1. `TryReadSpanWithLengthHeader`（garnet/libs/common/RespReadUtils.cs:862）与同文件
   `TrySliceWithLengthHeader`（:758）逐句同体：同为 `TryReadUnsignedLengthHeader` → 长度上限校验 →
   `ptr += length + 2` 越界判 → `\r\n` 双字节哨兵校验 → 造 `ReadOnlySpan`，差异仅 `result = null`
   与 `result = default` 初值写法及 `scoped` 修饰。rust 侧单点
   `wresp/src/read.rs:215 try_slice_with_length_header`（其文档锚即 `:TrySliceWithLengthHeader`），
   不设第二套同名件。两处 C# 消费点在 rust 均有对位：
   - `RespReadUtils.cs:844`（`TryReadStringWithLengthHeader` 内部转调）←
     `wconn/src/parser.rs:105 try_read_string_with_length_header`（走
     `try_read_ptr_with_signed_length_header` 严格 UTF-8 臂，与 wresp 同名件的 lossy 版分设，
     现网行为保持）；
   - `LuaRunner.cs:678`（redis.call RESP2 bulk 字符串入 Lua 栈臂）←
     `wlua/src/runner/resp_convert.rs:691`（该臂即 RESP2 → Lua 栈的 `b'$'` 分支）。
   测试消费点 `test/standalone/Garnet.test/Resp/RespReadUtilsTests.cs:456,463` 随同源承接面覆盖。

2. `TryReadByteArrayWithLengthHeader`（:725）为 `TrySliceWithLengthHeader` + `ToArray` 薄包装。
   全仓 grep（`libs/`、`server/`、`test/`、`playground/`）除自身定义行外零引用 —— C# 侧即死码，
   按「零调用方死代码不实现」口径登记。rust 需 owned 字节的消费点就地 `to_vec`
   （如 `resp_convert.rs:119 RespObject::BulkString(bulk_str.to_vec())`），不另立同名件。

## 二、ConvertU8ToF32：rust 已实现，锚形制不合

C# `VectorManager.ElementData.cs` 的 `ConvertI8ToF32`/`ConvertU8ToF32`（:198 及同族）是两份仅元素
类型不同的重复循环本体。rust 按「单实现消除双份循环」归并为泛型
`wvector/src/element_data.rs:120 convert_int_to_f32<T: bytemuck::Pod>`（`f32: From<T>`），
调用侧 `VectorValueType::XI8 → convert_int_to_f32::<i8>`、`XU8 → convert_int_to_f32::<u8>` 分派。

原锚把两名合写一行（`:ConvertI8ToF32 / ConvertU8ToF32`），门禁只识别冒号后首名，第二名为
「词元提及」，故 `ConvertU8ToF32` 恒判缺失。修正为逐名两行同径锚，实现零改动。

## 三、复验

`bun js/check.js`：`# 实现缺失` 段整段消失，`js/check/miss/` 目录被清空（该目录内容由门禁重新
生成，未纳入版本控制），EXIT=0。

## 遗留（不属本票）

- `# 重复定义` 新簇 `TsavoriteLogScanIterator.cs:GetNext`（`waof/src/wal/iterator.rs:120/:167`，
  HEAD 与在途脏档两态均在册）属 waof 段管理域，且该文件为并发席在途未提交面 —— 按禁跨域代修
  只登记不代修，留所属席收口。
- `js/check/ignore/storage.yml` 的「可淘汰 ignore」提示（`--prune-ignore` 落盘收口）另计。

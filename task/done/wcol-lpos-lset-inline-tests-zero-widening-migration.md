# wcol-lpos-lset-inline-migration（R435-1，P3 测试形态票）

## 前置
r3 checkjs 单点化票同触 wedb/wcol/src/list/list_object_impl.rs 注释面
（read_list_position_params/read_list_position_input 锚位叙述化），**待 r3 合入
dev 后开工**，开工时以现树行号重测。

## 甄别结论：通过（r435 只读预审席，2026-09-28；详见
task/issue/r435-a-list-inline-test-screening-20260928.md）
wcol/src/list/list_object_impl.rs:571-926 内联 mod tests 357 行/6 测，私触面仅
read_list_position_input（私）与 list_position/list_set（pub(crate)）；其余断言全走
pub 面（operate Lset=14/Lpos=17 臂直落、read_list_position_params 系 input 的 pub 壳、
ObjectOutput::mount/result1/payload_view）。零扩面可迁，不合「不为搬运扩 pub」红线。

## 任务
内联块整体外迁新建 wcol/tests/lpos_lpar_lset_frames_zero_alloc.rs：
- 解析用例改走 pub 壳 read_list_position_params；
- 命令用例改走 ListObject::operate 直驱（对标 C# LPOS/LSET 命令位）；
- probe/分配计数器随迁 tests/ 独立二进制，逐用例进程语义不变。

## 验证
cargo nextest run -p wcol 全绿；重点回归 lpos_default_form_zero_alloc 经 operate
路由后分配基线不漂移（operate 仅增 from_repr+is_empty 判定，预期零增量）；
src 内联块删除后 cargo check -q --workspace --all-targets 零告警。
子代理只跑 cargo check 与 -p wcol 定向测试，门禁归主控。

## 收口记录（2026-09-28 主控）
- 席提交 `743137a4`（起点 dev 尖 `79f362f5`），2 文件 +403/-357，src 侧 0 新增行
  （纯删除，可见性零扩面实证）；内联 `mod tests` 已归零。
- 用例数守恒：新册 6 例（`#[compio::test]` 计 6），与甄别段所述 6 测一一对应；
  `lpos_default_form_zero_alloc` 经 operate 路由后无分配基线漂移申报。
- 主控复算：`nextest run -p wcol` = 108/108 绿；`cargo check -q --workspace
  --all-targets` EXIT=0 零告警。
- 合并：dev `08ce8d7c`（merge-tree 预检 rc=0，`git diff --name-only <第一父>
  <merge>` 复查只含本票 2 文件）。
- 门禁复算：`bun js/check.js` 缺失/重复定义/可淘汰三段归零（仅 C# 语料降级
  信息段）；`symbolCheck.js` 违规 0（外迁册头锚点未新增重复定义簇）。

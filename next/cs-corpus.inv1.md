# cs-corpus 遗留票源盘点（inv1）

基线：分支 dev，盘点时 HEAD = fbd285951530bdbf301e7454e9afbcddf3e8d981（盘点为只读，未改任何代码；所有
grep/判定均按该现刻 HEAD 的树内容）。产出本文件外无新增/修改文件。

三批对象与移交来源：

1. 10 份新 miss（31 名、byte* 形参族）——来源 `task/done/garnet-scan-cs-corpus-parse-gate.md:123-126`。
2. 109 条 ignore 暗条目逐条判（改注释/删/留）——来源同档「遗留复核」段 :112-121 与 `js/check/README.md`
   第 1 节末段（实测 109 条）。
3. 225 处裸文件名锚点按 crate 分批——来源 check.js B 层锚点口径（`js/check/symbolCheck.js`），
   前序判例 muse.design：CS_REF_REGEX 不认「路径.cs:行号」形态裸文件名锚点。

状态：盘点进行中（分节追加）。

## 第一节 10 份新 miss（31 名，byte* 形参族）

（待补）

## 第二节 ignore 暗条目逐条判

（待补）

## 第三节 裸文件名锚点按 crate 分批

（待补）

# wext_json 查询表达式热路径正则预编译与缓存

来源：next/zcode.my.md 问题十

## 问题

wext_json 的 matches_regex 内部在每次匹配时直接执行 Regex::new(&regex_builder) 动态构造并编译正则表达式，
违反禁止在热路径动态编译正则的代码规范。

## 涉及路径

- wedb/wext_json/src/json_path/expression.rs

## 解决建议

1. 在 QueryOperand::Regex 解析构造期即完成正则编译，运行时直接使用已编译好的 Regex。
2. 对动态传入的右操作数字符串正则，增加小型 LRU 缓存或线程局部预编译缓存。

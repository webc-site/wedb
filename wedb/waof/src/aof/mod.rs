//! 语义层：AOF 条目格式（GarnetAppendOnlyFile 对标；变长语义头 + 子日志地址
//! 向量 + 参数序列区布局）

pub mod address;

pub mod args;

pub mod entry_type;

pub mod header;

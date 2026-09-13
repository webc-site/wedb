use std::{fs, path::Path};

use gxhash::HashMap;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// 语言包结构定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct I18nTexts {
  pub title: String,
  #[serde(default)]
  pub description: String,
  pub system_info_title: String,
  pub cpu: String,
  pub cores: String,
  pub arch: String,
  pub memory: String,
  #[serde(default = "default_disk_type")]
  pub disk_type: String,
  #[serde(default)]
  pub disk: String,
  pub os: String,
  pub kernel: String,
  #[serde(default = "default_cores_format")]
  pub cores_format: String,
  pub params_title: String,
  pub key_size: String,
  pub value_size: String,
  pub cache_size: String,
  pub elements: String,
  #[serde(default = "default_comparison_title")]
  pub comparison_title: String,
  #[serde(default = "default_metric_col")]
  pub metric_col: String,
  #[serde(default = "default_param_col")]
  pub param_col: String,
  #[serde(default = "default_val_col")]
  pub val_col: String,
  #[serde(default = "default_hardware_col")]
  pub hardware_col: String,
  #[serde(default = "default_spec_col")]
  pub spec_col: String,
  pub benchmarks: HashMap<String, String>,
  pub notes: String,
  pub source_code: String,
}

fn default_param_col() -> String {
  "配置项".to_string()
}

fn default_val_col() -> String {
  "设定值".to_string()
}

fn default_hardware_col() -> String {
  "硬件项".to_string()
}

fn default_spec_col() -> String {
  "规格".to_string()
}

fn default_disk_type() -> String {
  "磁盘类型".to_string()
}

fn default_cores_format() -> String {
  "{physical} 物理核心 / {logical} 逻辑核心".to_string()
}

fn default_comparison_title() -> String {
  "性能评测".to_string()
}

fn default_metric_col() -> String {
  "指标".to_string()
}

impl I18nTexts {
  /// 从 YAML 文件加载语言包
  pub fn load(path: impl AsRef<Path>) -> Result<Self> {
    let content = fs::read_to_string(path)?;
    let texts: Self = serde_yaml::from_str(&content)?;
    Ok(texts)
  }

  /// 格式化核心数显示
  pub fn format_cores(&self, physical: usize, logical: usize) -> String {
    let mut p_buf = itoa::Buffer::new();
    let mut l_buf = itoa::Buffer::new();
    self
      .cores_format
      .replace("{physical}", p_buf.format(physical))
      .replace("{logical}", l_buf.format(logical))
  }

  /// 获取对应评测指标的本地化名称，不存在则回退为默认项
  pub fn metric_name<'a>(&'a self, key: &'a str) -> &'a str {
    self.benchmarks.get(key).map(|s| s.as_str()).unwrap_or(key)
  }
}

//! 命令元数据 JSON 供给（对标 libs/server/Resp/RespCommandDataProvider.cs）
//!
//! C# 以 `IRespCommandsDataProvider` 抽象 JSON 数组的导入 / 导出（默认实现
//! `DefaultRespCommandsDataProvider` 单例）；Rust 侧以泛型函数承接，导入校验
//! （空名 / 重名）与 C# 逐条对应。

use gxhash::{GxBuildHasher, HashMap};
use serde::{Serialize, de::DeserializeOwned};

/// 命令元数据的最小面（C# IRespCommandData）
pub trait IRespCommandData: Sized {
  /// 命令名（C# Name；子命令为 `ACL|CAT` 形式）
  fn name(&self) -> &str;
}

/// 默认供给单例（C# RespCommandsDataProviderFactory.GetRespCommandsDataProvider）
///
/// libs/server/Resp/RespCommandDataProvider.cs:GetRespCommandsDataProvider
pub fn get_resp_commands_data_provider() -> DefaultRespCommandsDataProvider {
  DefaultRespCommandsDataProvider
}

/// 默认 JSON 供给（C# DefaultRespCommandsDataProvider）
pub struct DefaultRespCommandsDataProvider;

impl DefaultRespCommandsDataProvider {
  /// 导入 JSON 数组并做名称校验，返回全量条目（保持声明序）
  ///
  /// C# 侧返回以 Name（OrdinalIgnoreCase）为键的只读字典，消费方仅按值
  /// 迭代；Rust 侧直接给出有序 Vec，名称唯一性校验保持一致。
  ///
  /// libs/server/Resp/RespCommandDataProvider.cs:TryImportRespCommandsData
  pub fn try_import_resp_commands_data<T: IRespCommandData + DeserializeOwned>(
    &self,
    json: &str,
  ) -> Option<Vec<T>> {
    let entries: Vec<T> = sonic_rs::from_str(json).ok()?;

    // 名称为空 / 重复即导入失败（C# LogError + return false）
    let mut seen: HashMap<String, ()> =
      HashMap::with_capacity_and_hasher(entries.len(), GxBuildHasher::default());
    for entry in &entries {
      let name = entry.name();
      if name.trim().is_empty() {
        return None;
      }
      if seen.insert(name.to_lowercase(), ()).is_some() {
        return None;
      }
    }

    Some(entries)
  }

  /// 按名称排序后导出为缩进 JSON
  ///
  /// libs/server/Resp/RespCommandDataProvider.cs:TryExportRespCommandsData
  pub fn try_export_resp_commands_data<T: IRespCommandData + Serialize>(
    &self,
    commands_data: &[T],
  ) -> Option<String> {
    let mut sorted: Vec<&T> = commands_data.iter().collect();
    sorted.sort_by_key(|entry| entry.name().to_lowercase());
    sonic_rs::to_string_pretty(&sorted).ok()
  }
}

#[cfg(test)]
mod tests {
  use super::{IRespCommandData, get_resp_commands_data_provider};

  #[derive(serde::Deserialize, serde::Serialize)]
  struct ProbeOut {
    #[serde(rename = "Name")]
    name: String,
  }

  #[derive(serde::Deserialize)]
  struct Probe {
    #[serde(rename = "Name")]
    name: String,
  }

  impl IRespCommandData for Probe {
    fn name(&self) -> &str {
      &self.name
    }
  }

  impl IRespCommandData for ProbeOut {
    fn name(&self) -> &str {
      &self.name
    }
  }

  #[test]
  fn import_validates_names() {
    let provider = get_resp_commands_data_provider();

    let ok = provider.try_import_resp_commands_data::<Probe>(r#"[{"Name":"SET"},{"Name":"GET"}]"#);
    assert_eq!(ok.unwrap().len(), 2);

    // 空名
    assert!(
      provider
        .try_import_resp_commands_data::<Probe>(r#"[{"Name":""}]"#)
        .is_none()
    );
    // 重名
    assert!(
      provider
        .try_import_resp_commands_data::<Probe>(r#"[{"Name":"SET"},{"Name":"set"}]"#)
        .is_none()
    );
    // 非 JSON
    assert!(
      provider
        .try_import_resp_commands_data::<Probe>("n/a")
        .is_none()
    );
  }

  #[test]
  fn export_sorts_by_name() {
    let provider = get_resp_commands_data_provider();
    let data = vec![ProbeOut { name: "b".into() }, ProbeOut { name: "a".into() }];
    let json = provider.try_export_resp_commands_data(&data).unwrap();
    assert!(json.find("\"a\"").unwrap() < json.find("\"b\"").unwrap());
  }
}

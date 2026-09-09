use std::io::{Read, Write};

use jsonpath_rust::JsonPath;
use sonic_rs::Value;

use crate::error::{Error, Result};

pub struct GarnetJsonObject {
  pub value: Value,
}

impl GarnetJsonObject {
  pub fn create() -> Self {
    Self {
      value: sonic_rs::json!({}),
    }
  }
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    let mut buf = String::new();
    reader
      .read_to_string(&mut buf)
      .map_err(|e| Error::Json(e.to_string()))?;
    let value = sonic_rs::from_str(&buf).map_err(|e| Error::Json(e.to_string()))?;
    Ok(Self { value })
  }
  pub fn clone_object(&self) -> Self {
    Self {
      value: self.value.clone(),
    }
  }
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    let s = sonic_rs::to_string(&self.value).map_err(|e| Error::Json(e.to_string()))?;
    writer
      .write_all(s.as_bytes())
      .map_err(|e| Error::Json(e.to_string()))?;
    Ok(())
  }
  pub fn try_get(&self, path_str: &str) -> Result<Vec<Value>> {
    let s = sonic_rs::to_string(&self.value).map_err(|e| Error::Json(e.to_string()))?;
    let v: serde_json::Value = serde_json::from_str(&s).map_err(|e| Error::Json(e.to_string()))?;
    let found = v
      .query_with_path(path_str)
      .map_err(|e| Error::Json(e.to_string()))?;
    let mut results = Vec::new();
    for r in found {
      let r_val = r.val();
      let res_str = serde_json::to_string(r_val).map_err(|e| Error::Json(e.to_string()))?;
      let res: Value = sonic_rs::from_str(&res_str).map_err(|e| Error::Json(e.to_string()))?;
      results.push(res);
    }
    Ok(results)
  }
  pub fn try_get_root(&self) -> Result<&Value> {
    Ok(&self.value)
  }
  pub fn try_get_to_writer<W: Write>(&self, _path_str: &str, _writer: &mut W) -> Result<()> {
    Err(Error::NotImplemented)
  }
  pub fn get_parent_path(&self, _path_str: &str) -> Result<String> {
    Err(Error::NotImplemented)
  }
  pub fn get_property_name(&self, _path_str: &str) -> Result<String> {
    Err(Error::NotImplemented)
  }
}

use bitcode::{Decode, Encode};
use std::fmt;

/// 批量操作类型
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum BatchOp {
  Set(String, Vec<u8>),
  Del(String),
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub enum Operation {
  Update(Vec<u8>),
  Delete,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct UpsertKV {
  pub key: String,
  pub value: Operation,
  pub expire_at_ms: Option<u64>,
}

impl UpsertKV {
  pub fn insert(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.into()),
      expire_at_ms: None,
    }
  }

  pub fn insert_with_ttl(
    key: impl ToString,
    value: impl Into<Vec<u8>>,
    expire_at_ms: Option<u64>,
  ) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.into()),
      expire_at_ms,
    }
  }

  pub fn delete(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Delete,
      expire_at_ms: None,
    }
  }
}

impl fmt::Display for UpsertKV {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match &self.value {
      Operation::Update(val) => write!(f, "{}=<{} bytes>", self.key, val.len()),
      Operation::Delete => write!(f, "delete({})", self.key),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub enum RaftTxnOp {
  Exists,
  NotExists,
  Equal(Vec<u8>),
  NotEqual(Vec<u8>),
  Greater(Vec<u8>),
  Less(Vec<u8>),
  GreaterEqual(Vec<u8>),
  LessEqual(Vec<u8>),
}

impl RaftTxnOp {
  #[inline]
  pub fn evaluate(&self, actual: Option<&[u8]>) -> bool {
    match (self, actual) {
      (Self::Exists, actual) => actual.is_some(),
      (Self::NotExists, actual) => actual.is_none(),
      (Self::Equal(exp), Some(act)) => act == exp.as_slice(),
      (Self::NotEqual(exp), Some(act)) => act != exp.as_slice(),
      (Self::NotEqual(_), None) => true,
      (Self::Greater(exp), Some(act)) => act > exp.as_slice(),
      (Self::Less(exp), Some(act)) => act < exp.as_slice(),
      (Self::GreaterEqual(exp), Some(act)) => act >= exp.as_slice(),
      (Self::LessEqual(exp), Some(act)) => act <= exp.as_slice(),
      _ => false,
    }
  }
}

pub type TxnOp = RaftTxnOp;

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct TxnCondition {
  pub key: String,
  pub expected: RaftTxnOp,
}

impl TxnCondition {
  pub fn exists(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::Exists,
    }
  }

  pub fn not_exists(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::NotExists,
    }
  }

  pub fn eq(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::Equal(value.into()),
    }
  }

  pub fn ne(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::NotEqual(value.into()),
    }
  }

  pub fn gt(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::Greater(value.into()),
    }
  }

  pub fn lt(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::Less(value.into()),
    }
  }

  pub fn ge(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::GreaterEqual(value.into()),
    }
  }

  pub fn le(key: impl ToString, value: impl Into<Vec<u8>>) -> Self {
    Self {
      key: key.to_string(),
      expected: RaftTxnOp::LessEqual(value.into()),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct TxnReq {
  pub condition: Vec<TxnCondition>,
  pub if_then: Vec<UpsertKV>,
  pub else_then: Vec<UpsertKV>,
  pub return_previous: bool,
}

impl TxnReq {
  pub fn new(condition: Vec<TxnCondition>) -> Self {
    Self {
      condition,
      if_then: Vec::new(),
      else_then: Vec::new(),
      return_previous: false,
    }
  }

  pub fn if_then(mut self, op: UpsertKV) -> Self {
    self.if_then.push(op);
    self
  }

  pub fn if_then_ops(mut self, ops: Vec<UpsertKV>) -> Self {
    self.if_then.extend(ops);
    self
  }

  pub fn else_then(mut self, op: UpsertKV) -> Self {
    self.else_then.push(op);
    self
  }

  pub fn else_then_ops(mut self, ops: Vec<UpsertKV>) -> Self {
    self.else_then.extend(ops);
    self
  }

  pub fn with_return_previous(mut self) -> Self {
    self.return_previous = true;
    self
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub enum TxnReply {
  Success {
    branch: bool,
    prev_values: Vec<Option<Vec<u8>>>,
  },
}

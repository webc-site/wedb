//! 会话函数工具（对标 libs/server/Storage/Functions/SessionFunctionsUtils.cs）

/// 过期裁决结果（对标 C# EvaluateExpire 三态）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireEval {
  /// 未设置过期或未到期：值有效
  Valid,
  /// 已到期：值失效，须物理清除
  Expired,
  /// 过期时间戳非法（记录损坏）：按有效处理
  Invalid,
}

/// 会话函数纯工具集（无状态，全部为可内联纯函数）
pub struct SessionFunctionsUtils;

impl SessionFunctionsUtils {
  /// 过期裁决：给定记录内嵌过期时间戳与当前毫秒
  ///
  /// `expiry_ms` 为 None 表示无过期记录。
  ///
  /// libs/server/Storage/Functions/SessionFunctionsUtils.cs:EvaluateExpire
  pub fn evaluate_expire(expiry_ms: Option<u64>, now_ms: u64) -> ExpireEval {
    match expiry_ms {
      None | Some(0) => ExpireEval::Valid,
      Some(t) if t <= now_ms => ExpireEval::Expired,
      Some(_) => ExpireEval::Valid,
    }
  }

  /// 堆对象值原位写入器
  ///
  /// 缺口说明：C# 侧对 IGarnetObject 堆值做 Tsavorite 原位更新（值不换址）；
  /// wkv 信封模型下对象值整体序列化落盘，无"原位堆对象"概念，本方法以
  /// 回调返回的载荷判定是否可原位覆写（等长载荷才可原位）。
  ///
  /// libs/server/Storage/Functions/SessionFunctionsUtils.cs:InPlaceWriterForHeapObjectValue
  pub fn in_place_writer_for_heap_object_value(current: &[u8], replacement: &[u8]) -> bool {
    current.len() == replacement.len()
  }

  /// 更新记录过期时间戳（纯函数：产出新 TTL 值字节，8 字节大端）
  ///
  /// libs/server/Storage/Functions/SessionFunctionsUtils.cs:UpdateExpiration
  pub fn update_expiration(expiry_ms: u64) -> [u8; 8] {
    expiry_ms.to_be_bytes()
  }
}

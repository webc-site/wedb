//! 扩展对象静态描述单点（信封标签 + TYPE 注册名 + 按名命令解析入口 +
//! 命令键作用域）
//!
//! 对标 C# libs/server/Custom/CustomCommandManager.cs 的集中分配面
//! （CustomObjectTypeMinId 标签分配 + :406 `(GarnetObjectType)(CustomObjectTypeMinId + id)`
//! + CustomObjectFactory 的 Create/Deserialize 工厂集中持有类型与序列化能力）。
//!
//! 动态注册管理层按转写规范删除后，集中分配这件事本身由本模块以编译期
//! 静态描述承接——每个扩展 crate 以 const [`CustomObjectEntry`] 提供自身
//! 那一条，server 层（wnode）以 const 清单按特性组装，零运行时注册、
//! 零锁、零分配。
//!
//! 标签值统一取自 [`wval::CustomObjectType`]（wval 分配单点的投影），
//! 全仓严禁 `CUSTOM_OBJECT_TYPE_BASE + n` 裸偏移。

use wval::CustomObjectType;

use crate::custom_object_fns::{CommandType, CustomObjectFns};

/// 命令键作用域（静态清单形态位：执行面据此分型，不在执行期比命令名）
///
/// 对标 C# 事实：`libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd`
/// 只有 `libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand`
/// 一条单键路径，`modules/GarnetJSON/JsonModule.cs:OnLoad` 亦只注册
/// JSON.SET / JSON.GET 两条——C# 不存在多键扩展对象命令，故无
/// 「哪条命令是多键读」的元数据。
/// rust 侧为 RedisJSON 兼容另立多键读命令（JSON.MGET），这条知识入本静态
/// 元数据由执行面按型别分派（转写规范「编译期静态特性 + 静态枚举分发，
/// 杜绝运行时按名比串」）：新增多键扩展命令只加清单项，
/// 会话执行臂不再持第二份真相。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyScope {
  /// 单键：参数域首参为用户键，其余为命令入参（C# 唯一形态）
  Single,
  /// 多键只读：参数域末尾 `tail` 个为命令入参、之前全部为用户键，
  /// 每键一次 reader 调用共用同一入参尾段。只与 [`CommandType::Read`]
  /// 并置——由扩展清单 const fn 定义点与 server 层静态清单自校验共同保证
  MultiRead { tail: usize },
}

/// [`KeyScope::split`] 的参数域拆分结果（全为切片，按位拷贝传参）
#[derive(Clone, Copy)]
pub enum CustomArgs<'a> {
  /// 单键形态：用户键 + 命令入参
  Single { key: &'a [u8], args: &'a [&'a [u8]] },
  /// 多键读形态：用户键序列 + 各键共用的命令入参
  Multi {
    keys: &'a [&'a [u8]],
    args: &'a [&'a [u8]],
  },
}

impl KeyScope {
  /// 按键作用域拆参数域（键与入参的布局判定单点，执行面不再自算下标）；
  /// None = 参数域不足以构成任何键
  pub fn split<'a>(self, args: &'a [&'a [u8]]) -> Option<CustomArgs<'a>> {
    match self {
      Self::Single => args
        .split_first()
        .map(|(key, rest)| CustomArgs::Single { key, args: rest }),
      Self::MultiRead { tail } => {
        // 至少一个键：参数数须严格大于尾部入参数
        let key_count = args.len().checked_sub(tail).filter(|&n| n > 0)?;
        let (keys, cmd_args) = args.split_at(key_count);
        Some(CustomArgs::Multi {
          keys,
          args: cmd_args,
        })
      }
    }
  }
}

/// 按名解析出的自定义命令元数据（执行面四元组 + 键作用域）
///
/// 信封标签不随命令走：一个扩展对象类型一个标签，由清单项描述单点
/// 持有（[`CustomObjectEntry::tag`]），命令元数据只承载执行面。
pub struct CustomCommandMeta {
  /// 命令名（静态清单规范形，零分配）
  pub name: &'static str,
  /// 命令类型（Read / ReadModifyWrite）
  pub command_type: CommandType,
  /// 键作用域（单键 / 多键只读；执行面分型依据）
  pub key_scope: KeyScope,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
  /// 静态执行体（编译期函数指针集）
  pub fns: CustomObjectFns,
}

/// 扩展对象静态描述清单项（一个扩展对象类型一条）
///
/// server 层的扩展对象分发面（TYPE 注册名、扩展命令解析落槽、MEMORY USAGE
/// 扩展段堆估算）统一以本描述清单为准，不再手写标签字面比对臂。对标 C#
/// `IHeapObject`（libs/storage/Tsavorite/cs/src/core/Allocator/IHeapObject.cs:22）
/// 的对象自身单点提供形态：`HandleMemoryUsage` 多态读 `ValueObject.HeapMemorySize`，
/// 实现落在各对象内（RoaringBitmapObject.cs 真实记账，GarnetJsonObject.cs:350
/// 备注 Set 不更新、恒构造初值）——值语义各异，入口同型，故以一个函数指针
/// 字段入清单，server 消费点零按 tag 分派。
#[derive(Clone, Copy)]
pub struct CustomObjectEntry {
  /// 信封内层类型标签（[`wval::CustomObjectType`] 分配单点）
  pub tag: CustomObjectType,
  /// Redis TYPE 应答类型串（C# modules 注册名，如 "GarnetRoaringBitmap"）
  pub type_name: &'static str,
  /// 按名解析命令（大小写不敏感；None = 名称不在本类型命令清单）
  pub match_command: fn(&[u8]) -> Option<CustomCommandMeta>,
  /// 信封内层载荷堆内存估算（字节；入参为剥掉 1B 类型标签后的序列化载荷）
  ///
  /// 对标各对象 `IHeapObject.HeapMemorySize`：值语义由各扩展 crate 单点
  /// 自持（真实记账 / 恒定开销），损坏载荷按 0 计
  pub heap_estimate: fn(&[u8]) -> i64,
}

#[cfg(test)]
mod tests {
  use super::{CustomArgs, KeyScope};

  /// 取拆分后的键数与入参数（形态断言用）
  fn shape(args: CustomArgs) -> (usize, usize) {
    match args {
      CustomArgs::Single { args, .. } => (1, args.len()),
      CustomArgs::Multi { keys, args } => (keys.len(), args.len()),
    }
  }

  /// 单键：首参为键，其余为入参；空参数域拆不出键
  #[test]
  fn single_scope_splits_first_arg_as_key() {
    let args = [b"k".as_slice(), b"$.a".as_slice()];
    assert_eq!(shape(KeyScope::Single.split(&args).unwrap()), (1, 1));
    assert!(KeyScope::Single.split(&[]).is_none());
  }

  /// 多键读：末 tail 个为入参、其余全为键；键数归零即拆分失败
  #[test]
  fn multi_read_scope_splits_trailing_args() {
    let args = [b"k1".as_slice(), b"k2".as_slice(), b"$".as_slice()];
    assert_eq!(
      shape(KeyScope::MultiRead { tail: 1 }.split(&args).unwrap()),
      (2, 1)
    );
    // 参数数不大于尾部入参数：无键可读，拒绝而非把入参当键
    let no_key = [b"$".as_slice()];
    assert!(KeyScope::MultiRead { tail: 1 }.split(&no_key).is_none());
    assert!(KeyScope::MultiRead { tail: 3 }.split(&args).is_none());
  }
}

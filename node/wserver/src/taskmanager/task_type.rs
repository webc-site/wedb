//! 后台维护任务类型与放置类别
//!
//! 对标 libs/server/TaskManager/TaskType.cs（TaskType + TaskTypeExtensions）与
//! libs/server/TaskManager/TaskPlacementCategory.cs（[Flags] 位枚举）

use bitflags::bitflags;

bitflags! {
  /// 任务放置类别约束
  ///
  /// libs/server/TaskManager/TaskPlacementCategory.cs:TaskPlacementCategory
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct TaskPlacementCategory: u8 {
    /// 仅可安全运行于主节点
    const PRIMARY = 1 << 0;
    /// 仅可安全运行于副本节点
    const REPLICA = 1 << 1;
    /// 所有节点类型均可安全运行
    const ALL = Self::PRIMARY.bits() | Self::REPLICA.bits();
  }
}

/// 可由 TaskManager 托管的后台维护任务类型
///
/// libs/server/TaskManager/TaskType.cs:TaskType
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
  /// 监控 AOF 大小，超限触发 checkpoint
  AofSizeLimitTask = 0,
  /// 周期性提交 AOF 保证持久性
  CommitTask,
  /// 日志紧缩回收已删除记录空间
  CompactionTask,
  /// 收集对象存储集合中的过期成员
  ObjectCollectTask,
  /// 扫描并删除主/对象存储中的过期键
  ExpiredKeyDeletionTask,
  /// 溢出阈值满足时自动扩容哈希索引
  IndexAutoGrowTask,
  /// 副本端并行重放 VADD
  VectorReplicationReplayTask,
}

/// 按枚举下标索引的"任务类型 → 放置类别"映射表（编译期定长，与 C#
/// Enum.GetValues 长度一致；新增变体时在 [`TaskType::ALL`] 与本表尾部同步补位）
///
/// libs/server/TaskManager/TaskType.cs:TaskTypeExtensions.TaskPlacementMapping
const TASK_PLACEMENT_MAPPING: [TaskPlacementCategory; TaskType::COUNT] = [
  TaskPlacementCategory::PRIMARY, // AofSizeLimitTask
  TaskPlacementCategory::PRIMARY, // CommitTask
  TaskPlacementCategory::PRIMARY, // CompactionTask
  TaskPlacementCategory::PRIMARY, // ObjectCollectTask
  TaskPlacementCategory::PRIMARY, // ExpiredKeyDeletionTask
  TaskPlacementCategory::ALL,     // IndexAutoGrowTask
  TaskPlacementCategory::REPLICA, // VectorReplicationReplayTask
];

impl TaskType {
  /// 枚举成员总数
  pub const COUNT: usize = 7;

  /// 全部任务类型（枚举声明序）
  pub const ALL: [TaskType; Self::COUNT] = [
    TaskType::AofSizeLimitTask,
    TaskType::CommitTask,
    TaskType::CompactionTask,
    TaskType::ObjectCollectTask,
    TaskType::ExpiredKeyDeletionTask,
    TaskType::IndexAutoGrowTask,
    TaskType::VectorReplicationReplayTask,
  ];

  /// 按下标取任务类型（表驱动，下标来自 0..COUNT 内部遍历，恒安全）
  #[inline]
  #[must_use]
  pub const fn from_index(i: usize) -> Self {
    Self::ALL[i]
  }

  /// 该任务的放置类别
  #[inline]
  #[must_use]
  pub const fn placement(self) -> TaskPlacementCategory {
    TASK_PLACEMENT_MAPPING[self as usize]
  }

  /// 取出匹配放置类别的全部任务类型（保持枚举声明序，零分配）
  ///
  /// libs/server/TaskManager/TaskType.cs:TaskTypeExtensions.GetTaskTypes
  pub fn get_task_types(
    lookup_placement_category: TaskPlacementCategory,
  ) -> impl Iterator<Item = Self> + Clone {
    Self::ALL.into_iter().filter(move |&task_type| {
      Self::match_placement_category(task_type.placement(), lookup_placement_category)
    })
  }

  /// 判定任务放置类别是否匹配查询类别
  ///
  /// "All" 任务可在任意节点运行；查询 "All" 时恒匹配；其余按位包含判定
  ///
  /// libs/server/TaskManager/TaskType.cs:TaskTypeExtensions.MatchPlacementCategory
  #[inline]
  #[must_use]
  pub const fn match_placement_category(
    task_placement_category: TaskPlacementCategory,
    lookup_placement_category: TaskPlacementCategory,
  ) -> bool {
    // const 上下文禁用派生的 PartialEq，按位比较（bitflags 无别名位，语义等价）
    if task_placement_category.bits() == TaskPlacementCategory::ALL.bits()
      || lookup_placement_category.bits() == TaskPlacementCategory::ALL.bits()
    {
      return true;
    }
    lookup_placement_category.contains(task_placement_category)
  }
}

#[cfg(test)]
mod tests {
  use super::{TaskPlacementCategory as P, TaskType as T};

  #[test]
  fn placement_mapping_matches_csharp() {
    assert_eq!(T::AofSizeLimitTask.placement(), P::PRIMARY);
    assert_eq!(T::CommitTask.placement(), P::PRIMARY);
    assert_eq!(T::CompactionTask.placement(), P::PRIMARY);
    assert_eq!(T::ObjectCollectTask.placement(), P::PRIMARY);
    assert_eq!(T::ExpiredKeyDeletionTask.placement(), P::PRIMARY);
    assert_eq!(T::IndexAutoGrowTask.placement(), P::ALL);
    assert_eq!(T::VectorReplicationReplayTask.placement(), P::REPLICA);
  }

  #[test]
  fn match_placement_category_semantics() {
    // "All" 任务任意类别可运行；查询 "All" 恒匹配
    assert!(T::match_placement_category(P::ALL, P::PRIMARY));
    assert!(T::match_placement_category(P::PRIMARY, P::ALL));
    // 位包含判定
    assert!(T::match_placement_category(P::PRIMARY, P::PRIMARY));
    assert!(!T::match_placement_category(P::PRIMARY, P::REPLICA));
    assert!(!T::match_placement_category(P::REPLICA, P::PRIMARY));
  }

  #[test]
  fn get_task_types_filters_in_declaration_order() {
    let primary: Vec<T> = T::get_task_types(P::PRIMARY).collect();
    assert_eq!(
      primary,
      vec![
        T::AofSizeLimitTask,
        T::CommitTask,
        T::CompactionTask,
        T::ObjectCollectTask,
        T::ExpiredKeyDeletionTask,
        // C# MatchPlacementCategory(All, Primary) 恒真：All 任务命中任意查询
        T::IndexAutoGrowTask,
      ]
    );

    let replica: Vec<T> = T::get_task_types(P::REPLICA).collect();
    assert_eq!(
      replica,
      vec![T::IndexAutoGrowTask, T::VectorReplicationReplayTask]
    );

    // C# 查询 All 时：All 任务与位包含判定双双命中 → 全量按声明序
    let all: Vec<T> = T::get_task_types(P::ALL).collect();
    assert_eq!(all, T::ALL.to_vec());
  }
}

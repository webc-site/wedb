use mimalloc::MiMalloc;

/// 吞吐测量期直接挂裸 MiMalloc，不包监控层：
/// 内存指标统一走进程物理常驻（RSS）口径（见 harness 第 12 节 get_process_physical_memory），
/// 避免每次分配/释放的原子 RMW 税进入各引擎定时路径，消除跨引擎横比偏置
#[global_allocator]
static GLOBAL_ALLOC: MiMalloc = MiMalloc;
